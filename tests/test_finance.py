import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import jwt
import pika
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select, text

import app.main as billing


@pytest.fixture(autouse=True)
def clean_database():
    assert "_test_db" in os.environ["DATABASE_URL"]
    assert billing.BILLING_EVENTS_EXCHANGE.endswith(".test.events")
    with billing.SessionLocal() as db:
        db.execute(
            text(
                "TRUNCATE financial_parameters, billable_apartments, generations, idempotency_keys, outbox_events, scheduled_runs RESTART IDENTITY CASCADE"
            )
        )
        db.add(
            billing.FinancialParameter(
                base_value=Decimal("300000.00"),
                monthly_late_rate=Decimal("0.025"),
                due_days=10,
                effective_from=date(2025, 1, 1),
            )
        )
        db.commit()


def client(role: str = "ADMINISTRACION", uid: int = 1) -> TestClient:
    result = TestClient(billing.app)
    token = jwt.encode(
        {"uid": uid, "roles": [role], "exp": datetime.now(UTC) + timedelta(hours=1)},
        billing.JWT_SECRET,
        algorithm="HS256",
    )
    result.cookies.set("access_token", token)
    result.cookies.set("XSRF-TOKEN", "test-csrf")
    return result


def headers(key: str | None = None) -> dict:
    result = {"X-XSRF-TOKEN": "test-csrf"}
    if key:
        result["Idempotency-Key"] = key
    return result


def previous_month() -> date:
    return (billing.business_today().replace(day=1) - timedelta(days=1)).replace(day=1)


def billable(monkeypatch):
    monkeypatch.setattr(
        billing,
        "billable_from_directory",
        lambda: [{"id": 101, "torre": "A", "numero": "101", "activo": True, "coeficienteCopropiedad": 0.5}],
    )
    period = previous_month()
    response = client().post(f"/api/v1/finanzas/cobros/generar?periodo={period:%Y-%m}", headers=headers())
    assert response.status_code == 202, response.text
    assert response.json()["payload"]["status"] == "EN_COLA"
    assert billing.process_generation_jobs() == 1
    return period


def test_generation_runs_in_bounded_batches_and_exposes_progress(monkeypatch):
    rows = [
        {"id": apartment_id, "torre": "A", "numero": str(apartment_id), "activo": True, "coeficienteCopropiedad": 0.5}
        for apartment_id in range(1, 206)
    ]
    monkeypatch.setattr(billing, "billable_from_directory", lambda: rows)
    period = previous_month()
    response = client().post(f"/api/v1/finanzas/cobros/generar?periodo={period:%Y-%m}", headers=headers())
    assert response.status_code == 202
    generation_id = response.json()["payload"]["generacionId"]
    assert billing.process_generation_jobs(max_jobs=1) == 1
    progress = client().get(f"/api/v1/finanzas/cobros/generaciones/{generation_id}")
    assert progress.json()["payload"]["status"] == "EN_PROCESO"
    assert progress.json()["payload"]["processed"] == 100
    assert billing.process_generation_jobs(max_jobs=1) == 1
    assert billing.process_generation_jobs(max_jobs=1) == 1
    completed = client().get(f"/api/v1/finanzas/cobros/generaciones/{generation_id}").json()["payload"]
    assert completed["status"] == "COMPLETADA"
    assert completed["generated"] == completed["total"] == 205


def test_generation_payment_idempotency_and_reversal(monkeypatch):
    period = billable(monkeypatch)
    admin = client()
    repeat = admin.post(f"/api/v1/finanzas/cobros/generar?periodo={period:%Y-%m}", headers=headers())
    assert repeat.status_code == 202
    with billing.SessionLocal() as db:
        assert db.scalar(select(billing.Charge.value)) == Decimal("150000.00")
        assert db.scalar(select(billing.Charge.id).order_by(billing.Charge.id.desc())) == 1
    body = {
        "apartment_id": 101,
        "value": "150000.00",
        "paid_at": billing.business_today().isoformat(),
        "method": "TRANSFERENCIA",
    }
    key = str(uuid4())
    first = admin.post("/api/v1/finanzas/pagos", json=body, headers=headers(key))
    second = admin.post("/api/v1/finanzas/pagos", json=body, headers=headers(key))
    assert first.status_code == second.status_code == 201
    assert first.json() == second.json()
    with billing.SessionLocal() as db:
        assert db.scalar(select(billing.Payment.id).order_by(billing.Payment.id.desc())) == 1
    reversal = admin.post("/api/v1/finanzas/pagos/1/reverso", json={"reason": "Pago duplicado"}, headers=headers())
    assert reversal.status_code == 200, reversal.text
    assert (
        admin.post("/api/v1/finanzas/pagos/1/reverso", json={"reason": "Pago duplicado"}, headers=headers()).json()
        == reversal.json()
    )
    account = admin.get("/api/v1/finanzas/estado-cuenta/101")
    assert account.status_code == 200
    assert account.json()["payload"]["saldoTotal"] == "150000.00"


def test_interest_graphql_and_read_permissions(monkeypatch):
    billable(monkeypatch)
    admin = client()
    period = billing.business_today().replace(day=1)
    first = admin.post("/api/v1/finanzas/intereses/aplicar", json={"periodo": f"{period:%Y-%m}"}, headers=headers())
    assert first.status_code == 200, first.text
    assert first.json()["payload"]["causados"] == 1
    repeat = admin.post("/api/v1/finanzas/intereses/aplicar", json={"periodo": f"{period:%Y-%m}"}, headers=headers())
    assert repeat.json()["payload"]["causados"] == 0
    graphql = admin.post(
        "/api/v1/finanzas/graphql", json={"query": "{ cartera { totalPendiente apartamentos { apartamentoId } } }"}
    )
    assert graphql.status_code == 200, graphql.text
    assert graphql.json()["data"]["cartera"]["totalPendiente"] == "153750.00"
    assert client("VIGILANTE").get("/api/v1/finanzas/cartera").status_code == 403
    payment = {
        "apartment_id": 101,
        "value": "100.00",
        "paid_at": billing.business_today().isoformat(),
        "method": "TRANSFERENCIA",
    }
    assert (
        admin.post("/api/v1/finanzas/pagos", json=payment, headers={"Idempotency-Key": str(uuid4())}).status_code == 403
    )


def test_proof_review_and_finance_event(monkeypatch):
    billable(monkeypatch)
    monkeypatch.setattr(
        billing, "internal_get", lambda path, params=None: {"apartment": {"id": 101, "tipoResidente": "PROPIETARIO"}}
    )
    resident = client("RESIDENTE", 9)
    key = str(uuid4())
    data = {
        "valorDeclarado": "150000.00",
        "fechaTransferencia": billing.business_today().isoformat(),
        "banco": "Banco de prueba",
        "referencia": "REF-001",
    }
    files = {"archivo": ("prueba.pdf", b"%PDF-1.4\nprueba local", "application/pdf")}
    response = resident.post("/api/v1/finanzas/comprobantes", data=data, files=files, headers=headers(key))
    assert response.status_code == 201, response.text
    assert response.json()["payload"]["estado"] == "EN_REVISION"
    assert (
        resident.post("/api/v1/finanzas/comprobantes", data=data, files=files, headers=headers(key)).json()
        == response.json()
    )
    approved = client().patch("/api/v1/finanzas/comprobantes/1/aprobacion", headers=headers())
    assert approved.status_code == 200, approved.text
    assert approved.json()["payload"]["pagoId"] == 1
    connection = pika.BlockingConnection(pika.URLParameters(billing.RABBITMQ_URL))
    try:
        channel = connection.channel()
        channel.exchange_declare(exchange=billing.BILLING_EVENTS_EXCHANGE, exchange_type="topic", durable=True)
        queue = channel.queue_declare(queue="", exclusive=True).method.queue
        channel.queue_bind(
            queue=queue, exchange=billing.BILLING_EVENTS_EXCHANGE, routing_key="cartera.estado-actualizado"
        )
        billing.publish_outbox()
        method, _, body = channel.basic_get(queue=queue, auto_ack=True)
        assert method is not None
        event = json.loads(body)
        assert event["apartamentoId"] == 101
        assert event["type"] == "cartera.estado-actualizado"
    finally:
        connection.close()


def test_outbox_retries_after_broker_restart(monkeypatch):
    billable(monkeypatch)
    real_connection = billing.pika.BlockingConnection

    def unavailable(_parameters):
        raise pika.exceptions.AMQPConnectionError()

    monkeypatch.setattr(billing.pika, "BlockingConnection", unavailable)
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        billing.publish_outbox()

    with billing.SessionLocal() as db:
        pending = db.scalar(
            select(func.count())
            .select_from(billing.OutboxEvent)
            .where(billing.OutboxEvent.published_at.is_(None))
        )
        assert pending > 0

    monkeypatch.setattr(billing.pika, "BlockingConnection", real_connection)
    connection = real_connection(pika.URLParameters(billing.RABBITMQ_URL))
    try:
        channel = connection.channel()
        channel.exchange_declare(
            exchange=billing.BILLING_EVENTS_EXCHANGE,
            exchange_type="topic",
            durable=True,
        )
        queue = channel.queue_declare(queue="", exclusive=True).method.queue
        channel.queue_bind(
            queue=queue,
            exchange=billing.BILLING_EVENTS_EXCHANGE,
            routing_key="cartera.estado-actualizado",
        )

        billing.publish_outbox()

        method, _, body = channel.basic_get(queue=queue, auto_ack=True)
        assert method is not None
        assert json.loads(body)["type"] == "cartera.estado-actualizado"
        with billing.SessionLocal() as db:
            pending = db.scalar(
                select(func.count())
                .select_from(billing.OutboxEvent)
                .where(billing.OutboxEvent.published_at.is_(None))
            )
            assert pending == 0
    finally:
        connection.close()


def test_concurrent_payments_apply_to_a_charge_without_overapplying(monkeypatch):
    billable(monkeypatch)
    payload = {
        "apartment_id": 101,
        "value": "100000.00",
        "paid_at": billing.business_today().isoformat(),
        "method": "TRANSFERENCIA",
    }

    def submit_payment(_index: int):
        return client().post(
            "/api/v1/finanzas/pagos",
            json=payload,
            headers=headers(str(uuid4())),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        responses = list(executor.map(submit_payment, range(2)))

    assert [response.status_code for response in responses] == [201, 201]
    with billing.SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(billing.Payment)) == 2
        applied = db.scalar(select(func.sum(billing.PaymentApplication.value)))
        assert applied == Decimal("150000.00")
    statement = client().get("/api/v1/finanzas/estado-cuenta/101")
    assert statement.status_code == 200
    assert statement.json()["payload"]["saldoAFavor"] == "50000.00"


def test_paid_charge_receipt_is_a_pdf_and_reuses_its_number(monkeypatch):
    billable(monkeypatch)
    admin = client()
    payment = admin.post(
        "/api/v1/finanzas/pagos",
        json={
            "apartment_id": 101,
            "value": "150000.00",
            "paid_at": billing.business_today().isoformat(),
            "method": "TRANSFERENCIA",
        },
        headers=headers(str(uuid4())),
    )
    assert payment.status_code == 201, payment.text
    with billing.SessionLocal() as db:
        charge_id = db.scalar(select(billing.Charge.id))

    first = admin.get(f"/api/v1/finanzas/cobros/{charge_id}/recibo")
    second = admin.get(f"/api/v1/finanzas/cobros/{charge_id}/recibo")
    assert first.status_code == second.status_code == 200
    assert first.headers["content-type"] == "application/pdf"
    assert first.content.startswith(b"%PDF-")
    assert second.content.startswith(b"%PDF-")
    assert second.headers["content-disposition"] == first.headers["content-disposition"]
    with billing.SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(billing.Receipt)) == 1


def test_finance_reports_export_expected_formats_and_require_admin(monkeypatch):
    billable(monkeypatch)
    admin = client()
    cutoff = billing.business_today().isoformat()
    reports = {
        "/api/v1/finanzas/reportes/cartera.csv": ("text/csv", b"apartamentoId"),
        "/api/v1/finanzas/reportes/cartera.pdf": ("application/pdf", b"%PDF-"),
        f"/api/v1/finanzas/reportes/recaudo.csv?desde=2026-01-01&hasta={cutoff}": ("text/csv", b"pagoId"),
        f"/api/v1/finanzas/reportes/comprobantes.csv?desde=2026-01-01&hasta={cutoff}": (
            "text/csv",
            b"comprobanteId",
        ),
    }
    for path, (media_type, signature) in reports.items():
        response = admin.get(path)
        assert response.status_code == 200, response.text
        assert response.headers["content-type"].startswith(media_type)
        assert signature in response.content
        assert client("VIGILANTE").get(path).status_code == 403


def test_proof_upload_rejects_invalid_signature_and_files_over_five_mb(monkeypatch):
    billable(monkeypatch)
    monkeypatch.setattr(billing, "owner_apartment", lambda claims: 101)
    resident = client("RESIDENTE", 9)
    fields = {
        "valorDeclarado": "150000.00",
        "fechaTransferencia": billing.business_today().isoformat(),
        "banco": "Banco de prueba",
    }
    invalid = resident.post(
        "/api/v1/finanzas/comprobantes",
        data={**fields, "referencia": "REF-BAD-SIGNATURE"},
        files={"archivo": ("prueba.png", b"not a real image", "image/png")},
        headers=headers(str(uuid4())),
    )
    oversized = resident.post(
        "/api/v1/finanzas/comprobantes",
        data={**fields, "referencia": "REF-TOO-LARGE"},
        files={"archivo": ("prueba.pdf", b"x" * (5 * 1024 * 1024 + 1), "application/pdf")},
        headers=headers(str(uuid4())),
    )
    assert invalid.status_code == 422
    error = invalid.json()
    error_code = (error.get("error") or {}).get("code") or (error.get("detail") or {}).get("code")
    assert error_code == "ARCHIVO_INVALIDO"
    assert oversized.status_code == 413
    with billing.SessionLocal() as db:
        assert db.scalar(select(func.count()).select_from(billing.Proof)) == 0
