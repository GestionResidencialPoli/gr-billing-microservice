import calendar
import csv
import hashlib
import hmac
import io
import json
import os
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from threading import Event, Thread
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import httpx
import jwt
import pika
import strawberry
from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from sqlalchemy import (
    Date,
    DateTime,
    ForeignKey,
    Integer,
    LargeBinary,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    select,
    text,
)
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from strawberry.extensions import DisableIntrospection, MaxTokensLimiter, QueryDepthLimiter
from strawberry.fastapi import GraphQLRouter

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://gr_user:gr_password@localhost:5432/gr_billing_db")
JWT_SECRET = os.getenv("JWT_SECRET", "local-development-secret-change-me")
USER_SERVICE_URL = os.getenv("USER_SERVICE_URL", "http://localhost:8080")
INTERNAL_SERVICE_TOKEN = os.getenv("INTERNAL_SERVICE_TOKEN", "")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://localhost:5672")
BILLING_EVENTS_EXCHANGE = os.getenv("BILLING_EVENTS_EXCHANGE", "gr.finance.events")


def business_today() -> date:
    return datetime.now(ZoneInfo("America/Bogota")).date()


class Base(DeclarativeBase):
    pass


class ChargeStatus(str, Enum):
    PENDIENTE = "PENDIENTE"
    PARCIAL = "PARCIAL"
    PAGADO = "PAGADO"


class PaymentMethod(str, Enum):
    TRANSFERENCIA = "TRANSFERENCIA"
    EFECTIVO = "EFECTIVO"
    CONSIGNACION = "CONSIGNACION"
    OTRO = "OTRO"


class FinancialParameter(Base):
    __tablename__ = "financial_parameters"
    id: Mapped[int] = mapped_column(primary_key=True)
    base_value: Mapped[Decimal] = mapped_column(Numeric(15, 2))
    monthly_late_rate: Mapped[Decimal] = mapped_column(Numeric(8, 5), default=0)
    due_days: Mapped[int] = mapped_column(Integer, default=10)
    effective_from: Mapped[date] = mapped_column(Date)


class BillableApartment(Base):
    __tablename__ = "billable_apartments"
    id: Mapped[int] = mapped_column(primary_key=True)
    torre: Mapped[str] = mapped_column(String(20))
    numero: Mapped[str] = mapped_column(String(20))
    coefficient: Mapped[Decimal | None] = mapped_column(Numeric(8, 5), nullable=True)
    active: Mapped[bool] = mapped_column(default=True)
    synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class ApartmentValue(Base):
    __tablename__ = "apartment_values"
    __table_args__ = (UniqueConstraint("apartment_id", "effective_from", name="uq_apartment_value_period"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    apartment_id: Mapped[int] = mapped_column(ForeignKey("billable_apartments.id"))
    value: Mapped[Decimal] = mapped_column(Numeric(15, 2))
    effective_from: Mapped[date] = mapped_column(Date)
    reason: Mapped[str] = mapped_column(String(500))
    created_by_user_id: Mapped[int] = mapped_column(Integer)


class Charge(Base):
    __tablename__ = "charges"
    __table_args__ = (UniqueConstraint("apartment_id", "period", name="uq_charge_apartment_period"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    apartment_id: Mapped[int] = mapped_column(ForeignKey("billable_apartments.id"))
    period: Mapped[date] = mapped_column(Date)
    value: Mapped[Decimal] = mapped_column(Numeric(15, 2))
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    due_at: Mapped[date] = mapped_column(Date)
    state: Mapped[ChargeStatus] = mapped_column(
        SqlEnum(ChargeStatus, name="charge_status"), default=ChargeStatus.PENDIENTE
    )


class Payment(Base):
    __tablename__ = "payments"
    id: Mapped[int] = mapped_column(primary_key=True)
    apartment_id: Mapped[int] = mapped_column(ForeignKey("billable_apartments.id"))
    value: Mapped[Decimal] = mapped_column(Numeric(15, 2))
    paid_at: Mapped[date] = mapped_column(Date)
    method: Mapped[PaymentMethod] = mapped_column(SqlEnum(PaymentMethod, name="payment_method"))
    reference: Mapped[str | None] = mapped_column(String(80), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    origin: Mapped[str] = mapped_column(String(20), default="MANUAL")
    proof_id: Mapped[int | None] = mapped_column(ForeignKey("proofs.id"), nullable=True)
    registered_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    reversal_of_payment_id: Mapped[int | None] = mapped_column(ForeignKey("payments.id"), nullable=True, unique=True)
    reversal_reason: Mapped[str | None] = mapped_column(String(500), nullable=True)


class PaymentApplication(Base):
    __tablename__ = "payment_applications"
    id: Mapped[int] = mapped_column(primary_key=True)
    payment_id: Mapped[int] = mapped_column(ForeignKey("payments.id"))
    charge_id: Mapped[int | None] = mapped_column(ForeignKey("charges.id"), nullable=True)
    interest_id: Mapped[int | None] = mapped_column(ForeignKey("late_interests.id"), nullable=True)
    value: Mapped[Decimal] = mapped_column(Numeric(15, 2))


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"
    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    response_json: Mapped[str] = mapped_column(Text)
    user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    route: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status_code: Mapped[int] = mapped_column(Integer, default=201)


class Generation(Base):
    __tablename__ = "generations"
    id: Mapped[int] = mapped_column(primary_key=True)
    period: Mapped[date] = mapped_column(Date)
    total: Mapped[int] = mapped_column(default=0)
    generated: Mapped[int] = mapped_column(default=0)
    skipped: Mapped[int] = mapped_column(default=0)
    failed: Mapped[int] = mapped_column(default=0)
    status: Mapped[str] = mapped_column(String(30), default="COMPLETADA")
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_apartment_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(String(500), nullable=True)


class LateInterest(Base):
    __tablename__ = "late_interests"
    __table_args__ = (UniqueConstraint("charge_id", "period", name="uq_late_interest_charge_period"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    charge_id: Mapped[int] = mapped_column(ForeignKey("charges.id"))
    period: Mapped[date] = mapped_column(Date)
    base_capital: Mapped[Decimal] = mapped_column(Numeric(15, 2))
    rate: Mapped[Decimal] = mapped_column(Numeric(8, 5))
    value: Mapped[Decimal] = mapped_column(Numeric(15, 2))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class Proof(Base):
    __tablename__ = "proofs"
    __table_args__ = (UniqueConstraint("bank", "reference", name="uq_proof_bank_reference"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    apartment_id: Mapped[int] = mapped_column(ForeignKey("billable_apartments.id"))
    uploaded_by_user_id: Mapped[int] = mapped_column(Integer)
    declared_value: Mapped[Decimal] = mapped_column(Numeric(15, 2))
    transfer_date: Mapped[date] = mapped_column(Date)
    bank: Mapped[str] = mapped_column(String(100))
    reference: Mapped[str] = mapped_column(String(100))
    mime_type: Mapped[str] = mapped_column(String(40))
    sha256: Mapped[str] = mapped_column(String(64), unique=True)
    state: Mapped[str] = mapped_column(String(20), default="EN_REVISION")
    findings: Mapped[str] = mapped_column(Text, default="[]")
    reviewed_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rejection_reason: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class ProofFile(Base):
    __tablename__ = "proof_files"
    id: Mapped[int] = mapped_column(primary_key=True)
    proof_id: Mapped[int] = mapped_column(ForeignKey("proofs.id"), unique=True)
    content: Mapped[bytes] = mapped_column(LargeBinary)


class Receipt(Base):
    __tablename__ = "receipts"
    id: Mapped[int] = mapped_column(primary_key=True)
    charge_id: Mapped[int] = mapped_column(ForeignKey("charges.id"), unique=True)
    number: Mapped[int] = mapped_column(Integer, unique=True)
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))


class ScheduledRun(Base):
    __tablename__ = "scheduled_runs"
    id: Mapped[int] = mapped_column(primary_key=True)
    task: Mapped[str] = mapped_column(String(50))
    period: Mapped[date] = mapped_column(Date)
    completed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    __table_args__ = (UniqueConstraint("task", "period", name="uq_scheduled_task_period"),)


class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    id: Mapped[int] = mapped_column(primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def get_db() -> Generator[Session]:
    with SessionLocal() as db:
        yield db


def claims_for(request: Request, required: str | None = None) -> dict:
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(401, detail={"code": "NO_AUTENTICADO", "message": "Sesión requerida"})
    try:
        claims = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.PyJWTError as exc:
        raise HTTPException(401, detail={"code": "NO_AUTENTICADO", "message": "Sesión inválida"}) from exc
    if required and required not in claims.get("roles", []):
        raise HTTPException(403, detail={"code": "SIN_PERMISOS", "message": "Rol insuficiente"})
    return claims


def require_csrf(request: Request) -> None:
    cookie = request.cookies.get("XSRF-TOKEN")
    header = request.headers.get("X-XSRF-TOKEN")
    if not cookie or not header or not hmac.compare_digest(cookie, header):
        raise HTTPException(403, detail={"code": "CSRF_INVALIDO", "message": "Token CSRF inválido"})


def claim_user_id(claims: dict) -> int:
    try:
        return int(claims["uid"])
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(
            403, detail={"code": "USUARIO_INVALIDO", "message": "La sesión no identifica al usuario"}
        ) from exc


def internal_get(path: str, params: dict | None = None) -> dict:
    if not INTERNAL_SERVICE_TOKEN:
        raise HTTPException(
            502,
            detail={
                "code": "DIRECTORIO_NO_DISPONIBLE",
                "message": "Falta configurar la conexión interna con Identidad",
            },
        )
    try:
        response = httpx.get(
            f"{USER_SERVICE_URL}{path}",
            headers={"X-Internal-Token": INTERNAL_SERVICE_TOKEN},
            params=params,
            timeout=3.0,
        )
        response.raise_for_status()
        return response.json()
    except (httpx.HTTPError, ValueError) as exc:
        raise HTTPException(
            502, detail={"code": "DIRECTORIO_NO_DISPONIBLE", "message": "No fue posible consultar Identidad"}
        ) from exc


def owner_apartment(claims: dict) -> int:
    if "RESIDENTE" not in claims.get("roles", []):
        raise HTTPException(403, detail={"code": "SIN_PERMISOS", "message": "Se requiere una sesión de propietario"})
    user = internal_get(f"/api/v1/internal/users/{claim_user_id(claims)}")
    apartment = user.get("apartment")
    if not apartment or apartment.get("tipoResidente") != "PROPIETARIO":
        raise HTTPException(
            403, detail={"code": "SIN_PERMISOS", "message": "Solo el propietario puede consultar este apartamento"}
        )
    return int(apartment["id"])


def billable_from_directory() -> list[dict]:
    rows: list[dict] = []
    page = 0
    while True:
        result = internal_get("/api/v1/internal/apartments/facturables", {"page": page, "size": 500})
        rows.extend(result["content"])
        if page + 1 >= result["totalPages"]:
            return rows
        page += 1


def sync_billable(db: Session, rows: list[dict]) -> None:
    now = datetime.now(UTC)
    ids = set()
    for row in rows:
        apartment_id = int(row["id"])
        ids.add(apartment_id)
        item = db.get(BillableApartment, apartment_id)
        if item is None:
            item = BillableApartment(id=apartment_id)
            db.add(item)
        item.torre = str(row["torre"])
        item.numero = str(row["numero"])
        item.active = bool(row["activo"])
        item.coefficient = (
            Decimal(str(row["coeficienteCopropiedad"])) if row.get("coeficienteCopropiedad") is not None else None
        )
        item.synced_at = now
    for item in db.scalars(select(BillableApartment)).all():
        if item.id not in ids:
            item.active = False


def interest_pending(db: Session, interest: LateInterest, cutoff: date | None = None) -> Decimal:
    query = select(func.coalesce(func.sum(PaymentApplication.value), 0)).where(
        PaymentApplication.interest_id == interest.id
    )
    if cutoff is not None:
        query = query.join(Payment, PaymentApplication.payment_id == Payment.id).where(Payment.paid_at <= cutoff)
    applied = db.scalar(query) or Decimal(0)
    return Decimal(interest.value) - Decimal(applied)


def overdue_balance(db: Session, apartment_id: int, cutoff: date) -> Decimal:
    charges = db.scalars(select(Charge).where(Charge.apartment_id == apartment_id, Charge.due_at < cutoff)).all()
    capital = sum((max(charge_pending(db, item), Decimal(0)) for item in charges), Decimal(0))
    interests = db.scalars(
        select(LateInterest)
        .join(Charge, LateInterest.charge_id == Charge.id)
        .where(Charge.apartment_id == apartment_id, LateInterest.period <= cutoff)
    ).all()
    return capital + sum((max(interest_pending(db, item), Decimal(0)) for item in interests), Decimal(0))


def enqueue_balance(db: Session, apartment_id: int) -> None:
    db.add(OutboxEvent(event_type="cartera.estado-actualizado", payload=json.dumps({"apartamentoId": apartment_id})))


def effective_value(db: Session, apartment: BillableApartment, period: date, base_value: Decimal) -> Decimal | None:
    manual = db.scalar(
        select(ApartmentValue)
        .where(ApartmentValue.apartment_id == apartment.id, ApartmentValue.effective_from <= period)
        .order_by(ApartmentValue.effective_from.desc())
    )
    if manual is not None:
        return Decimal(manual.value)
    if apartment.coefficient is None:
        return None
    return (base_value * Decimal(apartment.coefficient)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def statement(db: Session, apartment_id: int, cutoff: date) -> dict:
    apartment = db.get(BillableApartment, apartment_id)
    if apartment is None:
        raise HTTPException(404, detail={"code": "APARTAMENTO_NO_ENCONTRADO", "message": "Apartamento no encontrado"})
    charges = db.scalars(
        select(Charge).where(Charge.apartment_id == apartment_id, Charge.period <= cutoff).order_by(Charge.period)
    ).all()
    entries = []
    total = Decimal(0)
    overdue = Decimal(0)
    for charge in charges:
        capital = max(charge_pending(db, charge, cutoff), Decimal(0))
        interests = db.scalars(
            select(LateInterest).where(LateInterest.charge_id == charge.id, LateInterest.period <= cutoff)
        ).all()
        late = sum((max(interest_pending(db, interest, cutoff), Decimal(0)) for interest in interests), Decimal(0))
        total += capital + late
        if charge.due_at < cutoff:
            overdue += capital + late
        entries.append(
            {
                "id": charge.id,
                "periodo": charge.period.isoformat(),
                "capital": money(charge.value),
                "intereses": money(late),
                "pendiente": money(capital + late),
                "fechaVencimiento": charge.due_at.isoformat(),
            }
        )
    payments = db.scalars(
        select(Payment).where(Payment.apartment_id == apartment_id, Payment.paid_at <= cutoff).order_by(Payment.paid_at)
    ).all()
    paid = sum((Decimal(payment.value) for payment in payments), Decimal(0))
    applied = db.scalar(
        select(func.coalesce(func.sum(PaymentApplication.value), 0))
        .join(Payment, PaymentApplication.payment_id == Payment.id)
        .where(Payment.apartment_id == apartment_id, Payment.paid_at <= cutoff)
    ) or Decimal(0)
    credit = max(paid - Decimal(applied), Decimal(0))
    return {
        "apartamento": {"id": apartment.id, "torre": apartment.torre, "numero": apartment.numero},
        "saldoTotal": money(max(total - credit, Decimal(0))),
        "saldoVencido": money(max(overdue - credit, Decimal(0))),
        "saldoAFavor": money(credit),
        "pazYSalvo": overdue <= credit,
        "cobros": entries,
        "pagos": [
            {
                "id": payment.id,
                "valor": money(payment.value),
                "fechaPago": payment.paid_at.isoformat(),
                "origen": payment.origin,
            }
            for payment in payments
        ],
    }


class ParameterInput(BaseModel):
    base_value: Decimal = Field(gt=0)
    monthly_late_rate: Decimal = Field(ge=0)
    due_days: int = Field(ge=0, le=90)
    effective_from: date


class PaymentInput(BaseModel):
    apartment_id: int = Field(gt=0)
    value: Decimal = Field(gt=0, max_digits=15, decimal_places=2)
    paid_at: date
    method: PaymentMethod
    reference: str | None = Field(default=None, max_length=80)


class ApartmentValueInput(BaseModel):
    value: Decimal = Field(gt=0)
    effective_from: date
    reason: str = Field(min_length=3, max_length=500)


def money(value: Decimal) -> str:
    return str(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def month_start(value: str) -> date:
    try:
        return date.fromisoformat(f"{value}-01")
    except ValueError as exc:
        raise HTTPException(422, detail={"code": "PERIODO_INVALIDO", "message": "El período debe ser YYYY-MM"}) from exc


def charge_pending(db: Session, charge: Charge, cutoff: date | None = None) -> Decimal:
    query = select(func.coalesce(func.sum(PaymentApplication.value), 0)).where(
        PaymentApplication.charge_id == charge.id
    )
    if cutoff is not None:
        query = query.join(Payment, PaymentApplication.payment_id == Payment.id).where(Payment.paid_at <= cutoff)
    applied = db.scalar(query) or Decimal(0)
    return Decimal(charge.value) - Decimal(applied)


def apply_new_payment(
    db: Session,
    apartment_id: int,
    value: Decimal,
    paid_at: date,
    method: PaymentMethod,
    reference: str | None,
    origin: str,
    user_id: int,
    proof_id: int | None = None,
) -> tuple[Payment, Decimal]:
    db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": 175100000 + apartment_id})
    payment = Payment(
        apartment_id=apartment_id,
        value=value,
        paid_at=paid_at,
        method=method,
        reference=reference,
        origin=origin,
        proof_id=proof_id,
        registered_by_user_id=user_id,
    )
    db.add(payment)
    db.flush()
    remaining = value
    charges = db.scalars(
        select(Charge).where(Charge.apartment_id == apartment_id).order_by(Charge.period).with_for_update()
    ).all()
    for charge in charges:
        pending = charge_pending(db, charge)
        if pending <= 0:
            continue
        applied = min(remaining, pending)
        db.add(PaymentApplication(payment_id=payment.id, charge_id=charge.id, value=applied))
        remaining -= applied
        charge.state = ChargeStatus.PAGADO if applied == pending else ChargeStatus.PARCIAL
        if remaining <= 0:
            break
    if remaining > 0:
        interests = db.scalars(
            select(LateInterest)
            .join(Charge, LateInterest.charge_id == Charge.id)
            .where(Charge.apartment_id == apartment_id)
            .order_by(Charge.period, LateInterest.period)
            .with_for_update()
        ).all()
        for interest in interests:
            pending = interest_pending(db, interest)
            if pending <= 0:
                continue
            applied = min(remaining, pending)
            db.add(PaymentApplication(payment_id=payment.id, interest_id=interest.id, value=applied))
            remaining -= applied
            if remaining <= 0:
                break
    enqueue_balance(db, apartment_id)
    return payment, remaining


stop_workers = Event()


@asynccontextmanager
async def lifespan(_: FastAPI):
    stop_workers.clear()
    worker = Thread(target=background_worker, daemon=True)
    worker.start()
    try:
        yield
    finally:
        stop_workers.set()
        worker.join(timeout=10)


app = FastAPI(title="GR Billing Microservice", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:3007,http://localhost:3001").split(","),
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT"],
    allow_headers=["Content-Type", "X-XSRF-TOKEN", "Idempotency-Key"],
)


@app.exception_handler(HTTPException)
async def http_error(_: Request, exc: HTTPException) -> JSONResponse:
    detail = exc.detail if isinstance(exc.detail, dict) else {"code": "ERROR_HTTP", "message": str(exc.detail)}
    return JSONResponse(status_code=exc.status_code, content={"error": detail})


@app.exception_handler(RequestValidationError)
async def validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    details = [
        {"path": ".".join(str(part) for part in error["loc"]), "message": error["msg"]} for error in exc.errors()
    ]
    return JSONResponse(
        status_code=422,
        content={"error": {"code": "SOLICITUD_INVALIDA", "message": "Revisa los datos enviados", "details": details}},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
def readiness(db: Session = Depends(get_db)) -> dict[str, str]:
    db.execute(select(1))
    return {"status": "ready"}


@app.get("/api/v1/finanzas/parametros")
def get_parameters(request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    items = db.scalars(select(FinancialParameter).order_by(FinancialParameter.effective_from.desc())).all()
    return {
        "payload": [
            {
                "id": x.id,
                "baseValue": money(x.base_value),
                "monthlyLateRate": str(x.monthly_late_rate),
                "dueDays": x.due_days,
                "effectiveFrom": x.effective_from.isoformat(),
            }
            for x in items
        ]
    }


@app.put("/api/v1/finanzas/parametros")
def set_parameters(payload: ParameterInput, request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    require_csrf(request)
    if payload.effective_from.day != 1:
        raise HTTPException(
            422, detail={"code": "VIGENCIA_INVALIDA", "message": "La vigencia debe comenzar el primer día del mes"}
        )
    item = FinancialParameter(**payload.model_dump())
    db.add(item)
    db.commit()
    return {"payload": {"id": item.id, "baseValue": money(item.base_value)}}


@app.get("/api/v1/finanzas/valores-apartamento/{apartment_id}")
def get_apartment_value(apartment_id: int, request: Request, fecha: date | None = None, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    apartment = db.get(BillableApartment, apartment_id)
    if apartment is None:
        raise HTTPException(404, detail={"code": "APARTAMENTO_NO_ENCONTRADO", "message": "Apartamento no encontrado"})
    cutoff = fecha or business_today()
    parameter = db.scalar(
        select(FinancialParameter)
        .where(FinancialParameter.effective_from <= cutoff)
        .order_by(FinancialParameter.effective_from.desc())
    )
    if parameter is None:
        raise HTTPException(422, detail={"code": "PARAMETRO_NO_CONFIGURADO", "message": "No hay parámetros vigentes"})
    manual = db.scalar(
        select(ApartmentValue)
        .where(ApartmentValue.apartment_id == apartment_id, ApartmentValue.effective_from <= cutoff)
        .order_by(ApartmentValue.effective_from.desc())
    )
    value = effective_value(db, apartment, cutoff, Decimal(parameter.base_value))
    return {
        "payload": {
            "apartamentoId": apartment_id,
            "valor": money(value) if value is not None else None,
            "origen": "MANUAL" if manual else "CALCULADO",
        }
    }


@app.put("/api/v1/finanzas/valores-apartamento/{apartment_id}")
def set_apartment_value(
    apartment_id: int, payload: ApartmentValueInput, request: Request, db: Session = Depends(get_db)
):
    claims = claims_for(request, "ADMINISTRACION")
    require_csrf(request)
    if db.get(BillableApartment, apartment_id) is None:
        raise HTTPException(404, detail={"code": "APARTAMENTO_NO_ENCONTRADO", "message": "Apartamento no encontrado"})
    if payload.effective_from.day != 1:
        raise HTTPException(
            422, detail={"code": "VIGENCIA_INVALIDA", "message": "La vigencia debe comenzar el primer día del mes"}
        )
    item = db.scalar(
        select(ApartmentValue).where(
            ApartmentValue.apartment_id == apartment_id, ApartmentValue.effective_from == payload.effective_from
        )
    )
    if item is None:
        item = ApartmentValue(apartment_id=apartment_id, effective_from=payload.effective_from)
        db.add(item)
    item.value = payload.value
    item.reason = payload.reason
    item.created_by_user_id = claim_user_id(claims)
    db.commit()
    return {"payload": {"apartamentoId": apartment_id, "valor": money(item.value)}}


@app.post("/api/v1/finanzas/cobros/generar", status_code=202)
def generate_charges(periodo: str, request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    require_csrf(request)
    period = month_start(periodo)
    if period > business_today().replace(day=1):
        raise HTTPException(422, detail={"code": "PERIODO_FUTURO", "message": "No se puede generar un período futuro"})
    generation = enqueue_generation(period, db)
    db.commit()
    return {"payload": {"generacionId": generation.id, "status": generation.status}}


def enqueue_generation(period: date, db: Session) -> Generation:
    directory_rows = billable_from_directory()
    db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": 175000000 + period.year * 100 + period.month})
    parameter = db.scalar(
        select(FinancialParameter)
        .where(FinancialParameter.effective_from <= period)
        .order_by(FinancialParameter.effective_from.desc())
    )
    if parameter is None:
        raise HTTPException(422, detail={"code": "PARAMETRO_NO_CONFIGURADO", "message": "No hay parámetros vigentes"})
    sync_billable(db, directory_rows)
    queued = db.scalar(
        select(Generation)
        .where(Generation.period == period, Generation.status.in_(["EN_COLA", "EN_PROCESO"]))
        .order_by(Generation.id.desc())
    )
    if queued is not None:
        return queued
    total = (
        db.scalar(select(func.count()).select_from(BillableApartment).where(BillableApartment.active.is_(True))) or 0
    )
    generation = Generation(period=period, total=total, status="EN_COLA")
    db.add(generation)
    db.flush()
    return generation


@app.get("/api/v1/finanzas/cobros/generaciones/{generation_id}")
def generation(generation_id: int, request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    item = db.get(Generation, generation_id)
    if not item:
        raise HTTPException(404, detail={"code": "GENERACION_NO_ENCONTRADA", "message": "Generación no encontrada"})
    return {
        "payload": {
            "id": item.id,
            "period": item.period.isoformat(),
            "total": item.total,
            "generated": item.generated,
            "skipped": item.skipped,
            "failed": item.failed,
            "processed": item.generated + item.skipped + item.failed,
            "status": item.status,
            "error": item.error,
        }
    }


@app.get("/api/v1/finanzas/cobros")
def list_charges(
    request: Request,
    apartamento_id: int | None = Query(default=None, alias="apartamentoId"),
    db: Session = Depends(get_db),
):
    claims_for(request, "ADMINISTRACION")
    query = select(Charge).order_by(Charge.period.desc(), Charge.id.desc())
    if apartamento_id is not None:
        query = query.where(Charge.apartment_id == apartamento_id)
    return {
        "payload": [
            {
                "id": item.id,
                "apartamentoId": item.apartment_id,
                "periodo": item.period.isoformat(),
                "valor": money(item.value),
                "pendiente": money(charge_pending(db, item)),
                "estado": item.state.value,
            }
            for item in db.scalars(query).all()
        ]
    }


@app.get("/api/v1/finanzas/cobros/{charge_id}/recibo")
def receipt_pdf(charge_id: int, request: Request, db: Session = Depends(get_db)):
    claims = claims_for(request)
    charge = db.get(Charge, charge_id)
    if charge is None:
        raise HTTPException(404, detail={"code": "COBRO_NO_ENCONTRADO", "message": "Cobro no encontrado"})
    if "ADMINISTRACION" not in claims.get("roles", []) and owner_apartment(claims) != charge.apartment_id:
        raise HTTPException(403, detail={"code": "SIN_PERMISOS", "message": "No puedes descargar este recibo"})
    if charge_pending(db, charge) > 0:
        raise HTTPException(422, detail={"code": "COBRO_PENDIENTE", "message": "El cobro aún tiene saldo pendiente"})
    receipt = db.scalar(select(Receipt).where(Receipt.charge_id == charge_id))
    if receipt is None:
        db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": 175300000})
        receipt = db.scalar(select(Receipt).where(Receipt.charge_id == charge_id))
        if receipt is None:
            number = int(db.scalar(select(func.coalesce(func.max(Receipt.number), 0))) or 0) + 1
            receipt = Receipt(charge_id=charge_id, number=number)
            db.add(receipt)
            db.commit()
    apartment = db.get(BillableApartment, charge.apartment_id)
    lines = [
        f"Recibo numero: {receipt.number}",
        f"Apartamento: {apartment.torre}-{apartment.numero}",
        f"Periodo: {charge.period.isoformat()}",
        f"Valor del cobro: COP {money(charge.value)}",
        "Estado: pagado",
    ]
    return Response(
        render_pdf("Recibo de administracion", lines),
        media_type="application/pdf",
        headers={"Content-Disposition": f"attachment; filename=recibo-{receipt.number}.pdf"},
    )


@app.post("/api/v1/finanzas/pagos")
def create_payment(payload: PaymentInput, request: Request, db: Session = Depends(get_db)):
    claims = claims_for(request, "ADMINISTRACION")
    require_csrf(request)
    user_id = claim_user_id(claims)
    key = request.headers.get("Idempotency-Key")
    if not key:
        raise HTTPException(
            400, detail={"code": "IDEMPOTENCY_KEY_REQUERIDA", "message": "Idempotency-Key es obligatorio"}
        )
    try:
        UUID(key)
    except ValueError as exc:
        raise HTTPException(
            400, detail={"code": "IDEMPOTENCY_KEY_INVALIDA", "message": "Idempotency-Key debe ser un UUID"}
        ) from exc
    request_json = json.dumps(payload.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    request_hash = hashlib.sha256(request_json.encode()).hexdigest()
    db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": int(key.replace("-", "")[:8], 16)})
    existing_key = db.get(IdempotencyKey, key)
    if existing_key:
        if (
            existing_key.user_id != user_id
            or existing_key.route != "/api/v1/finanzas/pagos"
            or existing_key.request_hash != request_hash
        ):
            raise HTTPException(
                409,
                detail={"code": "IDEMPOTENCY_KEY_REUTILIZADA", "message": "La clave ya fue usada con otra solicitud"},
            )
        return Response(
            content=existing_key.response_json, media_type="application/json", status_code=existing_key.status_code
        )
    apartment = db.get(BillableApartment, payload.apartment_id)
    if not apartment:
        raise HTTPException(404, detail={"code": "APARTAMENTO_NO_ENCONTRADO", "message": "Apartamento no encontrado"})
    payment, remaining = apply_new_payment(
        db, payload.apartment_id, payload.value, payload.paid_at, payload.method, payload.reference, "MANUAL", user_id
    )
    body = {"payload": {"id": payment.id, "value": money(payment.value), "saldoAFavor": money(remaining)}}
    serialized = json.dumps(body)
    db.add(
        IdempotencyKey(
            key=key,
            request_hash=request_hash,
            response_json=serialized,
            user_id=user_id,
            route="/api/v1/finanzas/pagos",
            status_code=201,
        )
    )
    db.commit()
    return Response(content=serialized, media_type="application/json", status_code=201)


class ReversalInput(BaseModel):
    reason: str = Field(min_length=3, max_length=500)


@app.post("/api/v1/finanzas/pagos/{payment_id}/reverso")
def reverse_payment(payment_id: int, payload: ReversalInput, request: Request, db: Session = Depends(get_db)):
    claims = claims_for(request, "ADMINISTRACION")
    require_csrf(request)
    original = db.get(Payment, payment_id)
    if original is None:
        raise HTTPException(404, detail={"code": "PAGO_NO_ENCONTRADO", "message": "Pago no encontrado"})
    if original.reversal_of_payment_id is not None:
        raise HTTPException(422, detail={"code": "REVERSO_INVALIDO", "message": "No se puede reversar un reverso"})
    db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": 175100000 + original.apartment_id})
    existing = db.scalar(select(Payment).where(Payment.reversal_of_payment_id == payment_id))
    if existing is not None:
        return {"payload": {"id": existing.id, "pagoOriginalId": payment_id, "valor": money(existing.value)}}
    reversal = Payment(
        apartment_id=original.apartment_id,
        value=-Decimal(original.value),
        paid_at=business_today(),
        method=original.method,
        reference=original.reference,
        origin="REVERSO",
        registered_by_user_id=claim_user_id(claims),
        reversal_of_payment_id=payment_id,
        reversal_reason=payload.reason,
    )
    db.add(reversal)
    db.flush()
    for application in db.scalars(select(PaymentApplication).where(PaymentApplication.payment_id == payment_id)).all():
        db.add(
            PaymentApplication(
                payment_id=reversal.id,
                charge_id=application.charge_id,
                interest_id=application.interest_id,
                value=-Decimal(application.value),
            )
        )
    db.flush()
    for charge in db.scalars(select(Charge).where(Charge.apartment_id == original.apartment_id)).all():
        pending = charge_pending(db, charge)
        charge.state = (
            ChargeStatus.PAGADO
            if pending <= 0
            else ChargeStatus.PENDIENTE
            if pending >= charge.value
            else ChargeStatus.PARCIAL
        )
    enqueue_balance(db, original.apartment_id)
    db.commit()
    return {"payload": {"id": reversal.id, "pagoOriginalId": payment_id, "valor": money(reversal.value)}}


@app.get("/api/v1/finanzas/pagos")
def list_payments(
    request: Request,
    apartamento_id: int | None = Query(default=None, alias="apartamentoId"),
    db: Session = Depends(get_db),
):
    claims_for(request, "ADMINISTRACION")
    query = select(Payment).order_by(Payment.paid_at.desc(), Payment.id.desc())
    if apartamento_id is not None:
        query = query.where(Payment.apartment_id == apartamento_id)
    return {
        "payload": [
            {
                "id": item.id,
                "apartamentoId": item.apartment_id,
                "valor": money(item.value),
                "fechaPago": item.paid_at.isoformat(),
                "medio": item.method.value,
                "origen": item.origin,
            }
            for item in db.scalars(query).all()
        ]
    }


class InterestInput(BaseModel):
    periodo: str


@app.post("/api/v1/finanzas/intereses/aplicar")
def apply_interest(payload: InterestInput, request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    require_csrf(request)
    period = month_start(payload.periodo)
    if period > business_today().replace(day=1):
        raise HTTPException(422, detail={"code": "PERIODO_FUTURO", "message": "No se pueden causar intereses futuros"})
    created = cause_interest(period, db)
    db.commit()
    return {"payload": {"periodo": payload.periodo, "causados": created}}


def cause_interest(period: date, db: Session) -> int:
    db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": 175200000 + period.year * 100 + period.month})
    cutoff = min(date(period.year, period.month, calendar.monthrange(period.year, period.month)[1]), business_today())
    parameter = db.scalar(
        select(FinancialParameter)
        .where(FinancialParameter.effective_from <= period)
        .order_by(FinancialParameter.effective_from.desc())
    )
    if parameter is None:
        raise HTTPException(422, detail={"code": "PARAMETRO_NO_CONFIGURADO", "message": "No hay parámetros vigentes"})
    created = 0
    for charge in db.scalars(select(Charge).where(Charge.due_at < cutoff)).all():
        if (
            db.scalar(select(LateInterest.id).where(LateInterest.charge_id == charge.id, LateInterest.period == period))
            is not None
        ):
            continue
        capital = max(charge_pending(db, charge, cutoff), Decimal(0))
        if capital <= 0:
            continue
        amount = (capital * Decimal(parameter.monthly_late_rate)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if amount <= 0:
            continue
        db.add(
            LateInterest(
                charge_id=charge.id, period=period, base_capital=capital, rate=parameter.monthly_late_rate, value=amount
            )
        )
        enqueue_balance(db, charge.apartment_id)
        created += 1
    return created


@app.get("/api/v1/finanzas/estado-cuenta")
def own_statement(request: Request, db: Session = Depends(get_db)):
    apartment_id = owner_apartment(claims_for(request))
    return {"payload": statement(db, apartment_id, business_today())}


@app.get("/api/v1/finanzas/estado-cuenta/{apartment_id}")
def apartment_statement(apartment_id: int, request: Request, db: Session = Depends(get_db)):
    claims = claims_for(request)
    if "ADMINISTRACION" not in claims.get("roles", []) and owner_apartment(claims) != apartment_id:
        raise HTTPException(403, detail={"code": "SIN_PERMISOS", "message": "No puedes consultar otro apartamento"})
    return {"payload": statement(db, apartment_id, business_today())}


def proof_payload(item: Proof) -> dict:
    return {
        "id": item.id,
        "apartamentoId": item.apartment_id,
        "valorDeclarado": money(item.declared_value),
        "fechaTransferencia": item.transfer_date.isoformat(),
        "banco": item.bank,
        "referencia": item.reference,
        "estado": item.state,
        "verificacion": "SIMULADA",
        "hallazgos": json.loads(item.findings),
    }


@app.post("/api/v1/finanzas/comprobantes", status_code=201)
async def upload_proof(
    request: Request,
    valor_declarado: Decimal = Form(alias="valorDeclarado"),
    fecha_transferencia: date = Form(alias="fechaTransferencia"),
    banco: str = Form(),
    referencia: str = Form(),
    archivo: UploadFile = File(),
    db: Session = Depends(get_db),
):
    claims = claims_for(request)
    require_csrf(request)
    apartment_id = owner_apartment(claims)
    user_id = claim_user_id(claims)
    if db.get(BillableApartment, apartment_id) is None:
        raise HTTPException(
            404, detail={"code": "APARTAMENTO_NO_ENCONTRADO", "message": "El apartamento aún no está sincronizado"}
        )
    if valor_declarado <= 0 or valor_declarado.as_tuple().exponent < -2:
        raise HTTPException(
            422, detail={"code": "VALOR_INVALIDO", "message": "El valor debe ser positivo y tener máximo dos decimales"}
        )
    bank = banco.strip()
    reference = referencia.strip()
    if not bank or not reference or len(bank) > 100 or len(reference) > 100:
        raise HTTPException(
            422, detail={"code": "COMPROBANTE_INVALIDO", "message": "Banco y referencia son obligatorios"}
        )
    content = await archivo.read(5 * 1024 * 1024 + 1)
    if len(content) > 5 * 1024 * 1024:
        raise HTTPException(413, detail={"code": "ARCHIVO_DEMASIADO_GRANDE", "message": "El archivo supera 5 MB"})
    mime = (
        "application/pdf"
        if content.startswith(b"%PDF-")
        else "image/png"
        if content.startswith(b"\x89PNG\r\n\x1a\n")
        else "image/jpeg"
        if content.startswith(b"\xff\xd8\xff")
        else None
    )
    if mime is None:
        raise HTTPException(422, detail={"code": "ARCHIVO_INVALIDO", "message": "Se requiere un PDF, PNG o JPG válido"})
    if fecha_transferencia > business_today() or (business_today() - fecha_transferencia).days > 30:
        raise HTTPException(
            422,
            detail={
                "code": "FECHA_INVALIDA",
                "message": "La fecha de transferencia debe estar dentro de los últimos 30 días",
            },
        )
    key = request.headers.get("Idempotency-Key")
    if not key:
        raise HTTPException(
            400, detail={"code": "IDEMPOTENCY_KEY_REQUERIDA", "message": "Idempotency-Key es obligatorio"}
        )
    try:
        UUID(key)
    except ValueError as exc:
        raise HTTPException(
            400, detail={"code": "IDEMPOTENCY_KEY_INVALIDA", "message": "Idempotency-Key debe ser un UUID"}
        ) from exc
    digest = hashlib.sha256(content).hexdigest()
    request_hash = hashlib.sha256(
        f"{digest}|{money(valor_declarado)}|{fecha_transferencia}|{bank}|{reference}".encode()
    ).hexdigest()
    db.execute(text("SELECT pg_advisory_xact_lock(:key)"), {"key": int(key.replace("-", "")[:8], 16)})
    existing_key = db.get(IdempotencyKey, key)
    if existing_key:
        if (
            existing_key.user_id != user_id
            or existing_key.route != "/api/v1/finanzas/comprobantes"
            or existing_key.request_hash != request_hash
        ):
            raise HTTPException(
                409,
                detail={"code": "IDEMPOTENCY_KEY_REUTILIZADA", "message": "La clave ya fue usada con otra solicitud"},
            )
        return Response(
            content=existing_key.response_json, media_type="application/json", status_code=existing_key.status_code
        )
    duplicate = db.scalar(
        select(Proof.id).where((Proof.sha256 == digest) | ((Proof.bank == bank) & (Proof.reference == reference)))
    )
    if duplicate is not None:
        raise HTTPException(
            409, detail={"code": "COMPROBANTE_DUPLICADO", "message": "El comprobante o la referencia ya fue registrada"}
        )
    item = Proof(
        apartment_id=apartment_id,
        uploaded_by_user_id=user_id,
        declared_value=valor_declarado,
        transfer_date=fecha_transferencia,
        bank=bank,
        reference=reference,
        mime_type=mime,
        sha256=digest,
        state="EN_REVISION",
        findings=json.dumps(["Verificación simulada: no confirma la transferencia bancaria"]),
    )
    db.add(item)
    db.flush()
    db.add(ProofFile(proof_id=item.id, content=content))
    body = {"payload": proof_payload(item)}
    serialized = json.dumps(body)
    db.add(
        IdempotencyKey(
            key=key,
            request_hash=request_hash,
            response_json=serialized,
            user_id=user_id,
            route="/api/v1/finanzas/comprobantes",
            status_code=201,
        )
    )
    db.commit()
    return Response(content=serialized, media_type="application/json", status_code=201)


@app.get("/api/v1/finanzas/comprobantes/mios")
def my_proofs(request: Request, db: Session = Depends(get_db)):
    apartment_id = owner_apartment(claims_for(request))
    items = db.scalars(select(Proof).where(Proof.apartment_id == apartment_id).order_by(Proof.created_at.desc())).all()
    return {"payload": [proof_payload(item) for item in items]}


@app.get("/api/v1/finanzas/comprobantes")
def list_proofs(request: Request, estado: str | None = None, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    query = select(Proof).order_by(Proof.created_at.desc())
    if estado:
        query = query.where(Proof.state == estado)
    return {"payload": [proof_payload(item) for item in db.scalars(query).all()]}


@app.get("/api/v1/finanzas/comprobantes/{proof_id}/archivo")
def proof_file(proof_id: int, request: Request, db: Session = Depends(get_db)):
    claims = claims_for(request)
    item = db.get(Proof, proof_id)
    if item is None:
        raise HTTPException(404, detail={"code": "COMPROBANTE_NO_ENCONTRADO", "message": "Comprobante no encontrado"})
    if "ADMINISTRACION" not in claims.get("roles", []) and owner_apartment(claims) != item.apartment_id:
        raise HTTPException(403, detail={"code": "SIN_PERMISOS", "message": "No puedes consultar este comprobante"})
    data = db.scalar(select(ProofFile.content).where(ProofFile.proof_id == proof_id))
    return Response(content=data, media_type=item.mime_type)


@app.patch("/api/v1/finanzas/comprobantes/{proof_id}/aprobacion")
def approve_proof(proof_id: int, request: Request, db: Session = Depends(get_db)):
    claims = claims_for(request, "ADMINISTRACION")
    require_csrf(request)
    item = db.scalar(select(Proof).where(Proof.id == proof_id).with_for_update())
    if item is None:
        raise HTTPException(404, detail={"code": "COMPROBANTE_NO_ENCONTRADO", "message": "Comprobante no encontrado"})
    existing = db.scalar(select(Payment).where(Payment.proof_id == proof_id))
    if item.state == "APROBADO" and existing is not None:
        return {"payload": {**proof_payload(item), "pagoId": existing.id}}
    if item.state == "RECHAZADO":
        raise HTTPException(409, detail={"code": "COMPROBANTE_RECHAZADO", "message": "El comprobante fue rechazado"})
    payment, _ = apply_new_payment(
        db,
        item.apartment_id,
        Decimal(item.declared_value),
        item.transfer_date,
        PaymentMethod.TRANSFERENCIA,
        item.reference,
        "COMPROBANTE",
        claim_user_id(claims),
        item.id,
    )
    item.state = "APROBADO"
    item.reviewed_by_user_id = claim_user_id(claims)
    db.commit()
    return {"payload": {**proof_payload(item), "pagoId": payment.id}}


class RejectionInput(BaseModel):
    reason: str = Field(min_length=3, max_length=500)


@app.patch("/api/v1/finanzas/comprobantes/{proof_id}/rechazo")
def reject_proof(proof_id: int, payload: RejectionInput, request: Request, db: Session = Depends(get_db)):
    claims = claims_for(request, "ADMINISTRACION")
    require_csrf(request)
    item = db.scalar(select(Proof).where(Proof.id == proof_id).with_for_update())
    if item is None:
        raise HTTPException(404, detail={"code": "COMPROBANTE_NO_ENCONTRADO", "message": "Comprobante no encontrado"})
    if item.state == "APROBADO":
        raise HTTPException(
            409, detail={"code": "COMPROBANTE_YA_APROBADO", "message": "El comprobante ya fue aprobado"}
        )
    if item.state == "RECHAZADO":
        return {"payload": proof_payload(item)}
    item.state = "RECHAZADO"
    item.rejection_reason = payload.reason
    item.reviewed_by_user_id = claim_user_id(claims)
    db.commit()
    return {"payload": proof_payload(item)}


@app.get("/api/v1/finanzas/cartera")
def portfolio(
    request: Request, fecha_corte: date | None = Query(default=None, alias="fechaCorte"), db: Session = Depends(get_db)
):
    claims_for(request, "ADMINISTRACION")
    cutoff = fecha_corte or business_today()
    return {"payload": portfolio_data(db, cutoff)}


def portfolio_data(db: Session, cutoff: date) -> dict:
    rows = []
    for apartment in db.scalars(
        select(BillableApartment).order_by(BillableApartment.torre, BillableApartment.numero)
    ).all():
        state = statement(db, apartment.id, cutoff)
        rows.append(
            {
                "apartamentoId": apartment.id,
                "torre": apartment.torre,
                "numero": apartment.numero,
                "pendiente": state["saldoTotal"],
                "vencido": state["saldoVencido"],
            }
        )
    return {
        "fechaCorte": cutoff.isoformat(),
        "apartamentos": rows,
        "totalPendiente": money(sum((Decimal(row["pendiente"]) for row in rows), Decimal(0))),
        "totalVencido": money(sum((Decimal(row["vencido"]) for row in rows), Decimal(0))),
    }


@app.get("/api/v1/finanzas/reportes/cartera.csv")
def portfolio_csv(request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    rows = portfolio_data(db, business_today())["apartamentos"]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["apartamentoId", "torre", "numero", "pendiente", "vencido"])
    writer.writeheader()
    writer.writerows(rows)
    return Response(
        output.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=cartera.csv"}
    )


def render_pdf(title: str, lines: list[str]) -> bytes:
    output = io.BytesIO()
    document = canvas.Canvas(output, pagesize=A4)
    document.setTitle(title)
    document.setFont("Helvetica-Bold", 16)
    document.drawString(45, 800, title)
    document.setFont("Helvetica", 10)
    y = 770
    for line in lines:
        if y < 50:
            document.showPage()
            document.setFont("Helvetica", 10)
            y = 800
        document.drawString(45, y, line[:110])
        y -= 17
    document.save()
    return output.getvalue()


@app.get("/api/v1/finanzas/reportes/cartera.pdf")
def portfolio_pdf(
    request: Request, fecha_corte: date | None = Query(default=None, alias="fechaCorte"), db: Session = Depends(get_db)
):
    claims_for(request, "ADMINISTRACION")
    data = portfolio_data(db, fecha_corte or business_today())
    lines = [f"Corte: {data['fechaCorte']}", f"Total pendiente: COP {data['totalPendiente']}"]
    lines.extend(
        f"{row['torre']}-{row['numero']}: COP {row['pendiente']} (vencido {row['vencido']})"
        for row in data["apartamentos"]
    )
    return Response(
        render_pdf("Cartera de la copropiedad", lines),
        media_type="application/pdf",
        headers={"Content-Disposition": "attachment; filename=cartera.pdf"},
    )


@app.get("/api/v1/finanzas/reportes/recaudo.csv")
def collection_csv(request: Request, desde: date, hasta: date, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["pagoId", "apartamentoId", "fechaPago", "valor", "medio", "origen"])
    for item in db.scalars(
        select(Payment).where(Payment.paid_at.between(desde, hasta)).order_by(Payment.paid_at)
    ).all():
        writer.writerow(
            [item.id, item.apartment_id, item.paid_at.isoformat(), money(item.value), item.method.value, item.origin]
        )
    return Response(
        output.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=recaudo.csv"}
    )


@app.get("/api/v1/finanzas/reportes/comprobantes.csv")
def proof_csv(request: Request, desde: date, hasta: date, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["comprobanteId", "apartamentoId", "fechaTransferencia", "valor", "banco", "referencia", "estado"])
    for item in db.scalars(
        select(Proof).where(Proof.transfer_date.between(desde, hasta)).order_by(Proof.transfer_date)
    ).all():
        writer.writerow(
            [
                item.id,
                item.apartment_id,
                item.transfer_date.isoformat(),
                money(item.declared_value),
                item.bank,
                item.reference,
                item.state,
            ]
        )
    return Response(
        output.getvalue(),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=comprobantes.csv"},
    )


@app.post("/api/v1/finanzas/cartera/republicar")
def republish_balances(request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    require_csrf(request)
    apartments = db.scalars(select(BillableApartment.id)).all()
    for apartment_id in apartments:
        enqueue_balance(db, apartment_id)
    db.commit()
    return {"payload": {"enCola": len(apartments)}}


@strawberry.type
class PortfolioApartment:
    apartamento_id: int
    torre: str
    numero: str
    pendiente: str
    vencido: str


@strawberry.type
class PortfolioResult:
    fecha_corte: str
    total_pendiente: str
    apartamentos: list[PortfolioApartment]


@strawberry.type
class QueryType:
    @strawberry.field
    def cartera(self, info: strawberry.Info, fecha_corte: str | None = None) -> PortfolioResult:
        db: Session = info.context["db"]
        cutoff = date.fromisoformat(fecha_corte) if fecha_corte else business_today()
        data = portfolio_data(db, cutoff)
        rows = [
            PortfolioApartment(
                apartamento_id=row["apartamentoId"],
                torre=row["torre"],
                numero=row["numero"],
                pendiente=row["pendiente"],
                vencido=row["vencido"],
            )
            for row in data["apartamentos"]
        ]
        return PortfolioResult(
            fecha_corte=cutoff.isoformat(), total_pendiente=data["totalPendiente"], apartamentos=rows
        )


async def graphql_context(request: Request):
    claims_for(request, "ADMINISTRACION")
    with SessionLocal() as db:
        yield {"request": request, "db": db}


graphql_extensions = [QueryDepthLimiter(max_depth=4), MaxTokensLimiter(max_token_count=1000)]
if os.getenv("APP_ENV", "development").lower() == "production":
    graphql_extensions.append(DisableIntrospection())
app.include_router(
    GraphQLRouter(strawberry.Schema(query=QueryType, extensions=graphql_extensions), context_getter=graphql_context),
    prefix="/api/v1/finanzas/graphql",
)


def publish_outbox() -> None:
    with SessionLocal() as db:
        events = db.scalars(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(100)
            .with_for_update(skip_locked=True)
        ).all()
        if not events:
            return
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        try:
            channel = connection.channel()
            channel.exchange_declare(exchange=BILLING_EVENTS_EXCHANGE, exchange_type="topic", durable=True)
            channel.confirm_delivery()
            for event in events:
                payload = json.loads(event.payload)
                if event.event_type == "cartera.estado-actualizado":
                    apartment_id = int(payload["apartamentoId"])
                    payload = {
                        "type": "cartera.estado-actualizado",
                        "apartamentoId": apartment_id,
                        "saldoVencido": float(Decimal(statement(db, apartment_id, business_today())["saldoVencido"])),
                        "occurredAt": datetime.now(UTC).isoformat(),
                        "correlationId": str(uuid4()),
                    }
                # Pika raises NackError when publisher confirms reject a message.
                # BlockingChannel.basic_publish returns None on successful confirms.
                channel.basic_publish(
                    exchange=BILLING_EVENTS_EXCHANGE,
                    routing_key=event.event_type,
                    body=json.dumps(payload).encode(),
                    properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
                )
                event.published_at = datetime.now(UTC)
            db.commit()
        finally:
            connection.close()


def scheduled_once(task: str, period: date, lock_key: int, action) -> None:
    with SessionLocal() as db:
        acquired = db.scalar(text("SELECT pg_try_advisory_xact_lock(:key)"), {"key": lock_key})
        if not acquired:
            return
        exists = db.scalar(select(ScheduledRun.id).where(ScheduledRun.task == task, ScheduledRun.period == period))
        if exists is not None:
            return
        action(db)
        db.add(ScheduledRun(task=task, period=period))
        db.commit()


GENERATION_BATCH_SIZE = 100
GENERATION_MAX_WORKERS = 4


def _calculate_generation_value(
    values: tuple[int, Decimal | None, Decimal | None, Decimal],
) -> tuple[int, Decimal | None]:
    apartment_id, coefficient, manual_value, base_value = values
    if manual_value is not None:
        return apartment_id, manual_value
    if coefficient is None:
        return apartment_id, None
    amount = base_value * coefficient
    return apartment_id, amount.quantize(Decimal("1"), rounding=ROUND_HALF_UP)


def process_generation_jobs(max_jobs: int = GENERATION_MAX_WORKERS) -> int:
    processed = 0
    for _ in range(max_jobs):
        with SessionLocal() as db:
            item = db.scalar(
                select(Generation)
                .where(Generation.status.in_(["EN_COLA", "EN_PROCESO"]))
                .order_by(Generation.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if item is None:
                break
            lock_key = 175700000 + item.id
            acquired = db.scalar(text("SELECT pg_try_advisory_lock(:key)"), {"key": lock_key})
            if not acquired:
                db.rollback()
                continue
            try:
                period = item.period
                if item.status == "EN_COLA":
                    item.status = "EN_PROCESO"
                    item.started_at = datetime.now(UTC)
                    db.commit()
                parameter = db.scalar(
                    select(FinancialParameter)
                    .where(FinancialParameter.effective_from <= period)
                    .order_by(FinancialParameter.effective_from.desc())
                )
                if parameter is None:
                    raise RuntimeError("PARAMETRO_NO_CONFIGURADO")
                apartments = db.scalars(
                    select(BillableApartment)
                    .where(BillableApartment.active.is_(True), BillableApartment.id > (item.last_apartment_id or 0))
                    .order_by(BillableApartment.id)
                    .limit(GENERATION_BATCH_SIZE)
                ).all()
                if not apartments:
                    item.status = "COMPLETADA"
                    item.finished_at = datetime.now(UTC)
                    db.commit()
                    processed += 1
                    continue
                ids = [apartment.id for apartment in apartments]
                manual_values: dict[int, Decimal] = {}
                for override in db.scalars(
                    select(ApartmentValue)
                    .where(ApartmentValue.apartment_id.in_(ids), ApartmentValue.effective_from <= period)
                    .order_by(ApartmentValue.apartment_id, ApartmentValue.effective_from.desc())
                ).all():
                    manual_values.setdefault(override.apartment_id, Decimal(override.value))
                values = [
                    (
                        apartment.id,
                        Decimal(apartment.coefficient) if apartment.coefficient is not None else None,
                        manual_values.get(apartment.id),
                        Decimal(parameter.base_value),
                    )
                    for apartment in apartments
                ]
                with ThreadPoolExecutor(max_workers=min(GENERATION_MAX_WORKERS, len(values))) as executor:
                    computed = dict(executor.map(_calculate_generation_value, values))
                existing = set(
                    db.scalars(
                        select(Charge.apartment_id).where(Charge.period == period, Charge.apartment_id.in_(ids))
                    ).all()
                )
                for apartment in apartments:
                    value = computed[apartment.id]
                    if apartment.id in existing:
                        item.skipped += 1
                    elif value is None:
                        item.failed += 1
                    else:
                        db.add(
                            Charge(
                                apartment_id=apartment.id,
                                period=period,
                                value=value,
                                due_at=period + timedelta(days=parameter.due_days),
                                state=ChargeStatus.PENDIENTE,
                            )
                        )
                        enqueue_balance(db, apartment.id)
                        item.generated += 1
                    item.last_apartment_id = apartment.id
                db.commit()
                has_more = (
                    db.scalar(
                        select(BillableApartment.id)
                        .where(BillableApartment.active.is_(True), BillableApartment.id > (item.last_apartment_id or 0))
                        .limit(1)
                    )
                    is not None
                )
                if not has_more:
                    item.status = "COMPLETADA"
                    item.finished_at = datetime.now(UTC)
                    db.commit()
                processed += 1
            except Exception as exc:
                db.rollback()
                failed = db.get(Generation, item.id)
                if failed is not None:
                    failed.status = "FALLIDA"
                    failed.error = type(exc).__name__
                    failed.finished_at = datetime.now(UTC)
                    db.commit()
            finally:
                db.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": lock_key})
                db.commit()
    return processed


def run_due_jobs() -> None:
    now = datetime.now(ZoneInfo("America/Bogota"))
    today = now.date()
    if now.hour == 0 and now.minute >= 5 or now.hour > 0:

        def vencimientos(db: Session) -> None:
            apartment_ids = db.scalars(
                select(Charge.apartment_id).where(Charge.due_at == today - timedelta(days=1)).distinct()
            ).all()
            for apartment_id in apartment_ids:
                enqueue_balance(db, apartment_id)

        scheduled_once("vencimientos", today, 175400000 + today.toordinal(), vencimientos)
    if today.day == 1 and now.hour >= 6:
        scheduled_once(
            "generacion", today, 175500000 + today.year * 100 + today.month, lambda db: enqueue_generation(today, db)
        )
    if today.day == 2 and now.hour >= 6:
        prior = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
        scheduled_once(
            "intereses", prior, 175600000 + prior.year * 100 + prior.month, lambda db: cause_interest(prior, db)
        )


def background_worker() -> None:
    initial_sync = False
    while not stop_workers.is_set():
        try:
            if not initial_sync:
                with SessionLocal() as db:
                    for apartment_id in db.scalars(select(BillableApartment.id)).all():
                        enqueue_balance(db, apartment_id)
                    db.commit()
                initial_sync = True
            run_due_jobs()
            process_generation_jobs()
            publish_outbox()
        except Exception as exc:
            print(f"Billing worker: {type(exc).__name__}", flush=True)
        stop_workers.wait(5)
