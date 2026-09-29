from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
import csv
import hashlib
import io
import json
import os
from typing import Generator

import jwt
import strawberry
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from sqlalchemy import Date, DateTime, Enum as SqlEnum, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, create_engine, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, sessionmaker
from strawberry.fastapi import GraphQLRouter

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+psycopg://gr_user:gr_password@localhost:5432/gr_billing_db")
JWT_SECRET = os.getenv("JWT_SECRET", "local-development-secret-change-me")

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

class Charge(Base):
    __tablename__ = "charges"
    __table_args__ = (UniqueConstraint("apartment_id", "period", name="uq_charge_apartment_period"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    apartment_id: Mapped[int] = mapped_column(ForeignKey("billable_apartments.id"))
    period: Mapped[date] = mapped_column(Date)
    value: Mapped[Decimal] = mapped_column(Numeric(15, 2))
    issued_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    due_at: Mapped[date] = mapped_column(Date)
    state: Mapped[ChargeStatus] = mapped_column(SqlEnum(ChargeStatus, name="charge_status"), default=ChargeStatus.PENDIENTE)

class Payment(Base):
    __tablename__ = "payments"
    id: Mapped[int] = mapped_column(primary_key=True)
    apartment_id: Mapped[int] = mapped_column(ForeignKey("billable_apartments.id"))
    value: Mapped[Decimal] = mapped_column(Numeric(15, 2))
    paid_at: Mapped[date] = mapped_column(Date)
    method: Mapped[PaymentMethod] = mapped_column(SqlEnum(PaymentMethod, name="payment_method"))
    reference: Mapped[str | None] = mapped_column(String(80), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))

class PaymentApplication(Base):
    __tablename__ = "payment_applications"
    id: Mapped[int] = mapped_column(primary_key=True)
    payment_id: Mapped[int] = mapped_column(ForeignKey("payments.id"))
    charge_id: Mapped[int] = mapped_column(ForeignKey("charges.id"))
    value: Mapped[Decimal] = mapped_column(Numeric(15, 2))

class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"
    key: Mapped[str] = mapped_column(String(80), primary_key=True)
    request_hash: Mapped[str] = mapped_column(String(64))
    response_json: Mapped[str] = mapped_column(Text)

class Generation(Base):
    __tablename__ = "generations"
    id: Mapped[int] = mapped_column(primary_key=True)
    period: Mapped[date] = mapped_column(Date)
    total: Mapped[int] = mapped_column(default=0)
    generated: Mapped[int] = mapped_column(default=0)
    skipped: Mapped[int] = mapped_column(default=0)
    failed: Mapped[int] = mapped_column(default=0)
    status: Mapped[str] = mapped_column(String(30), default="COMPLETADA")

class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    id: Mapped[int] = mapped_column(primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100))
    payload: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(UTC))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

def get_db() -> Generator[Session, None, None]:
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

class ParameterInput(BaseModel):
    base_value: Decimal = Field(gt=0)
    monthly_late_rate: Decimal = Field(ge=0)
    due_days: int = Field(ge=0, le=90)
    effective_from: date

class PaymentInput(BaseModel):
    apartment_id: int = Field(gt=0)
    value: Decimal = Field(gt=0)
    paid_at: date
    method: PaymentMethod
    reference: str | None = Field(default=None, max_length=80)

def money(value: Decimal) -> str:
    return str(Decimal(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def month_start(value: str) -> date:
    try:
        return date.fromisoformat(f"{value}-01")
    except ValueError as exc:
        raise HTTPException(422, detail={"code": "PERIODO_INVALIDO", "message": "El período debe ser YYYY-MM"}) from exc

def charge_pending(db: Session, charge: Charge) -> Decimal:
    applied = db.scalar(select(func.coalesce(func.sum(PaymentApplication.value), 0)).where(PaymentApplication.charge_id == charge.id)) or Decimal(0)
    return Decimal(charge.value) - Decimal(applied)

app = FastAPI(title="GR Billing Microservice", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:3007,http://localhost:3001").split(","), allow_credentials=True, allow_methods=["GET", "POST", "PUT"], allow_headers=["Content-Type", "X-XSRF-TOKEN", "Idempotency-Key"])

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
    return {"payload": [{"id": x.id, "baseValue": money(x.base_value), "monthlyLateRate": str(x.monthly_late_rate), "dueDays": x.due_days, "effectiveFrom": x.effective_from.isoformat()} for x in items]}

@app.put("/api/v1/finanzas/parametros")
def set_parameters(payload: ParameterInput, request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    item = FinancialParameter(**payload.model_dump())
    db.add(item)
    db.commit()
    return {"payload": {"id": item.id, "baseValue": money(item.base_value)}}

@app.post("/api/v1/finanzas/cobros/generar", status_code=202)
def generate_charges(periodo: str, request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    period = month_start(periodo)
    if period > date.today().replace(day=1):
        raise HTTPException(422, detail={"code": "PERIODO_FUTURO", "message": "No se puede generar un período futuro"})
    parameter = db.scalar(select(FinancialParameter).where(FinancialParameter.effective_from <= period).order_by(FinancialParameter.effective_from.desc()))
    if parameter is None:
        raise HTTPException(422, detail={"code": "PARAMETRO_NO_CONFIGURADO", "message": "No hay parámetros vigentes"})
    apartments = db.scalars(select(BillableApartment).where(BillableApartment.active.is_(True))).all()
    generation = Generation(period=period, total=len(apartments), status="EN_PROCESO")
    db.add(generation)
    db.flush()
    for apartment in apartments:
        if apartment.coefficient is None:
            generation.failed += 1
            continue
        value = (Decimal(parameter.base_value) * Decimal(apartment.coefficient)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        existing = db.scalar(select(Charge).where(Charge.apartment_id == apartment.id, Charge.period == period))
        if existing:
            generation.skipped += 1
            continue
        db.add(Charge(apartment_id=apartment.id, period=period, value=value, due_at=period + timedelta(days=parameter.due_days), state=ChargeStatus.PENDIENTE))
        generation.generated += 1
    generation.status = "COMPLETADA"
    db.add(OutboxEvent(event_type="finanzas.cobros_generados", payload=json.dumps({"generacionId": generation.id, "periodo": periodo})))
    db.commit()
    return {"payload": {"generacionId": generation.id, "status": generation.status}}

@app.get("/api/v1/finanzas/cobros/generaciones/{generation_id}")
def generation(generation_id: int, request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    item = db.get(Generation, generation_id)
    if not item:
        raise HTTPException(404, detail={"code": "GENERACION_NO_ENCONTRADA", "message": "Generación no encontrada"})
    return {"payload": {"id": item.id, "period": item.period.isoformat(), "total": item.total, "generated": item.generated, "skipped": item.skipped, "failed": item.failed, "status": item.status}}

@app.post("/api/v1/finanzas/pagos")
def create_payment(payload: PaymentInput, request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    key = request.headers.get("Idempotency-Key")
    if not key:
        raise HTTPException(400, detail={"code": "IDEMPOTENCY_KEY_REQUERIDA", "message": "Idempotency-Key es obligatorio"})
    request_json = json.dumps(payload.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    request_hash = hashlib.sha256(request_json.encode()).hexdigest()
    existing_key = db.get(IdempotencyKey, key)
    if existing_key:
        if existing_key.request_hash != request_hash:
            raise HTTPException(409, detail={"code": "IDEMPOTENCY_KEY_REUTILIZADA", "message": "La clave ya fue usada con otra solicitud"})
        return Response(content=existing_key.response_json, media_type="application/json")
    apartment = db.get(BillableApartment, payload.apartment_id)
    if not apartment:
        raise HTTPException(404, detail={"code": "APARTAMENTO_NO_ENCONTRADO", "message": "Apartamento no encontrado"})
    payment = Payment(**payload.model_dump())
    db.add(payment)
    db.flush()
    remaining = payload.value
    charges = db.scalars(select(Charge).where(Charge.apartment_id == payload.apartment_id).order_by(Charge.period.asc())).all()
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
    body = {"payload": {"id": payment.id, "value": money(payment.value), "saldoAFavor": money(remaining)}}
    serialized = json.dumps(body)
    db.add(IdempotencyKey(key=key, request_hash=request_hash, response_json=serialized))
    db.add(OutboxEvent(event_type="finanzas.pago_registrado", payload=json.dumps({"pagoId": payment.id, "apartamentoId": payment.apartment_id, "valor": money(payment.value)})))
    db.commit()
    return Response(content=serialized, media_type="application/json", status_code=201)

@app.get("/api/v1/finanzas/cartera")
def portfolio(request: Request, fecha_corte: date | None = Query(default=None, alias="fechaCorte"), db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    cutoff = fecha_corte or date.today()
    rows = []
    for apartment in db.scalars(select(BillableApartment).order_by(BillableApartment.torre, BillableApartment.numero)).all():
        charges = db.scalars(select(Charge).where(Charge.apartment_id == apartment.id, Charge.period <= cutoff)).all()
        pending = sum((charge_pending(db, c) for c in charges), Decimal(0))
        overdue = sum((charge_pending(db, c) for c in charges if c.due_at < cutoff), Decimal(0))
        rows.append({"apartamentoId": apartment.id, "torre": apartment.torre, "numero": apartment.numero, "pendiente": money(pending), "vencido": money(overdue)})
    return {"payload": {"fechaCorte": cutoff.isoformat(), "apartamentos": rows, "totalPendiente": money(sum((Decimal(row["pendiente"]) for row in rows), Decimal(0)))}}

@app.get("/api/v1/finanzas/reportes/cartera.csv")
def portfolio_csv(request: Request, db: Session = Depends(get_db)):
    claims_for(request, "ADMINISTRACION")
    rows = portfolio(request, db=db)["payload"]["apartamentos"]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=["apartamentoId", "torre", "numero", "pendiente", "vencido"])
    writer.writeheader()
    writer.writerows(rows)
    return Response(output.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=cartera.csv"})

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
        cutoff = date.fromisoformat(fecha_corte) if fecha_corte else date.today()
        rows = []
        for apartment in db.scalars(select(BillableApartment).order_by(BillableApartment.torre, BillableApartment.numero)).all():
            charges = db.scalars(select(Charge).where(Charge.apartment_id == apartment.id, Charge.period <= cutoff)).all()
            pending = sum((charge_pending(db, c) for c in charges), Decimal(0))
            overdue = sum((charge_pending(db, c) for c in charges if c.due_at < cutoff), Decimal(0))
            rows.append(PortfolioApartment(apartamento_id=apartment.id, torre=apartment.torre, numero=apartment.numero, pendiente=money(pending), vencido=money(overdue)))
        return PortfolioResult(fecha_corte=cutoff.isoformat(), total_pendiente=money(sum((Decimal(row.pendiente) for row in rows), Decimal(0))), apartamentos=rows)

async def graphql_context(request: Request):
    claims_for(request, "ADMINISTRACION")
    db = SessionLocal()
    return {"request": request, "db": db}

app.include_router(GraphQLRouter(strawberry.Schema(query=QueryType), context_getter=graphql_context), prefix="/api/v1/finanzas/graphql")
