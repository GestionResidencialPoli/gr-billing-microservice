"""initial financial schema"""

import sqlalchemy as sa
from alembic import op

revision = "0001_finanzas"
down_revision = None
branch_labels = None
depends_on = None

charge_status = sa.Enum("PENDIENTE", "PARCIAL", "PAGADO", name="charge_status")
payment_method = sa.Enum("TRANSFERENCIA", "EFECTIVO", "CONSIGNACION", "OTRO", name="payment_method")


def upgrade():
    op.create_table(
        "financial_parameters",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("base_value", sa.Numeric(15, 2), nullable=False),
        sa.Column("monthly_late_rate", sa.Numeric(8, 5), nullable=False),
        sa.Column("due_days", sa.Integer(), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=False),
    )
    op.create_table(
        "billable_apartments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("torre", sa.String(20), nullable=False),
        sa.Column("numero", sa.String(20), nullable=False),
        sa.Column("coefficient", sa.Numeric(8, 5)),
        sa.Column("active", sa.Boolean(), nullable=False, server_default=sa.true()),
    )
    op.create_unique_constraint("uq_billable_apartment_torre_numero", "billable_apartments", ["torre", "numero"])
    op.create_table(
        "charges",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("apartment_id", sa.Integer(), sa.ForeignKey("billable_apartments.id"), nullable=False),
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("value", sa.Numeric(15, 2), nullable=False),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("due_at", sa.Date(), nullable=False),
        sa.Column("state", charge_status, nullable=False),
    )
    op.create_unique_constraint("uq_charge_apartment_period", "charges", ["apartment_id", "period"])
    op.create_index("ix_charges_due_at", "charges", ["due_at"])
    op.create_table(
        "payments",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("apartment_id", sa.Integer(), sa.ForeignKey("billable_apartments.id"), nullable=False),
        sa.Column("value", sa.Numeric(15, 2), nullable=False),
        sa.Column("paid_at", sa.Date(), nullable=False),
        sa.Column("method", payment_method, nullable=False),
        sa.Column("reference", sa.String(80)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "payment_applications",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("payment_id", sa.Integer(), sa.ForeignKey("payments.id"), nullable=False),
        sa.Column("charge_id", sa.Integer(), sa.ForeignKey("charges.id"), nullable=False),
        sa.Column("value", sa.Numeric(15, 2), nullable=False),
    )
    op.create_index("ix_payment_applications_charge", "payment_applications", ["charge_id"])
    op.create_table(
        "idempotency_keys",
        sa.Column("key", sa.String(80), primary_key=True),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("response_json", sa.Text(), nullable=False),
    )
    op.create_table(
        "generations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("total", sa.Integer(), nullable=False),
        sa.Column("generated", sa.Integer(), nullable=False),
        sa.Column("skipped", sa.Integer(), nullable=False),
        sa.Column("failed", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(30), nullable=False),
    )
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("payload", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True)),
    )
    op.execute(
        "INSERT INTO financial_parameters (base_value, monthly_late_rate, due_days, effective_from) VALUES (300000.00, 0.02500, 10, DATE '2026-01-01')"
    )
    op.execute(
        "INSERT INTO billable_apartments (id, torre, numero, coefficient, active) VALUES (1, 'A', '101', 0.50000, true), (2, 'A', '102', 0.50000, true)"
    )


def downgrade():
    for table in [
        "outbox_events",
        "generations",
        "idempotency_keys",
        "payment_applications",
        "payments",
        "charges",
        "billable_apartments",
        "financial_parameters",
    ]:
        op.drop_table(table)
    payment_method.drop(op.get_bind(), checkfirst=True)
    charge_status.drop(op.get_bind(), checkfirst=True)
