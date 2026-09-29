"""extend the existing financial schema without replacing local data"""

import sqlalchemy as sa
from alembic import op

revision = "0002_finanzas_completa"
down_revision = "0001_finanzas"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("billable_apartments", sa.Column("synced_at", sa.DateTime(timezone=True)))
    op.create_table(
        "apartment_values",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("apartment_id", sa.Integer(), sa.ForeignKey("billable_apartments.id"), nullable=False),
        sa.Column("value", sa.Numeric(15, 2), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("reason", sa.String(500), nullable=False),
        sa.Column("created_by_user_id", sa.Integer(), nullable=False),
        sa.UniqueConstraint("apartment_id", "effective_from", name="uq_apartment_value_period"),
    )
    op.create_table(
        "late_interests",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("charge_id", sa.Integer(), sa.ForeignKey("charges.id"), nullable=False),
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("base_capital", sa.Numeric(15, 2), nullable=False),
        sa.Column("rate", sa.Numeric(8, 5), nullable=False),
        sa.Column("value", sa.Numeric(15, 2), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("charge_id", "period", name="uq_late_interest_charge_period"),
    )
    op.alter_column("payment_applications", "charge_id", nullable=True)
    op.add_column("payment_applications", sa.Column("interest_id", sa.Integer(), sa.ForeignKey("late_interests.id")))
    op.create_index("ix_payment_applications_interest", "payment_applications", ["interest_id"])
    op.create_table(
        "proofs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("apartment_id", sa.Integer(), sa.ForeignKey("billable_apartments.id"), nullable=False),
        sa.Column("uploaded_by_user_id", sa.Integer(), nullable=False),
        sa.Column("declared_value", sa.Numeric(15, 2), nullable=False),
        sa.Column("transfer_date", sa.Date(), nullable=False),
        sa.Column("bank", sa.String(100), nullable=False),
        sa.Column("reference", sa.String(100), nullable=False),
        sa.Column("mime_type", sa.String(40), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False, unique=True),
        sa.Column("state", sa.String(20), nullable=False),
        sa.Column("findings", sa.Text(), nullable=False),
        sa.Column("reviewed_by_user_id", sa.Integer()),
        sa.Column("rejection_reason", sa.String(500)),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("bank", "reference", name="uq_proof_bank_reference"),
    )
    op.create_table(
        "proof_files",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("proof_id", sa.Integer(), sa.ForeignKey("proofs.id"), nullable=False, unique=True),
        sa.Column("content", sa.LargeBinary(), nullable=False),
    )
    op.create_table(
        "receipts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("charge_id", sa.Integer(), sa.ForeignKey("charges.id"), nullable=False, unique=True),
        sa.Column("number", sa.Integer(), nullable=False, unique=True),
        sa.Column("issued_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "scheduled_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("task", sa.String(50), nullable=False),
        sa.Column("period", sa.Date(), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("task", "period", name="uq_scheduled_task_period"),
    )
    op.add_column("payments", sa.Column("origin", sa.String(20), nullable=False, server_default="MANUAL"))
    op.add_column("payments", sa.Column("proof_id", sa.Integer(), sa.ForeignKey("proofs.id")))
    op.add_column("payments", sa.Column("registered_by_user_id", sa.Integer()))
    op.add_column("payments", sa.Column("reversal_of_payment_id", sa.Integer(), sa.ForeignKey("payments.id")))
    op.add_column("payments", sa.Column("reversal_reason", sa.String(500)))
    op.create_unique_constraint("uq_payments_reversal", "payments", ["reversal_of_payment_id"])
    op.add_column("idempotency_keys", sa.Column("user_id", sa.Integer()))
    op.add_column("idempotency_keys", sa.Column("route", sa.String(100)))
    op.add_column("idempotency_keys", sa.Column("status_code", sa.Integer(), nullable=False, server_default="201"))
    op.add_column("generations", sa.Column("started_at", sa.DateTime(timezone=True)))
    op.add_column("generations", sa.Column("finished_at", sa.DateTime(timezone=True)))


def downgrade():
    op.drop_column("generations", "finished_at")
    op.drop_column("generations", "started_at")
    op.drop_column("idempotency_keys", "status_code")
    op.drop_column("idempotency_keys", "route")
    op.drop_column("idempotency_keys", "user_id")
    op.drop_constraint("uq_payments_reversal", "payments")
    for column in ["reversal_reason", "reversal_of_payment_id", "registered_by_user_id", "proof_id", "origin"]:
        op.drop_column("payments", column)
    op.drop_table("receipts")
    op.drop_table("scheduled_runs")
    op.drop_table("proof_files")
    op.drop_table("proofs")
    op.drop_index("ix_payment_applications_interest", table_name="payment_applications")
    op.drop_column("payment_applications", "interest_id")
    op.alter_column("payment_applications", "charge_id", nullable=False)
    op.drop_table("late_interests")
    op.drop_table("apartment_values")
    op.drop_column("billable_apartments", "synced_at")
