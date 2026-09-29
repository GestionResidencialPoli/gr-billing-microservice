"""track queued monthly billing generation progress"""

import sqlalchemy as sa
from alembic import op

revision = "0003_generacion_async"
down_revision = "0002_finanzas_completa"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("generations", sa.Column("last_apartment_id", sa.Integer(), nullable=True))
    op.add_column("generations", sa.Column("error", sa.String(500), nullable=True))


def downgrade():
    op.drop_column("generations", "error")
    op.drop_column("generations", "last_apartment_id")
