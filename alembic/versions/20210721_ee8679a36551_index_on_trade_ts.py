"""index on trade.ts

Revision ID: ee8679a36551
Revises: 38da51f64872
Create Date: 2021-07-21 08:08:22.031814

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'ee8679a36551'
down_revision = '38da51f64872'
branch_labels = None
depends_on = None


def upgrade():
    op.execute('create index ix_trade_exchange_market_id_dts on trade(exchange_market_id, to_timestamp(ts::numeric / 1000::numeric))')


def downgrade():
    op.drop_index('ix_trade_exchange_market_id_dts', table_name='trade1m')
