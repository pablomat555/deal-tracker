# deal_tracker/models.py
"""
Централизованное определение структур данных (моделей) для проекта.
Использование dataclasses обеспечивает строгую типизацию и предсказуемость
объектов, передаваемых между различными модулями системы.
"""
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional


@dataclass
class TradeData:
    """Модель для одной торговой операции (из листа Core_Trades)."""
    # Обязательные поля, получаемые при парсинге
    timestamp: datetime
    exchange: str
    symbol: str
    trade_type: str
    amount: Decimal
    price: Decimal
    trade_id: str
    # Опциональные поля
    row_number: Optional[int] = None
    order_id: Optional[str] = None
    total_quote_amount: Optional[Decimal] = None
    commission: Optional[Decimal] = None
    commission_asset: Optional[str] = None
    notes: Optional[str] = None
    trade_pnl: Optional[Decimal] = None
    fifo_consumed_qty: Optional[Decimal] = None
    fifo_sell_processed: Optional[bool] = None
    # Дополнительные поля для гибкости
    strategy: Optional[str] = None
    source: Optional[str] = None
    asset_type: Optional[str] = 'SPOT'
    sl: Optional[Decimal] = None
    tp1: Optional[Decimal] = None
    tp2: Optional[Decimal] = None
    tp3: Optional[Decimal] = None
    risk_usd: Optional[Decimal] = None


@dataclass
class MovementData:
    """Модель для движения средств (из листа Fund_Movements)."""
    timestamp: datetime
    movement_type: str
    asset: str
    amount: Decimal
    # Опциональные поля
    row_number: Optional[int] = None
    movement_id: Optional[str] = None
    source_entity_type: Optional[str] = None
    source_name: Optional[str] = None
    destination_entity_type: Optional[str] = None
    destination_name: Optional[str] = None
    fee_amount: Optional[Decimal] = None
    fee_asset: Optional[str] = None
    transaction_id_blockchain: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class PositionData:
    """Модель для открытой позиции (из листа Open_Positions)."""
    symbol: str
    exchange: str
    net_amount: Decimal
    avg_entry_price: Decimal
    # Опциональные поля
    row_number: Optional[int] = None
    current_price: Optional[Decimal] = None
    unrealized_pnl: Optional[Decimal] = None
    last_updated: Optional[datetime] = None


@dataclass
class BalanceData:
    """Модель для баланса счета (из листа Account_Balances)."""
    account_name: str
    asset: str
    balance: Decimal
    # Опциональные поля
    row_number: Optional[int] = None
    entity_type: Optional[str] = None
    last_updated: Optional[datetime] = None


@dataclass
class FifoLogData:
    """Модель для записи в лог FIFO (из листа Fifo_Log)."""
    symbol: str
    buy_trade_id: str
    sell_trade_id: str
    matched_qty: Decimal
    buy_price: Decimal
    sell_price: Decimal
    fifo_pnl: Decimal
    timestamp_closed: datetime
    # Опциональные поля
    row_number: Optional[int] = None
    buy_timestamp: Optional[datetime] = None
    exchange: Optional[str] = None


@dataclass
class AnalyticsData:
    """Модель для итоговой строки в листе Analytics."""
    date_generated: datetime
    total_realized_pnl: Decimal
    total_unrealized_pnl: Decimal
    net_total_pnl: Decimal
    total_trades_closed: int
    winning_trades_closed: int
    losing_trades_closed: int
    win_rate_percent: Decimal
    average_win_amount: Decimal
    average_loss_amount: Decimal
    profit_factor: str  # Может быть "Infinity"
    expectancy: Decimal
    total_commissions_paid: Decimal
    net_invested_funds: Decimal
    portfolio_current_value: Decimal
    total_equity: Decimal
    notes: Optional[str] = None
