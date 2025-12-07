"""
Position restoration for bot-folio position isolation.

When multiple bots share the same Alpaca account, each bot needs to track
only its own positions. This module restores a bot's positions from the
backend database into the Nautilus cache on startup.

IMPORTANT: Position restoration requires instruments to be loaded first.
Call `restore_positions_from_env` from strategy's `on_start()` method,
or use `get_restored_position_qty` to get the bot's position quantity
for a specific instrument.
"""
import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from nautilus_trader.cache.cache import Cache
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import StrategyId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.position import Position

# Global cache of position data from environment (parsed once)
_position_data_cache: list[dict] | None = None


def _log(message: str, logger: Any = None) -> None:
    """Log a message using logger if available, otherwise print."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"{ts} [PositionRestore] {message}"
    if logger is not None:
        try:
            logger.info(message)
        except Exception:
            print(formatted)
    else:
        print(formatted)


def _log_warning(message: str, logger: Any = None) -> None:
    """Log a warning using logger if available, otherwise print."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"{ts} [PositionRestore] WARNING: {message}"
    if logger is not None:
        try:
            logger.warning(message)
        except Exception:
            print(formatted)
    else:
        print(formatted)


def _log_error(message: str, logger: Any = None) -> None:
    """Log an error using logger if available, otherwise print."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    formatted = f"{ts} [PositionRestore] ERROR: {message}"
    if logger is not None:
        try:
            logger.error(message)
        except Exception:
            print(formatted)
    else:
        print(formatted)


def restore_positions_from_env(
    cache: Cache,
    account_id: AccountId,
    trader_id: TraderId,
    strategy_id: StrategyId | None,
    venue: str,
    logger: Any = None,
) -> int:
    """
    Restore bot positions from BOTFOLIO_POSITIONS environment variable.

    This function reads positions serialized by the backend and creates
    Position objects in the Nautilus cache. This ensures the bot sees
    only its own positions, not the aggregate Alpaca account positions.

    Parameters
    ----------
    cache : Cache
        The Nautilus cache to populate with positions.
    account_id : AccountId
        The account ID for the positions.
    trader_id : TraderId
        The trader ID for the positions.
    strategy_id : StrategyId, optional
        The strategy ID for the positions.
    venue : str
        The venue string (e.g., "ALPACA") for instrument IDs.
    logger : Any, optional
        Logger for diagnostic output (uses print if None).

    Returns
    -------
    int
        Number of positions restored.

    """
    positions_json = os.environ.get("BOTFOLIO_POSITIONS", "[]")

    try:
        positions_data = json.loads(positions_json)
    except json.JSONDecodeError:
        _log_warning("Failed to parse BOTFOLIO_POSITIONS JSON", logger)
        return 0

    if not positions_data:
        _log("No positions to restore", logger)
        return 0

    restored_count = 0

    for pos_data in positions_data:
        try:
            symbol = pos_data.get("symbol")
            quantity = Decimal(str(pos_data.get("quantity", 0)))
            avg_price = Decimal(str(pos_data.get("averagePrice", 0)))

            if abs(quantity) < Decimal("0.00000001"):
                continue  # Skip zero positions

            instrument_id = InstrumentId.from_str(f"{symbol}.{venue}")

            # Get the instrument from cache to determine precision
            instrument = cache.instrument(instrument_id)
            if instrument is None:
                _log_warning(f"Instrument {instrument_id} not in cache, skipping position restore", logger)
                continue

            # Determine order side from quantity sign
            order_side = OrderSide.BUY if quantity > 0 else OrderSide.SELL
            abs_quantity = abs(quantity)

            # Use a default strategy ID if none provided
            effective_strategy_id = strategy_id or StrategyId("RESTORE")

            # Create a synthetic fill event to establish the position
            # This mimics how Nautilus creates positions from fills
            fill = OrderFilled(
                trader_id=trader_id,
                strategy_id=effective_strategy_id,
                instrument_id=instrument_id,
                client_order_id=ClientOrderId(f"RESTORE-{symbol}-{UUID4().value[:8]}"),
                venue_order_id=VenueOrderId(f"RESTORE-{UUID4().value[:8]}"),
                account_id=account_id,
                trade_id=TradeId(f"RESTORE-{UUID4().value[:8]}"),
                order_side=order_side,
                order_type=None,  # Not applicable for restored positions
                last_qty=Quantity(abs_quantity, instrument.size_precision),
                last_px=Price(avg_price, instrument.price_precision),
                currency=Currency.from_str("USD"),
                liquidity_side=None,
                event_id=UUID4(),
                ts_event=0,
                ts_init=0,
                reconciliation=True,  # Mark as reconciliation to avoid triggering events
            )

            # Create position from the fill
            position = Position(instrument=instrument, fill=fill)

            # Add to cache with NETTING OMS type (positions are per-instrument-per-strategy)
            cache.add_position(position, OmsType.NETTING)

            _log(f"Restored position: {symbol} qty={quantity} avg_px={avg_price}", logger)

            restored_count += 1

        except Exception as e:
            _log_error(f"Failed to restore position {pos_data}: {e}", logger)
            continue

    _log(f"Position isolation: restored {restored_count} position(s)", logger)

    return restored_count


def _get_position_data() -> list[dict]:
    """Parse and cache position data from environment."""
    global _position_data_cache
    if _position_data_cache is not None:
        return _position_data_cache

    positions_json = os.environ.get("BOTFOLIO_POSITIONS", "[]")
    try:
        _position_data_cache = json.loads(positions_json)
    except json.JSONDecodeError:
        _position_data_cache = []

    return _position_data_cache


def get_restored_position_qty(symbol: str) -> Decimal:
    """
    Get the bot's restored position quantity for a symbol.

    This reads from the BOTFOLIO_POSITIONS environment variable without
    requiring instruments to be loaded. Use this in strategy logic to
    know the bot's starting position before the first trade.

    Parameters
    ----------
    symbol : str
        The symbol to look up (e.g., "BTC/USD").

    Returns
    -------
    Decimal
        The position quantity (positive for long, negative for short, 0 if none).

    Example
    -------
    >>> # In strategy's on_start or on_bar:
    >>> from nautilus_trader.botfolio.position_restore import get_restored_position_qty
    >>> restored_qty = get_restored_position_qty("BTC/USD")
    >>> if restored_qty > 0:
    ...     self.log.info(f"Bot has restored position of {restored_qty} BTC")

    """
    positions = _get_position_data()
    for pos in positions:
        if pos.get("symbol") == symbol:
            return Decimal(str(pos.get("quantity", 0)))
    return Decimal("0")


def get_restored_position_avg_price(symbol: str) -> Decimal:
    """
    Get the bot's restored position average price for a symbol.

    Parameters
    ----------
    symbol : str
        The symbol to look up (e.g., "BTC/USD").

    Returns
    -------
    Decimal
        The average price, or 0 if no position.

    """
    positions = _get_position_data()
    for pos in positions:
        if pos.get("symbol") == symbol:
            return Decimal(str(pos.get("averagePrice", 0)))
    return Decimal("0")

