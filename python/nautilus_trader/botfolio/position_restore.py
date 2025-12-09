"""
Position restoration for bot-folio position isolation.

When multiple bots share the same Alpaca account, each bot needs to track
only its own positions. This module restores a bot's positions from the
backend database into the Nautilus cache on startup.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

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

if TYPE_CHECKING:
    from nautilus_trader.trading.strategy import Strategy


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

            # Use bot_id-based strategy ID if none provided (stable across deploys)
            if strategy_id:
                effective_strategy_id = strategy_id
            else:
                bot_id = os.environ.get("BOTFOLIO_BOT_ID", "")
                effective_strategy_id = StrategyId(bot_id) if bot_id else StrategyId("RESTORE")

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


def _get_position_strategy_id(strategy: Strategy) -> StrategyId:
    """Get the strategy ID to use for position isolation."""
    bot_id = os.environ.get("BOTFOLIO_BOT_ID", "")
    if not bot_id:
        strategy.log.warning("Position restore: BOTFOLIO_BOT_ID not set, using strategy.id")
        return strategy.id
    return StrategyId(bot_id)


def _get_account_for_restore(
    strategy: Strategy,
    instrument_ids: list[InstrumentId] | None,
) -> Any | None:
    """Get account from portfolio for position restore."""
    account = strategy.portfolio.account(strategy.portfolio.default_venue())
    if account is None:
        for iid in instrument_ids or []:
            account = strategy.portfolio.account(iid.venue)
            if account:
                break
    return account


def _restore_single_position(
    strategy: Strategy,
    pos_data: dict[str, Any],
    venue: str,
    position_strategy_id: StrategyId,
    account: Any,
) -> bool:
    """Restore a single position. Returns True if restored successfully."""
    symbol = pos_data.get("symbol")
    quantity = Decimal(str(pos_data.get("quantity", 0)))
    avg_price = Decimal(str(pos_data.get("averagePrice", 0)))

    if abs(quantity) < Decimal("0.00000001"):
        return False  # Skip zero positions

    instrument_id = InstrumentId.from_str(f"{symbol}.{venue}")
    cache = strategy.cache

    # Check if position already exists
    if cache.positions_open(instrument_id=instrument_id):
        strategy.log.info(f"Position restore: Position already exists for {instrument_id}, skipping")
        return False

    instrument = cache.instrument(instrument_id)
    if instrument is None:
        strategy.log.warning(f"Position restore: Instrument {instrument_id} not in cache, skipping")
        return False

    # Create synthetic fill to establish position
    order_side = OrderSide.BUY if quantity > 0 else OrderSide.SELL
    fill = OrderFilled(
        trader_id=strategy.trader_id,
        strategy_id=position_strategy_id,
        instrument_id=instrument_id,
        client_order_id=ClientOrderId(f"RESTORE-{symbol}-{UUID4().value[:8]}"),
        venue_order_id=VenueOrderId(f"RESTORE-{UUID4().value[:8]}"),
        account_id=account.id,
        trade_id=TradeId(f"RESTORE-{UUID4().value[:8]}"),
        order_side=order_side,
        order_type=None,
        last_qty=Quantity(abs(quantity), instrument.size_precision),
        last_px=Price(avg_price, instrument.price_precision),
        currency=Currency.from_str("USD"),
        liquidity_side=None,
        event_id=UUID4(),
        ts_event=0,
        ts_init=0,
        reconciliation=True,
    )

    position = Position(instrument=instrument, fill=fill)
    cache.add_position(position, OmsType.NETTING)

    strategy.log.info(
        f"Position restore: Restored {symbol} qty={quantity} avg_px={avg_price} "
        f"(strategy_id={position_strategy_id})",
    )
    return True


def restore_positions_for_strategy(
    strategy: Strategy,
    venue: str = "ALPACA",
    instrument_ids: list[InstrumentId] | None = None,
) -> int:
    """
    Restore bot positions for a strategy from BOTFOLIO_POSITIONS.

    This is the recommended way to restore positions. Call this from your
    strategy's on_start() method after the account is connected.

    Position isolation uses the bot_id (from BOTFOLIO_BOT_ID env var) rather
    than the Nautilus strategy_id. This ensures positions persist correctly
    even if the strategy class name changes between deploys.

    Parameters
    ----------
    strategy : Strategy
        The strategy instance to restore positions for.
    venue : str, default "ALPACA"
        The venue string for instrument IDs.
    instrument_ids : list[InstrumentId], optional
        If provided, only restore positions for these instruments.
        If None, restores all positions from BOTFOLIO_POSITIONS.

    Returns
    -------
    int
        Number of positions restored.

    Example
    -------
    ```python
    from nautilus_trader.botfolio.position_restore import restore_positions_for_strategy

    class MyStrategy(Strategy):
        def __init__(self, config):
            super().__init__(config)
            self._positions_restored = False

        def on_start(self):
            self.subscribe_bars(self.bar_type)

        def on_bar(self, bar):
            # Restore positions once account is connected
            if not self._positions_restored:
                account = self.portfolio.account(self.instrument_id.venue)
                if account is None:
                    return  # Wait for account
                restore_positions_for_strategy(self, instrument_ids=[self.instrument_id])
                self._positions_restored = True

            # ... rest of strategy logic
    ```

    """
    positions_json = os.environ.get("BOTFOLIO_POSITIONS", "[]")

    try:
        positions_data = json.loads(positions_json)
    except json.JSONDecodeError:
        strategy.log.warning("Failed to parse BOTFOLIO_POSITIONS JSON")
        return 0

    if not positions_data:
        strategy.log.info("Position restore: No positions to restore")
        return 0

    position_strategy_id = _get_position_strategy_id(strategy)
    strategy.log.info(f"Position restore: Using strategy_id={position_strategy_id}")

    account = _get_account_for_restore(strategy, instrument_ids)
    if account is None:
        strategy.log.warning("Position restore: No account found, cannot restore positions")
        return 0

    # Build set of instrument symbols to filter by (if specified)
    filter_symbols: set[str] | None = None
    if instrument_ids:
        filter_symbols = {str(iid).split(".")[0] for iid in instrument_ids}

    restored_count = 0
    for pos_data in positions_data:
        symbol = pos_data.get("symbol")
        if filter_symbols and symbol not in filter_symbols:
            continue
        try:
            if _restore_single_position(strategy, pos_data, venue, position_strategy_id, account):
                restored_count += 1
        except Exception as e:
            strategy.log.error(f"Position restore: Failed for {pos_data}: {e}")

    strategy.log.info(f"Position restore: Restored {restored_count} position(s)")
    return restored_count

