// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! A common in-memory `MessageBus` supporting multiple messaging patterns:
//!
//! - Point-to-Point
//! - Pub/Sub
//! - Request/Response

pub mod core;
pub mod database;
pub mod handler;
pub mod matching;
pub mod message;
pub mod mstr;
pub mod stubs;
pub mod switchboard;
pub mod typed_endpoints;
pub mod typed_handler;
pub mod typed_router;

pub use core::{MessageBus, Subscription};
use std::{
    any::Any,
    cell::{OnceCell, RefCell},
    rc::Rc,
    thread::LocalKey,
};

use handler::ShareableMessageHandler;
use matching::is_matching_backtracking;
pub use mstr::{Endpoint, MStr, Pattern, Topic};
use nautilus_core::UUID4;
#[cfg(feature = "defi")]
use nautilus_model::defi::{Block, Pool, PoolFeeCollect, PoolFlash, PoolLiquidityUpdate, PoolSwap};
use nautilus_model::{
    data::{
        Bar, FundingRateUpdate, GreeksData, IndexPriceUpdate, MarkPriceUpdate, OrderBookDeltas,
        OrderBookDepth10, QuoteTick, TradeTick,
    },
    events::{AccountState, OrderEventAny, PositionEvent},
    orderbook::OrderBook,
    orders::OrderAny,
    position::Position,
};
use smallvec::SmallVec;
pub use typed_endpoints::EndpointMap;
pub use typed_handler::{CallbackHandler, Handler, TypedHandler};
pub use typed_router::{TopicRouter, TypedSubscription};

use crate::messages::data::DataResponse;
pub use crate::msgbus::message::BusMessage;

/// Inline capacity for handler buffers before heap allocation.
const HANDLER_BUFFER_CAP: usize = 64;

// MessageBus is designed for single-threaded use within each async runtime.
// Thread-local storage ensures each thread gets its own instance, eliminating
// the need for unsafe Send/Sync implementations.
//
// Handler buffers provide zero-allocation publish on hot paths.
// Each buffer stores up to 64 handlers inline before spilling to heap.
// Publish functions use move-out/move-back to avoid holding RefCell borrows
// during handler calls (enabling re-entrant publishes).
thread_local! {
    static MESSAGE_BUS: OnceCell<Rc<RefCell<MessageBus>>> = const { OnceCell::new() };

    static ANY_HANDLERS: RefCell<SmallVec<[ShareableMessageHandler; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());

    static DELTAS_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderBookDeltas>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static DEPTH10_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderBookDepth10>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static BOOK_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderBook>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static QUOTE_HANDLERS: RefCell<SmallVec<[TypedHandler<QuoteTick>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static TRADE_HANDLERS: RefCell<SmallVec<[TypedHandler<TradeTick>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static BAR_HANDLERS: RefCell<SmallVec<[TypedHandler<Bar>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static MARK_PRICE_HANDLERS: RefCell<SmallVec<[TypedHandler<MarkPriceUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static INDEX_PRICE_HANDLERS: RefCell<SmallVec<[TypedHandler<IndexPriceUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static FUNDING_RATE_HANDLERS: RefCell<SmallVec<[TypedHandler<FundingRateUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static GREEKS_HANDLERS: RefCell<SmallVec<[TypedHandler<GreeksData>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static ACCOUNT_STATE_HANDLERS: RefCell<SmallVec<[TypedHandler<AccountState>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static ORDER_EVENT_HANDLERS: RefCell<SmallVec<[TypedHandler<OrderEventAny>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    static POSITION_EVENT_HANDLERS: RefCell<SmallVec<[TypedHandler<PositionEvent>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());

    #[cfg(feature = "defi")]
    static DEFI_BLOCK_HANDLERS: RefCell<SmallVec<[TypedHandler<Block>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    static DEFI_POOL_HANDLERS: RefCell<SmallVec<[TypedHandler<Pool>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    static DEFI_SWAP_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolSwap>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    static DEFI_LIQUIDITY_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolLiquidityUpdate>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    static DEFI_COLLECT_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolFeeCollect>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
    #[cfg(feature = "defi")]
    static DEFI_FLASH_HANDLERS: RefCell<SmallVec<[TypedHandler<PoolFlash>; HANDLER_BUFFER_CAP]>> =
        RefCell::new(SmallVec::new());
}

/// Sets the thread-local message bus.
///
/// # Panics
///
/// Panics if a message bus has already been set for this thread.
pub fn set_message_bus(msgbus: Rc<RefCell<MessageBus>>) {
    MESSAGE_BUS.with(|bus| {
        assert!(
            bus.set(msgbus).is_ok(),
            "Failed to set MessageBus: already initialized for this thread"
        );
    });
}

/// Gets the thread-local message bus.
///
/// If no message bus has been set for this thread, a default one is created and initialized.
pub fn get_message_bus() -> Rc<RefCell<MessageBus>> {
    MESSAGE_BUS.with(|bus| {
        bus.get_or_init(|| {
            let msgbus = MessageBus::default();
            Rc::new(RefCell::new(msgbus))
        })
        .clone()
    })
}

/// Registers a handler for an endpoint using runtime type dispatch (Any).
pub fn register_any(endpoint: MStr<Endpoint>, handler: ShareableMessageHandler) {
    log::debug!(
        "Registering endpoint '{endpoint}' with handler ID {}",
        handler.0.id(),
    );
    get_message_bus()
        .borrow_mut()
        .endpoints
        .insert(endpoint, handler);
}

/// Registers a response handler for a correlation ID.
pub fn register_response_handler(correlation_id: &UUID4, handler: ShareableMessageHandler) {
    if let Err(e) = get_message_bus()
        .borrow_mut()
        .register_response_handler(correlation_id, handler)
    {
        log::error!("Failed to register request handler: {e}");
    }
}

/// Registers a quote tick handler at an endpoint.
pub fn register_quote_endpoint(endpoint: MStr<Endpoint>, handler: TypedHandler<QuoteTick>) {
    get_message_bus()
        .borrow_mut()
        .endpoints_quotes
        .register(endpoint, handler);
}

/// Registers a trade tick handler at an endpoint.
pub fn register_trade_endpoint(endpoint: MStr<Endpoint>, handler: TypedHandler<TradeTick>) {
    get_message_bus()
        .borrow_mut()
        .endpoints_trades
        .register(endpoint, handler);
}

/// Registers a bar handler at an endpoint.
pub fn register_bar_endpoint(endpoint: MStr<Endpoint>, handler: TypedHandler<Bar>) {
    get_message_bus()
        .borrow_mut()
        .endpoints_bars
        .register(endpoint, handler);
}

/// Registers an order event handler at an endpoint.
pub fn register_order_event_endpoint(
    endpoint: MStr<Endpoint>,
    handler: TypedHandler<OrderEventAny>,
) {
    get_message_bus()
        .borrow_mut()
        .endpoints_order_events
        .register(endpoint, handler);
}

/// Registers an account state handler at an endpoint.
pub fn register_account_state_endpoint(
    endpoint: MStr<Endpoint>,
    handler: TypedHandler<AccountState>,
) {
    get_message_bus()
        .borrow_mut()
        .endpoints_account_state
        .register(endpoint, handler);
}

/// Deregisters the handler for an endpoint (Any-based).
pub fn deregister_any(endpoint: MStr<Endpoint>) {
    log::debug!("Deregistering endpoint '{endpoint}'");
    get_message_bus()
        .borrow_mut()
        .endpoints
        .shift_remove(&endpoint);
}

/// Subscribes a handler to a pattern using runtime type dispatch (Any).
///
/// # Warnings
///
/// Assigning priority handling is an advanced feature which *shouldn't
/// normally be needed by most users*. **Only assign a higher priority to the
/// subscription if you are certain of what you're doing**. If an inappropriate
/// priority is assigned then the handler may receive messages before core
/// system components have been able to process necessary calculations and
/// produce potential side effects for logically sound behavior.
pub fn subscribe_any(
    pattern: MStr<Pattern>,
    handler: ShareableMessageHandler,
    priority: Option<u8>,
) {
    let msgbus = get_message_bus();
    let mut msgbus_ref_mut = msgbus.borrow_mut();
    let sub = Subscription::new(pattern, handler, priority);

    log::debug!(
        "Subscribing {:?} for pattern '{}'",
        sub.handler,
        sub.pattern
    );

    if msgbus_ref_mut.subscriptions.contains(&sub) {
        log::warn!("{sub:?} already exists");
        return;
    }

    for (topic, subs) in &mut msgbus_ref_mut.topics {
        if is_matching_backtracking(*topic, sub.pattern) {
            subs.push(sub.clone());
            subs.sort();
            log::debug!("Added subscription for '{topic}'");
        }
    }

    msgbus_ref_mut.subscriptions.insert(sub);
}

/// Subscribes a handler to instrument messages matching a pattern.
pub fn subscribe_instruments(
    pattern: MStr<Pattern>,
    handler: ShareableMessageHandler,
    priority: Option<u8>,
) {
    subscribe_any(pattern, handler, priority);
}

/// Subscribes a handler to instrument close messages matching a pattern.
pub fn subscribe_instrument_close(
    pattern: MStr<Pattern>,
    handler: ShareableMessageHandler,
    priority: Option<u8>,
) {
    subscribe_any(pattern, handler, priority);
}

/// Subscribes a handler to order book deltas matching a pattern.
pub fn subscribe_book_deltas(
    pattern: MStr<Pattern>,
    handler: TypedHandler<OrderBookDeltas>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_deltas
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to order book depth10 snapshots matching a pattern.
pub fn subscribe_book_depth10(
    pattern: MStr<Pattern>,
    handler: TypedHandler<OrderBookDepth10>,
    priority: Option<u8>,
) {
    get_message_bus().borrow_mut().router_depth10.subscribe(
        pattern,
        handler,
        priority.unwrap_or(0),
    );
}

/// Subscribes a handler to order book snapshots matching a pattern.
pub fn subscribe_book_snapshots(
    pattern: MStr<Pattern>,
    handler: TypedHandler<OrderBook>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_book_snapshots
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to quote ticks matching a pattern.
pub fn subscribe_quotes(
    pattern: MStr<Pattern>,
    handler: TypedHandler<QuoteTick>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_quotes
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to trade ticks matching a pattern.
pub fn subscribe_trades(
    pattern: MStr<Pattern>,
    handler: TypedHandler<TradeTick>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_trades
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to bars matching a pattern.
pub fn subscribe_bars(pattern: MStr<Pattern>, handler: TypedHandler<Bar>, priority: Option<u8>) {
    get_message_bus()
        .borrow_mut()
        .router_bars
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to mark price updates matching a pattern.
pub fn subscribe_mark_prices(
    pattern: MStr<Pattern>,
    handler: TypedHandler<MarkPriceUpdate>,
    priority: Option<u8>,
) {
    get_message_bus().borrow_mut().router_mark_prices.subscribe(
        pattern,
        handler,
        priority.unwrap_or(0),
    );
}

/// Subscribes a handler to index price updates matching a pattern.
pub fn subscribe_index_prices(
    pattern: MStr<Pattern>,
    handler: TypedHandler<IndexPriceUpdate>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_index_prices
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to funding rate updates matching a pattern.
pub fn subscribe_funding_rates(
    pattern: MStr<Pattern>,
    handler: TypedHandler<FundingRateUpdate>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_funding_rates
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to greeks data matching a pattern.
pub fn subscribe_greeks(
    pattern: MStr<Pattern>,
    handler: TypedHandler<GreeksData>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_greeks
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to order events matching a pattern.
pub fn subscribe_order_events(
    pattern: MStr<Pattern>,
    handler: TypedHandler<OrderEventAny>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_order_events
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to position events matching a pattern.
pub fn subscribe_position_events(
    pattern: MStr<Pattern>,
    handler: TypedHandler<PositionEvent>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_position_events
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to account state updates matching a pattern.
pub fn subscribe_account_state(
    pattern: MStr<Pattern>,
    handler: TypedHandler<AccountState>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_account_state
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to positions matching a pattern.
pub fn subscribe_positions(
    pattern: MStr<Pattern>,
    handler: TypedHandler<Position>,
    priority: Option<u8>,
) {
    get_message_bus().borrow_mut().router_positions.subscribe(
        pattern,
        handler,
        priority.unwrap_or(0),
    );
}

/// Subscribes a handler to DeFi blocks matching a pattern.
#[cfg(feature = "defi")]
pub fn subscribe_defi_blocks(
    pattern: MStr<Pattern>,
    handler: TypedHandler<Block>,
    priority: Option<u8>,
) {
    get_message_bus().borrow_mut().router_defi_blocks.subscribe(
        pattern,
        handler,
        priority.unwrap_or(0),
    );
}

/// Subscribes a handler to DeFi pools matching a pattern.
#[cfg(feature = "defi")]
pub fn subscribe_defi_pools(
    pattern: MStr<Pattern>,
    handler: TypedHandler<Pool>,
    priority: Option<u8>,
) {
    get_message_bus().borrow_mut().router_defi_pools.subscribe(
        pattern,
        handler,
        priority.unwrap_or(0),
    );
}

/// Subscribes a handler to DeFi pool swaps matching a pattern.
#[cfg(feature = "defi")]
pub fn subscribe_defi_swaps(
    pattern: MStr<Pattern>,
    handler: TypedHandler<PoolSwap>,
    priority: Option<u8>,
) {
    get_message_bus().borrow_mut().router_defi_swaps.subscribe(
        pattern,
        handler,
        priority.unwrap_or(0),
    );
}

/// Subscribes a handler to DeFi liquidity updates matching a pattern.
#[cfg(feature = "defi")]
pub fn subscribe_defi_liquidity(
    pattern: MStr<Pattern>,
    handler: TypedHandler<PoolLiquidityUpdate>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_defi_liquidity
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to DeFi fee collects matching a pattern.
#[cfg(feature = "defi")]
pub fn subscribe_defi_collects(
    pattern: MStr<Pattern>,
    handler: TypedHandler<PoolFeeCollect>,
    priority: Option<u8>,
) {
    get_message_bus()
        .borrow_mut()
        .router_defi_collects
        .subscribe(pattern, handler, priority.unwrap_or(0));
}

/// Subscribes a handler to DeFi flash loans matching a pattern.
#[cfg(feature = "defi")]
pub fn subscribe_defi_flash(
    pattern: MStr<Pattern>,
    handler: TypedHandler<PoolFlash>,
    priority: Option<u8>,
) {
    get_message_bus().borrow_mut().router_defi_flash.subscribe(
        pattern,
        handler,
        priority.unwrap_or(0),
    );
}

/// Unsubscribes a handler from instrument messages.
pub fn unsubscribe_instruments(pattern: MStr<Pattern>, handler: ShareableMessageHandler) {
    unsubscribe_any(pattern, handler);
}

/// Unsubscribes a handler from instrument close messages.
pub fn unsubscribe_instrument_close(pattern: MStr<Pattern>, handler: ShareableMessageHandler) {
    unsubscribe_any(pattern, handler);
}

/// Unsubscribes a handler from order book deltas.
pub fn unsubscribe_book_deltas(pattern: MStr<Pattern>, handler: &TypedHandler<OrderBookDeltas>) {
    get_message_bus()
        .borrow_mut()
        .router_deltas
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from order book depth10 snapshots.
pub fn unsubscribe_book_depth10(pattern: MStr<Pattern>, handler: &TypedHandler<OrderBookDepth10>) {
    get_message_bus()
        .borrow_mut()
        .router_depth10
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from order book snapshots.
pub fn unsubscribe_book_snapshots(pattern: MStr<Pattern>, handler: &TypedHandler<OrderBook>) {
    get_message_bus()
        .borrow_mut()
        .router_book_snapshots
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from quote ticks.
pub fn unsubscribe_quotes(pattern: MStr<Pattern>, handler: &TypedHandler<QuoteTick>) {
    get_message_bus()
        .borrow_mut()
        .router_quotes
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from trade ticks.
pub fn unsubscribe_trades(pattern: MStr<Pattern>, handler: &TypedHandler<TradeTick>) {
    get_message_bus()
        .borrow_mut()
        .router_trades
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from bars.
pub fn unsubscribe_bars(pattern: MStr<Pattern>, handler: &TypedHandler<Bar>) {
    get_message_bus()
        .borrow_mut()
        .router_bars
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from mark price updates.
pub fn unsubscribe_mark_prices(pattern: MStr<Pattern>, handler: &TypedHandler<MarkPriceUpdate>) {
    get_message_bus()
        .borrow_mut()
        .router_mark_prices
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from index price updates.
pub fn unsubscribe_index_prices(pattern: MStr<Pattern>, handler: &TypedHandler<IndexPriceUpdate>) {
    get_message_bus()
        .borrow_mut()
        .router_index_prices
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from funding rate updates.
pub fn unsubscribe_funding_rates(
    pattern: MStr<Pattern>,
    handler: &TypedHandler<FundingRateUpdate>,
) {
    get_message_bus()
        .borrow_mut()
        .router_funding_rates
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from account state updates.
pub fn unsubscribe_account_state(pattern: MStr<Pattern>, handler: &TypedHandler<AccountState>) {
    get_message_bus()
        .borrow_mut()
        .router_account_state
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from order events.
pub fn unsubscribe_order_events(pattern: MStr<Pattern>, handler: &TypedHandler<OrderEventAny>) {
    get_message_bus()
        .borrow_mut()
        .router_order_events
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from position events.
pub fn unsubscribe_position_events(pattern: MStr<Pattern>, handler: &TypedHandler<PositionEvent>) {
    get_message_bus()
        .borrow_mut()
        .router_position_events
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from orders.
pub fn unsubscribe_orders(pattern: MStr<Pattern>, handler: &TypedHandler<OrderAny>) {
    get_message_bus()
        .borrow_mut()
        .router_orders
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from positions.
pub fn unsubscribe_positions(pattern: MStr<Pattern>, handler: &TypedHandler<Position>) {
    get_message_bus()
        .borrow_mut()
        .router_positions
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from greeks data.
pub fn unsubscribe_greeks(pattern: MStr<Pattern>, handler: &TypedHandler<GreeksData>) {
    get_message_bus()
        .borrow_mut()
        .router_greeks
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from DeFi blocks.
#[cfg(feature = "defi")]
pub fn unsubscribe_defi_blocks(pattern: MStr<Pattern>, handler: &TypedHandler<Block>) {
    get_message_bus()
        .borrow_mut()
        .router_defi_blocks
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from DeFi pools.
#[cfg(feature = "defi")]
pub fn unsubscribe_defi_pools(pattern: MStr<Pattern>, handler: &TypedHandler<Pool>) {
    get_message_bus()
        .borrow_mut()
        .router_defi_pools
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from DeFi pool swaps.
#[cfg(feature = "defi")]
pub fn unsubscribe_defi_swaps(pattern: MStr<Pattern>, handler: &TypedHandler<PoolSwap>) {
    get_message_bus()
        .borrow_mut()
        .router_defi_swaps
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from DeFi liquidity updates.
#[cfg(feature = "defi")]
pub fn unsubscribe_defi_liquidity(
    pattern: MStr<Pattern>,
    handler: &TypedHandler<PoolLiquidityUpdate>,
) {
    get_message_bus()
        .borrow_mut()
        .router_defi_liquidity
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from DeFi fee collects.
#[cfg(feature = "defi")]
pub fn unsubscribe_defi_collects(pattern: MStr<Pattern>, handler: &TypedHandler<PoolFeeCollect>) {
    get_message_bus()
        .borrow_mut()
        .router_defi_collects
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from DeFi flash loans.
#[cfg(feature = "defi")]
pub fn unsubscribe_defi_flash(pattern: MStr<Pattern>, handler: &TypedHandler<PoolFlash>) {
    get_message_bus()
        .borrow_mut()
        .router_defi_flash
        .unsubscribe(pattern, handler);
}

/// Unsubscribes a handler from a pattern (Any-based).
pub fn unsubscribe_any(pattern: MStr<Pattern>, handler: ShareableMessageHandler) {
    log::debug!("Unsubscribing {handler:?} from pattern '{pattern}'");

    let handler_id = handler.0.id();
    let bus_rc = get_message_bus();
    let mut bus = bus_rc.borrow_mut();

    let count_before = bus.subscriptions.len();

    bus.topics.values_mut().for_each(|subs| {
        subs.retain(|s| !(s.pattern == pattern && s.handler_id == handler_id));
    });

    bus.subscriptions
        .retain(|s| !(s.pattern == pattern && s.handler_id == handler_id));

    let removed = bus.subscriptions.len() < count_before;

    if removed {
        log::debug!("Handler for pattern '{pattern}' was removed");
    } else {
        log::debug!("No matching handler for pattern '{pattern}' was found");
    }
}

/// Checks if a handler is subscribed to a pattern (Any-based).
pub fn is_subscribed_any<T: AsRef<str>>(pattern: T, handler: ShareableMessageHandler) -> bool {
    let pattern = MStr::from(pattern.as_ref());
    let sub = Subscription::new(pattern, handler, None);
    get_message_bus().borrow().subscriptions.contains(&sub)
}

/// Returns the count of Any-based subscriptions for a topic.
pub fn subscriptions_count_any<S: AsRef<str>>(topic: S) -> usize {
    get_message_bus().borrow().subscriptions_count(topic)
}

/// Returns the subscriber count for order book deltas on a topic.
pub fn subscriber_count_deltas(topic: MStr<Topic>) -> usize {
    get_message_bus()
        .borrow()
        .router_deltas
        .subscriber_count(topic)
}

/// Returns the subscriber count for order book depth10 on a topic.
pub fn subscriber_count_depth10(topic: MStr<Topic>) -> usize {
    get_message_bus()
        .borrow()
        .router_depth10
        .subscriber_count(topic)
}

/// Returns the subscriber count for order book snapshots on a topic.
pub fn subscriber_count_book_snapshots(topic: MStr<Topic>) -> usize {
    get_message_bus()
        .borrow()
        .router_book_snapshots
        .subscriber_count(topic)
}

/// Publishes a message to the topic using runtime type dispatch (Any).
pub fn publish_any(topic: MStr<Topic>, message: &dyn Any) {
    // SAFETY: Take buffer (re-entrancy safe)
    let mut handlers = ANY_HANDLERS.with_borrow_mut(std::mem::take);

    get_message_bus()
        .borrow_mut()
        .fill_matching_any_handlers(topic, &mut handlers);

    for handler in &handlers {
        handler.0.handle(message);
    }

    handlers.clear(); // Release refs before restore
    ANY_HANDLERS.with_borrow_mut(|buf| *buf = handlers);
}

/// Publishes a message to typed handlers using thread-local buffer reuse.
///
/// The `fill_fn` receives a mutable reference to the MessageBus, avoiding
/// redundant TLS access and Rc clone/drop overhead per publish.
///
/// # Invariants
///
/// - `fill_fn` must not call any publish path (would panic from RefCell double-borrow).
/// - Handler panics drop the buffer, losing reuse optimization (acceptable as panics are fatal).
#[inline]
fn publish_typed<T: 'static>(
    tls: &'static LocalKey<RefCell<SmallVec<[TypedHandler<T>; HANDLER_BUFFER_CAP]>>>,
    fill_fn: impl FnOnce(&mut MessageBus, &mut SmallVec<[TypedHandler<T>; HANDLER_BUFFER_CAP]>),
    message: &T,
) {
    // SAFETY: Take buffer (re-entrancy safe)
    let mut handlers = tls.with_borrow_mut(std::mem::take);

    // Borrow scope ends before dispatch to support re-entrant publishes
    MESSAGE_BUS.with(|cell| {
        let rc = cell.get_or_init(|| Rc::new(RefCell::new(MessageBus::default())));
        fill_fn(&mut rc.borrow_mut(), &mut handlers);
    });

    for handler in &handlers {
        handler.handle(message);
    }

    handlers.clear(); // Release refs before restore
    tls.with_borrow_mut(|buf| *buf = handlers);
}

/// Publishes order book deltas to subscribers on a topic.
pub fn publish_deltas(topic: MStr<Topic>, deltas: &OrderBookDeltas) {
    publish_typed(
        &DELTAS_HANDLERS,
        |bus, h| bus.router_deltas.fill_matching_handlers(topic, h),
        deltas,
    );
}

/// Publishes order book depth10 to subscribers on a topic.
pub fn publish_depth10(topic: MStr<Topic>, depth: &OrderBookDepth10) {
    publish_typed(
        &DEPTH10_HANDLERS,
        |bus, h| bus.router_depth10.fill_matching_handlers(topic, h),
        depth,
    );
}

/// Publishes an order book snapshot to subscribers on a topic.
pub fn publish_book(topic: MStr<Topic>, book: &OrderBook) {
    publish_typed(
        &BOOK_HANDLERS,
        |bus, h| bus.router_book_snapshots.fill_matching_handlers(topic, h),
        book,
    );
}

/// Publishes a quote tick to subscribers on a topic.
pub fn publish_quote(topic: MStr<Topic>, quote: &QuoteTick) {
    publish_typed(
        &QUOTE_HANDLERS,
        |bus, h| bus.router_quotes.fill_matching_handlers(topic, h),
        quote,
    );
}

/// Publishes a trade tick to subscribers on a topic.
pub fn publish_trade(topic: MStr<Topic>, trade: &TradeTick) {
    publish_typed(
        &TRADE_HANDLERS,
        |bus, h| bus.router_trades.fill_matching_handlers(topic, h),
        trade,
    );
}

/// Publishes a bar to subscribers on a topic.
pub fn publish_bar(topic: MStr<Topic>, bar: &Bar) {
    publish_typed(
        &BAR_HANDLERS,
        |bus, h| bus.router_bars.fill_matching_handlers(topic, h),
        bar,
    );
}

/// Publishes a mark price update to subscribers on a topic.
pub fn publish_mark_price(topic: MStr<Topic>, mark_price: &MarkPriceUpdate) {
    publish_typed(
        &MARK_PRICE_HANDLERS,
        |bus, h| bus.router_mark_prices.fill_matching_handlers(topic, h),
        mark_price,
    );
}

/// Publishes an index price update to subscribers on a topic.
pub fn publish_index_price(topic: MStr<Topic>, index_price: &IndexPriceUpdate) {
    publish_typed(
        &INDEX_PRICE_HANDLERS,
        |bus, h| bus.router_index_prices.fill_matching_handlers(topic, h),
        index_price,
    );
}

/// Publishes a funding rate update to subscribers on a topic.
pub fn publish_funding_rate(topic: MStr<Topic>, funding_rate: &FundingRateUpdate) {
    publish_typed(
        &FUNDING_RATE_HANDLERS,
        |bus, h| bus.router_funding_rates.fill_matching_handlers(topic, h),
        funding_rate,
    );
}

/// Publishes greeks data to subscribers on a topic.
pub fn publish_greeks(topic: MStr<Topic>, greeks: &GreeksData) {
    publish_typed(
        &GREEKS_HANDLERS,
        |bus, h| bus.router_greeks.fill_matching_handlers(topic, h),
        greeks,
    );
}

/// Publishes an account state to subscribers on a topic.
pub fn publish_account_state(topic: MStr<Topic>, state: &AccountState) {
    publish_typed(
        &ACCOUNT_STATE_HANDLERS,
        |bus, h| bus.router_account_state.fill_matching_handlers(topic, h),
        state,
    );
}

/// Publishes an order event to subscribers on a topic.
pub fn publish_order_event(topic: MStr<Topic>, event: &OrderEventAny) {
    publish_typed(
        &ORDER_EVENT_HANDLERS,
        |bus, h| bus.router_order_events.fill_matching_handlers(topic, h),
        event,
    );
}

/// Publishes a position event to subscribers on a topic.
pub fn publish_position_event(topic: MStr<Topic>, event: &PositionEvent) {
    publish_typed(
        &POSITION_EVENT_HANDLERS,
        |bus, h| bus.router_position_events.fill_matching_handlers(topic, h),
        event,
    );
}

/// Publishes a DeFi block to subscribers on a topic.
#[cfg(feature = "defi")]
pub fn publish_defi_block(topic: MStr<Topic>, block: &Block) {
    publish_typed(
        &DEFI_BLOCK_HANDLERS,
        |bus, h| bus.router_defi_blocks.fill_matching_handlers(topic, h),
        block,
    );
}

/// Publishes a DeFi pool to subscribers on a topic.
#[cfg(feature = "defi")]
pub fn publish_defi_pool(topic: MStr<Topic>, pool: &Pool) {
    publish_typed(
        &DEFI_POOL_HANDLERS,
        |bus, h| bus.router_defi_pools.fill_matching_handlers(topic, h),
        pool,
    );
}

/// Publishes a DeFi pool swap to subscribers on a topic.
#[cfg(feature = "defi")]
pub fn publish_defi_swap(topic: MStr<Topic>, swap: &PoolSwap) {
    publish_typed(
        &DEFI_SWAP_HANDLERS,
        |bus, h| bus.router_defi_swaps.fill_matching_handlers(topic, h),
        swap,
    );
}

/// Publishes a DeFi liquidity update to subscribers on a topic.
#[cfg(feature = "defi")]
pub fn publish_defi_liquidity(topic: MStr<Topic>, update: &PoolLiquidityUpdate) {
    publish_typed(
        &DEFI_LIQUIDITY_HANDLERS,
        |bus, h| bus.router_defi_liquidity.fill_matching_handlers(topic, h),
        update,
    );
}

/// Publishes a DeFi fee collect to subscribers on a topic.
#[cfg(feature = "defi")]
pub fn publish_defi_collect(topic: MStr<Topic>, collect: &PoolFeeCollect) {
    publish_typed(
        &DEFI_COLLECT_HANDLERS,
        |bus, h| bus.router_defi_collects.fill_matching_handlers(topic, h),
        collect,
    );
}

/// Publishes a DeFi flash loan to subscribers on a topic.
#[cfg(feature = "defi")]
pub fn publish_defi_flash(topic: MStr<Topic>, flash: &PoolFlash) {
    publish_typed(
        &DEFI_FLASH_HANDLERS,
        |bus, h| bus.router_defi_flash.fill_matching_handlers(topic, h),
        flash,
    );
}

/// Sends a message to an endpoint handler using runtime type dispatch (Any).
pub fn send_any(endpoint: MStr<Endpoint>, message: &dyn Any) {
    let handler = get_message_bus().borrow().get_endpoint(endpoint).cloned();
    if let Some(handler) = handler {
        handler.0.handle(message);
    } else {
        log::error!("send_any: no registered endpoint '{endpoint}'");
    }
}

/// Sends a message to an endpoint, converting to Any (convenience wrapper).
pub fn send_any_value<T: 'static>(endpoint: MStr<Endpoint>, message: T) {
    let handler = get_message_bus().borrow().get_endpoint(endpoint).cloned();
    if let Some(handler) = handler {
        handler.0.handle(&message);
    } else {
        log::error!("send_any_value: no registered endpoint '{endpoint}'");
    }
}

/// Sends the [`DataResponse`] to the registered correlation ID handler.
pub fn send_response(correlation_id: &UUID4, message: &DataResponse) {
    let handler = get_message_bus()
        .borrow()
        .get_response_handler(correlation_id)
        .cloned();

    if let Some(handler) = handler {
        match message {
            DataResponse::Data(resp) => handler.0.handle(resp),
            DataResponse::Instrument(resp) => handler.0.handle(resp.as_ref()),
            DataResponse::Instruments(resp) => handler.0.handle(resp),
            DataResponse::Book(resp) => handler.0.handle(resp),
            DataResponse::Quotes(resp) => handler.0.handle(resp),
            DataResponse::Trades(resp) => handler.0.handle(resp),
            DataResponse::Bars(resp) => handler.0.handle(resp),
        }
    } else {
        log::error!("send_response: handler not found for correlation_id '{correlation_id}'");
    }
}

/// Sends a quote tick to an endpoint handler.
pub fn send_quote(endpoint: MStr<Endpoint>, quote: &QuoteTick) {
    let handler = get_message_bus()
        .borrow()
        .endpoints_quotes
        .get(endpoint)
        .cloned();
    if let Some(handler) = handler {
        handler.handle(quote);
    } else {
        log::error!("send_quote: no registered endpoint '{endpoint}'");
    }
}

/// Sends a trade tick to an endpoint handler.
pub fn send_trade(endpoint: MStr<Endpoint>, trade: &TradeTick) {
    let handler = get_message_bus()
        .borrow()
        .endpoints_trades
        .get(endpoint)
        .cloned();
    if let Some(handler) = handler {
        handler.handle(trade);
    } else {
        log::error!("send_trade: no registered endpoint '{endpoint}'");
    }
}

/// Sends a bar to an endpoint handler.
pub fn send_bar(endpoint: MStr<Endpoint>, bar: &Bar) {
    let handler = get_message_bus()
        .borrow()
        .endpoints_bars
        .get(endpoint)
        .cloned();
    if let Some(handler) = handler {
        handler.handle(bar);
    } else {
        log::error!("send_bar: no registered endpoint '{endpoint}'");
    }
}

/// Sends an order event to an endpoint handler.
pub fn send_order_event(endpoint: MStr<Endpoint>, event: &OrderEventAny) {
    let handler = get_message_bus()
        .borrow()
        .endpoints_order_events
        .get(endpoint)
        .cloned();
    if let Some(handler) = handler {
        handler.handle(event);
    } else {
        log::error!("send_order_event: no registered endpoint '{endpoint}'");
    }
}

/// Sends an account state to an endpoint handler.
pub fn send_account_state(endpoint: MStr<Endpoint>, state: &AccountState) {
    let handler = get_message_bus()
        .borrow()
        .endpoints_account_state
        .get(endpoint)
        .cloned();
    if let Some(handler) = handler {
        handler.handle(state);
    } else {
        log::error!("send_account_state: no registered endpoint '{endpoint}'");
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use nautilus_model::{
        data::{Bar, OrderBookDelta, OrderBookDeltas, QuoteTick, TradeTick},
        identifiers::InstrumentId,
    };
    use rstest::rstest;

    use super::*;

    #[rstest]
    fn test_typed_quote_publish_subscribe_integration() {
        let _msgbus = get_message_bus();
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedHandler::from(move |quote: &QuoteTick| {
            received_clone.borrow_mut().push(*quote);
        });

        subscribe_quotes("data.quotes.*".into(), handler, None);

        let quote = QuoteTick::default();
        publish_quote("data.quotes.TEST".into(), &quote);
        publish_quote("data.quotes.TEST".into(), &quote);

        assert_eq!(received.borrow().len(), 2);
    }

    #[rstest]
    fn test_typed_trade_publish_subscribe_integration() {
        let _msgbus = get_message_bus();
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedHandler::from(move |trade: &TradeTick| {
            received_clone.borrow_mut().push(*trade);
        });

        subscribe_trades("data.trades.*".into(), handler, None);

        let trade = TradeTick::default();
        publish_trade("data.trades.TEST".into(), &trade);

        assert_eq!(received.borrow().len(), 1);
    }

    #[rstest]
    fn test_typed_bar_publish_subscribe_integration() {
        let _msgbus = get_message_bus();
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedHandler::from(move |bar: &Bar| {
            received_clone.borrow_mut().push(*bar);
        });

        subscribe_bars("data.bars.*".into(), handler, None);

        let bar = Bar::default();
        publish_bar("data.bars.TEST".into(), &bar);

        assert_eq!(received.borrow().len(), 1);
    }

    #[rstest]
    fn test_typed_deltas_publish_subscribe_integration() {
        let _msgbus = get_message_bus();
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedHandler::from(move |deltas: &OrderBookDeltas| {
            received_clone.borrow_mut().push(deltas.clone());
        });

        subscribe_book_deltas("data.book.deltas.*".into(), handler, None);

        let instrument_id = InstrumentId::from("TEST.VENUE");
        let delta = OrderBookDelta::clear(instrument_id, 0, 1.into(), 2.into());
        let deltas = OrderBookDeltas::new(instrument_id, vec![delta]);
        publish_deltas("data.book.deltas.TEST".into(), &deltas);

        assert_eq!(received.borrow().len(), 1);
    }

    #[rstest]
    fn test_typed_unsubscribe_stops_delivery() {
        let _msgbus = get_message_bus();
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedHandler::from_with_id("unsub-test", move |quote: &QuoteTick| {
            received_clone.borrow_mut().push(*quote);
        });

        subscribe_quotes("data.quotes.UNSUB".into(), handler.clone(), None);

        let quote = QuoteTick::default();
        publish_quote("data.quotes.UNSUB".into(), &quote);
        assert_eq!(received.borrow().len(), 1);

        unsubscribe_quotes("data.quotes.UNSUB".into(), &handler);

        publish_quote("data.quotes.UNSUB".into(), &quote);
        assert_eq!(received.borrow().len(), 1);
    }

    #[rstest]
    fn test_typed_wildcard_pattern_matching() {
        let _msgbus = get_message_bus();
        let received = Rc::new(RefCell::new(Vec::new()));
        let received_clone = received.clone();

        let handler = TypedHandler::from(move |quote: &QuoteTick| {
            received_clone.borrow_mut().push(*quote);
        });

        subscribe_quotes("data.quotes.WILD.*".into(), handler, None);

        let quote = QuoteTick::default();
        publish_quote("data.quotes.WILD.AAPL".into(), &quote);
        publish_quote("data.quotes.WILD.MSFT".into(), &quote);
        publish_quote("data.quotes.OTHER.AAPL".into(), &quote);

        assert_eq!(received.borrow().len(), 2);
    }

    #[rstest]
    fn test_typed_priority_ordering() {
        let _msgbus = get_message_bus();
        let order = Rc::new(RefCell::new(Vec::new()));

        let order1 = order.clone();
        let handler_low = TypedHandler::from_with_id("low-priority", move |_: &QuoteTick| {
            order1.borrow_mut().push("low");
        });

        let order2 = order.clone();
        let handler_high = TypedHandler::from_with_id("high-priority", move |_: &QuoteTick| {
            order2.borrow_mut().push("high");
        });

        subscribe_quotes("data.quotes.PRIO".into(), handler_low, Some(5));
        subscribe_quotes("data.quotes.PRIO".into(), handler_high, Some(10));

        let quote = QuoteTick::default();
        publish_quote("data.quotes.PRIO".into(), &quote);

        assert_eq!(*order.borrow(), vec!["high", "low"]);
    }

    #[rstest]
    fn test_typed_routing_isolation() {
        let _msgbus = get_message_bus();
        let quote_received = Rc::new(RefCell::new(false));
        let trade_received = Rc::new(RefCell::new(false));

        let qr = quote_received.clone();
        let quote_handler = TypedHandler::from(move |_: &QuoteTick| {
            *qr.borrow_mut() = true;
        });

        let tr = trade_received.clone();
        let trade_handler = TypedHandler::from(move |_: &TradeTick| {
            *tr.borrow_mut() = true;
        });

        subscribe_quotes("data.iso.*".into(), quote_handler, None);
        subscribe_trades("data.iso.*".into(), trade_handler, None);

        let quote = QuoteTick::default();
        publish_quote("data.iso.TEST".into(), &quote);

        assert!(*quote_received.borrow());
        assert!(!*trade_received.borrow());
    }
}
