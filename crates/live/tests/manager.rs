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

//! Integration tests for ExecutionManager.
//!
//! These tests focus on observable behavior through the public API.
//! Internal state tests are in the in-module tests in manager.rs.

use std::{cell::RefCell, rc::Rc};

use nautilus_common::{cache::Cache, clock::TestClock};
use nautilus_core::{UUID4, UnixNanos};
use nautilus_live::manager::{ExecutionManager, ExecutionManagerConfig, ExecutionReport};
use nautilus_model::{
    enums::{LiquiditySide, OrderSide, OrderStatus, OrderType, PositionSideSpecified, TimeInForce},
    events::OrderEventAny,
    identifiers::{
        AccountId, ClientId, ClientOrderId, InstrumentId, StrategyId, TradeId, TraderId, Venue,
        VenueOrderId,
    },
    instruments::{Instrument, InstrumentAny, stubs::crypto_perpetual_ethusdt},
    orders::{Order, OrderAny, OrderTestBuilder, stubs::TestOrderEventStubs},
    reports::{ExecutionMassStatus, FillReport, OrderStatusReport, PositionStatusReport},
    types::{Money, Price, Quantity},
};
use rstest::rstest;
use rust_decimal_macros::dec;

struct TestContext {
    clock: Rc<RefCell<TestClock>>,
    cache: Rc<RefCell<Cache>>,
    manager: ExecutionManager,
}

impl TestContext {
    fn new() -> Self {
        Self::with_config(ExecutionManagerConfig::default())
    }

    fn with_config(config: ExecutionManagerConfig) -> Self {
        let clock = Rc::new(RefCell::new(TestClock::new()));
        let cache = Rc::new(RefCell::new(Cache::default()));
        let manager = ExecutionManager::new(clock.clone(), cache.clone(), config);
        Self {
            clock,
            cache,
            manager,
        }
    }

    fn advance_time(&self, delta_nanos: u64) {
        let current = self.clock.borrow().get_time_ns();
        self.clock
            .borrow_mut()
            .advance_time(UnixNanos::from(current.as_u64() + delta_nanos), true);
    }

    fn add_instrument(&self, instrument: InstrumentAny) {
        self.cache.borrow_mut().add_instrument(instrument).unwrap();
    }

    fn add_order(&self, order: OrderAny) {
        self.cache
            .borrow_mut()
            .add_order(order, None, None, false)
            .unwrap();
    }

    fn get_order(&self, client_order_id: &ClientOrderId) -> Option<OrderAny> {
        self.cache.borrow().order(client_order_id).cloned()
    }
}

fn test_instrument() -> InstrumentAny {
    InstrumentAny::CryptoPerpetual(crypto_perpetual_ethusdt())
}

fn test_instrument_id() -> InstrumentId {
    crypto_perpetual_ethusdt().id()
}

fn test_account_id() -> AccountId {
    AccountId::from("BINANCE-001")
}

fn test_venue() -> Venue {
    Venue::from("BINANCE")
}

fn test_client_id() -> ClientId {
    ClientId::from("BINANCE")
}

fn create_limit_order(
    client_order_id: &str,
    instrument_id: InstrumentId,
    side: OrderSide,
    quantity: &str,
    price: &str,
) -> OrderAny {
    OrderTestBuilder::new(OrderType::Limit)
        .client_order_id(ClientOrderId::from(client_order_id))
        .instrument_id(instrument_id)
        .side(side)
        .quantity(Quantity::from(quantity))
        .price(Price::from(price))
        .build()
}

/// Creates an order that has been submitted (has account_id set)
fn create_submitted_order(
    client_order_id: &str,
    instrument_id: InstrumentId,
    side: OrderSide,
    quantity: &str,
    price: &str,
) -> OrderAny {
    let mut order = create_limit_order(client_order_id, instrument_id, side, quantity, price);
    let submitted = TestOrderEventStubs::submitted(&order, test_account_id());
    order.apply(submitted).unwrap();
    order
}

fn create_order_status_report(
    client_order_id: Option<ClientOrderId>,
    venue_order_id: VenueOrderId,
    instrument_id: InstrumentId,
    status: OrderStatus,
    quantity: Quantity,
    filled_qty: Quantity,
) -> OrderStatusReport {
    OrderStatusReport::new(
        test_account_id(),
        instrument_id,
        client_order_id,
        venue_order_id,
        OrderSide::Buy,
        OrderType::Limit,
        TimeInForce::Gtc,
        status,
        quantity,
        filled_qty,
        UnixNanos::from(1_000_000),
        UnixNanos::from(1_000_000),
        UnixNanos::from(1_000_000),
        None,
    )
    .with_price(Price::from("3000.00"))
}

#[rstest]
fn test_fill_deduplication_new_fill_not_processed() {
    let ctx = TestContext::new();
    let trade_id = TradeId::from("T-001");

    assert!(!ctx.manager.is_fill_recently_processed(&trade_id));
}

#[rstest]
fn test_fill_deduplication_tracks_processed_fill() {
    let mut ctx = TestContext::new();
    let trade_id = TradeId::from("T-001");

    ctx.manager.mark_fill_processed(trade_id);

    assert!(ctx.manager.is_fill_recently_processed(&trade_id));
}

#[rstest]
fn test_fill_deduplication_prune_removes_expired() {
    let mut ctx = TestContext::new();
    let old_trade = TradeId::from("T-OLD");
    let new_trade = TradeId::from("T-NEW");

    ctx.manager.mark_fill_processed(old_trade);
    ctx.advance_time(120_000_000_000); // 120 seconds
    ctx.manager.mark_fill_processed(new_trade);

    ctx.manager.prune_recent_fills_cache(60.0); // 60 second TTL

    assert!(!ctx.manager.is_fill_recently_processed(&old_trade));
    assert!(ctx.manager.is_fill_recently_processed(&new_trade));
}

#[rstest]
fn test_reconcile_report_returns_empty_when_order_not_in_cache() {
    let mut ctx = TestContext::new();
    let client_order_id = ClientOrderId::from("O-MISSING");

    let report = ExecutionReport {
        client_order_id,
        venue_order_id: Some(VenueOrderId::from("V-001")),
        status: OrderStatus::Accepted,
        filled_qty: Quantity::from(0),
        avg_px: None,
        ts_event: UnixNanos::from(1_000_000),
    };

    let events = ctx.manager.reconcile_report(report).unwrap();

    assert!(events.is_empty());
}

#[rstest]
fn test_reconcile_report_handles_missing_venue_order_id() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();

    ctx.add_instrument(test_instrument());
    let order = create_limit_order("O-001", instrument_id, OrderSide::Buy, "1.0", "3000.00");
    ctx.add_order(order);

    let report = ExecutionReport {
        client_order_id: ClientOrderId::from("O-001"),
        venue_order_id: None, // Missing venue order ID
        status: OrderStatus::Accepted,
        filled_qty: Quantity::from(0),
        avg_px: None,
        ts_event: UnixNanos::from(1_000_000),
    };

    let events = ctx.manager.reconcile_report(report).unwrap();

    // Should return empty since venue_order_id is required
    assert!(events.is_empty());
}

#[tokio::test]
async fn test_reconcile_mass_status_with_empty_reports() {
    let mut ctx = TestContext::new();
    let mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert!(result.events.is_empty());
}

#[tokio::test]
async fn test_reconcile_mass_status_creates_external_order_accepted() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();

    ctx.add_instrument(test_instrument());

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        None, // No client_order_id = external order
        VenueOrderId::from("V-EXT-001"),
        instrument_id,
        OrderStatus::Accepted,
        Quantity::from("1.0"),
        Quantity::from("0"),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    assert!(matches!(result.events[0], OrderEventAny::Accepted(_)));

    // Verify order was added to cache
    let client_order_id = ClientOrderId::from("V-EXT-001");
    let order = ctx.get_order(&client_order_id);
    assert!(order.is_some());
}

#[tokio::test]
async fn test_reconcile_mass_status_creates_external_order_canceled() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();

    ctx.add_instrument(test_instrument());

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        None,
        VenueOrderId::from("V-EXT-002"),
        instrument_id,
        OrderStatus::Canceled,
        Quantity::from("1.0"),
        Quantity::from("0"),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 2);
    assert!(matches!(result.events[0], OrderEventAny::Accepted(_)));
    assert!(matches!(result.events[1], OrderEventAny::Canceled(_)));
}

#[tokio::test]
async fn test_reconcile_mass_status_creates_external_order_filled() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();

    ctx.add_instrument(test_instrument());

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        None,
        VenueOrderId::from("V-EXT-003"),
        instrument_id,
        OrderStatus::Filled,
        Quantity::from("1.0"),
        Quantity::from("1.0"),
    )
    .with_avg_px(3000.50)
    .unwrap();
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 2);
    assert!(matches!(result.events[0], OrderEventAny::Accepted(_)));
    assert!(matches!(result.events[1], OrderEventAny::Filled(_)));

    if let OrderEventAny::Filled(filled) = &result.events[1] {
        assert_eq!(filled.last_qty, Quantity::from("1.0"));
        assert!(filled.reconciliation);
    }
}

#[tokio::test]
async fn test_reconcile_mass_status_skips_external_when_filtered() {
    let config = ExecutionManagerConfig {
        filter_unclaimed_external: true,
        ..Default::default()
    };
    let mut ctx = TestContext::with_config(config);
    let instrument_id = test_instrument_id();

    ctx.add_instrument(test_instrument());

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        None,
        VenueOrderId::from("V-EXT-001"),
        instrument_id,
        OrderStatus::Accepted,
        Quantity::from("1.0"),
        Quantity::from("0"),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert!(result.events.is_empty());
}

#[tokio::test]
async fn test_reconcile_mass_status_uses_claimed_strategy() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let strategy_id = StrategyId::from("MY-STRATEGY");

    ctx.add_instrument(test_instrument());
    ctx.manager
        .claim_external_orders(instrument_id, strategy_id);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        None,
        VenueOrderId::from("V-EXT-001"),
        instrument_id,
        OrderStatus::Accepted,
        Quantity::from("1.0"),
        Quantity::from("0"),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);

    let client_order_id = ClientOrderId::from("V-EXT-001");
    let order = ctx.get_order(&client_order_id).unwrap();
    assert_eq!(order.strategy_id(), strategy_id);
}

#[tokio::test]
async fn test_reconcile_mass_status_processes_fills_for_cached_order() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-001");
    let venue_order_id = VenueOrderId::from("V-001");

    ctx.add_instrument(test_instrument());
    let order = create_limit_order("O-001", instrument_id, OrderSide::Buy, "2.0", "3000.00");
    ctx.add_order(order);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let fill = FillReport::new(
        test_account_id(),
        instrument_id,
        venue_order_id,
        TradeId::from("T-001"),
        OrderSide::Buy,
        Quantity::from("1.0"),
        Price::from("3000.00"),
        Money::from("0.50 USDT"),
        LiquiditySide::Maker,
        Some(client_order_id),
        None,
        UnixNanos::from(1_000_000),
        UnixNanos::from(1_000_000),
        None,
    );
    mass_status.add_fill_reports(vec![fill]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    assert!(matches!(result.events[0], OrderEventAny::Filled(_)));
}

#[tokio::test]
async fn test_reconcile_mass_status_deduplicates_fills() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-001");
    let venue_order_id = VenueOrderId::from("V-001");
    let trade_id = TradeId::from("T-001");

    ctx.add_instrument(test_instrument());
    let order = create_limit_order("O-001", instrument_id, OrderSide::Buy, "2.0", "3000.00");
    ctx.add_order(order);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    // Add same fill twice
    let fill = FillReport::new(
        test_account_id(),
        instrument_id,
        venue_order_id,
        trade_id,
        OrderSide::Buy,
        Quantity::from("1.0"),
        Price::from("3000.00"),
        Money::from("0.50 USDT"),
        LiquiditySide::Maker,
        Some(client_order_id),
        None,
        UnixNanos::from(1_000_000),
        UnixNanos::from(1_000_000),
        None,
    );
    mass_status.add_fill_reports(vec![fill.clone(), fill]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    // Only one fill should be processed
    assert_eq!(result.events.len(), 1);
}

#[tokio::test]
async fn test_reconcile_mass_status_skips_order_without_instrument() {
    let mut ctx = TestContext::new();
    // Don't add instrument to cache

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        None,
        VenueOrderId::from("V-EXT-001"),
        test_instrument_id(),
        OrderStatus::Accepted,
        Quantity::from("1.0"),
        Quantity::from("0"),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert!(result.events.is_empty());
}

#[tokio::test]
async fn test_reconcile_mass_status_sorts_events_chronologically() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-001");
    let venue_order_id = VenueOrderId::from("V-001");

    ctx.add_instrument(test_instrument());
    let order = create_limit_order("O-001", instrument_id, OrderSide::Buy, "2.0", "3000.00");
    ctx.add_order(order);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    // Add fills in reverse chronological order
    let fill2 = FillReport::new(
        test_account_id(),
        instrument_id,
        venue_order_id,
        TradeId::from("T-002"),
        OrderSide::Buy,
        Quantity::from("0.5"),
        Price::from("3001.00"),
        Money::from("0.25 USDT"),
        LiquiditySide::Maker,
        Some(client_order_id),
        None,
        UnixNanos::from(2_000_000), // Later
        UnixNanos::from(2_000_000),
        None,
    );
    let fill1 = FillReport::new(
        test_account_id(),
        instrument_id,
        venue_order_id,
        TradeId::from("T-001"),
        OrderSide::Buy,
        Quantity::from("0.5"),
        Price::from("3000.00"),
        Money::from("0.25 USDT"),
        LiquiditySide::Maker,
        Some(client_order_id),
        None,
        UnixNanos::from(1_000_000), // Earlier
        UnixNanos::from(1_000_000),
        None,
    );
    mass_status.add_fill_reports(vec![fill2, fill1]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 2);

    // Verify chronological ordering
    assert!(result.events[0].ts_event() < result.events[1].ts_event());
}

#[rstest]
fn test_inflight_order_generates_rejection_after_max_retries() {
    let config = ExecutionManagerConfig {
        inflight_threshold_ms: 100,
        inflight_max_retries: 1,
        ..Default::default()
    };
    let mut ctx = TestContext::with_config(config);
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-001");

    ctx.add_instrument(test_instrument());

    // Order must be submitted (have account_id) to generate rejection
    let order = create_submitted_order("O-001", instrument_id, OrderSide::Buy, "1.0", "3000.00");
    ctx.add_order(order);

    ctx.manager.register_inflight(client_order_id);
    ctx.advance_time(200_000_000); // 200ms, past threshold

    let events = ctx.manager.check_inflight_orders();

    assert_eq!(events.len(), 1);
    assert!(matches!(events[0], OrderEventAny::Rejected(_)));

    if let OrderEventAny::Rejected(rejected) = &events[0] {
        assert_eq!(rejected.client_order_id, client_order_id);
        assert_eq!(rejected.reason.as_str(), "INFLIGHT_TIMEOUT");
    }
}

#[rstest]
fn test_inflight_check_skips_filtered_order_ids() {
    let filtered_id = ClientOrderId::from("O-FILTERED");
    let mut config = ExecutionManagerConfig {
        inflight_threshold_ms: 100,
        inflight_max_retries: 1,
        ..Default::default()
    };
    config.filtered_client_order_ids.insert(filtered_id);
    let mut ctx = TestContext::with_config(config);
    let instrument_id = test_instrument_id();

    ctx.add_instrument(test_instrument());

    // Use submitted order (has account_id) to verify filtering, not missing account_id
    let order = create_submitted_order(
        "O-FILTERED",
        instrument_id,
        OrderSide::Buy,
        "1.0",
        "3000.00",
    );
    ctx.add_order(order);

    ctx.manager.register_inflight(filtered_id);
    ctx.advance_time(200_000_000);

    let events = ctx.manager.check_inflight_orders();

    // Filtered order should not generate rejection
    assert!(events.is_empty());
}

#[rstest]
fn test_config_default_values() {
    let config = ExecutionManagerConfig::default();

    assert!(config.reconciliation);
    assert_eq!(config.reconciliation_startup_delay_secs, 10.0);
    assert_eq!(config.lookback_mins, Some(60));
    assert!(!config.filter_unclaimed_external);
    assert!(!config.filter_position_reports);
    assert!(config.generate_missing_orders);
    assert_eq!(config.inflight_check_interval_ms, 2_000);
    assert_eq!(config.inflight_threshold_ms, 5_000);
    assert_eq!(config.inflight_max_retries, 5);
}

#[rstest]
fn test_config_with_trader_id() {
    let trader_id = TraderId::from("TRADER-001");
    let config = ExecutionManagerConfig::default().with_trader_id(trader_id);

    assert_eq!(config.trader_id, trader_id);
}

#[rstest]
fn test_purge_operations_do_nothing_when_disabled() {
    let config = ExecutionManagerConfig {
        purge_closed_orders_buffer_mins: None,
        purge_closed_positions_buffer_mins: None,
        purge_account_events_lookback_mins: None,
        ..Default::default()
    };
    let mut ctx = TestContext::with_config(config);

    ctx.manager.purge_closed_orders();
    ctx.manager.purge_closed_positions();
    ctx.manager.purge_account_events();
}

#[tokio::test]
async fn test_reconcile_mass_status_accepted_order_canceled_at_venue() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-001");
    let venue_order_id = VenueOrderId::from("V-001");

    ctx.add_instrument(test_instrument());

    // Create and accept order locally
    let mut order =
        create_submitted_order("O-001", instrument_id, OrderSide::Buy, "1.0", "3000.00");

    // Apply accepted event to put order in ACCEPTED state
    let accepted = TestOrderEventStubs::accepted(&order, test_account_id(), venue_order_id);
    order.apply(accepted).unwrap();
    ctx.add_order(order);

    // Venue reports order was canceled
    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::Canceled,
        Quantity::from("1.0"),
        Quantity::from("0"),
    )
    .with_cancel_reason("USER_REQUEST".to_string());
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    assert!(matches!(result.events[0], OrderEventAny::Canceled(_)));

    if let OrderEventAny::Canceled(canceled) = &result.events[0] {
        assert_eq!(canceled.client_order_id, client_order_id);
        assert!(canceled.reconciliation != 0); // Verify reconciliation flag is set
    }
}

#[tokio::test]
async fn test_reconcile_mass_status_accepted_order_expired_at_venue() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-002");
    let venue_order_id = VenueOrderId::from("V-002");

    ctx.add_instrument(test_instrument());

    // Create and accept order locally
    let mut order =
        create_submitted_order("O-002", instrument_id, OrderSide::Sell, "2.0", "3100.00");

    let accepted = TestOrderEventStubs::accepted(&order, test_account_id(), venue_order_id);
    order.apply(accepted).unwrap();
    ctx.add_order(order);

    // Venue reports order expired
    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::Expired,
        Quantity::from("2.0"),
        Quantity::from("0"),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    assert!(matches!(result.events[0], OrderEventAny::Expired(_)));
}

#[rstest]
fn test_inflight_increments_retry_count_before_max() {
    let config = ExecutionManagerConfig {
        inflight_threshold_ms: 100,
        inflight_max_retries: 3,
        ..Default::default()
    };
    let mut ctx = TestContext::with_config(config);
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-001");

    ctx.add_instrument(test_instrument());
    let order = create_submitted_order("O-001", instrument_id, OrderSide::Buy, "1.0", "3000.00");
    ctx.add_order(order);

    ctx.manager.register_inflight(client_order_id);

    // First check - past threshold, retry count becomes 1
    ctx.advance_time(200_000_000);
    let events1 = ctx.manager.check_inflight_orders();
    assert!(events1.is_empty()); // Not at max yet

    // Second check - retry count becomes 2
    ctx.advance_time(200_000_000);
    let events2 = ctx.manager.check_inflight_orders();
    assert!(events2.is_empty()); // Still not at max

    // Third check - retry count becomes 3, equals max, generates rejection
    ctx.advance_time(200_000_000);
    let events3 = ctx.manager.check_inflight_orders();
    assert_eq!(events3.len(), 1);
    assert!(matches!(events3[0], OrderEventAny::Rejected(_)));
}

#[tokio::test]
async fn test_reconcile_mass_status_external_order_partially_filled() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();

    ctx.add_instrument(test_instrument());

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        None, // External order
        VenueOrderId::from("V-EXT-PARTIAL"),
        instrument_id,
        OrderStatus::PartiallyFilled,
        Quantity::from("10.0"),
        Quantity::from("3.0"),
    )
    .with_avg_px(3000.50)
    .unwrap();
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    // External orders get: Accepted + Filled (for the partial fill)
    assert_eq!(result.events.len(), 2);
    assert!(matches!(result.events[0], OrderEventAny::Accepted(_)));
    assert!(matches!(result.events[1], OrderEventAny::Filled(_)));

    if let OrderEventAny::Filled(filled) = &result.events[1] {
        assert_eq!(filled.last_qty, Quantity::from("3.0"));
        assert!(filled.reconciliation);
    }

    // Verify order was created in cache (status is Initialized since events haven't been applied)
    let client_order_id = ClientOrderId::from("V-EXT-PARTIAL");
    let order = ctx.get_order(&client_order_id);
    assert!(order.is_some());
}

#[tokio::test]
async fn test_reconcile_mass_status_order_already_in_sync() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-SYNC");
    let venue_order_id = VenueOrderId::from("V-SYNC");

    ctx.add_instrument(test_instrument());

    // Create accepted order locally
    let mut order =
        create_submitted_order("O-SYNC", instrument_id, OrderSide::Buy, "5.0", "3000.00");
    let accepted = TestOrderEventStubs::accepted(&order, test_account_id(), venue_order_id);
    order.apply(accepted).unwrap();
    ctx.add_order(order);

    // Venue reports exact same state
    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::Accepted,
        Quantity::from("5.0"),
        Quantity::from("0"),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    // No events needed - already in sync
    assert!(result.events.is_empty());
}

#[rstest]
fn test_clear_recon_tracking_removes_inflight() {
    let config = ExecutionManagerConfig {
        inflight_threshold_ms: 100,
        inflight_max_retries: 5,
        ..Default::default()
    };
    let mut ctx = TestContext::with_config(config);
    let client_order_id = ClientOrderId::from("O-001");

    ctx.manager.register_inflight(client_order_id);

    // Simulate order being resolved externally (e.g., accepted by venue)
    ctx.manager.clear_recon_tracking(&client_order_id, true);

    // Advance time past threshold
    ctx.advance_time(200_000_000);

    // Check should not generate events since order was cleared
    let events = ctx.manager.check_inflight_orders();
    assert!(events.is_empty());
}

/// Creates an accepted order with venue_order_id set
fn create_accepted_order(
    client_order_id: &str,
    instrument_id: InstrumentId,
    side: OrderSide,
    quantity: &str,
    price: &str,
    venue_order_id: VenueOrderId,
) -> OrderAny {
    let mut order = create_submitted_order(client_order_id, instrument_id, side, quantity, price);
    let accepted = TestOrderEventStubs::accepted(&order, test_account_id(), venue_order_id);
    order.apply(accepted).unwrap();
    order
}

#[tokio::test]
async fn test_inferred_fill_generated_when_venue_reports_filled() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-FILL-001");
    let venue_order_id = VenueOrderId::from("V-FILL-001");

    ctx.add_instrument(test_instrument());

    // Create accepted order with no fills yet
    let order = create_accepted_order(
        "O-FILL-001",
        instrument_id,
        OrderSide::Buy,
        "10.0",
        "3000.00",
        venue_order_id,
    );
    ctx.add_order(order);

    // Venue reports order as partially filled (no FillReport, just status)
    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::PartiallyFilled,
        Quantity::from("10.0"),
        Quantity::from("5.0"), // 5 filled
    )
    .with_avg_px(3001.50)
    .unwrap();
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    // Should generate an inferred fill
    assert_eq!(result.events.len(), 1);
    assert!(matches!(result.events[0], OrderEventAny::Filled(_)));

    if let OrderEventAny::Filled(filled) = &result.events[0] {
        assert_eq!(filled.client_order_id, client_order_id);
        assert_eq!(filled.last_qty, Quantity::from("5.0"));
        assert!(filled.reconciliation);
        assert!(filled.trade_id.as_str().starts_with("INFERRED-"));
    }
}

#[tokio::test]
async fn test_inferred_fill_uses_avg_px_for_first_fill() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-AVG-001");
    let venue_order_id = VenueOrderId::from("V-AVG-001");

    ctx.add_instrument(test_instrument());

    let order = create_accepted_order(
        "O-AVG-001",
        instrument_id,
        OrderSide::Buy,
        "10.0",
        "3000.00",
        venue_order_id,
    );
    ctx.add_order(order);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::PartiallyFilled,
        Quantity::from("10.0"),
        Quantity::from("3.0"),
    )
    .with_avg_px(2999.75)
    .unwrap();
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    if let OrderEventAny::Filled(filled) = &result.events[0] {
        // First fill should use avg_px directly
        assert_eq!(filled.last_px.as_f64(), 2999.75);
    }
}

#[tokio::test]
async fn test_no_inferred_fill_when_already_in_sync() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-SYNC-001");
    let venue_order_id = VenueOrderId::from("V-SYNC-001");

    ctx.add_instrument(test_instrument());

    // Create an order that is already partially filled
    let mut order = create_accepted_order(
        "O-SYNC-001",
        instrument_id,
        OrderSide::Buy,
        "10.0",
        "3000.00",
        venue_order_id,
    );

    // Apply a fill to the order
    let fill = TestOrderEventStubs::filled(
        &order,
        &test_instrument(),
        None,                        // trade_id
        None,                        // position_id
        None,                        // last_px
        Some(Quantity::from("5.0")), // last_qty
        None,                        // liquidity_side
        None,                        // commission
        None,                        // ts_filled_ns
        None,                        // account_id
    );
    order.apply(fill).unwrap();
    ctx.add_order(order);

    // Venue reports same fill state
    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::PartiallyFilled,
        Quantity::from("10.0"),
        Quantity::from("5.0"), // Same as local
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    // No events needed - already in sync
    assert!(result.events.is_empty());
}

#[tokio::test]
async fn test_fill_qty_mismatch_venue_less_logs_error() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-MISMATCH");
    let venue_order_id = VenueOrderId::from("V-MISMATCH");

    ctx.add_instrument(test_instrument());

    // Create an order that is already partially filled with 5
    let mut order = create_accepted_order(
        "O-MISMATCH",
        instrument_id,
        OrderSide::Buy,
        "10.0",
        "3000.00",
        venue_order_id,
    );
    let fill = TestOrderEventStubs::filled(
        &order,
        &test_instrument(),
        None,                        // trade_id
        None,                        // position_id
        None,                        // last_px
        Some(Quantity::from("5.0")), // last_qty
        None,                        // liquidity_side
        None,                        // commission
        None,                        // ts_filled_ns
        None,                        // account_id
    );
    order.apply(fill).unwrap();
    ctx.add_order(order);

    // Venue reports LESS filled than we have (anomaly)
    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::PartiallyFilled,
        Quantity::from("10.0"),
        Quantity::from("3.0"), // Less than our 5
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    // Should not generate events (error condition)
    assert!(result.events.is_empty());
}

#[tokio::test]
async fn test_market_order_inferred_fill_is_taker() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-MKT-001");
    let venue_order_id = VenueOrderId::from("V-MKT-001");

    ctx.add_instrument(test_instrument());

    // Create a market order (submitted and accepted)
    let mut order = OrderTestBuilder::new(OrderType::Market)
        .client_order_id(ClientOrderId::from("O-MKT-001"))
        .instrument_id(instrument_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from("10.0"))
        .build();
    let submitted = TestOrderEventStubs::submitted(&order, test_account_id());
    order.apply(submitted).unwrap();
    let accepted = TestOrderEventStubs::accepted(&order, test_account_id(), venue_order_id);
    order.apply(accepted).unwrap();
    ctx.add_order(order);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = OrderStatusReport::new(
        test_account_id(),
        instrument_id,
        Some(client_order_id),
        venue_order_id,
        OrderSide::Buy,
        OrderType::Market,
        TimeInForce::Ioc,
        OrderStatus::Filled,
        Quantity::from("10.0"),
        Quantity::from("10.0"),
        UnixNanos::default(),
        UnixNanos::default(),
        UnixNanos::default(),
        Some(UUID4::new()),
    )
    .with_avg_px(3005.00)
    .unwrap();
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    if let OrderEventAny::Filled(filled) = &result.events[0] {
        assert_eq!(filled.liquidity_side, LiquiditySide::Taker);
    }
}

#[tokio::test]
async fn test_pending_cancel_status_no_event() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-PEND-001");
    let venue_order_id = VenueOrderId::from("V-PEND-001");

    ctx.add_instrument(test_instrument());

    let order = create_accepted_order(
        "O-PEND-001",
        instrument_id,
        OrderSide::Buy,
        "10.0",
        "3000.00",
        venue_order_id,
    );
    ctx.add_order(order);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::PendingCancel,
        Quantity::from("10.0"),
        Quantity::from("0"),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    // Pending states don't generate events
    assert!(result.events.is_empty());
}

#[tokio::test]
async fn test_incremental_fill_calculates_weighted_price() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-INCR-001");
    let venue_order_id = VenueOrderId::from("V-INCR-001");

    ctx.add_instrument(test_instrument());

    // Create an order that already has 5 filled at 3000.00
    let mut order = create_accepted_order(
        "O-INCR-001",
        instrument_id,
        OrderSide::Buy,
        "10.0",
        "3000.00",
        venue_order_id,
    );
    let fill = TestOrderEventStubs::filled(
        &order,
        &test_instrument(),
        None,                         // trade_id
        None,                         // position_id
        Some(Price::from("3000.00")), // last_px
        Some(Quantity::from("5.0")),  // last_qty
        None,                         // liquidity_side
        None,                         // commission
        None,                         // ts_filled_ns
        None,                         // account_id
    );
    order.apply(fill).unwrap();
    ctx.add_order(order);

    // Venue reports 8 filled total at avg_px 3002.50
    // Original: 5 @ 3000.00 = 15000
    // New avg: 8 @ 3002.50 = 24020
    // Incremental: 3 @ (24020 - 15000) / 3 = 3006.67
    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::PartiallyFilled,
        Quantity::from("10.0"),
        Quantity::from("8.0"),
    )
    .with_avg_px(3002.50)
    .unwrap();
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    if let OrderEventAny::Filled(filled) = &result.events[0] {
        assert_eq!(filled.last_qty, Quantity::from("3.0"));
        // (8 * 3002.50 - 5 * 3000.00) / 3 ≈ 3006.67
        let expected_px = (8.0 * 3002.50 - 5.0 * 3000.00) / 3.0;
        assert!((filled.last_px.as_f64() - expected_px).abs() < 0.01);
    }
}

#[rstest]
#[tokio::test]
async fn test_mass_status_skips_exact_duplicate_orders() {
    let mut ctx = TestContext::new();
    ctx.add_instrument(test_instrument());

    let client_order_id = ClientOrderId::from("O-001");
    let venue_order_id = VenueOrderId::from("V-001");
    let instrument_id = test_instrument_id();

    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .client_order_id(client_order_id)
        .instrument_id(instrument_id)
        .quantity(Quantity::from("1.0"))
        .price(Price::from("100.0"))
        .build();
    let submitted = TestOrderEventStubs::submitted(&order, test_account_id());
    order.apply(submitted).unwrap();
    let accepted = TestOrderEventStubs::accepted(&order, test_account_id(), venue_order_id);
    order.apply(accepted).unwrap();
    ctx.add_order(order);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );
    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::Accepted,
        Quantity::from("1.0"),
        Quantity::from("0.0"),
    )
    .with_price(Price::from("100.0"));
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert!(result.events.is_empty());
}

#[rstest]
#[tokio::test]
async fn test_mass_status_deduplicates_within_batch() {
    let mut ctx = TestContext::new();
    ctx.add_instrument(test_instrument());

    let client_order_id = ClientOrderId::from("O-001");
    let venue_order_id = VenueOrderId::from("V-001");
    let instrument_id = test_instrument_id();

    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .client_order_id(client_order_id)
        .instrument_id(instrument_id)
        .quantity(Quantity::from("1.0"))
        .price(Price::from("100.0"))
        .build();
    let submitted = TestOrderEventStubs::submitted(&order, test_account_id());
    order.apply(submitted).unwrap();
    ctx.add_order(order);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let report1 = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::Accepted,
        Quantity::from("1.0"),
        Quantity::from("0.0"),
    );
    let report2 = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::Accepted,
        Quantity::from("1.0"),
        Quantity::from("0.0"),
    );
    mass_status.add_order_reports(vec![report1, report2]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    assert!(matches!(result.events[0], OrderEventAny::Accepted(_)));
}

#[rstest]
#[tokio::test]
async fn test_mass_status_reconciles_when_status_differs() {
    let mut ctx = TestContext::new();
    ctx.add_instrument(test_instrument());

    let client_order_id = ClientOrderId::from("O-001");
    let venue_order_id = VenueOrderId::from("V-001");
    let instrument_id = test_instrument_id();

    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .client_order_id(client_order_id)
        .instrument_id(instrument_id)
        .quantity(Quantity::from("1.0"))
        .price(Price::from("100.0"))
        .build();
    let submitted = TestOrderEventStubs::submitted(&order, test_account_id());
    order.apply(submitted).unwrap();
    ctx.add_order(order);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );
    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::Canceled,
        Quantity::from("1.0"),
        Quantity::from("0.0"),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    assert!(matches!(result.events[0], OrderEventAny::Canceled(_)));
}

#[rstest]
#[tokio::test]
async fn test_mass_status_reconciles_when_filled_qty_differs() {
    let mut ctx = TestContext::new();
    ctx.add_instrument(test_instrument());

    let client_order_id = ClientOrderId::from("O-001");
    let venue_order_id = VenueOrderId::from("V-001");
    let instrument_id = test_instrument_id();

    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .client_order_id(client_order_id)
        .instrument_id(instrument_id)
        .quantity(Quantity::from("10.0"))
        .price(Price::from("100.0"))
        .build();
    let submitted = TestOrderEventStubs::submitted(&order, test_account_id());
    order.apply(submitted).unwrap();
    let accepted = TestOrderEventStubs::accepted(&order, test_account_id(), venue_order_id);
    order.apply(accepted).unwrap();
    ctx.add_order(order);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );
    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::PartiallyFilled,
        Quantity::from("10.0"),
        Quantity::from("5.0"),
    )
    .with_avg_px(100.0)
    .unwrap();
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    if let OrderEventAny::Filled(filled) = &result.events[0] {
        assert_eq!(filled.last_qty, Quantity::from("5.0"));
    } else {
        panic!("Expected OrderFilled event");
    }
}

#[rstest]
#[tokio::test]
async fn test_mass_status_matches_order_by_venue_order_id() {
    let mut ctx = TestContext::new();
    ctx.add_instrument(test_instrument());

    let client_order_id = ClientOrderId::from("O-001");
    let venue_order_id = VenueOrderId::from("V-001");
    let instrument_id = test_instrument_id();

    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .client_order_id(client_order_id)
        .instrument_id(instrument_id)
        .quantity(Quantity::from("1.0"))
        .price(Price::from("100.0"))
        .build();
    let submitted = TestOrderEventStubs::submitted(&order, test_account_id());
    order.apply(submitted).unwrap();
    let accepted = TestOrderEventStubs::accepted(&order, test_account_id(), venue_order_id);
    order.apply(accepted).unwrap();
    ctx.add_order(order);

    ctx.cache
        .borrow_mut()
        .add_venue_order_id(&client_order_id, &venue_order_id, false)
        .unwrap();

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );
    let report = create_order_status_report(
        None,
        venue_order_id,
        instrument_id,
        OrderStatus::Canceled,
        Quantity::from("1.0"),
        Quantity::from("0.0"),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    assert!(matches!(result.events[0], OrderEventAny::Canceled(_)));
    if let OrderEventAny::Canceled(canceled) = &result.events[0] {
        assert_eq!(canceled.client_order_id, client_order_id);
    }
}

#[rstest]
#[tokio::test]
async fn test_mass_status_matches_order_by_venue_order_id_with_mismatched_client_id() {
    let mut ctx = TestContext::new();
    ctx.add_instrument(test_instrument());

    let client_order_id = ClientOrderId::from("O-001");
    let venue_order_id = VenueOrderId::from("V-001");
    let instrument_id = test_instrument_id();

    let mut order = OrderTestBuilder::new(OrderType::Limit)
        .client_order_id(client_order_id)
        .instrument_id(instrument_id)
        .quantity(Quantity::from("1.0"))
        .price(Price::from("100.0"))
        .build();
    let submitted = TestOrderEventStubs::submitted(&order, test_account_id());
    order.apply(submitted).unwrap();
    let accepted = TestOrderEventStubs::accepted(&order, test_account_id(), venue_order_id);
    order.apply(accepted).unwrap();
    ctx.add_order(order);

    ctx.cache
        .borrow_mut()
        .add_venue_order_id(&client_order_id, &venue_order_id, false)
        .unwrap();

    // Report has wrong client_order_id but correct venue_order_id
    let wrong_client_order_id = ClientOrderId::from("O-WRONG");
    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );
    let report = create_order_status_report(
        Some(wrong_client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::Canceled,
        Quantity::from("1.0"),
        Quantity::from("0.0"),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 1);
    assert!(matches!(result.events[0], OrderEventAny::Canceled(_)));
    if let OrderEventAny::Canceled(canceled) = &result.events[0] {
        assert_eq!(canceled.client_order_id, client_order_id);
    }
}

#[tokio::test]
async fn test_reconcile_mass_status_indexes_venue_order_id_for_accepted_orders() {
    // Test that venue_order_id is properly indexed during reconciliation for orders
    // that are already in ACCEPTED state and don't generate new OrderAccepted events.
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let instrument = test_instrument();
    ctx.add_instrument(instrument.clone());

    let client_order_id = ClientOrderId::from("O-TEST");
    let venue_order_id = VenueOrderId::from("V-123");

    let mut order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.0"))
        .client_order_id(client_order_id)
        .build();

    let submitted = TestOrderEventStubs::submitted(&order, test_account_id());
    order.apply(submitted).unwrap();

    let accepted = TestOrderEventStubs::accepted(&order, test_account_id(), venue_order_id);
    order.apply(accepted).unwrap();
    ctx.add_order(order.clone());

    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::Accepted,
        Quantity::from("1.0"),
        Quantity::from("0.0"),
    );

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        Venue::from("SIM"),
        UnixNanos::default(),
        Some(UUID4::new()),
    );
    mass_status.add_order_reports(vec![report]);

    let _events = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(
        ctx.cache.borrow().client_order_id(&venue_order_id),
        Some(&client_order_id),
        "venue_order_id should be indexed after reconciliation"
    );
}

#[tokio::test]
async fn test_reconcile_mass_status_indexes_venue_order_id_for_external_orders() {
    // Test that venue_order_id is properly indexed for external orders discovered
    // during reconciliation.
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let instrument = test_instrument();
    ctx.add_instrument(instrument.clone());

    let venue_order_id = VenueOrderId::from("V-EXT-001");

    let report = create_order_status_report(
        None,
        venue_order_id,
        instrument_id,
        OrderStatus::Accepted,
        Quantity::from("1.0"),
        Quantity::from("0.0"),
    );

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        Venue::from("SIM"),
        UnixNanos::default(),
        Some(UUID4::new()),
    );
    mass_status.add_order_reports(vec![report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert!(
        !result.events.is_empty(),
        "Should generate events for external order"
    );

    let cache_borrow = ctx.cache.borrow();
    let indexed_client_id = cache_borrow.client_order_id(&venue_order_id);
    assert!(
        indexed_client_id.is_some(),
        "venue_order_id should be indexed for external order"
    );
}

#[tokio::test]
async fn test_reconcile_mass_status_indexes_venue_order_id_for_filled_orders() {
    // Test that venue_order_id is properly indexed for orders that are already
    // FILLED and don't generate new OrderAccepted events.
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let instrument = test_instrument();
    ctx.add_instrument(instrument.clone());

    let client_order_id = ClientOrderId::from("O-FILLED");
    let venue_order_id = VenueOrderId::from("V-456");

    // Create order and process to FILLED state
    let mut order = OrderTestBuilder::new(OrderType::Market)
        .instrument_id(instrument_id)
        .side(OrderSide::Buy)
        .quantity(Quantity::from("1.0"))
        .client_order_id(client_order_id)
        .build();

    let submitted = TestOrderEventStubs::submitted(&order, test_account_id());
    order.apply(submitted).unwrap();

    let accepted = TestOrderEventStubs::accepted(&order, test_account_id(), venue_order_id);
    order.apply(accepted).unwrap();

    let filled = TestOrderEventStubs::filled(
        &order,
        &instrument,
        Some(TradeId::from("T-1")),
        None,                          // position_id
        Some(Price::from("1.0")),      // last_px
        Some(Quantity::from("1.0")),   // last_qty
        Some(LiquiditySide::Taker),    // liquidity_side
        Some(Money::from("0.01 USD")), // commission
        None,                          // ts_filled_ns
        Some(test_account_id()),
    );
    order.apply(filled).unwrap();
    ctx.add_order(order.clone());

    let report = create_order_status_report(
        Some(client_order_id),
        venue_order_id,
        instrument_id,
        OrderStatus::Filled,
        Quantity::from("1.0"),
        Quantity::from("1.0"),
    );

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        Venue::from("SIM"),
        UnixNanos::default(),
        Some(UUID4::new()),
    );
    mass_status.add_order_reports(vec![report]);

    let _events = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    let cache_borrow = ctx.cache.borrow();
    assert_eq!(
        cache_borrow.client_order_id(&venue_order_id),
        Some(&client_order_id),
        "venue_order_id should be indexed for filled order"
    );
}

#[tokio::test]
async fn test_reconcile_mass_status_skips_orders_without_loaded_instruments() {
    // Test that reconciliation properly skips orders for instruments that aren't loaded,
    // and these skipped orders don't cause validation warnings.
    let mut ctx = TestContext::new();
    let loaded_instrument_id = test_instrument_id();
    let loaded_instrument = test_instrument();
    ctx.add_instrument(loaded_instrument.clone());

    let unloaded_instrument_id = InstrumentId::from("BTCUSDT.SIM");

    let loaded_venue_order_id = VenueOrderId::from("V-LOADED");
    let unloaded_venue_order_id = VenueOrderId::from("V-UNLOADED");

    let loaded_report = create_order_status_report(
        Some(ClientOrderId::from("O-LOADED")),
        loaded_venue_order_id,
        loaded_instrument_id,
        OrderStatus::Filled,
        Quantity::from("1.0"),
        Quantity::from("1.0"),
    );

    let unloaded_report = create_order_status_report(
        Some(ClientOrderId::from("O-UNLOADED")),
        unloaded_venue_order_id,
        unloaded_instrument_id,
        OrderStatus::Filled,
        Quantity::from("1.0"),
        Quantity::from("1.0"),
    );

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        Venue::from("SIM"),
        UnixNanos::default(),
        Some(UUID4::new()),
    );
    mass_status.add_order_reports(vec![loaded_report, unloaded_report]);

    let _events = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    let cache_borrow = ctx.cache.borrow();
    let loaded_client_id = cache_borrow.client_order_id(&loaded_venue_order_id);
    assert!(
        loaded_client_id.is_some(),
        "Loaded instrument order should be indexed"
    );

    let unloaded_client_id = cache_borrow.client_order_id(&unloaded_venue_order_id);
    assert!(
        unloaded_client_id.is_none(),
        "Unloaded instrument order should not be indexed (skipped during reconciliation)"
    );
}

#[tokio::test]
async fn test_reconcile_mass_status_creates_position_from_position_report() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    ctx.add_instrument(test_instrument());

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    // Add a position report with no corresponding order reports
    let position_report = PositionStatusReport::new(
        test_account_id(),
        instrument_id,
        PositionSideSpecified::Long,
        Quantity::from("5.0"),
        UnixNanos::from(1_000_000),
        UnixNanos::from(1_000_000),
        None,
        None,
        Some(dec!(3000.50)),
    );
    mass_status.add_position_reports(vec![position_report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    // Should generate Accepted + Filled events to create the position
    assert_eq!(result.events.len(), 2);
    assert!(matches!(result.events[0], OrderEventAny::Accepted(_)));
    assert!(matches!(result.events[1], OrderEventAny::Filled(_)));

    if let OrderEventAny::Filled(filled) = &result.events[1] {
        assert_eq!(filled.last_qty, Quantity::from("5.0"));
        assert_eq!(filled.last_px.as_f64(), 3000.50);
        assert!(filled.reconciliation);
    }
}

#[tokio::test]
async fn test_reconcile_mass_status_skips_flat_position_report() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    ctx.add_instrument(test_instrument());

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    // Add a flat position report
    let position_report = PositionStatusReport::new(
        test_account_id(),
        instrument_id,
        PositionSideSpecified::Flat,
        Quantity::from("0"),
        UnixNanos::from(1_000_000),
        UnixNanos::from(1_000_000),
        None,
        None,
        None,
    );
    mass_status.add_position_reports(vec![position_report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    // No events should be generated for flat position
    assert!(result.events.is_empty());
}

#[tokio::test]
async fn test_reconcile_mass_status_skips_position_report_when_filtered() {
    let config = ExecutionManagerConfig {
        filter_position_reports: true,
        ..Default::default()
    };
    let mut ctx = TestContext::with_config(config);
    let instrument_id = test_instrument_id();
    ctx.add_instrument(test_instrument());

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    let position_report = PositionStatusReport::new(
        test_account_id(),
        instrument_id,
        PositionSideSpecified::Long,
        Quantity::from("5.0"),
        UnixNanos::from(1_000_000),
        UnixNanos::from(1_000_000),
        None,
        None,
        Some(dec!(3000.50)),
    );
    mass_status.add_position_reports(vec![position_report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    // Position reports should be filtered
    assert!(result.events.is_empty());
}

#[tokio::test]
async fn test_reconcile_mass_status_creates_short_position_from_report() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    ctx.add_instrument(test_instrument());

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    // Add a short position report
    let position_report = PositionStatusReport::new(
        test_account_id(),
        instrument_id,
        PositionSideSpecified::Short,
        Quantity::from("3.0"),
        UnixNanos::from(1_000_000),
        UnixNanos::from(1_000_000),
        None,
        None,
        Some(dec!(2950.25)),
    );
    mass_status.add_position_reports(vec![position_report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    assert_eq!(result.events.len(), 2);
    assert!(matches!(result.events[0], OrderEventAny::Accepted(_)));
    assert!(matches!(result.events[1], OrderEventAny::Filled(_)));

    if let OrderEventAny::Filled(filled) = &result.events[1] {
        assert_eq!(filled.last_qty, Quantity::from("3.0"));
        assert_eq!(filled.order_side, OrderSide::Sell);
    }
}

#[tokio::test]
async fn test_reconcile_mass_status_skips_position_report_when_fills_exist() {
    let mut ctx = TestContext::new();
    let instrument_id = test_instrument_id();
    let client_order_id = ClientOrderId::from("O-001");
    let venue_order_id = VenueOrderId::from("V-001");

    ctx.add_instrument(test_instrument());
    let order = create_limit_order("O-001", instrument_id, OrderSide::Buy, "5.0", "3000.00");
    ctx.add_order(order);

    let mut mass_status = ExecutionMassStatus::new(
        test_client_id(),
        test_account_id(),
        test_venue(),
        UnixNanos::default(),
        Some(UUID4::new()),
    );

    // Add a fill report for 5.0 qty
    let fill = FillReport::new(
        test_account_id(),
        instrument_id,
        venue_order_id,
        TradeId::from("T-001"),
        OrderSide::Buy,
        Quantity::from("5.0"),
        Price::from("3000.00"),
        Money::from("0.50 USDT"),
        LiquiditySide::Maker,
        Some(client_order_id),
        None,
        UnixNanos::from(1_000_000),
        UnixNanos::from(1_000_000),
        None,
    );
    mass_status.add_fill_reports(vec![fill]);

    // Add a position report for the same instrument (would duplicate if not skipped)
    let position_report = PositionStatusReport::new(
        test_account_id(),
        instrument_id,
        PositionSideSpecified::Long,
        Quantity::from("5.0"),
        UnixNanos::from(1_000_000),
        UnixNanos::from(1_000_000),
        None,
        None,
        Some(dec!(3000.00)),
    );
    mass_status.add_position_reports(vec![position_report]);

    let result = ctx
        .manager
        .reconcile_execution_mass_status(mass_status)
        .await;

    // Should only have 1 fill event from the fill report, not additional events
    // from the position report (which would double-count)
    assert_eq!(result.events.len(), 1);
    assert!(matches!(result.events[0], OrderEventAny::Filled(_)));
}
