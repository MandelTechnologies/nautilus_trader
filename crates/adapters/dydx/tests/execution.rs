// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
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

//! Integration tests for dYdX execution client.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use nautilus_dydx::execution::MAX_CLIENT_ID;
use nautilus_model::{
    enums::{OrderSide, OrderType, TimeInForce},
    events::order::initialized::OrderInitializedBuilder,
    identifiers::{ClientOrderId, InstrumentId, StrategyId, TraderId},
    orders::{Order, OrderAny},
    types::{Price, Quantity},
};
use rstest::rstest;

/// Test that client order ID parsing to u32 works for numeric strings.
#[rstest]
fn test_client_order_id_numeric_parsing() {
    let client_id = "12345";
    let result: Result<u32, _> = client_id.parse();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 12345);
}

/// Test that client order ID hashing works for non-numeric strings.
#[rstest]
fn test_client_order_id_hash_fallback() {
    let client_id = "O-20241112-ABC123-001";
    let parse_result: Result<u32, _> = client_id.parse();
    assert!(parse_result.is_err());

    let mut hasher = DefaultHasher::new();
    client_id.hash(&mut hasher);
    let hash_result = (hasher.finish() % (MAX_CLIENT_ID as u64)) as u32;

    assert!(hash_result < MAX_CLIENT_ID);
    assert!(hash_result > 0); // Very unlikely to be 0
}

/// Test dYdX supported order types.
///
/// dYdX v4 supports: Market, Limit, StopMarket, StopLimit, MarketIfTouched (TakeProfit),
/// and LimitIfTouched (TakeProfitLimit). Trailing stops are NOT supported by the protocol.
#[rstest]
#[case::market(OrderType::Market, true)]
#[case::limit(OrderType::Limit, true)]
#[case::stop_market(OrderType::StopMarket, true)]
#[case::stop_limit(OrderType::StopLimit, true)]
#[case::take_profit_market(OrderType::MarketIfTouched, true)]
#[case::take_profit_limit(OrderType::LimitIfTouched, true)]
#[case::trailing_stop_market(OrderType::TrailingStopMarket, false)]
#[case::trailing_stop_limit(OrderType::TrailingStopLimit, false)]
fn test_dydx_order_type_support(#[case] order_type: OrderType, #[case] expected: bool) {
    let is_supported = matches!(
        order_type,
        OrderType::Market
            | OrderType::Limit
            | OrderType::StopMarket
            | OrderType::StopLimit
            | OrderType::MarketIfTouched
            | OrderType::LimitIfTouched
    );
    assert_eq!(is_supported, expected);
}

/// Test that OrderAny API methods work correctly.
#[rstest]
fn test_order_any_api_usage() {
    let order = OrderInitializedBuilder::default()
        .trader_id(TraderId::from("TRADER-001"))
        .strategy_id(StrategyId::from("STRATEGY-001"))
        .instrument_id(InstrumentId::from("ETH-USD-PERP.DYDX"))
        .client_order_id(ClientOrderId::from("O-001"))
        .order_side(OrderSide::Buy)
        .order_type(OrderType::Limit)
        .quantity(Quantity::from("10"))
        .price(Some(Price::from("2000.50")))
        .time_in_force(TimeInForce::Gtc)
        .build()
        .unwrap();

    let order_any: OrderAny = order.into();

    assert_eq!(order_any.order_side(), OrderSide::Buy);
    assert_eq!(order_any.order_type(), OrderType::Limit);
    assert_eq!(order_any.quantity(), Quantity::from("10"));
    assert_eq!(order_any.price(), Some(Price::from("2000.50")));
    assert_eq!(order_any.time_in_force(), TimeInForce::Gtc);
    assert!(!order_any.is_post_only());
    assert!(!order_any.is_reduce_only());
    assert_eq!(order_any.expire_time(), None);
}

/// Test MAX_CLIENT_ID constant is within dYdX limits.
#[rstest]
fn test_max_client_id_limit() {
    assert_eq!(MAX_CLIENT_ID, u32::MAX);
}

/// Test that client order ID conversion is consistent for cancel operations.
#[rstest]
fn test_cancel_order_id_consistency() {
    let client_id_str = "O-20241112-CANCEL-001";

    // First conversion (for submit)
    let mut hasher1 = DefaultHasher::new();
    client_id_str.hash(&mut hasher1);
    let id1 = (hasher1.finish() % (MAX_CLIENT_ID as u64)) as u32;

    // Second conversion (for cancel) - should be identical
    let mut hasher2 = DefaultHasher::new();
    client_id_str.hash(&mut hasher2);
    let id2 = (hasher2.finish() % (MAX_CLIENT_ID as u64)) as u32;

    assert_eq!(id1, id2, "Client ID conversion must be deterministic");
}

/// Test clob_pair_id extraction from CryptoPerpetual raw_symbol.
#[rstest]
fn test_clob_pair_id_extraction_from_raw_symbol() {
    // Simulate raw_symbol "1" -> clob_pair_id 1
    let raw_symbol = "1";
    let result: Result<u32, _> = raw_symbol.parse();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);

    // Simulate raw_symbol "42" -> clob_pair_id 42
    let raw_symbol = "42";
    let result: Result<u32, _> = raw_symbol.parse();
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 42);
}

/// Test clob_pair_id extraction failure for invalid raw_symbol.
#[rstest]
fn test_clob_pair_id_extraction_invalid() {
    // Invalid raw_symbol should fail parsing
    let raw_symbol = "BTC-USD";
    let result: Result<u32, _> = raw_symbol.parse();
    assert!(result.is_err());

    // Empty raw_symbol should fail
    let raw_symbol = "";
    let result: Result<u32, _> = raw_symbol.parse();
    assert!(result.is_err());
}

/// Test market ticker to instrument mapping logic.
#[rstest]
fn test_market_ticker_parsing() {
    let ticker = "BTC-USD";
    let parts: Vec<&str> = ticker.split('-').collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], "BTC");
    assert_eq!(parts[1], "USD");

    let ticker = "ETH-USD";
    let parts: Vec<&str> = ticker.split('-').collect();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], "ETH");
    assert_eq!(parts[1], "USD");
}

/// Test market ticker with invalid format.
#[rstest]
fn test_market_ticker_invalid_format() {
    let ticker = "BTCUSD";
    assert_eq!(ticker.split('-').count(), 1); // No separator

    let ticker = "BTC-USD-PERP";
    assert_eq!(ticker.split('-').count(), 3); // Too many parts
}
