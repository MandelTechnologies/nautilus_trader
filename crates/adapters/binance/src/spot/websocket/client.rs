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

//! Binance Spot WebSocket client for SBE market data streams.
//!
//! ## Connection Details
//!
//! - Endpoint: `stream-sbe.binance.com` or `stream-sbe.binance.com:9443`
//! - Authentication: Ed25519 API key in `X-MBX-APIKEY` header
//! - Max streams: 1024 per connection
//! - Connection validity: 24 hours
//! - Ping/pong: Every 20 seconds

/// SBE stream endpoint.
pub const SBE_STREAM_URL: &str = "wss://stream-sbe.binance.com";

/// Binance Spot WebSocket client for SBE market data streams.
#[derive(Debug)]
pub struct BinanceSpotWebSocketClient {
    // TODO: Add fields
    // - ws_client: WebSocketClient
    // - subscriptions: AHashMap<String, Subscription>
    // - handler: BinanceSpotWebSocketHandler
    _private: (),
}

impl BinanceSpotWebSocketClient {
    /// Create a new Binance Spot WebSocket client.
    #[must_use]
    pub fn new() -> Self {
        Self { _private: () }
    }
}

impl Default for BinanceSpotWebSocketClient {
    fn default() -> Self {
        Self::new()
    }
}
