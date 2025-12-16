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

//! Internal WebSocket message types.
//!
//! These types represent the internal message flow between the WebSocket client
//! and handler, including subscription management and data events.

// Re-export SBE stream types for convenience
pub use crate::common::sbe::stream::{
    BestBidAskStreamEvent, DepthDiffStreamEvent, DepthSnapshotStreamEvent, PriceLevel, Trade,
    TradesStreamEvent,
};

// TODO: Add internal message types
// - SubscriptionRequest
// - UnsubscriptionRequest
// - ConnectionState
// - etc.
