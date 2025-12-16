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

//! Binance Spot WebSocket message handler.
//!
//! This handler processes incoming SBE binary frames and routes them to the
//! appropriate decoder based on the message template ID. All decoders validate
//! schema ID and return errors for malformed/truncated data instead of panicking.

use crate::common::sbe::stream::{
    BestBidAskStreamEvent, DepthDiffStreamEvent, DepthSnapshotStreamEvent, MessageHeader,
    StreamDecodeError, TradesStreamEvent, template_id,
};

/// Decoded market data message.
#[derive(Debug)]
pub enum MarketDataMessage {
    /// Trade event.
    Trades(TradesStreamEvent),
    /// Best bid/ask update.
    BestBidAsk(BestBidAskStreamEvent),
    /// Order book snapshot.
    DepthSnapshot(DepthSnapshotStreamEvent),
    /// Order book diff update.
    DepthDiff(DepthDiffStreamEvent),
}

/// Decode an SBE binary frame into a market data message.
///
/// Validates the message header (including schema ID) and routes to the
/// appropriate decoder based on template ID. All decode operations are
/// bounds-checked and will return errors for malformed/truncated data.
///
/// # Errors
///
/// Returns an error if:
/// - Buffer is too short to contain message header
/// - Schema ID doesn't match expected stream schema
/// - Template ID is unknown
/// - Message body is malformed or truncated
/// - Group counts exceed safety limits
pub fn decode_market_data(buf: &[u8]) -> Result<MarketDataMessage, StreamDecodeError> {
    let header = MessageHeader::decode(buf)?;
    header.validate_schema()?;

    // Each decoder also validates schema internally, but we check here first
    // to give a better error for unknown templates
    match header.template_id {
        template_id::TRADES_STREAM_EVENT => {
            Ok(MarketDataMessage::Trades(TradesStreamEvent::decode(buf)?))
        }
        template_id::BEST_BID_ASK_STREAM_EVENT => Ok(MarketDataMessage::BestBidAsk(
            BestBidAskStreamEvent::decode(buf)?,
        )),
        template_id::DEPTH_SNAPSHOT_STREAM_EVENT => Ok(MarketDataMessage::DepthSnapshot(
            DepthSnapshotStreamEvent::decode(buf)?,
        )),
        template_id::DEPTH_DIFF_STREAM_EVENT => Ok(MarketDataMessage::DepthDiff(
            DepthDiffStreamEvent::decode(buf)?,
        )),
        _ => Err(StreamDecodeError::UnknownTemplateId(header.template_id)),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::common::sbe::stream::STREAM_SCHEMA_ID;

    #[rstest]
    fn test_decode_empty_buffer() {
        let err = decode_market_data(&[]).unwrap_err();
        assert!(matches!(err, StreamDecodeError::BufferTooShort { .. }));
    }

    #[rstest]
    fn test_decode_short_buffer() {
        let buf = [0u8; 5];
        let err = decode_market_data(&buf).unwrap_err();
        assert!(matches!(err, StreamDecodeError::BufferTooShort { .. }));
    }

    #[rstest]
    fn test_decode_wrong_schema() {
        let mut buf = [0u8; 100];
        buf[0..2].copy_from_slice(&50u16.to_le_bytes()); // block_length
        buf[2..4].copy_from_slice(&template_id::BEST_BID_ASK_STREAM_EVENT.to_le_bytes());
        buf[4..6].copy_from_slice(&99u16.to_le_bytes()); // Wrong schema
        buf[6..8].copy_from_slice(&0u16.to_le_bytes()); // version

        let err = decode_market_data(&buf).unwrap_err();
        assert!(matches!(err, StreamDecodeError::SchemaMismatch { .. }));
    }

    #[rstest]
    fn test_decode_unknown_template() {
        let mut buf = [0u8; 100];
        buf[0..2].copy_from_slice(&50u16.to_le_bytes()); // block_length
        buf[2..4].copy_from_slice(&9999u16.to_le_bytes()); // Unknown template
        buf[4..6].copy_from_slice(&STREAM_SCHEMA_ID.to_le_bytes());
        buf[6..8].copy_from_slice(&0u16.to_le_bytes()); // version

        let err = decode_market_data(&buf).unwrap_err();
        assert!(matches!(err, StreamDecodeError::UnknownTemplateId(9999)));
    }
}
