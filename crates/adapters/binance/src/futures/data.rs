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

//! Live market data client implementation for the Binance Futures adapter.

use std::sync::{
    Arc, RwLock,
    atomic::{AtomicBool, Ordering},
};

use ahash::AHashMap;
use anyhow::Context;
use futures_util::{StreamExt, pin_mut};
use nautilus_common::{
    clients::DataClient,
    live::{runner::get_data_event_sender, runtime::get_runtime},
    messages::{
        DataEvent,
        data::{
            DataResponse, InstrumentResponse, InstrumentsResponse, RequestBars, RequestInstrument,
            RequestInstruments, RequestTrades, SubscribeBars, SubscribeBookDeltas,
            SubscribeInstrument, SubscribeInstruments, SubscribeQuotes, SubscribeTrades,
            UnsubscribeBars, UnsubscribeBookDeltas, UnsubscribeQuotes, UnsubscribeTrades,
        },
    },
};
use nautilus_core::{
    MUTEX_POISONED,
    datetime::datetime_to_unix_nanos,
    time::{AtomicTime, get_atomic_clock_realtime},
};
use nautilus_model::{
    data::{Data, OrderBookDeltas_API},
    enums::BookType,
    identifiers::{ClientId, InstrumentId, Venue},
    instruments::{Instrument, InstrumentAny},
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    common::{
        consts::BINANCE_VENUE, enums::BinanceProductType, parse::bar_spec_to_binance_interval,
        symbol::format_binance_stream_symbol,
    },
    config::BinanceDataClientConfig,
    futures::{
        http::client::BinanceFuturesHttpClient,
        websocket::{
            client::BinanceFuturesWebSocketClient,
            messages::{BinanceFuturesWsMessage, NautilusFuturesDataWsMessage},
        },
    },
};

/// Binance Futures data client for USD-M and COIN-M markets.
#[derive(Debug)]
pub struct BinanceFuturesDataClient {
    clock: &'static AtomicTime,
    client_id: ClientId,
    config: BinanceDataClientConfig,
    product_type: BinanceProductType,
    http_client: BinanceFuturesHttpClient,
    ws_client: BinanceFuturesWebSocketClient,
    data_sender: tokio::sync::mpsc::UnboundedSender<DataEvent>,
    is_connected: AtomicBool,
    cancellation_token: CancellationToken,
    tasks: Vec<JoinHandle<()>>,
    instruments: Arc<RwLock<AHashMap<InstrumentId, InstrumentAny>>>,
}

impl BinanceFuturesDataClient {
    /// Creates a new [`BinanceFuturesDataClient`] instance.
    ///
    /// # Errors
    ///
    /// Returns an error if the client fails to initialize or if the product type
    /// is not a futures type (UsdM or CoinM).
    pub fn new(
        client_id: ClientId,
        config: BinanceDataClientConfig,
        product_type: BinanceProductType,
    ) -> anyhow::Result<Self> {
        match product_type {
            BinanceProductType::UsdM | BinanceProductType::CoinM => {}
            _ => {
                anyhow::bail!(
                    "BinanceFuturesDataClient requires UsdM or CoinM product type, got {product_type:?}"
                );
            }
        }

        let clock = get_atomic_clock_realtime();
        let data_sender = get_data_event_sender();

        let http_client = BinanceFuturesHttpClient::new(
            product_type,
            config.environment,
            config.api_key.clone(),
            config.api_secret.clone(),
            config.base_url_http.clone(),
            None, // recv_window
            None, // timeout_secs
            None, // proxy_url
        )?;

        let ws_client = BinanceFuturesWebSocketClient::new(
            product_type,
            config.environment,
            config.api_key.clone(),
            config.api_secret.clone(),
            config.base_url_ws.clone(),
            Some(20), // Heartbeat interval
        )?;

        Ok(Self {
            clock,
            client_id,
            config,
            product_type,
            http_client,
            ws_client,
            data_sender,
            is_connected: AtomicBool::new(false),
            cancellation_token: CancellationToken::new(),
            tasks: Vec::new(),
            instruments: Arc::new(RwLock::new(AHashMap::new())),
        })
    }

    fn venue(&self) -> Venue {
        *BINANCE_VENUE
    }

    fn send_data(sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>, data: Data) {
        if let Err(e) = sender.send(DataEvent::Data(data)) {
            log::error!("Failed to emit data event: {e}");
        }
    }

    fn spawn_ws<F>(&self, fut: F, context: &'static str)
    where
        F: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        get_runtime().spawn(async move {
            if let Err(e) = fut.await {
                log::error!("{context}: {e:?}");
            }
        });
    }

    fn handle_ws_message(
        message: BinanceFuturesWsMessage,
        data_sender: &tokio::sync::mpsc::UnboundedSender<DataEvent>,
        instruments: &Arc<RwLock<AHashMap<InstrumentId, InstrumentAny>>>,
    ) {
        match message {
            BinanceFuturesWsMessage::Data(data_msg) => match data_msg {
                NautilusFuturesDataWsMessage::Data(payloads) => {
                    for data in payloads {
                        Self::send_data(data_sender, data);
                    }
                }
                NautilusFuturesDataWsMessage::Deltas(deltas) => {
                    Self::send_data(data_sender, Data::Deltas(OrderBookDeltas_API::new(deltas)));
                }
                NautilusFuturesDataWsMessage::Instrument(instrument) => {
                    upsert_instrument(instruments, *instrument);
                }
                NautilusFuturesDataWsMessage::RawJson(value) => {
                    log::debug!("Unhandled JSON message: {value:?}");
                }
            },
            BinanceFuturesWsMessage::Exec(exec_msg) => {
                log::debug!("Received exec message in data client (ignored): {exec_msg:?}");
            }
            BinanceFuturesWsMessage::Error(e) => {
                log::error!(
                    "Binance Futures WebSocket error: code={}, msg={}",
                    e.code,
                    e.msg
                );
            }
            BinanceFuturesWsMessage::Reconnected => {
                log::info!("WebSocket reconnected");
            }
        }
    }
}

fn upsert_instrument(
    cache: &Arc<RwLock<AHashMap<InstrumentId, InstrumentAny>>>,
    instrument: InstrumentAny,
) {
    let mut guard = cache.write().expect(MUTEX_POISONED);
    guard.insert(instrument.id(), instrument);
}

#[async_trait::async_trait(?Send)]
impl DataClient for BinanceFuturesDataClient {
    fn client_id(&self) -> ClientId {
        self.client_id
    }

    fn venue(&self) -> Option<Venue> {
        Some(self.venue())
    }

    fn start(&mut self) -> anyhow::Result<()> {
        log::info!(
            "Started: client_id={}, product_type={:?}, environment={:?}",
            self.client_id,
            self.product_type,
            self.config.environment,
        );
        Ok(())
    }

    fn stop(&mut self) -> anyhow::Result<()> {
        log::info!("Stopping {id}", id = self.client_id);
        self.cancellation_token.cancel();
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(())
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        log::debug!("Resetting {id}", id = self.client_id);

        self.cancellation_token.cancel();

        for task in self.tasks.drain(..) {
            task.abort();
        }

        let mut ws = self.ws_client.clone();
        get_runtime().spawn(async move {
            let _ = ws.close().await;
        });

        self.is_connected.store(false, Ordering::Relaxed);
        self.cancellation_token = CancellationToken::new();
        Ok(())
    }

    fn dispose(&mut self) -> anyhow::Result<()> {
        log::debug!("Disposing {id}", id = self.client_id);
        self.stop()
    }

    async fn connect(&mut self) -> anyhow::Result<()> {
        if self.is_connected() {
            return Ok(());
        }

        // Reinitialize token in case of reconnection after disconnect
        self.cancellation_token = CancellationToken::new();

        let instruments = self
            .http_client
            .request_instruments()
            .await
            .context("failed to request Binance Futures instruments")?;

        {
            let mut guard = self.instruments.write().expect(MUTEX_POISONED);
            for instrument in &instruments {
                guard.insert(instrument.id(), instrument.clone());
            }
        }

        for instrument in instruments.clone() {
            if let Err(e) = self.data_sender.send(DataEvent::Instrument(instrument)) {
                log::warn!("Failed to send instrument: {e}");
            }
        }

        self.ws_client.cache_instruments(instruments);

        log::info!("Connecting to Binance Futures WebSocket...");
        self.ws_client.connect().await.map_err(|e| {
            log::error!("Binance Futures WebSocket connection failed: {e:?}");
            anyhow::anyhow!("failed to connect Binance Futures WebSocket: {e}")
        })?;
        log::info!("Binance Futures WebSocket connected");

        let stream = self.ws_client.stream();
        let sender = self.data_sender.clone();
        let insts = self.instruments.clone();
        let cancel = self.cancellation_token.clone();

        let handle = get_runtime().spawn(async move {
            pin_mut!(stream);
            loop {
                tokio::select! {
                    Some(message) = stream.next() => {
                        Self::handle_ws_message(message, &sender, &insts);
                    }
                    () = cancel.cancelled() => {
                        log::debug!("WebSocket stream task cancelled");
                        break;
                    }
                }
            }
        });
        self.tasks.push(handle);

        self.is_connected.store(true, Ordering::Release);
        log::info!("Connected: client_id={}", self.client_id);
        Ok(())
    }

    async fn disconnect(&mut self) -> anyhow::Result<()> {
        if self.is_disconnected() {
            return Ok(());
        }

        self.cancellation_token.cancel();

        let _ = self.ws_client.close().await;

        let handles: Vec<_> = self.tasks.drain(..).collect();
        for handle in handles {
            if let Err(e) = handle.await {
                log::error!("Error joining WebSocket task: {e}");
            }
        }

        self.is_connected.store(false, Ordering::Release);
        log::info!("Disconnected: client_id={}", self.client_id);
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.is_connected.load(Ordering::Relaxed)
    }

    fn is_disconnected(&self) -> bool {
        !self.is_connected()
    }

    fn subscribe_instruments(&mut self, _cmd: &SubscribeInstruments) -> anyhow::Result<()> {
        log::debug!(
            "subscribe_instruments: Binance Futures instruments are fetched via HTTP on connect"
        );
        Ok(())
    }

    fn subscribe_instrument(&mut self, _cmd: &SubscribeInstrument) -> anyhow::Result<()> {
        log::debug!(
            "subscribe_instrument: Binance Futures instruments are fetched via HTTP on connect"
        );
        Ok(())
    }

    fn subscribe_book_deltas(&mut self, cmd: &SubscribeBookDeltas) -> anyhow::Result<()> {
        if cmd.book_type != BookType::L2_MBP {
            anyhow::bail!("Binance Futures only supports L2_MBP order book deltas");
        }

        let instrument_id = cmd.instrument_id;
        let ws = self.ws_client.clone();

        // Binance Futures depth streams: @depth (diff) or @depth@100ms/@depth@250ms/@depth@500ms
        let stream = format!(
            "{}@depth@100ms",
            format_binance_stream_symbol(&instrument_id)
        );

        self.spawn_ws(
            async move {
                ws.subscribe(vec![stream])
                    .await
                    .context("book deltas subscription")
            },
            "order book subscription",
        );
        Ok(())
    }

    fn subscribe_quotes(&mut self, cmd: &SubscribeQuotes) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let ws = self.ws_client.clone();

        // Binance Futures uses bookTicker for best bid/ask
        let stream = format!(
            "{}@bookTicker",
            format_binance_stream_symbol(&instrument_id)
        );

        self.spawn_ws(
            async move {
                ws.subscribe(vec![stream])
                    .await
                    .context("quotes subscription")
            },
            "quote subscription",
        );
        Ok(())
    }

    fn subscribe_trades(&mut self, cmd: &SubscribeTrades) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let ws = self.ws_client.clone();

        // Binance Futures uses aggTrade for aggregate trades
        let stream = format!("{}@aggTrade", format_binance_stream_symbol(&instrument_id));

        self.spawn_ws(
            async move {
                ws.subscribe(vec![stream])
                    .await
                    .context("trades subscription")
            },
            "trade subscription",
        );
        Ok(())
    }

    fn subscribe_bars(&mut self, cmd: &SubscribeBars) -> anyhow::Result<()> {
        let bar_type = cmd.bar_type;
        let ws = self.ws_client.clone();
        let interval = bar_spec_to_binance_interval(bar_type.spec())?;

        let stream = format!(
            "{}@kline_{}",
            format_binance_stream_symbol(&bar_type.instrument_id()),
            interval.as_str()
        );

        self.spawn_ws(
            async move {
                ws.subscribe(vec![stream])
                    .await
                    .context("bars subscription")
            },
            "bar subscription",
        );
        Ok(())
    }

    fn unsubscribe_book_deltas(&mut self, cmd: &UnsubscribeBookDeltas) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let ws = self.ws_client.clone();

        let symbol_lower = format_binance_stream_symbol(&instrument_id);
        let streams = vec![
            format!("{symbol_lower}@depth"),
            format!("{symbol_lower}@depth@100ms"),
            format!("{symbol_lower}@depth@250ms"),
            format!("{symbol_lower}@depth@500ms"),
        ];

        self.spawn_ws(
            async move {
                ws.unsubscribe(streams)
                    .await
                    .context("book deltas unsubscribe")
            },
            "order book unsubscribe",
        );
        Ok(())
    }

    fn unsubscribe_quotes(&mut self, cmd: &UnsubscribeQuotes) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let ws = self.ws_client.clone();

        let stream = format!(
            "{}@bookTicker",
            format_binance_stream_symbol(&instrument_id)
        );

        self.spawn_ws(
            async move {
                ws.unsubscribe(vec![stream])
                    .await
                    .context("quotes unsubscribe")
            },
            "quote unsubscribe",
        );
        Ok(())
    }

    fn unsubscribe_trades(&mut self, cmd: &UnsubscribeTrades) -> anyhow::Result<()> {
        let instrument_id = cmd.instrument_id;
        let ws = self.ws_client.clone();

        let stream = format!("{}@aggTrade", format_binance_stream_symbol(&instrument_id));

        self.spawn_ws(
            async move {
                ws.unsubscribe(vec![stream])
                    .await
                    .context("trades unsubscribe")
            },
            "trade unsubscribe",
        );
        Ok(())
    }

    fn unsubscribe_bars(&mut self, cmd: &UnsubscribeBars) -> anyhow::Result<()> {
        let bar_type = cmd.bar_type;
        let ws = self.ws_client.clone();
        let interval = bar_spec_to_binance_interval(bar_type.spec())?;

        let stream = format!(
            "{}@kline_{}",
            format_binance_stream_symbol(&bar_type.instrument_id()),
            interval.as_str()
        );

        self.spawn_ws(
            async move {
                ws.unsubscribe(vec![stream])
                    .await
                    .context("bars unsubscribe")
            },
            "bar unsubscribe",
        );
        Ok(())
    }

    fn request_instruments(&self, request: &RequestInstruments) -> anyhow::Result<()> {
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments_cache = self.instruments.clone();
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let venue = self.venue();
        let start = request.start;
        let end = request.end;
        let params = request.params.clone();
        let clock = self.clock;
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);

        get_runtime().spawn(async move {
            match http.request_instruments().await {
                Ok(instruments) => {
                    for instrument in &instruments {
                        upsert_instrument(&instruments_cache, instrument.clone());
                    }

                    let response = DataResponse::Instruments(InstrumentsResponse::new(
                        request_id,
                        client_id,
                        venue,
                        instruments,
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    ));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send instruments response: {e}");
                    }
                }
                Err(e) => log::error!("Instruments request failed: {e:?}"),
            }
        });

        Ok(())
    }

    fn request_instrument(&self, request: &RequestInstrument) -> anyhow::Result<()> {
        let http = self.http_client.clone();
        let sender = self.data_sender.clone();
        let instruments = self.instruments.clone();
        let instrument_id = request.instrument_id;
        let request_id = request.request_id;
        let client_id = request.client_id.unwrap_or(self.client_id);
        let start = request.start;
        let end = request.end;
        let params = request.params.clone();
        let clock = self.clock;
        let start_nanos = datetime_to_unix_nanos(start);
        let end_nanos = datetime_to_unix_nanos(end);

        get_runtime().spawn(async move {
            {
                let guard = instruments.read().expect(MUTEX_POISONED);
                if let Some(instrument) = guard.get(&instrument_id) {
                    let response = DataResponse::Instrument(Box::new(InstrumentResponse::new(
                        request_id,
                        client_id,
                        instrument.id(),
                        instrument.clone(),
                        start_nanos,
                        end_nanos,
                        clock.get_time_ns(),
                        params,
                    )));

                    if let Err(e) = sender.send(DataEvent::Response(response)) {
                        log::error!("Failed to send instrument response: {e}");
                    }
                    return;
                }
            }

            match http.request_instruments().await {
                Ok(all_instruments) => {
                    for instrument in &all_instruments {
                        upsert_instrument(&instruments, instrument.clone());
                    }

                    let instrument = all_instruments
                        .into_iter()
                        .find(|i| i.id() == instrument_id);

                    if let Some(instrument) = instrument {
                        let response = DataResponse::Instrument(Box::new(InstrumentResponse::new(
                            request_id,
                            client_id,
                            instrument.id(),
                            instrument,
                            start_nanos,
                            end_nanos,
                            clock.get_time_ns(),
                            params,
                        )));

                        if let Err(e) = sender.send(DataEvent::Response(response)) {
                            log::error!("Failed to send instrument response: {e}");
                        }
                    } else {
                        log::error!("Instrument not found: {instrument_id}");
                    }
                }
                Err(e) => log::error!("Instrument request failed: {e:?}"),
            }
        });

        Ok(())
    }

    fn request_trades(&self, _request: &RequestTrades) -> anyhow::Result<()> {
        anyhow::bail!("request_trades not yet implemented for Binance Futures")
    }

    fn request_bars(&self, _request: &RequestBars) -> anyhow::Result<()> {
        anyhow::bail!("request_bars not yet implemented for Binance Futures")
    }
}
