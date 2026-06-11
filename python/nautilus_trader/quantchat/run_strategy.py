"""
Trading Engine entry point for quantchat.

Fetches the compiled strategy plan and config from Redis, sets up credentials as
environment variables, then executes the plan using Nautilus Trader.

All output is captured and persisted to Redis before exit so logs are always available
even after the container is removed.

"""

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
import io
import json
import os
import sys
import time
import traceback

import redis

from nautilus_trader.quantchat.run_backtest import run_backtest_plan
from nautilus_trader.quantchat.run_live import run_live_strategy_plan


class TeeWriter:
    """
    Write to multiple streams simultaneously.
    """

    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for stream in self.streams:
            stream.write(data)
            stream.flush()

    def flush(self):
        for stream in self.streams:
            stream.flush()


# Global log capture buffer
_log_buffer = io.StringIO()
_redis_client: redis.Redis | None = None
_bot_id: str | None = None
_log_key: str | None = None


@dataclass(frozen=True)
class LaunchConfig:
    redis_url: str
    run_mode: str
    bot_id: str | None
    backtest_id: str | None
    deploy_secret: str
    log_key: str


def _persist_logs():
    """
    Persist captured logs to Redis before exit.
    """
    if not _redis_client or not _log_key:
        return
    try:
        logs = _log_buffer.getvalue()
        timestamp = datetime.now(UTC).isoformat()
        log_entry = json.dumps(
            {
                "logs": logs,
                "timestamp": timestamp,
                "exitedAt": timestamp,
            },
        )
        # Persist to Redis with 24h TTL so logs are available after container dies
        _redis_client.setex(_log_key, 86400, log_entry)
    except Exception as e:
        # Last resort - print to original stderr
        sys.__stderr__.write(f"[Trading Node] Failed to persist logs: {e}\n")


def log(message: str):
    """
    Log a message.

    Docker adds timestamps, so we don't add our own.

    """
    print(f"[INFO] [Trading Node] {message}")


def fetch_from_redis(r: redis.Redis, key: str, max_attempts: int = 10) -> str | None:
    """
    Fetch a value from Redis with retry logic.
    """
    for attempt in range(max_attempts):
        value = r.get(key)
        if value:
            return value
        log(f"Waiting for {key}... (attempt {attempt + 1})")
        time.sleep(1)
    return None


def setup_credentials_env(config: dict) -> None:
    """
    Set up environment variables from the config credentials.

    This allows the strategy code to access credentials via standard env vars.

    """
    credentials = config.get("credentials", {})

    # Provider (e.g., 'alpaca', 'quantchat')
    provider = credentials.get("provider", "")
    os.environ["QUANTCHAT_PROVIDER"] = provider

    # Trading mode (paper/live)
    trading_mode = credentials.get("tradingMode", "paper")
    os.environ["QUANTCHAT_TRADING_MODE"] = trading_mode

    if provider == "quantchat":
        # Local paper trading with quantchat adapter
        # No external credentials needed - uses Redis for market data
        os.environ["QUANTCHAT_ADAPTER"] = "local"
        os.environ["QUANTCHAT_REDIS_URL"] = os.environ.get("REDIS_URL", "redis://localhost:6379")
        log("Using quantchat local adapter for paper trading")

    elif provider == "alpaca":
        # Alpaca external broker
        os.environ["QUANTCHAT_ADAPTER"] = "alpaca"

        # API Key authentication
        if "apiKey" in credentials:
            os.environ["APCA_API_KEY_ID"] = credentials["apiKey"]
            os.environ["APCA_API_SECRET_KEY"] = credentials["apiSecret"]
        # OAuth authentication
        elif "accessToken" in credentials:
            os.environ["APCA_API_ACCESS_TOKEN"] = credentials["accessToken"]

        # Set paper trading flag
        is_paper = trading_mode == "paper"
        os.environ["APCA_API_BASE_URL"] = (
            "https://paper-api.alpaca.markets" if is_paper else "https://api.alpaca.markets"
        )

    # Capital settings
    os.environ["QUANTCHAT_INITIAL_CAPITAL"] = str(config.get("initialCapital", 100000))
    os.environ["QUANTCHAT_VIRTUAL_CASH"] = str(config.get("virtualCash", 100000))

    # Membership tier feature flag - PRO/ELITE users can access tick data
    can_access_tick_data = config.get("canAccessTickData", False)
    os.environ["QUANTCHAT_CAN_ACCESS_TICK_DATA"] = "true" if can_access_tick_data else "false"
    if can_access_tick_data:
        log("Tick data access: enabled (PRO/ELITE membership)")
    else:
        log("Tick data access: disabled (HOBBYIST membership - upgrade for tick data)")

    positions = config.get("positions", [])
    if positions:
        log(f"Restart state: {len(positions)} position(s) to restore into the engine")


def _read_launch_config() -> LaunchConfig | None:
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    run_mode = os.environ.get("QUANTCHAT_RUN_MODE", "live").lower()
    bot_id = os.environ.get("BOT_ID")
    backtest_id = os.environ.get("BACKTEST_ID")
    deploy_secret = os.environ.get("DEPLOY_SECRET")

    if run_mode == "backtest":
        if not backtest_id:
            log("Error: BACKTEST_ID env var not set")
            return None
        log_key = f"backtest:{backtest_id}:logs"
    elif bot_id:
        log_key = f"bot:{bot_id}:logs"
    else:
        log("Error: BOT_ID env var not set")
        return None

    if not deploy_secret:
        log("Error: DEPLOY_SECRET env var not set")
        return None

    return LaunchConfig(
        redis_url=redis_url,
        run_mode=run_mode,
        bot_id=bot_id,
        backtest_id=backtest_id,
        deploy_secret=deploy_secret,
        log_key=log_key,
    )


def _activate_runtime(config: LaunchConfig) -> None:
    if config.run_mode == "backtest":
        log(f"Starting backtest {config.backtest_id}...")
        os.environ["QUANTCHAT_BACKTEST_ID"] = config.backtest_id or ""
    else:
        log(f"Starting strategy for bot {config.bot_id}...")
        os.environ["QUANTCHAT_BOT_ID"] = config.bot_id or ""


def _redis_key(config: LaunchConfig, suffix: str) -> str:
    if config.run_mode == "backtest":
        return f"backtest:{config.backtest_id}:{config.deploy_secret}:{suffix}"
    return f"bot:{config.bot_id}:{config.deploy_secret}:{suffix}"


def _load_runtime_config(r: redis.Redis, launch: LaunchConfig) -> dict:
    log("Fetching config from Redis...")
    config_json = fetch_from_redis(r, _redis_key(launch, "config"))

    if not config_json:
        log("Warning: No config found, running without credentials")
        return {}

    try:
        loaded = json.loads(config_json)
    except json.JSONDecodeError as e:
        log(f"Warning: Failed to parse config JSON: {e}")
        return {}

    if launch.run_mode != "backtest":
        setup_credentials_env(loaded)
    log("Credentials and config loaded")
    return loaded


def _execute_strategy(
    r: redis.Redis,
    launch: LaunchConfig,
    config: dict,
) -> int:
    log("Executing strategy...")
    sys.stdout.flush()

    try:
        if launch.run_mode == "backtest":
            # The bar series (and model signals) ride a separate Redis key so the
            # backend never materializes them into per-trial configs; optimization
            # trials all reference one shared payload.
            bars_key = config.pop("barsKey", None)
            if bars_key:
                blob = fetch_from_redis(r, bars_key)
                if blob is None:
                    raise ValueError(f"Bars payload missing at {bars_key}")
                side_channel = json.loads(blob)
                config["bars"] = side_channel["bars"]
                config["modelSignals"] = side_channel.get("modelSignals", {})
            result = run_backtest_plan(config)
            result_key = f"backtest:{launch.backtest_id}:result"
            # allow_nan=False: a stray NaN/Infinity would serialize as a bare
            # literal that is not JSON — fail here, loudly, not in the backend.
            r.setex(result_key, 86400, json.dumps(result, allow_nan=False))
            log(f"Backtest result persisted to {result_key}")
            return 0
        if not config.get("runtimeBindings"):
            raise ValueError("runtimeBindings are required for live strategy execution")
        config["redisUrl"] = os.environ.get("REDIS_URL", "redis://localhost:6379")
        if launch.bot_id:
            config["botId"] = launch.bot_id
        run_live_strategy_plan(config)
        return 0
    except Exception as e:
        log(f"FATAL: Strategy execution failed: {type(e).__name__}: {e}")
        traceback.print_exc()
        return 1


def _run() -> int:
    global _redis_client, _bot_id, _log_key

    launch = _read_launch_config()
    if launch is None:
        return 1

    _bot_id = launch.bot_id
    _log_key = launch.log_key
    _activate_runtime(launch)

    try:
        _redis_client = redis.from_url(launch.redis_url, decode_responses=True)
    except Exception as e:
        log(f"Error: Redis connection failed: {e}")
        return 1

    config = _load_runtime_config(_redis_client, launch)
    if not config.get("compiledPlan"):
        log("Error: compiledPlan missing from runtime config")
        return 1
    return _execute_strategy(_redis_client, launch, config)


def main():
    tee_stdout = TeeWriter(sys.__stdout__, _log_buffer)
    tee_stderr = TeeWriter(sys.__stderr__, _log_buffer)
    sys.stdout = tee_stdout
    sys.stderr = tee_stderr

    try:
        exit_code = _run()
    except Exception as e:
        log(f"FATAL: Unexpected error: {type(e).__name__}: {e}")
        traceback.print_exc()
        exit_code = 1

    finally:
        log(f"Container exiting with code {exit_code}")
        _persist_logs()
        sys.exit(exit_code)


if __name__ == "__main__":
    main()
