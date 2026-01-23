# Pin to specific digest for supply-chain security (python:3.13-slim as of 2025-11-29)
FROM python@sha256:326df678c20c78d465db501563f3492d17c42a4afe33a1f2bf5406a1d56b0e86 AS base
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
	    PIP_NO_CACHE_DIR=off \
	    PIP_DISABLE_PIP_VERSION_CHECK=on \
	    PIP_DEFAULT_TIMEOUT=100 \
	    PYO3_PYTHON="/usr/local/bin/python3" \
	    PYSETUP_PATH="/opt/pysetup" \
	    BUILD_MODE="release" \
	    CC="clang"
ENV PATH="/root/.local/bin:/root/.cargo/bin:$PATH"
WORKDIR $PYSETUP_PATH

FROM base AS builder

# Install build deps
RUN apt-get update && \
    apt-get install -y curl clang git make pkg-config capnproto libcapnp-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install pinned Rust toolchain (must match `rust-toolchain.toml`)
COPY rust-toolchain.toml ./
RUN TOOLCHAIN="$(python -c "import tomllib; print(tomllib.load(open('rust-toolchain.toml','rb'))['toolchain']['channel'])")" && \
    test -n "$TOOLCHAIN" && \
    curl https://sh.rustup.rs -sSf | bash -s -- -y --profile minimal --default-toolchain "$TOOLCHAIN"

# Install UV
COPY uv-version ./
RUN UV_VERSION=$(cat uv-version) && curl -LsSf https://astral.sh/uv/$UV_VERSION/install.sh | sh

# Install package requirements
COPY uv.lock pyproject.toml build.py ./
RUN uv sync --no-install-package nautilus_trader

# Build nautilus_trader
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY crates ./crates
RUN cargo build --lib --release --all-features

COPY nautilus_trader ./nautilus_trader
COPY README.md ./
RUN uv build --wheel
RUN uv pip install --system dist/*.whl
RUN find /usr/local/lib/python3.13/site-packages -name "*.pyc" -exec rm -f {} \;

# Copy QuantChat custom modules into installed package
COPY python/nautilus_trader/quantchat /usr/local/lib/python3.13/site-packages/nautilus_trader/quantchat
COPY python/nautilus_trader/adapters/alpaca /usr/local/lib/python3.13/site-packages/nautilus_trader/adapters/alpaca

# Final application image
FROM base AS application

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Install dependencies for QuantChat trading engine
# - redis: for config/code fetching
# - aiohttp, websockets: for Alpaca adapter
RUN pip install --no-cache-dir redis aiohttp websockets

# Default entrypoint for QuantChat trading engine
# Runs the strategy fetcher/executor from the quantchat module
CMD ["python", "-m", "nautilus_trader.quantchat.run_strategy"]
