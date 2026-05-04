# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/dotnet/sdk:10.0-jammy AS builder
WORKDIR /app

# Install build dependencies
RUN apt-get update && \
    apt-get -y install \
    build-essential \
    clang \
    cmake \
    git \
    libc++-dev \
    libboost-all-dev \
    libgmp-dev \
    libsodium-dev \
    libssl-dev \
    libzmq3-dev \
    libzmq5 \
    ninja-build \
    pkg-config \
    zlib1g-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy libs directory (contains WebSocketManager, ZeroMQ DLLs)
COPY libs/ ./libs/

# Copy project files for better layer caching
COPY src/Miningcore/*.csproj ./src/Miningcore/
COPY src/Miningcore.Tests/*.csproj ./src/Miningcore.Tests/

# Restore dependencies (cached unless .csproj changes)
RUN dotnet restore src/Miningcore/Miningcore.csproj

# Copy source code and native libraries
COPY src/ ./src/

# Initialize fake git repo for GitVersion (required by build)
RUN git init && \
    git config user.email "builder@docker" && \
    git config user.name "Docker Builder" && \
    git add -A && \
    git commit -m "Docker build" || true

# Build native libraries first (required for Miningcore)
WORKDIR /app/src/Miningcore
RUN chmod +x build-libs-linux.sh && \
    mkdir -p ../../build && \
    ./build-libs-linux.sh ../../build

# Build the .NET application
RUN dotnet publish -c Release --framework net10.0 --no-restore -o ../../build

# Runtime stage
FROM mcr.microsoft.com/dotnet/aspnet:8.0-jammy

LABEL maintainer="miningcore" \
      description="Miningcore - High-performance multi-coin mining pool server" \
      version="1.0" \
      org.opencontainers.image.source="https://github.com/oliverw/miningcore"

WORKDIR /app

# Install runtime dependencies and create non-root user
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        libboost-date-time1.74.0 \
        libboost-system1.74.0 \
        libsodium23 \
        libzmq5 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r miningcore --gid=1000 \
    && useradd -r -g miningcore --uid=1000 --home-dir=/app --shell=/sbin/nologin miningcore \
    && chown -R miningcore:miningcore /app

# Copy application from builder
COPY --from=builder --chown=miningcore:miningcore /app/build ./

# Switch to non-root user
USER miningcore

# Configuration path (can be overridden)
ENV CONFIG_PATH=/app/config.json

# Expose API port
EXPOSE 4000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl -f http://localhost:4000/api/pools || exit 1

ENTRYPOINT ["sh", "-c", "./Miningcore -c ${CONFIG_PATH}"]
