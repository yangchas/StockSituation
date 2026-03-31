#!/bin/bash
export RUSTUP_DIST_SERVER=https://mirrors.tuna.tsinghua.edu.cn/rustup
export RUSTUP_UPDATE_ROOT=https://mirrors.tuna.tsinghua.edu.cn/rustup/rustup
export CARGO_BUILD_JOBS=1
export RUSTFLAGS="-C codegen-units=1"
source $HOME/.cargo/env
cd /root/work/engine_v2
cargo build --release
cp target/release/libmarket_edge_v2_core.so ./market_edge_v2_core.so
echo "BUILD_SUCCESS"
