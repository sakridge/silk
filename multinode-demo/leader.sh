#!/bin/bash
export RUST_LOG=solana=info,solana::erasure=debug
sudo sysctl -w net.core.rmem_max=26214400
rm -f leader.log
cat genesis.log | \
#cargo run --features="cuda,erasure" --bin solana-fullnode -- \
./target/release/solana-fullnode \
-l leader.json 2>&1 | tee leader-tee.log
