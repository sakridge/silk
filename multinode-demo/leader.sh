#!/bin/bash

# if RUST_LOG is unset, default to info
export RUST_LOG=${RUST_LOG:-solana=info}

set -x
[[ $(uname) = Linux ]] && sudo sysctl -w net.core.rmem_max=26214400

#cargo run --release --bin solana-fullnode -- \
./target/release/solana-fullnode \
      -l leader.json -o leader.log < genesis.log tx-*.log 2>&1 | tee leader-tee.log
