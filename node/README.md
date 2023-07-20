## How to run

```bash
cargo build --release -p everscale-jrpc-node

target/release/everscale-jrpc-node \
    --config path/to/config.yaml \
    --global-config path/to/global-config.json
```

> NOTE: compile with `venom` feature to use the node for the Venom blockchain.

## Config example

```yaml
---
# Optional states endpoint (see docs below)
rpc_config:
  # States RPC endpoint
  listen_address: "0.0.0.0:8081"
  generate_stub_keyblock: true
  # # Or use minimal JRPC API without fields below:
  # type: simple
  type: full
  # Path to the separate DB for transactions and other JRPC stuff
  persistent_db_path: "/var/db/jrpc-storage"
  # # Virtual shards depth to use during shard state accounts processing
  # shard_split_depth: 4

metrics_settings:
  # Listen address of metrics. Used by the client to gather prometheus metrics.
  # Default: "127.0.0.1:10000"
  listen_address: "0.0.0.0:10000"
  # Metrics update interval in seconds. Default: 10
  collection_interval_sec: 10

node_settings:
  # Root directory for node DB. Default: "./db"
  db_path: "/var/db/jrpc"

  # UDP port, used for ADNL node. Default: 30303
  adnl_port: 30000

  # Path to temporary ADNL keys.
  # NOTE: Will be generated if it was not there.
  # Default: "./adnl-keys.json"
  temp_keys_path: "/etc/jrpc/adnl-keys.json"

  # Archives map queue. Default: 16
  parallel_archive_downloads: 32

  # # Specific block from which to sync the node
  # start_from: 12365000

  # Manual rocksdb memory options (will be computed from the
  # available memory otherwise).
  # db_options:
  #   rocksdb_lru_capacity: "512 MB"
  #   cells_cache_size: "4 GB"

  # Everscale specific network settings
  adnl_options:
    use_loopback_for_neighbours: true
    force_use_priority_channels: true
  rldp_options:
    force_compression: true
  overlay_shard_options:
    force_compression: true
```
