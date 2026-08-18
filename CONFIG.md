# rsLXMF configuration reference

`lxmd-rs` reads only `config.yaml` from its configuration directory. The old
ConfigObj/INI file named `config` is not read, detected, converted, or used as
a fallback. The loading pipeline is:

```text
YAML parse -> defaults and normalization -> semantic validation -> runtime
```

Unknown fields are errors. Names are case-sensitive and use `snake_case`.
When rsLXMF writes a configuration, fields equal to their defaults are omitted;
reading the compact form restores the same values.

Print a commented example with `lxmd-rs --exampleconfig`.

## Top-level document

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `lxmf` | mapping | `{}` | Delivery peer settings. |
| `propagation` | mapping | `{}` | Propagation-node and peering settings. |
| `storage` | mapping | `{}` | SQLite storage settings. |
| `logging` | mapping | `{}` | Logging settings. |

The minimal configuration is `{}`.

## `lxmf`

| Field | Type | Default | Valid values and meaning |
| --- | --- | --- | --- |
| `display_name` | string | `Anonymous Peer` | Display name announced by the delivery peer. |
| `announce_at_start` | boolean | `false` | Announce the delivery destination during startup. |
| `announce_interval` | integer or null | `null` | Periodic announce interval in minutes; `null` disables periodic announces. |
| `stamp_cost` | integer or null | `null` | Optional inbound delivery stamp cost, `0..=255`. |
| `delivery_transfer_max_accepted_size` | number | `1000.0` | Maximum accepted delivery transfer size in KiB; values below `0.38` normalize to `0.38`. |
| `on_inbound` | string or null | `null` | Command executed for each durably stored inbound message. |

## `propagation`

| Field | Type | Default | Valid values and meaning |
| --- | --- | --- | --- |
| `enable_node` | boolean | `false` | Enable propagation-node operation. |
| `node_name` | string or null | `null` | Optional announced propagation-node name. |
| `outbound_node` | string or null | `null` | Preferred propagation node; exactly 32 hexadecimal characters. |
| `auth_required` | boolean | `false` | Require identities to be present in `control_allowed` for control operations. |
| `announce_at_start` | boolean | `false` | Announce the propagation node during startup. |
| `announce_interval` | integer or null | `null` | Periodic node announce interval in minutes. |
| `autopeer` | boolean | `true` | Automatically peer with discovered propagation nodes. |
| `autopeer_maxdepth` | integer | `4` | Maximum autopeer path depth. |
| `max_peers` | integer | `20` | Maximum number of propagation peers. |
| `from_static_only` | boolean | `false` | Accept propagation traffic only from configured static peers. |
| `message_storage_limit` | number | `500.0` | Message storage limit in decimal MB; values below `0.005` normalize to `0.005`. |
| `propagation_message_max_accepted_size` | number | `256.0` | Maximum accepted propagation transfer in KiB; minimum `0.38`. |
| `propagation_sync_max_accepted_size` | number | `10240.0` | Maximum accepted propagation sync in KiB; minimum `0.38`. |
| `propagation_stamp_cost_target` | integer | `16` | Target propagation stamp cost, `0..=255`. |
| `propagation_stamp_cost_flexibility` | integer | `3` | Accepted cost flexibility, `0..=255`. |
| `peering_cost` | integer | `18` | Local peering cost, `0..=255`. |
| `remote_peering_cost_max` | integer | `26` | Maximum accepted remote peering cost, `0..=255`. |
| `control_allowed` | sequence of strings | `[]` | Allowed control identity hashes; each is exactly 32 hexadecimal characters. |
| `static_peers` | sequence of strings | `[]` | Static propagation peer hashes; each is exactly 32 hexadecimal characters. |
| `prioritise_destinations` | sequence of strings | `[]` | Prioritised LXMF destination hashes; each is exactly 32 hexadecimal characters. |
| `enforce_stamps` | boolean | `false` | Enforce stamp requirements on propagation traffic. |

Hash lists reject duplicate entries.

## `storage`

| Field | Type | Default | Valid values and meaning |
| --- | --- | --- | --- |
| `database_path` | path or null | `null` | SQLite database path. Relative paths resolve from the LXMF configuration directory. |
| `page_cache_size` | integer | `1024` | SQLite page-cache budget in KiB; normalized into `64..=65536`. |
| `vacuum_interval` | integer | `3600` | Maintenance interval in seconds; normalized to at least `60`. |
| `vacuum_pages` | integer | `128` | Maximum pages reclaimed per incremental-vacuum pass, `0..=4294967295`. |

## `logging`

| Field | Type | Default | Valid values and meaning |
| --- | --- | --- | --- |
| `level` | integer | `4` | Log level `0..=7`. |

## Example

```yaml
lxmf:
  display_name: Rat
  announce_at_start: true

propagation:
  enable_node: true
  node_name: Rat Nest
  static_peers:
    - e17f833c4ddf8890dd3a79a6fea8161d

storage:
  database_path: storage/lxmf/lxmf.sqlite
```
