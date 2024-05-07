# Someguy Environment Variables

`someguy` ships with some implicit defaults that can be adjusted via env variables below.

- [Configuration](#configuration)
  - [`SOMEGUY_LISTEN_ADDRESS`](#someguy_listen_address)
  - [`SOMEGUY_ACCELERATED_DHT`](#someguy_accelerated_dht)
  - [`SOMEGUY_PUT_ENABLED`](#someguy_put_enabled)
  - [`SOMEGUY_DATADIR`](#someguy_datadir)
  - [`SOMEGUY_PROVIDER_ENDPOINTS`](#someguy_provider_endpoints)
  - [`SOMEGUY_PEER_ENDPOINTS`](#someguy_peer_endpoints)
  - [`SOMEGUY_IPNS_ENDPOINTS`](#someguy_ipns_endpoints)
- [Logging](#logging)
  - [`GOLOG_LOG_LEVEL`](#golog_log_level)
  - [`GOLOG_LOG_FMT`](#golog_log_fmt)
  - [`GOLOG_FILE`](#golog_file)
  - [`GOLOG_TRACING_FILE`](#golog_tracing_file)

## Configuration

### `SOMEGUY_LISTEN_ADDRESS`

The address to listen on.

Default: `127.0.0.1:8190`

### `SOMEGUY_ACCELERATED_DHT`

Whether or not the Accelerated DHT is enabled or not.

Default: `true`

### `SOMEGUY_PUT_ENABLED`

Whether or not to accept Delegated Routing V1 PUT requests. Affects all PUT requests:
provider records, peer records and IPNS records.

By default, PUT requests are ignored. Therefore, they will neither be stored locally,
nor sent to other remote endpoints.

Default: `false`

### `SOMEGUY_DATADIR`

Used in conjunction with [`SOMEGUY_PUT_ENABLED`](#someguy_put_enabled).

The LevelDB data directory to persist PUT records. When receiving PUT requests,
the records will be stored in this database. The database is queried for GET
requests.

Default: none

### `SOMEGUY_PROVIDER_ENDPOINTS`

Comma-separated list of other Delegated Routing V1 endpoints to proxy provider requests to.

Default: `https://cid.contact`

### `SOMEGUY_PEER_ENDPOINTS`

Comma-separated list of other Delegated Routing V1 endpoints to proxy peer requests to.

Default: none

### `SOMEGUY_IPNS_ENDPOINTS`

Comma-separated list of other Delegated Routing V1 endpoints to proxy IPNS requests to.

Default: none

## Logging

### `GOLOG_LOG_LEVEL`

Specifies the log-level, both globally and on a per-subsystem basis. Level can
be one of:

* `debug`
* `info`
* `warn`
* `error`
* `dpanic`
* `panic`
* `fatal`

Per-subsystem levels can be specified with `subsystem=level`.  One global level
and one or more per-subsystem levels can be specified by separating them with
commas.

Default: `error`

Example:

```console
GOLOG_LOG_LEVEL="error,someguy=debug" someguy
```

### `GOLOG_LOG_FMT`

Specifies the log message format.  It supports the following values:

- `color` -- human readable, colorized (ANSI) output
- `nocolor` -- human readable, plain-text output.
- `json` -- structured JSON.

For example, to log structured JSON (for easier parsing):

```bash
export GOLOG_LOG_FMT="json"
```
The logging format defaults to `color` when the output is a terminal, and
`nocolor` otherwise.

### `GOLOG_FILE`

Sets the file to which the logs are saved. By default, they are printed to the standard error output.

### `GOLOG_TRACING_FILE`

Sets the file to which the tracing events are sent. By default, tracing is disabled.

Warning: Enabling tracing will likely affect performance.
