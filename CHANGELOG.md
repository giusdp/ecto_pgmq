# Change Log

All notable changes to this project will be documented in this file.

## [Unreleased]

### Changed

* Fix `EctoPGMQ.Producer` polling without a listener, which read again at once
  instead of waiting for the read interval

* Defer PGMQ's insert trigger so concurrent senders don't wait for each other's
  transactions

## [2.0.0] - 2026-07-15

### Changed

* Add support for topic-based message routing (see
  [Message Routing](message_routing.md))

* Add support for new PGMQ metadata "getter" functions to `EctoPGMQ.PGMQ`

* Add support for `:head` FIFO message grouping

### Breaking Changes

* `EctoPGMQ.send_messages/4` now returns a map of queues and message IDs

* The `:group` key has been removed from the `EctoPGMQ.Message` schema

* The `t:EctoPGMQ.Message.message/0` type has replaced the old `:spec` record

* The `EctoPGMQ.Throttle` schema no longer has a source and, therefore, can no
  longer be manipulated via `Ecto.Repo` functions
  (see `EctoPGMQ.Throttle.query/0`).

* `EctoPGMQ` now requires PGMQ v1.12.0 or higher

### Links

* [GitHub](https://github.com/gdwoolbert3/ecto_pgmq/releases/tag/v2.0.0)
* [HexDocs](https://hexdocs.pm/ecto_pgmq/2.0.0/readme.html)

## [1.1.0] - 2026-02-03

### Changed

* Add support for features introduced in PGMQ v1.10.0

### Breaking Changes

* `EctoPGMQ` now requires PGMQ v1.10.0

### Links

* [GitHub](https://github.com/gdwoolbert3/ecto_pgmq/releases/tag/v1.1.0)
* [HexDocs](https://hexdocs.pm/ecto_pgmq/1.1.0/readme.html)

## [1.0.0] - 2026-01-29

### Changed

* Initial implementation

### Links

* [GitHub](https://github.com/gdwoolbert3/ecto_pgmq/releases/tag/v1.0.0)
* [HexDocs](https://hexdocs.pm/ecto_pgmq/1.0.0/readme.html)
