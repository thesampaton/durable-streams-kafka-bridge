# Changelog

## 0.2.3

### Bug Fixes

- Discovery now handles JSON array payloads from control streams. Clients that
  coalesce concurrent appends into a single `[{...},{...}]` write (notably the
  official `@durable-streams/client` JS library) were silently dropped because
  `get_dotted()` against an array root always returned `None`. Discovery now
  iterates each element of an array payload individually.

## 0.2.2

### Bug Fixes

- Fixed deadlock when discovery tasks tried to spawn forwarders. Replaced
  `Arc<Mutex<JoinSet>>` with an mpsc channel so the main loop never holds a
  lock across an `.await`.

## 0.2.1

### Bug Fixes

- Fixed SSE subscriptions crashing after 30 seconds of idle time. The
  client-wide `reqwest` timeout is now applied only to bounded calls (metadata,
  read), not to long-lived SSE streams.

## 0.2.0

### Features

- Added dynamic stream discovery via `[[discovery]]` config blocks.

## 0.1.0

- Initial release.
