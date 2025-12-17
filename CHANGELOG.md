# Changelog (fork)

This fork tracks `travisbrown/wayback-rs`.

## Unreleased

## fork/wayback-rs/v0.6.0-chad.2 (based on upstream `v0.6.0`)

### Added
- Opt-in pacing hooks for Wayback requests via `with_pacer(...)` on `cdx::IndexClient` and `Downloader`.
- Opt-in synchronous request lifecycle observer hook via `with_observer(...)` for emitting request events without changing request/response APIs.
- `util::observe` module with event types (`Surface`, `Phase`, `ErrorClass`, `Event`) and the `Observer` trait.

### Changed
- CDX requests now send a default `User-Agent: wayback-rs/<version>` header to avoid intermittent non-JSON responses; callers can override via `with_user_agent(...)` or opt out via `without_user_agent()`.

### Testing
- Live-network CDX integration tests are `#[ignore]` by default; when run explicitly, they use a bounded retry loop and an explicit test User-Agent for better diagnostics and stability.

