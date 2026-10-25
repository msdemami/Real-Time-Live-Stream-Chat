# chat_service

Real-time WebSocket chat service for live streaming platforms.

## Run

```bash
go mod tidy
go run .
```

Server listens on `:8080` (or `PORT` environment variable).

## Endpoints

- `GET /health` -> `{"status": "ok"}`
- `GET /api/v1/rooms/{roomID}` -> `{"room_id": "...", "online_users": N}`
- `GET /ws/rooms/{roomID}` -> WebSocket upgrade

## Tests

```bash
go test -v -race ./...
```

## Concurrency & Slow Client Design

- Per-room `sync.RWMutex` protects client sets; room broadcasting uses non-blocking `select` so slow consumers never block peers.
- When a client's send buffer (64 slots) saturates, it is evicted immediately via non-blocking teardown to preserve room throughput.
- Teardown is synchronized with `sync.Once` and a `done` channel, preventing goroutine leaks and double-close panics while auto-pruning empty rooms.
