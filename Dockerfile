# ── Build ─────────────────────────────────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /src

# Copy manifests first so dependency download is cached independently of source
# changes — editing a .go file should not re-download the module graph.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# CGO_ENABLED=0 produces a static binary, which is what lets the final stage be a
# minimal image with no libc. -trimpath keeps build paths out of the binary.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/server ./cmd/server

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates tzdata wget \
 && addgroup -S -g 10001 app \
 && adduser -S -u 10001 -G app app

WORKDIR /app
COPY --from=builder /out/server ./server
COPY config.yaml ./config.yaml

# Run unprivileged. A rate limiter is reachable from the internet by definition, so
# a remote code execution bug should not also hand over root inside the container.
USER 10001:10001

EXPOSE 8080

# Liveness only — /healthz deliberately does not check Redis, so a Redis outage
# does not make the orchestrator restart every node during the exact incident the
# fallback strategy exists to survive.
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -qO- http://127.0.0.1:8080/healthz >/dev/null 2>&1 || exit 1

ENTRYPOINT ["./server"]
CMD ["-config", "config.yaml"]
