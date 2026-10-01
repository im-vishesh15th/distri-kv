# Build stage
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git make gcc musl-dev

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Three binaries in one image: the node, the HTTP gateway, and the tenant CLI.
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/distrikv   ./cmd/server \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/gateway    ./cmd/gateway \
 && CGO_ENABLED=0 GOOS=linux go build -o /out/gatewayctl ./cmd/gatewayctl

# Runtime stage
FROM alpine:3.20 AS runtime

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -g 1000 -S distrikv \
    && adduser -u 1000 -S -G distrikv -h /var/lib/distrikv distrikv

WORKDIR /app

COPY --from=builder /out/ /usr/local/bin/

RUN mkdir -p /var/lib/distrikv /var/lib/gateway \
    && chown -R distrikv:distrikv /var/lib/distrikv /var/lib/gateway

USER distrikv

EXPOSE 8080

VOLUME ["/var/lib/distrikv"]

ENV DISTRIKV_DATA_DIR=/var/lib/distrikv

ENTRYPOINT ["distrikv"]