# Build stage
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git make gcc musl-dev

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Extra `go build` flags. Empty by default; the production compose file passes
# "-p=1 -trimpath" so the image can be built on a 1 GiB VM (with swap).
ARG GO_BUILD_FLAGS=""

# Three binaries in one image: the node, the HTTP gateway, and the tenant CLI.
RUN CGO_ENABLED=0 GOOS=linux go build ${GO_BUILD_FLAGS} -o /out/distrikv   ./cmd/server \
 && CGO_ENABLED=0 GOOS=linux go build ${GO_BUILD_FLAGS} -o /out/gateway    ./cmd/gateway \
 && CGO_ENABLED=0 GOOS=linux go build ${GO_BUILD_FLAGS} -o /out/gatewayctl ./cmd/gatewayctl

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