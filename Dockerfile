# Build stage
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git make gcc musl-dev

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /distrikv ./cmd/server

# Runtime stage
FROM alpine:3.20 AS runtime

RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -g 1000 -S distrikv \
    && adduser -u 1000 -S -G distrikv -h /var/lib/distrikv distrikv

WORKDIR /app

COPY --from=builder /distrikv /usr/local/bin/distrikv

RUN mkdir -p /var/lib/distrikv \
    && chown -R distrikv:distrikv /var/lib/distrikv

USER distrikv

EXPOSE 8080

VOLUME ["/var/lib/distrikv"]

ENV DISTRIKV_DATA_DIR=/var/lib/distrikv

ENTRYPOINT ["distrikv"]