# e2e/Dockerfile

FROM golang:1.25-alpine AS builder

WORKDIR /app
COPY ../etl .

# Build etl and probe binaries.
RUN go build -o /app/bin/etl ./cmd/etl && \
    go build -o /app/bin/probe ./cmd/probe

# Runtime image
FROM alpine:3

WORKDIR /app
COPY --from=builder /app/bin/etl /usr/local/bin/etl
COPY --from=builder /app/bin/probe /usr/local/bin/probe
COPY e2e/testdata/sample.csv /data/sample.csv

# directory for generated configs and sqlite DB
RUN mkdir -p /configs /data/db && \
	apk add curl

ENTRYPOINT ["/bin/sh"]

