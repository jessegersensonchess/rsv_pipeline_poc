# ------------------ Build stage ------------------
FROM golang:1.25-alpine AS build

WORKDIR /src

# If your go.mod pulls private modules
RUN apk add --no-cache git

# Module download cache
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source
COPY . .

# Build only the cmd/importer entrypoint
# (pure Go, static binary)
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
    -o /out/importer ./cmd/importer

# ------------------ Runtime stage ------------------
FROM alpine:3.22

ENV TZ=Europe/Prague
WORKDIR /app

# Copy the binary
COPY --from=build /out/importer /app/importer

# Default command
ENTRYPOINT ["/app/importer"]

