# ------------------ Build stage ------------------
FROM golang:1.25-alpine AS build

WORKDIR /src

# Install git if your go.mod pulls private modules
RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

# Copy source and build statically
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/main .

# ------------------ Runtime stage ------------------
FROM alpine:3.22

# Install minimal runtime deps (certs + tzdata)
#RUN apk add --no-cache ca-certificates tzdata

ENV TZ=Europe/Prague
WORKDIR /app

COPY --from=build /out/main /app/main

ENTRYPOINT ["/app/main"]

