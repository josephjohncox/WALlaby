# syntax=docker/dockerfile:1.7
ARG GO_VERSION=1.25.5

FROM golang:${GO_VERSION}-bookworm AS build
ARG TARGETOS
ARG TARGETARCH

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    gcc \
    g++ \
    git \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .

RUN CGO_ENABLED=1 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/wallaby ./cmd/wallaby && \
    go build -trimpath -ldflags="-s -w" -o /out/wallaby-admin ./cmd/wallaby-admin && \
    go build -trimpath -ldflags="-s -w" -o /out/wallaby-worker ./cmd/wallaby-worker

FROM gcr.io/distroless/cc-debian12:nonroot

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build \
    /out/wallaby \
    /out/wallaby-admin \
    /out/wallaby-worker \
    /usr/local/bin/

USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/wallaby"]
