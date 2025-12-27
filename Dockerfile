# syntax=docker/dockerfile:1.7
ARG GO_VERSION=1.25.5

FROM golang:${GO_VERSION}-alpine AS build
ARG TARGETOS
ARG TARGETARCH
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags="-s -w" -o /out/wallaby ./cmd/wallaby
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags="-s -w" -o /out/wallaby-admin ./cmd/wallaby-admin
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} go build -trimpath -ldflags="-s -w" -o /out/wallaby-worker ./cmd/wallaby-worker

FROM gcr.io/distroless/base-debian12:nonroot
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/wallaby /usr/local/bin/wallaby
COPY --from=build /out/wallaby-admin /usr/local/bin/wallaby-admin
COPY --from=build /out/wallaby-worker /usr/local/bin/wallaby-worker
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/wallaby"]
