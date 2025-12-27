# syntax=docker/dockerfile:1.7
ARG GO_VERSION=1.25.5

FROM golang:${GO_VERSION}-alpine AS build
RUN apk add --no-cache git ca-certificates
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/ductstream ./cmd/ductstream
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/ductstream-admin ./cmd/ductstream-admin
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/ductstream-worker ./cmd/ductstream-worker

FROM gcr.io/distroless/base-debian12:nonroot
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/ductstream /usr/local/bin/ductstream
COPY --from=build /out/ductstream-admin /usr/local/bin/ductstream-admin
COPY --from=build /out/ductstream-worker /usr/local/bin/ductstream-worker
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/ductstream"]
