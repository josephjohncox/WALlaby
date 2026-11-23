# syntax=docker/dockerfile:1.7
ARG GO_VERSION
FROM golang:${GO_VERSION} AS build

# Upstream RELEASE.2025-09-07T16-13-09Z (AGPL-3.0).
# Build from public, checksum-pinned source: upstream images/binaries are gone.
ADD --checksum=sha256:8819e3e7817e46b7b3798f8f200ead208562e571563c2e040352378031abe9f2 https://codeload.github.com/minio/minio/tar.gz/07c3a429bfed433e49018cb0f78a52145d4bedeb /tmp/minio.tar.gz
WORKDIR /src
RUN tar -xzf /tmp/minio.tar.gz --strip-components=1 \
    && CGO_ENABLED=0 GOTOOLCHAIN=local go build -mod=readonly -trimpath \
       -ldflags="-s -w" -o /out/minio .

FROM scratch
LABEL org.opencontainers.image.source="https://github.com/minio/minio" \
      org.opencontainers.image.revision="07c3a429bfed433e49018cb0f78a52145d4bedeb" \
      org.opencontainers.image.licenses="AGPL-3.0-only"
COPY --from=build /out/minio /usr/local/bin/minio
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /src/LICENSE /licenses/LICENSE
ENTRYPOINT ["/usr/local/bin/minio"]
