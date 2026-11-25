FROM golang:1.24.10-alpine3.22 AS builder
ENV CGO_ENABLED=0 GOMAXPROCS=2 GOFLAGS=-p=2
RUN go install github.com/minio/minio@RELEASE.2025-10-15T17-29-55Z

FROM python:3.12.10-slim-bookworm
COPY --from=builder /go/bin/minio /usr/local/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
