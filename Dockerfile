# Builder and runtime use the same Alpine release, so the binary links
# against the musl it was built with.
FROM golang:1.27-alpine3.24 AS builder

RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
RUN CGO_ENABLED=1 go build -ldflags "-X main.version=${VERSION}" -o loghub ./cmd/loghub

FROM alpine:3.24

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 1000 loghub && \
    adduser -u 1000 -G loghub -s /bin/sh -D loghub

WORKDIR /app

COPY --from=builder /build/loghub .
RUN chown -R loghub:loghub /app

USER loghub

EXPOSE 9090

ENTRYPOINT ["./loghub"]
