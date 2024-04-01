FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /bin/clatterdb ./cmd/clatterdb
RUN CGO_ENABLED=0 go build -o /bin/clatter-cli ./cmd/clatter-cli

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/clatterdb /bin/clatterdb
COPY --from=builder /bin/clatter-cli /bin/clatter-cli

VOLUME /data
EXPOSE 9090 9091

ENTRYPOINT ["/bin/clatterdb"]
