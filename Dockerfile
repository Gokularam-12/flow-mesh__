FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o flowmesh ./cmd/flowmesh

FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/flowmesh .
COPY policies /policies
EXPOSE 18000 8081
CMD ["./flowmesh"]
