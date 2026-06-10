FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /h02-udp ./server/udp/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /h02-udp /h02-udp
EXPOSE 7021/udp 9093
ENTRYPOINT ["/h02-udp"]
