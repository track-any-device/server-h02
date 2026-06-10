FROM golang:1.23-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /h02-tcp ./server/tcp/cmd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /h02-tcp /h02-tcp
EXPOSE 7020 9092
ENTRYPOINT ["/h02-tcp"]
