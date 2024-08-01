FROM golang:1.20-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o main . 

FROM ubuntu:latest

WORKDIR /app

COPY --from=builder /app/main .

EXPOSE 8000

CMD ["./main"]
