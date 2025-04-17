FROM golang:1.22-alpine as builder

WORKDIR /app

COPY . .

RUN sed -i 's/github.com\/jmoiron\/sqlx v1.4.0/github.com\/jmoiron\/sqlx v1.3.5/g' go.mod

RUN go mod download
RUN go mod tidy

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o stackexchange-data-analysis ./cmd

FROM alpine:latest

RUN apk --no-cache add ca-certificates p7zip

WORKDIR /app

COPY --from=builder /app/stackexchange-data-analysis .
COPY scripts /app/scripts

RUN mkdir -p /data

ENTRYPOINT ["/app/stackexchange-data-analysis"]