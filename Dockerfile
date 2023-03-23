# syntax=docker/dockerfile:1

## Build
FROM golang:1.18-alpine AS build
WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download
RUN go mod download golang.org/x/sys

COPY ./ ./

WORKDIR /app/video_rec_service/server
RUN go build -o /video-rec-service

## Deploy
FROM alpine:latest

WORKDIR /

COPY --from=build /video-rec-service /video-rec-service

EXPOSE 8080

ENTRYPOINT [ "/video-rec-service" ]
