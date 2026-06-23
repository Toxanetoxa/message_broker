FROM golang:1.25-alpine AS build

WORKDIR /src
COPY main.go ./

RUN CGO_ENABLED=0 go build -o /out/message_broker main.go

FROM alpine:3.22

WORKDIR /app
COPY --from=build /out/message_broker /app/message_broker

EXPOSE 8080
ENTRYPOINT ["/app/message_broker"]
CMD ["8080"]
