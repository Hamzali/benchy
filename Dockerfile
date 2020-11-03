FROM golang:alpine as build

WORKDIR /app
ENV GO111MOD=on
COPY ./go.mod  .
COPY ./go.sum  .
RUN go mod download

COPY ./ .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build cmd/benchy.go

FROM scratch as final

COPY --from=build /app/benchy /bin/benchy
COPY ./config.json .

ENTRYPOINT ["benchy"]
CMD ["-config", "./config.json"]