FROM golang:1.13 AS build-env
WORKDIR /go/src/github.com/fairwindsops/polaris/

ENV GO111MODULE=on
ENV GOPROXY=https://proxy.golang.org

COPY . .
RUN go get -u github.com/gobuffalo/packr/v2/packr2

ENV CGO_ENABLED=0
ENV GOOS=linux
ENV GOARCH=amd64
RUN packr2 build -a -o polaris *.go

FROM alpine:3.10
WORKDIR /usr/local/bin
RUN apk --no-cache add ca-certificates

RUN addgroup -S polaris && adduser -u 1200 -S polaris -G polaris
USER 1200
COPY --from=build-env /go/src/github.com/fairwindsops/polaris/polaris .

WORKDIR /opt/app

CMD ["polaris"]
