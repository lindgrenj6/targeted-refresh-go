FROM golang:1.14-alpine AS builder
MAINTAINER jlindgre@redhat.com

WORKDIR /root
COPY . /root/
RUN go mod download && CGO_ENABLED=0 go build -o targeted_refresh .

FROM scratch
COPY --from=builder /root/targeted_refresh .
ENTRYPOINT ["/targeted_refresh"]
