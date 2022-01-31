FROM golang:1.17.6-alpine as go-image
WORKDIR /build

# install dependensies
RUN apk add --no-cache make git tzdata openssh build-base

# install delve debugger
RUN go get github.com/go-delve/delve/cmd/dlv

# copy source files
COPY . ./

WORKDIR /build/tests

RUN go mod vendor
RUN CGO_ENABLED=0 GODEBUG=tls13=0 GOFLAGS=-mod=vendor GOOS=linux go build -gcflags="all=-N -l" -ldflags "-extldflags -static" -o ./bin/go-worker.bin ./main

FROM python:3.10-slim
WORKDIR /build
ENV PYTHONPATH /build/tests

RUN mkdir -p ./tests

COPY ./tests/worker/*.py ./tests/worker/
COPY ./tests/worker/requirements.txt ./tests/worker/requirements.txt
COPY ./tests/util/*.py ./tests/util/

## python dependencies
RUN pip install -U pip && pip install --no-cache-dir -r ./tests/worker/requirements.txt
## go worker
COPY --from=go-image /build/tests/bin/go-worker.bin ./tests/worker/init.bin
## go debugging
COPY --from=go-image /go/bin/dlv /go/bin/dlv
## runtime debugging dependencies for go
RUN yes | apt update && yes | apt install musl-dev
## permissions
RUN chmod +x ./tests/worker/init.py ./tests/worker/init.bin