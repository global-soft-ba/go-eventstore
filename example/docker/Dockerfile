FROM golang:1.24.3-bullseye

WORKDIR /

RUN go install github.com/githubnemo/CompileDaemon@latest
RUN go install github.com/go-delve/delve/cmd/dlv@latest

WORKDIR /event

COPY . .

WORKDIR /event/example

RUN go mod tidy

EXPOSE 2345 8080

ENTRYPOINT CompileDaemon -exclude-dir=.git -graceful-kill=true \
            -build="/bin/bash ./docker/tools/build.sh" \
            -command="dlv exec --accept-multiclient --api-version=2 --continue --headless --listen=:2345 /main.e"