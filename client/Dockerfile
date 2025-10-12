# original source: https://github.com/juan518munoz/tp0-base/blob/master/client/Dockerfile

FROM golang:1.24.6 AS builder
# Client uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
# First stage is used to compile golang binary and second stage is used to only copy the
# binary generated to the deploy image.
# Docker multi stage does not delete intermediate stages used to build our image, so we need
# to delete it by ourselves. Since docker does not give a good alternative to delete the intermediate images
# we are adding a very specific label to the image to then find these kind of images and delete them
LABEL intermediateStageToBeDeleted=true

RUN mkdir -p /build
WORKDIR /build/
COPY . .
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -mod vendor -o bin/client github.com/fiuba-distribuidos-2C2025/tp1/client


FROM busybox:latest
COPY --from=builder /build/bin/client /client
VOLUME /config
VOLUME /data
VOLUME /results
ENTRYPOINT ["/bin/sh"]
