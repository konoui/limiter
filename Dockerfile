FROM --platform=$BUILDPLATFORM public.ecr.aws/docker/library/golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags "-s -w" -o /out/bootstrap ./cmd

FROM --platform=linux/arm64 public.ecr.aws/lambda/provided:al2023
COPY --from=build /out/bootstrap ${LAMBDA_RUNTIME_DIR}/bootstrap
COPY cmd/example-config.yaml /etc/limiter/config.yaml
ENV LIMITER_CONFIG_FILEPATH=/etc/limiter/config.yaml
CMD ["bootstrap"]
