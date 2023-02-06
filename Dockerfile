FROM --platform=arm64 public.ecr.aws/docker/library/golang:latest as build
ENV GOPROXY=direct
WORKDIR /usr/src/app
COPY go.mod go.sum ./
RUN go mod tidy && go mod  download
COPY . .
RUN make build

FROM --platform=arm64 public.ecr.aws/lambda/provided:al2
COPY --from=build /usr/src/app/bin/main ${LAMBDA_RUNTIME_DIR}/bootstrap
CMD [ "main"]
