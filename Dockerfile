# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvnode ./cmd/kvnode \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/kvbench ./cmd/kvbench

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/kvnode /out/kvbench /app/
USER nonroot:nonroot
ENTRYPOINT ["/app/kvnode"]
