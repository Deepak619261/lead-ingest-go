# Multi-stage build: ~15 MB static image, no shell, non-root.
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ingest ./cmd/ingest \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/mockengine ./cmd/mockengine

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
EXPOSE 8080
ENTRYPOINT ["ingest", "serve"]
