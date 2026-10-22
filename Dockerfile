# syntax=docker/dockerfile:1

# ---------- 构建 ----------
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags "-s -w \
      -X github.com/10dian-ai/newapi-channel-synchronization/internal/version.Version=${VERSION} \
      -X github.com/10dian-ai/newapi-channel-synchronization/internal/version.Commit=${COMMIT} \
      -X github.com/10dian-ai/newapi-channel-synchronization/internal/version.Date=${DATE}" \
    -o /out/newapi-sync ./cmd/newapi-sync

# ---------- 运行 ----------
FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata wget \
    && adduser -D -H -u 10001 newapi-sync
COPY --from=build /out/newapi-sync /usr/local/bin/newapi-sync
USER newapi-sync
WORKDIR /app
ENV NEWAPI_SYNC_CONFIG=/app/config.yaml
EXPOSE 8090
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8090/healthz || exit 1
ENTRYPOINT ["newapi-sync"]
CMD ["run"]
