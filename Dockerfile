ARG GO_VERSION=1.26.6

FROM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG SERVICE
RUN test -n "${SERVICE}" || (echo "SERVICE build-arg is required" >&2; exit 1)
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -ldflags="-s -w" \
        -o /out/app \
        ./cmd/${SERVICE}

FROM gcr.io/distroless/static-debian12:nonroot AS runtime
ARG SERVICE
ARG VERSION=dev
ARG COMMIT=unknown
LABEL org.opencontainers.image.title="payment-service-${SERVICE}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${COMMIT}" \
      org.opencontainers.image.source="https://github.com/blue-samarth/payment-service"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/app /app
USER nonroot:nonroot
ENTRYPOINT ["/app"]
