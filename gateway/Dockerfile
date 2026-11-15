# Builds both binaries: the gateway and the fake upstream.
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/gateway ./cmd/gateway \
 && CGO_ENABLED=0 go build -o /out/fakeupstream ./cmd/fakeupstream

# The runtime image holds the two static binaries and nothing else, and runs
# them as a non-root user.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/gateway /out/fakeupstream /usr/local/bin/
ENTRYPOINT ["gateway"]
