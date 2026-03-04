FROM golang:1.26-alpine

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download && go mod verify

ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache
COPY . .
CMD go test -v ./nwfs/...