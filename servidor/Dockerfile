FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y gcc libzmq3-dev libmsgpack-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY servidor.c .

RUN gcc servidor.c -o servidor -lzmq -lmsgpackc

CMD ["/app/servidor"]
