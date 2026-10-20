FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir pyzmq msgpack

COPY referencia.py .

CMD ["python", "-u", "referencia.py"]