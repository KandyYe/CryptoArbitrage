FROM python:3.10.16-alpine3.20

RUN pip install ccxt


COPY ./triangle_arbitrage_v2.py /app/

CMD ["python", "/app/triangle_arbitrage_v2.py"]