FROM python:3.10

RUN pip install numpy pandas pika

COPY . .