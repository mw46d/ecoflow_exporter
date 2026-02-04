FROM python:3.12-alpine

LABEL org.opencontainers.image.authors="Marco Walther <marco@sonic.net>"
LABEL org.opencontainers.image.description="An implementation of a Prometheus exporter for EcoFlow power devices"
LABEL org.opencontainers.image.source=https://github.com/mw46d/ecoflow_exporter
LABEL org.opencontainers.image.licenses=GPL-3.0

WORKDIR /app
RUN apk update && apk add py3-pip

ADD requirements.txt /app
RUN pip install -r /app/requirements.txt

ADD . /app/

CMD [ "python", "/app/ecoflow_exporter.py" ]
