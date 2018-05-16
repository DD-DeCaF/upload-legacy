FROM python:3.6-slim

RUN apt-get update && apt-get -y upgrade && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


RUN pip install --upgrade pip setuptools wheel
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade --process-dependency-links -r /tmp/requirements.txt && \
    rm -rf /root/.cache /tmp/* /var/tmp/*

ARG CWD=/app
WORKDIR "${CWD}/"

ENV PYTHONPATH=${CWD}/src

COPY . "${CWD}/"

CMD ["gunicorn", "-w", "4", "-b", "0.0.0.0:8000", "-t", "150", "-k", "aiohttp.worker.GunicornWebWorker", "upload.app:get_app()"]
