FROM python:3.6-slim

RUN apt-get update && apt-get -y upgrade && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


RUN pip install --upgrade pip setuptools wheel
COPY requirements.txt /tmp/requirements.txt
RUN pip install --upgrade --process-dependency-links -r /tmp/requirements.txt && \
    rm -rf /root/.cache /tmp/* /var/tmp/*

COPY . /opt/upload

# Setting the working directory is necessary for the tests.
WORKDIR /opt/upload

ENV PYTHONPATH "${PYTHONPATH}:/opt/upload"

ENTRYPOINT ["gunicorn"]
CMD ["-w", "4", "-b", "0.0.0.0:7100", "-t", "150", "-k", "aiohttp.worker.GunicornWebWorker", "upload.app:app"]
