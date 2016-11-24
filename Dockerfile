FROM python:3.5

ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt

ADD . ./upload
WORKDIR upload

ENV PYTHONPATH $PYTHONPATH:/upload

ENTRYPOINT ["gunicorn"]
CMD ["-w", "4", "-b", "0.0.0.0:7000", "-t", "150", "-k", "aiohttp.worker.GunicornWebWorker", "upload.app:app"]