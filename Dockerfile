FROM python:3.5-alpine

RUN apk --no-cache add --virtual build-dependencies build-base git

COPY requirements/prod.txt requirements/

RUN apk --no-cache add freetds freetds-dev \
    && pip install -r requirements/prod.txt

COPY config.yaml.template LICENSE LICENSE_AGPL README.rst setup.py build/
COPY roisto/__init__.py roisto/utcformatter.py roisto/mqttpublisher.py roisto/cmdline.py roisto/predictionpoller.py roisto/util.py roisto/roisto.py build/roisto/
COPY config.yaml build/

WORKDIR /build

RUN python setup.py install

RUN apk del build-dependencies

CMD ["roisto", "-c", "config.yaml"]
