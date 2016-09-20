FROM python:3.5-alpine

WORKDIR /build

RUN apk --no-cache add --virtual build-dependencies build-base git

COPY requirements/prod.txt requirements/

RUN apk --no-cache add freetds freetds-dev \
    && pip install -r requirements/prod.txt

COPY source-for-build .

RUN python setup.py install

RUN apk del build-dependencies

RUN rm -Rf source-for-build

WORKDIR /

COPY config.yaml .

CMD ["roisto", "-c", "config.yaml"]
