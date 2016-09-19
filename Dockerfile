FROM python:3.5-alpine

RUN apk --no-cache add freetds \
    && pip install -r requirements/prod.txt \
    && python setup.py install

ADD config.yaml /config.yaml

CMD ["roisto", "-c", "/config.yaml"]
