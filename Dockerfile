FROM python:3.7

RUN set -e \
    && mkdir /app && cd /app \
    && python -m pip install pipenv

WORKDIR /app

COPY Pipfile Pipfile.lock ./

RUN DEBIAN_FRONTEND=noninteractive apt-get update \
    && apt-get install -y build-essential \
    && pipenv install \
    && apt-get remove -y build-essential \
    && apt-get clean

COPY tweeter ./

ENTRYPOINT ["pipenv", "run", "tweeter"]
