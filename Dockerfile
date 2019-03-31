FROM python:3-alpine

RUN set -e \
    && mkdir /app && cd /app \
    && python -m pip install pipenv

WORKDIR /app

COPY Pipfile Pipfile.lock ./

RUN pipenv install

COPY listen.py ./

ENTRYPOINT ["pipenv", "run", "python", "listen.py"]
