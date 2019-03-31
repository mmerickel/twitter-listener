FROM python:3-alpine

RUN set -e \
    && mkdir /app && cd /app \
    && python -m pip install pipenv

WORKDIR /app

ADD Pipfile Pipfile.lock .

RUN pipenv install

ADD listen.py .

ENTRYPOINT ["pipenv", "run", "python", "listen.py"]
