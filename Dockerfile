FROM python:3-alpine

RUN set -e \
    && mkdir /app && cd /app \
    && python -m pip install pipenv

ADD Pipfile /app/Pipfile
ADD Pipfile.lock /app/Pipfile.lock
ADD listen.py /app/listen.py

WORKDIR /app

RUN pipenv install

ENTRYPOINT ["pipenv", "run", "python", "listen.py"]
