================
Twitter Listener
================

Configure a ``profile.yml`` containing your developer account credentials::

  access_token: '...'
  access_token_secret: '...'
  consumer_key: '...'
  consumer_secret: '...'

Configure a file containing filter parameters (``potus.yml``)::

  track:
    - potus

Start the listener::

  pipenv run python listen.py potus.yml potus.gz

Eventually Ctrl-C the listener or send a SIGHUP to the process which will trigger it to rotate the file. Now you have a file that you can convert to json or to a csv::

  pipenv run python tweets_to_csv.py potus.gz potus.csv
