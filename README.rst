================
Twitter Listener
================

Configure a ``profile.yml`` containing your developer account credentials::

  twitter:
    access_token: '...'
    access_token_secret: '...'
    consumer_key: '...'
    consumer_secret: '...'

  twilio:
    account_sid: '...'
    auth_token: '...'
    source_phone_number: '...'
    target_phone_number: '...'

Configure a file containing filter parameters (``potus.yml``)::

  track:
    - potus

Start the listener::

  pipenv run python listen.py potus.yml potus.gz

Eventually Ctrl-C the listener or send a SIGHUP to the process which will trigger it to rotate the file. Now you have a file that you can convert to json or to a csv::

  pipenv run python tweets_to_csv.py potus.gz potus.csv
