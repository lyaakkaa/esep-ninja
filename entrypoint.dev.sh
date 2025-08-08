#!/bin/sh
python manage.py collectstatic --noinput
python manage.py migrate
gunicorn -c config/server/gunicorn.conf.py --reload