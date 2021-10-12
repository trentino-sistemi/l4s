#!/bin/bash

echo "collecting static files"
python3 manage.py collectstatic --no-input
echo "compiling messages"
python3 manage.py compilemessages
echo "starting development server"
python3 -Wd manage.py runserver 0.0.0.0:8000
