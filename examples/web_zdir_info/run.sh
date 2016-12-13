#!/bin/bash
set -ex
env/bin/uwsgi --http :8084 --wsgi-file app.py
