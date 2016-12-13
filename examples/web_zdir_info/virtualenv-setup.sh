#!/bin/bash
set -ex
python=$(which python3.5)
env=env
rm -rf ${env}
${python} -m venv ${env}
${env}/bin/pip install -U pip
${env}/bin/pip install -r requirements.txt
${env}/bin/pip install --editable .
