#! /bin/bash

pip install -r requirements.txt

cp .env.example .env

python3 ./main.py