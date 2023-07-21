#!/bin/bash

echo Would you like to install the app globally? [y/n]

read response 

if [[ $response == 'y' ]]; then
	echo Installing in "$HOME/.local/bin" directory
	cp ./dist/main $HOME/.local/bin/sqldumper;
	echo Run 'sqldumper' to start.
	exit;
fi

echo Installing locally...

pip install -r requirements.txt
cp .env.example .env
python3 ./main.py

echo Run 'python3 ./main.py' to start
