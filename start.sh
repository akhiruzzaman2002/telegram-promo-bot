#!/bin/bash

echo "Starting PromoBot..."

if [ ! -f .env ]; then
    echo ".env file not found!"
    exit 1
fi

source .env
if [ -z "$BOT_TOKEN" ]; then
    echo "BOT_TOKEN not set!"
    exit 1
fi

pip install -r requirements.txt
python bot.py
