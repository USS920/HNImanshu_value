#!/bin/bash

set -e

# Check if file argument is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <file_path>"
    exit 1
fi

# Read from environment variables
API_TOKEN="5588744140:AAFMzYGBbQDzZ_hYDf9d1WSTHmC3I-Z3kZk"
CHAT_ID="-1002683944312"

if [ -z "$API_TOKEN" ] || [ -z "$CHAT_ID" ]; then
    echo "Missing environment variables"
    exit 1
fi

FILE_PATH="$1"

URL="https://api.telegram.org/bot$API_TOKEN/sendDocument"

if [ ! -f "$FILE_PATH" ]; then
    echo "File not found: $FILE_PATH"
    exit 1
fi

echo "Sending file to Telegram..."

curl -F "chat_id=$CHAT_ID" -F "document=@$FILE_PATH" $URL

echo "Done"
