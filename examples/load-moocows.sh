#!/usr/bin/env bash

cd "$(dirname "$0")" || exit

curl -X PUT 'http://calpaterson:password@localhost:6001/calpaterson/moocows' -H 'Content-Type: text/csv' --data-binary @moocows.csv
