#!/bin/bash
while :
do
    echo "Running Data Miner. Do not interrupt"
    date
    date >>progress.txt 2>&1
    ./newsMiner.py >>progress.txt 2>&1
    echo "Sleeping for twenty mins. You may kill process."
    date
    sleep 1200
done
