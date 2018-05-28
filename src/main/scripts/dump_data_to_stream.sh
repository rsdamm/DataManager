#!/bin/bash

SN="KinesisDBLoader2"
PK="partitionkey1"
RC="record dump to stream----->>>>>: " # make this record whatever string you want,no whitespace
counter=0

while :
do
  echo -n .
  counter=$((counter+1))
  aws kinesis put-record \
     --stream-name $SN   \
     --partition-key $PK \
     --data "$RC$counter" > /dev/null
  sleep 1
done