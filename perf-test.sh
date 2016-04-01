#!/bin/bash

start=$SECONDS

for i in {1001..1050}
do
  DOC_ID="sameple$i.xml"
  curl -w "%{time_total}\n" -o /dev/null -H "Content-Type: application/xml" -H "DOC-ID: $DOC_ID" -d @sample.xml -s "http://localhost:8080/xml-app/rest/addXML"
done

wait
duration=$(( SECONDS - start ))

echo "Total time(s): $duration"
