#!/bin/bash

start=$SECONDS

for i in {1..50}
do 
  curl -w "%{time_total}\n" -o /dev/null -H "Content-Type: application/xml" -H "DOC-ID: $i" -d @sample.xml -s "http://localhost:8080/loading-xml-example/rest/addXML"
done

wait
duration=$(( SECONDS - start ))

echo "Total time(s): $duration"
