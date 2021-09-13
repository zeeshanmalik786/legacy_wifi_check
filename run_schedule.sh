#!/bin/bash

cd "$(dirname "$0")"

docker build -t legacy_wifi .

if [ $? -eq 0 ]; then
        echo Docker Image Successfully Created
else
        echo Docker Image Creation Fail
fi


docker run -t -d --name legacy_wifi legacy_wifi 

docker cp ../legacy_wifi_check_prod_v1/ legacy_wifi:/

docker cp /Users/zeeshanmalik/server/spark-2.4.3-bin-hadoop2.7 legacy_wifi:/

#docker exec legacy_wifi /bin/sh -c "./run_container.sh"

#docker exec legacy_wifi /legacy_wifi_check_prod_v1/run_container.sh
