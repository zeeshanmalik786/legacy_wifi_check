#!/usr/bin/env bash

source ~/.bash_profile

conda init powershell

conda env update

conda activate legacy_wifi_check

unset SPARK_HOME
unset PYTHONPATH

#python main_entry.py

python migration_confusion_matrix.py

conda deactivate

export SPARK_HOME=/Users/zeeshanmalik/server/spark-2.4.3-bin-hadoop2.7
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:$PYTHONPATH
