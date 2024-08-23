#!/bin/bash
PARSER_PATH=/home/moustafa-mahmoud/address_poisoning_dataset
PARSER_PY=$PARSER_PATH/gather_addresses_metadata.py
PYTHON_PATH=/home/moustafa-mahmoud/.local/share/virtualenvs/address_poisoning_dataset-Hfv8_GC8/bin/python

cd $PARSER_PATH


ps -ef|grep "$PARSER_PY" |grep -v grep
if [ "$?" != "0" ]
then
  $PYTHON_PATH $PARSER_PY
fi
