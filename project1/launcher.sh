#!/bin/bash

# Change this to your netid
#netid=ash170000
netid=rdc180001

# Root directory of your project
#PROJECT_DIR=$HOME/DistributedComputing/Project1
PROJECT_DIR=/home/013/r/rd/rdc180001/DCProject1

# Directory where the config file is located on your local system
CONFIG_LOCAL=./config.txt
CONFIG_REMOTE=$PROJECT_DIR/config.txt

# Directory your java classes are in
BINARY_DIR=$PROJECT_DIR/cmake-build-debug

# Your main project class
PROGRAM=Project1

n=0

cat $CONFIG_LOCAL | sed -e "s/#.*//" | sed -e "/^\s*$/d" |
(
  read i
  echo $i
  while [[ $n -lt $i ]]
  do
    read line
    p=$( echo $line | awk '{ print $1 }' )
    host=$( echo $line | awk '{ print $2 }' )
    kitty --hold -e ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no $netid@$host $BINARY_DIR/$PROGRAM $p $CONFIG_REMOTE &
    n=$(( n + 1 ))
  done
)