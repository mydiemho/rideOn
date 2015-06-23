#!/bin/bash
NUM_SPAWNS=$1
SESSION=$2
tmux new-session -s $SESSION -n bash -d
for ID in `seq 1 $NUM_SPAWNS`;
do
    echo "data/locations" + $(($ID%4)) + ".csv"
#    tmux new-window -t $ID
#    tmux send-keys -t $SESSION:$ID 'kafka/locationProducer.py kafka/config.json data/locations' + $(($ID%4)) + '.csv' C-m
done
