#!/bin/bash

trap "trap - SIGTERM && kill -- -$$" SIGINT SIGTERM EXIT

echo Starting 5 controllers and 8 replicators

mkdir -p outs
rm outs/*

./controllerExe -addr localhost:20000 -raftId 1 -rb 2>&1 >"outs/cont00.txt" &
sleep 5s

for x in {2..5}; do
    myPort=$(( 20000 + $x - 1 ))
    ./controllerExe -addr "localhost:$myPort" -raftId $x -cl localhost:20000 2>&1 >"outs/cont0$(( $x - 1 )).txt" &
    sleep 0.1s # sleep for 50ms
    ./raftadminCli localhost:20000 add_voter $x "localhost:$myPort" 0
    echo
done

#start 8 replicators
for x in {1..7}; do
    myPort=$(( 30000 + $x  - 1 ))
    ./replicatorExe -controller localhost:20000 -meId $x -port "$myPort" 2>&1 >"outs/repl0$x.txt" &
    sleep 0.05s # sleep for 50ms
done

jobs
echo
echo All done press enter to kill all processes, and exit
read -r