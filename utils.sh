#!/bin/bash

replStart () {
    howMany=${1:-5}
    for x in $(seq 1 $howMany); do
        numstr=$(printf "%02d" $x)
        myPort=$(( 30000 + $x  - 1 ))
        ./replicatorExe -controller localhost:20000 -meId $x -port "$myPort" >"outs/repl$numstr.txt" 2>&1 &
    done
}

killAllJobs () {
    kill $(jobs -p)
}

#startController () {
#    mkdir -p outs
#    rm -r outs/*
#    ./controllerExe -addr "localhost:20000" -raftDataDir "outs/cont00/" -raftId 1 -rb localhost:20000 >"outs/cont00.txt" 2>&1 &
#}

startControllers () {
    mkdir -p outs
    rm -r outs/*

    howMany=${1:-3}
    #hosts=()
    #for x in $(seq 1 $howMany); do
    #    myPort=$(( 20000 + $x - 1 ))
    #    hosts+=("localhost:$myPort;id$x")
    #done
    mkdir -p "outs/cont00/"
    ./controllerExe -addr "localhost:20000" -raftDataDir "outs/cont00/" -raftId 1 -rb >"outs/cont00.txt" 2>&1 &
    if [ $howMany -eq 1 ]; then
        return
    fi
    sleep 2s # wait for leader to be elected
    for x in $(seq 1 $(($howMany - 1)) ); do
        myport=$(( 20000 + $x ))
        numstr=$(printf "%02d" $x)
        mkdir -p "outs/cont$numstr/"
        #./controllerExe -addr "${hosts[$x]/;*/}" -raftDataDir "outs/cont$numstr" -raftId $x -rb $(join_by , "${hosts[@]}") >"outs/cont$numstr.txt" 2>&1 &
        ./controllerExe -addr "localhost:$myport" -raftDataDir "outs/cont$numstr" -raftId $x -cl "localhost:20000" >"outs/cont$numstr.txt" 2>&1 &
        ./raftadminCli localhost:20000 add_voter $x "localhost:$myport" 0
    done
}

function join_by { local IFS="$1"; shift; echo "$*"; }

cmdofprogram () {
    program=${1?: "Please provide program name"}
    for x in $(ps -C $program -o pid=); do 
        printf "PID: %0.10s " $x
        cat /proc/$x/cmdline | tr '\0' ' '
        printf " -> "
        readlink /proc/$x/fd/1
    done
}