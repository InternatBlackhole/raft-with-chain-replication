#!/bin/bash

replStart () {
    make replicatorExe
    rm -rf outs/repl*
    local howMany=${1:-5}
    for x in $(seq 1 $howMany); do
        local numstr=$(printf "%02d" $x)
        local myPort=$(( 30000 + $x  - 1 ))
        ./replicatorExe -controller localhost:20000 -meId $x -port "$myPort" "${hosts[@]//;*/}" >"outs/repl$numstr.txt" 2>&1 &
    done
}

killAllJobs () {
    kill $(jobs -p)
}

startControllers () {
    make controllerExe
    mkdir -p outs
    rm -fr outs/cont*

    local howMany=${1:-3}
    hosts=()
    for x in $(seq 1 $howMany); do
        local myPort=$(( 20000 + $x - 1 ))
        hosts+=("localhost:$myPort;id$x")
    done
    mkdir -p "outs/cont00/"
    ./controllerExe -addr "localhost:20000" -raftDataDir "outs/cont00/" -raftId 0 -rb >"outs/cont00.txt" 2>&1 &
    if [ $howMany -eq 1 ]; then
        return
    fi
    sleep 3s # wait for leader to be elected
    for x in $(seq 1 $(($howMany - 1)) ); do
        local myport=$(( 20000 + $x ))
        local numstr=$(printf "%02d" $x)
        mkdir -p "outs/cont$numstr/"
        #./controllerExe -addr "${hosts[$x]/;*/}" -raftDataDir "outs/cont$numstr" -raftId $x -rb $(join_by , "${hosts[@]}") >"outs/cont$numstr.txt" 2>&1 &
        ./controllerExe -addr "localhost:$myport" -raftDataDir "outs/cont$numstr" -raftId $x "localhost:20000" >"outs/cont$numstr.txt" 2>&1 &
        #./raftadminCli localhost:20000 add_voter $x "localhost:$myport" 0
    done
}

controller () {
    make controllerExe
    local addr=${1:-"localhost:20000"}
    shift
    local id=${1:-"rng"}
    shift
    local dataDir=${1:?"set data dir"}
    shift
    local cluster=$@
    if [ -z "$cluster" ]; then
        cluster="localhost:20000"
    fi
    ./controllerExe -addr $addr -raftDataDir $dataDir -raftId $id $cluster >"$dataDir/out.txt" 2>&1 &
}

replicator () {
    make replicatorExe
    local addr=${1:-"localhost:20000"}
    shift
    local id=${1:-"rng"}
    shift
    local port=${1:?"set port"}
    shift
    local cluster=$@
    if [ -z "$cluster" ]; then
        cluster="localhost:20000"
    fi
    ./replicatorExe -controller $addr -meId $id -port $port $cluster >"$outs/out$id.txt" 2>&1 &
}

launch () {
    local contNum=${1:?"Please provide controller number"}
    local replNum=${2:?"Please provide replicator number"}

    startControllers $contNum
    sleep 1s
    replStart $replNum
}

function join_by { local IFS="$1"; shift; echo "$*"; }

cmdofprogram () {
    local program=${1?: "Please provide program name"}
    for x in $(ps -C $program -o pid=); do 
        printf "PID: %0.10s " $x
        cat /proc/$x/cmdline | tr '\0' ' '
        printf " -> "
        readlink /proc/$x/fd/1
    done
}

client () {
    make clientExe
    local cont=${1:-"localhost:20000"}
    ./clientExe -controller $cont
}