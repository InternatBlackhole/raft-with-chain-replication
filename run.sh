#!/bin/bash
#SBATCH --job-name=replikacija
#SBATCH --reservation=psistemi
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --array=0-10
#SBATCH --output=out-%a.txt
#SBATCH --time=00:00:16

#module load Go
#rm out-*.txt

#go mod tidy
#go build *.go

srun naloga04 -i $SLURM_ARRAY_TASK_ID -n $(( $SLURM_ARRAY_TASK_COUNT - 1 )) $@