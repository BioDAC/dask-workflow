#!/bin/bash
#SBATCH --account ACCOUNT-NAME
#SBATCH --partition icelake
#SBATCH --time 12:00:00

# This will give us `nodes` tasks, with (in this case) 76 CPUs per task, i.e.
# the whole node on each task.  The queue system assigns memory based on
# how many CPUs we request.  We could also just request memory explicitly.
# TODO: use --exclusive to get whole node, avoids knowing number of CPUS?  But
# what if we want to share nodes?  (We probably don't.)
#SBATCH --cpus-per-task=76 --nodes=7

# This spreads the tasks out on the nodes, which is needed for the `srun`
# below.
#SBATCH --distribution=cyclic

set -eou pipefail
echo -n "Started at: "
date

# Change to the working directory and load the Python venv
cd ~/hpc-work/BioDAC/full-run-correct
source /rds-d7/user/sjc306/hpc-work/BioDAC/venv/bin/activate

# This is the important bit.  We start N+2 tasks, where N is the number of
# tasks requested above.  The first two tasks are the scheduler and the client.
# The cyclic distribution spreads the tasks across the nodes, so there are
# workers on all the nodes, and the client and scheduler are on the first two
# nodes.
#
# We disable CPU binding, because the auto-binding will assume two tasks on a
# node should share the CPUs, which means our scheduler and client tasks hog
# half the CPUs on the first two nodes.
srun --cpu-bind=verbose,none --overcommit --ntasks=$((SLURM_JOB_NUM_NODES+2)) ./dask-new-overlap.py &

wait

echo -n "Finished at: "
date
