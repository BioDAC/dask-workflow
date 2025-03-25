# Dask workflow for denoising large images
A brief run-down of the Dask-based workflow for denoising large images.

## Setup
In a Python environment install the following packages:
`ndsafir>=4.1.1 dask[distributed] pylibczirw zarr<3`. If
you want to run the Jupyter notebooks then also install
`jupyter bokeh>=3.1.0 graphviz`. If you want to run on
a cluster then install `dask_jobqueue`.

E.g.
```
pip install 'ndsafir>=4.1.1' 'dask[distributed]' 'pylibczirw' 'zarr<3'
pip install 'jupyter' 'bokeh>=3.1.0 graphviz'
pip install 'dask_jobqueue'
```

## Files included

### Jupyter notebooks
The `explain-workflow.ipnb` notebook runs through the workflow
used to denoise the mouse embryo image on CSD3.  It has
been slightly modified so that it will run on a local machine
rather than the cluster.  You should change the `filename` and
`output_filename` parameters to point to input and output locations
on your local machine.  The code expects a CZI file for input, and
will produce a Zarr file for output.  It is set to run one worker
with one thread, using up to 16GiB of memory.  You can also adjust
this.

The `explain-dask.ipynb` notebook gives a painfully brief primer
on Dask.
Dask's own [documentation](https://docs.dask.org/en/stable/)
is pretty good.

### Python script
The `dask-new-overlap.sh` file contains the script that was run to
denoise the large mouse embryo image on CSD3.  It is largely the
same as the code in the Jupyter notebook.  The major difference
is the code:

```
with SLURMRunner(
    scheduler_file="/rds/user/sjc306/hpc-work/scheduler-{job_id}.json",
    worker_options={
        "interface": "ib0",
        "local_directory": "/local/sjc306",
        "memory_limit": "240GiB",
        "nthreads": 1,
    },
    scheduler_options={
        "dashboard": True,
    },      
) as runner:
        
    with Client(runner) as client: 
        """
        From here on the code only runs on the dask Client.
        """
                                          
        # PARAMETERS
        # Input file
...
```
This expects the job to be running in a SLURM job.  It will look for
SLURM job parameters in the local environment.  The settings here
are suitable for the CSD3 cluster (after changing the `sjc306` parts
to your username).

### Shell script
The `inv-n7-12hr.sh` script is an example job script for CSD3 that
configures a job for our denoising workflow.  You should adjust the
number of nodes and time requested to suit.  Note that the time
must be enough to denoise a single chunk!

The key parts are:
```
# This spreads the tasks out on the nodes, which is needed for the `srun`
# below.
#SBATCH --distribution=cyclic
```
and
```
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
```
These lines request N nodes, but start N+2 tasks, spread cyclically across the
N nodes. Dask jobqueue makes the first two tasks the Client and the Scheduler
which are needed, but which require very little CPU or memory. The remaining
tasks are Workers. Since it
would be wasteful to give the Client and Scheduler an entire node each we make them
share nodes with Workers.  So the first two nodes have the Client and Scheduler,
and also a Worker each, as the allocation cycles round.

### General notes
#### More than 1 worker on a node
The CSD3 example uses Dask Workers with 1 thread. When the Worker starts
the denoising code we assign all the threads on the node to the denoising
code. If we want to have more than 1 worker on each node we would start
extra tasks on each node by modifying the `srun --ntasks` line above. We
would then have to reduce the number of threads being asssigned to
the denoising by the Python script.  I can't see this being useful, since
we are always limited by memory, but this is how you'd do it.

#### Viewing the Dashboard
It is possible to view the Dask dashboard for a job running on the cluster.
The scripts above enable the dashboard, so all you need to do is connect to it.
First you need to SSH to a CSD3 login node with some extra flags:
```
ssh -D localhost:9000 login-q-2.hpc.cam.ac.uk
```
Now you must set up your web browser to use a proxy. I use FoxyProxy for
Firefox.  Try to make sure that you only enable the proxy for a single tab,
otherwise all your traffic will use the proxy and things will either not
work or be very slow. Set the proxy to be `localhost:9000`, or the proxy
host to be `localhost` and the proxy port to be `9000`.  No other parameters
should be needed.  Once you've done this you can start your job and look
in the output for lines like:
```
2025-03-01 11:36:45,271 - distributed.scheduler - INFO -   Scheduler at:   tcp://10.43.76.35:38611
2025-03-01 11:36:45,271 - distributed.scheduler - INFO -   dashboard at:  http://10.43.76.35:8787/status
```
Browse to the URL like `http://10.43.76.35:8787/status` and you should see the dashboard.  Don't forget
to disable the proxy once you're done!
