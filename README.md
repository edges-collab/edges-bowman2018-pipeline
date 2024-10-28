# edges-bowman2018-pipeline

Repo for formally reproducing the Bowman et al. 2018 Nature paper results.

## Installing the pipeline

The preferred method of constructing the environment for this repo is to use `uv`. 
Install uv with this command:

```
curl -LsSf https://astral.sh/uv/install.sh | sh
```

Then, at the top-level of this project, do `uv pip install .`.

## Running the pipeline

The primary entry-point of the pipeline is at `prefect-pipeline/pipeline.py`. 
This is a file that is meant to be run under the `prefect` framework (which
should have been installed as per above). To run it, start a prefect
server: `prefect server start` in a terminal, then in another terminal, `cd`
into the `prefect-cache` directory, and type `python ../prefect-pipline/pipeline.py`.
You can navigate to `localhost:4200` to get a running summary of the pipeline's progress.

Results will be cached between runs: they will be re-run on a follow-up run if and only
if one of the following is true:

1. The source code for the task is modified (i.e. the task function definition in `pipeline.py`)
2. The source code in the task notebook is modified
3. The input parameters are changed.

Overall, the pipeline should take ~20min to run on `excalibur`. 

The results are stored in the `output-notebooks/` directory. Each notebook that is run is saved there
(with all the plots etc), and also output `.gsh5` files are put in there for each night (and the averages).

