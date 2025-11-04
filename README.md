# edges-bowman2018-pipeline

A pipeline for formally reproducing the Bowman et al. 2018 Nature paper results using 
`nextflow`.

## Setting Up

To set up the environment to run this pipeline, follow these steps:

1. Install the python environment.
   1. First installl `uv` if you haven't already: 

      ```bash
      curl -LsSf https://astral.sh/uv/install.sh | sh
      ```
   2. Then at the top-level of this project, do `uv sync`.

2. Install the jupyter kernel: 
   
   ```bash
   uv run python -m ipykernel install --user  --name bowman2018-pipeline
   ```

   Note that if you choose a different kernel name, then you will need to update the
   `nextflow.config` file to reflect this (look for the line that says `kernel = ...`).

3. Installing `nextflow`:
   Follow the instructions [here](https://nextflow.io/docs/latest/install.html):

   ```bash
   curl -s https://get.nextflow.io | bash
   chmod +x nextflow
   mkdir -p $HOME/.local/bin/
   mv nextflow $HOME/.local/bin/
   ```


## Running the pipeline

The pipeline is run using `nextflow`. Once you are setup, running is very simple.
First ensure your python environment is activated:

```bash
source .venv/bin/activate
```

Then run the pipeline with the desired profile. For example, to run the "injectbeam-injectrfi" case, do the following from the project root:

```bash
nextflow run bowman2018.nf -profile injectbeam_injectrfi -resume
```

See the [NextFlow Docs](https://nextflow.io/docs/latest/cli.html) for more information 
about the arguments you can pass to `nextflow`.

Some of the more important aspects are:

* Always use `-resume` to avoid re-computing things you've already done.
* You can use `nextflow log` to inspect past runs and their status. I especially like to
  use `nextflow log <RUNNAME> -f workdir,process,script` to see where a particular 
  notebook ran (each process runs in its own keyed directory to isolate it, which is 
  great, but then you have to be able to go and find the outputs if you want to debug).


## Understanding how the pipeline works

The pipeline consists of a few components:

1. A top-level `bowman2018.nf` workflow file. This defines the overall
   run logic of the pipeline. 
2. A set of config files, in `configs/`. These are just different parameter sets for choices
   in running the pipeline. Each file controls the parameters for one notebook in 
   the pipeline (the notebook it controls is the prefix of the filename, so e.g. 
   the config controlling the beam factor computation is `beamfac.yaml`). If the YAML
   file has anything else in the title, it indicates a specific case, e.g. 
   `dayavg-injectflags.yaml` is the config for the day-averaging notebook for the
   case where we inject RFI flags from the legacy pipeline.
4. This pipeline depends on the `edges-pipeline-utils` package (which is installed
   when you run `uv sync` above). This gives access to the `epipe` program.
   This program has a few commands, for example: `get-ydays`, `gather` and `run`. 
   These commands are used within the `nextflow` workflow file, but you can also just
   use them from the CLI to test things out.
5. There is a `results/` folder, which is where the results of the pipeline will be 
   stored. Each `case` will be saved to a different subfolder. If you change the 
   parameters *within* a case, the results will be over-written. Importantly, within
   this folder there is a  `notebooks/` folder housing all the
   evaluated analysis notebooks (with plots included) from the runs. 
6. Once you do a run, there will also be a `work/` folder, which houses the intermediate
   cache products of the run. You'll only interact with this folder if you're debugging.
7. The `nextflow.config` file houses configuration options for `nextflow` itself, as well
   as some parameters for the pipeline (e.g. where the raw data is stored, what kernel
   to use for running the notebooks, etc).
8. Importantly, the `nextflow.config` also defines a list of "profiles", each of which
   corresponds to a different "case" for the pipeline. Each profile specifies the set
   of YAML config files that will be used. Thus, changing which entire case to run can
   be accomplished by changing the `-profile` on the CLI when running `nextflow`.
9. Even when running different `profiles`, still use `-resume` every time -- this way
   when you are only changing parameters affecting the end of the processing, you will
   avoid a lot of unnecessary recomputation.

### The `nextflow` workflow file

The `nextflow` workflow (a `.nf` file) drives the whole pipeline. You can read all about
how to write workflows at the `nextflow` documentation. Here I'll just give the barebones
so you can kinda grok how our pipeline works. The file consists of the following:

1. A set of `process` definitions, of the form `process NAME {... code ...}`. These
   are the backbone of the workflow. They take some `inputs:`, define some `outputs:`
   and run a `script:` to get between the inputs and outputs. The `script:` itself is
   generally written in `bash`, and we try to make it as short as possible (generally
   it just calls some other script written in a nicer language like Python to do the
   actual work). Within any `process`, the parameters we mentioned above can be 
   accessed via `${params.PARAMNAME}`. 
2. Finally, a single `workflow` definition, of the form `workflow {... code ...}`. This
   workflow is the top-level glue that puts all the processes together, sending the 
   data through the pipeline. 
3. An `output` clause that tells the pipeline where to put all the "published" files
   at the end.

Note that pretty much all of our `process` scripts are simply calls to our 
`epipe` CLI tool. Most of them use `epipe run`, which goes and runs a particular Jupyter
notebook. Any actual analysis logic you want to implement should be done in one of these
notebooks.


## Notes

* We switched from having a `configs/` directory with a separate folder for each "case" 
to having loose YAML files in the `configs/` directory, because `nextflow` caches on the
inputs to a process, which would change ALL processes for every case. This meant that if 
you updated something that only affected e.g. the final averaging 
over nights, it would still re-run all the xRFI etc. We maintain the ability to 
easily switch between parameter-sets by using the `-profile` argument to `nextflow run`.
* We also pulled out the `edges-pipelines` package to a separate repo, because it can
easily be re-used in other pipelines. 
* Notebooks for this pipeline are kept in this repo, because they are quite specific.
Furthermore, it's better to pass the notebook explicitly by path, so that if it is 
updated, it will invalidate the cache.