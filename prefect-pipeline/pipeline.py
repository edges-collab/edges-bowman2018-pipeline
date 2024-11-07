from pathlib import Path

from prefect import task, flow, unmapped
from prefect.futures import wait
from prefect.cache_policies import INPUTS, TASK_SOURCE
from datetime import datetime as dt, timedelta
from edges_pipelines.runners import run_notebook
from pygsdata import GSData
from prefect_dask import DaskTaskRunner
import inspect
here = Path(__file__).parent
from hashlib import md5
from edges_pipelines import NOTEBOOK_DIR

# Make hashes for all the notebooks
notebooks = NOTEBOOK_DIR.glob("*.ipynb")
nb_hashes = {}
for nb in notebooks:
    with open(nb, 'r') as fl:
        nb_hashes[nb.with_suffix("").name] = hash(fl.read())
        
def notebook_hasher(notebook):
    def fnc(context, parameters: dict) -> str:
        return str(hash(tuple(parameters.keys()) + tuple(parameters.values())) + hash(inspect.getsource(context.task.fn)) + nb_hashes[notebook])
    return fnc
    
@task(
    persist_result=True, 
    tags=["day-sized"], 
    task_run_name="avg-{yday[0]}-{yday[1]:>03}", 
    cache_key_fn=notebook_hasher('single-day'),
)
def run_daily_notebook(
    yday,
    kernel: str,
    output_dir: Path,
    cfgfile: Path | None = None,
    convert_args: str = "",
) -> Path:
    year, day = yday
    run_notebook(
        "single-day",
        kernel=kernel,
        output_dir=output_dir,
        convert_args=convert_args,
        cfgfile=cfgfile,
        basename=f"single-day-avg-{year}-{day:>03}",
        year=year,
        day=day,
        cachedir=str(output_dir.absolute()),
    )

    outfile = output_dir / f"{year}-{day:>03}.averaged.gsh5"
    
    if outfile.exists():
        return outfile
    else:
        # Just because it doesn't exist doesn't mean the notebook failed... there 
        # might just not be any data that night. If the notebook ERRORS it will still
        # be a failed task.
        print(f"No averaged file created for {year}-{day:>03}!")
        return None
    
@task(
    persist_result=True, 
    task_run_name="cal-{yday[0]}-{yday[1]:>03}", 
    cache_key_fn=notebook_hasher('single-day-calibration'),
)
def run_daily_cal_notebook(
    avgfile: Path | None,
    yday: tuple[int, int],
    calfile: Path,
    ants11file: Path,
    kernel: str,
    output_dir: Path,
    cfgfile: Path | None = None,
    convert_args: str = "",
) -> Path:
    if avgfile is None:
        # That's fine.
        return None

    year, day = yday
         
    run_notebook(
        "single-day-calibration",
        kernel=kernel,
        output_dir=output_dir,
        convert_args=convert_args,
        cfgfile=cfgfile,
        basename=f"single-day-cal-{year}-{day:>03}",
        year=year,
        day=day,
        datadir=str(output_dir.absolute()),
        calfile=str(calfile.absolute()),
        ants11file=str(ants11file.absolute())
    )

    outfile = avgfile.parent / avgfile.name.replace(".averaged.", ".finalspec.")
    if not outfile.exists():
        raise RuntimeError(f"Failed to create {outfile}!")
    
    return outfile

@task(
    persist_result=True,
    task_run_name="get-ants11",
    cache_key_fn=notebook_hasher('ants11')
)
def run_ants11_notebook(
    kernel: str,
    output_dir: Path,
    cfgfile: Path | None = None,
    convert_args: str = "",
) -> Path:
    
    run_notebook(
        "ants11",
        kernel=kernel,
        output_dir=str(output_dir),
        convert_args=convert_args,
        cfgfile=cfgfile,
        outdir=str(output_dir),
    )

    outfile = output_dir / "2015_ants11_modelled.h5"
    
    if not outfile.exists():
        raise RuntimeError(f"Failed to create {outfile}!")
    
    return outfile

@task(
    persist_result=True,
    task_run_name="receiver-calibration",
    cache_key_fn=notebook_hasher('receiver-calibration')
)
def run_receiver_cal_notebook(
    kernel: str,
    output_dir: Path,
    cfgfile: Path | None = None,
    convert_args: str = "",
) -> Path:
    
    run_notebook(
        "receiver-calibration",
        kernel=kernel,
        outpath=str(output_dir),
        convert_args=convert_args,
        cfgfile=cfgfile,
    )

    outfile = output_dir / "specal.txt"
    
    if not outfile.exists():
        raise RuntimeError(f"Failed to create {outfile}!")
    
    return outfile


@task(persist_result=True, cache_policy=INPUTS+TASK_SOURCE)
def gather_days_into_one_gsh5(day_files: list[Path | None]) -> Path:
    """Gather all the individual .finalspec. files into one GSH5 file."""
    day_files = sorted([d for d in day_files if d is not None])
    data = GSData.from_file(day_files, concat_axis='time')
    outfile = day_files[0].parent / "gathered-days.gsh5"
    data.write_gsh5(outfile)
    return outfile

@task
def get_ydays(first_yday, last_yday) -> list[tuple[int, int]]:
    datadir = Path("/data5/edges/data/2014_February_Boolardy/mro/low/")
    
    first = dt(year=first_yday[0], month=1, day=1) + timedelta(days=first_yday[1]-1)
    last = dt(year=last_yday[0], month=1, day=1) + timedelta(days=last_yday[1]-1)
    
    day=first
    ydays = []
    while day <= last:
        tt = day.timetuple()
        y,d = tt.tm_year, tt.tm_yday
        files_to_load = sorted((datadir / str(y)).glob(f"{y}_{d:>03}_*.acq"))
        if files_to_load:
            ydays.append((y, d))
        else:
            print(f"Skipping {y}:{d:>03} as it has no files!")
    
        day += timedelta(days=1)
    return ydays

@task(persist_result=False)
def average_over_days(
    fl: Path,
    kernel: str,
    cfgfile: Path | None = None,
    convert_args: str = "",
):
    
    run_notebook(
        "average-over-days",
        kernel=kernel,
        output_dir=str(fl.parent.absolute()),
        convert_args=convert_args,
        cfgfile=cfgfile,
        basename=f"average-over-days",
        datadir=str(fl.parent.absolute()),
        gathered_days_file=fl.name
    )
    outfile = fl.parent / "averaged_spectrum.gsh5"
    if not outfile.exists():
        raise RuntimeError("Something broke when running average_over_days")
    return outfile
    
@task(persist_result=False)
def interpret(
    fl: Path | None,
    kernel: str,
    cfgfile: Path | None = None,
    convert_args: str = "",
):
    
    run_notebook(
        "interpret",
        kernel=kernel,
        output_dir=str(fl.parent.absolute()),
        convert_args=convert_args,
        cfgfile=cfgfile,
        basename=f"average-over-days",
        datadir=str(fl.parent.absolute()),
        avgspec_file=fl.name
    )
    
@flow(
    flow_run_name="{first_yday[0]}:{first_yday[1]:>03}-{last_yday[0]}:{last_yday[1]:>03}",
    task_runner=DaskTaskRunner(cluster_kwargs={"n_workers": 30}),
)
def run_full_pipeline(
    first_yday: tuple[int, int],
    last_yday: tuple[int, int],
    kernel: str,
    output_dir: Path,
    rcvcal_cfgfile: Path | None = None,
    avg_cfgfile: Path | None = None,
    cal_cfgfile: Path | None = None,
    dayavg_cfgfile: Path | None = None,
    inspect_cfgfile: Path | None = None,
    convert_args: str = "",
):
    ydays = get_ydays(first_yday, last_yday)
    
    calfile = run_receiver_cal_notebook(
        kernel=kernel,
        output_dir=output_dir,
        cfgfile=rcvcal_cfgfile,
        convert_args=convert_args,
    )
    
    ants11 = run_ants11_notebook(
        kernel=kernel,
        output_dir=output_dir,
        cfgfile=rcvcal_cfgfile,
        convert_args=convert_args,
    )
    
    
    avgfiles = run_daily_notebook.map(
        ydays,
        kernel=unmapped(kernel),
        output_dir=unmapped(output_dir),
        cfgfile=unmapped(avg_cfgfile),
        convert_args=unmapped(convert_args),
    )

    calfiles = run_daily_cal_notebook.map(
        avgfiles,
        ydays,
        calfile=unmapped(calfile),
        ants11file=unmapped(ants11),
        kernel=unmapped(kernel),
        output_dir=unmapped(output_dir),
        cfgfile=unmapped(cal_cfgfile),
        convert_args=unmapped(convert_args),
        
    )
    
    gathered_fl = gather_days_into_one_gsh5(calfiles)
    specavg = average_over_days(
        gathered_fl,
        kernel=unmapped(kernel),
        cfgfile=unmapped(dayavg_cfgfile),
        convert_args=unmapped(convert_args)
    )
    interpret(
        specavg, 
        kernel=unmapped(kernel),
        cfgfile=unmapped(inspect_cfgfile),
        convert_args=unmapped(convert_args)
    )

if __name__ == "__main__":
    run_full_pipeline(
        first_yday = (2016, 250),
        last_yday = (2017, 95),
        kernel="b18",
        output_dir=here.parent / "output-notebooks",
    )
