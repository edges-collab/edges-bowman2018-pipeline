from pathlib import Path

from prefect import task, flow, unmapped
from prefect.futures import wait
from prefect.cache_policies import INPUTS, TASK_SOURCE
from datetime import datetime as dt, timedelta
from edges_pipelines.runners import run_notebook
from pygsdata import GSData
from prefect_dask import DaskTaskRunner
here = Path(__file__).parent


@task(persist_result=True, tags=["day-sized"], task_run_name="avg-{yday[0]}-{yday[1]:>03}", cache_policy=INPUTS+TASK_SOURCE, refresh_cache=True)
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
        basename=f"single-day-{year}-{day:>03}",
        year=year,
        day=day,
        cachedir=str(output_dir.absolute()),
    )

    return output_dir / f"{year}-{day}.finalspec.gsh5"

@task(persist_result=True, task_run_name="cal-{yday[0]}-{yday[1]:>03}", cache_policy=INPUTS+TASK_SOURCE)
def run_daily_cal_notebook(
    yday,
    kernel: str,
    output_dir: Path,
    cfgfile: Path | None = None,
    convert_args: str = "",
) -> Path:
    year, day = yday
    run_notebook(
        "single-day-calibration",
        kernel=kernel,
        output_dir=output_dir,
        convert_args=convert_args,
        cfgfile=cfgfile,
        basename=f"single-day-{year}-{day:>03}",
        year=year,
        day=day,
        datadir=str(output_dir.absolute()),
    )

    return output_dir / f"{year}-{day}.finalspec.gsh5"

@task(persist_result=True)
def gather_days_into_one_gsh5(day_files: list[Path]) -> Path:
    """Gather all the individual .finalspec. files into one GSH5 file."""
    day_files = sorted([d for d in day_files if d.exists()])
    
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

@flow(
    flow_run_name="{first_yday[0]}:{first_yday[1]:>03}-{last_yday[0]}:{last_yday[1]:>03}",
    task_runner=DaskTaskRunner(adapt_kwargs={"maximum": 30}),
)
def run_full_pipeline(
    first_yday: tuple[int, int],
    last_yday: tuple[int, int],
    kernel: str,
    output_dir: Path,
    cfgfile: Path | None = None,
    convert_args: str = "",
):
    ydays = get_ydays(first_yday, last_yday)
    
    output_gsh5s = run_daily_notebook.map(
        ydays,
        kernel=unmapped(kernel),
        output_dir=unmapped(output_dir),
        cfgfile=unmapped(cfgfile),
        convert_args=unmapped(convert_args),
    )
    wait(output_gsh5s)
    
    output_gsh5s = run_daily_cal_notebook.map(
        ydays,
        kernel=unmapped(kernel),
        output_dir=unmapped(output_dir),
        cfgfile=unmapped(cfgfile),
        convert_args=unmapped(convert_args),
    )
    wait(output_gsh5s)
    
    gather_days_into_one_gsh5(output_gsh5s)


if __name__ == "__main__":
    run_full_pipeline(
        first_yday = (2016, 250),
        last_yday = (2017, 95),
        kernel="edges-uv",
        output_dir=here.parent / "output-notebooks",
    )
