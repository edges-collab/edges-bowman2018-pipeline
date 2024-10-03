from prefect import task, flow
from prefect.futures import wait
from prefect.cache_policies import DEFAULT
from time import sleep

@task(persist_result=True, task_run_name="{year}-{day:>03}", cache_policy=DEFAULT)
def run_long_thing(
    year: int,
    day: int,
) -> str:
    # ... actually some long-running code that returns a Path
    sleep(1.5)
    return f"{year}-{day}"


@flow
def run_mini():
    ydays = [(2016, day) for day in range(50)]
    
    outputs = [] 
    for year, day in ydays:
        outputs.append(run_long_thing.submit(year, day))
    wait(outputs)
    print(outputs)


if __name__ == "__main__":
    run_mini()
