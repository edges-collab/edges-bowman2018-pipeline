import subprocess as sbp
from collections.abc import Sequence
from pathlib import Path

import jupyter_client
import papermill as pm
import toml
import yaml

k_manager = jupyter_client.kernelspec.KernelSpecManager()
avail_kernels = k_manager.find_kernel_specs()

here = Path(__file__).parent

NOTEBOOK_DICT = {fl.stem: fl for fl in (here / "notebooks").glob("*.ipynb")}


def run_notebook(
    notebook: str,
    kernel: str,
    formats: Sequence[str] = ("html",),
    ipynb: bool = True,
    output_dir: str | Path = Path(),
    convert_args: str = "",
    cfgfile: str | Path | None = None,
    basename: str | None = None,
    **kwargs,
):
    nbfile = NOTEBOOK_DICT[notebook]
    if basename is None:
        basename = notebook

    if cfgfile is not None:
        if cfgfile.endswith(".toml"):
            params = toml.load(cfgfile)
        elif cfgfile.endswith(".yaml"):
            with open(cfgfile) as fl:
                params = yaml.load(fl)
        else:
            raise ValueError(f"Unkown extension on --toml input: {cfgfile}")
    else:
        params = {}

    params |= kwargs

    output_path = Path(output_dir) / f"{basename}.ipynb"

    pm.execute_notebook(
        str(nbfile),
        output_path=output_path,
        kernel_name=kernel,
        parameters=params,
    )

    for fmt in formats:
        sbp.run(
            [
                "jupyter",
                "nbconvert",
                "--output",
                f"{basename}.{fmt}",
                "--output-dir",
                str(output_path.parent),
                "--to",
                fmt,
                convert_args,
                str(output_path),
            ],
            check=True,
        )

    if not ipynb:
        output_path.unlink()

    return output_path
