import subprocess as sbp
from pathlib import Path

import click
import jupyter_client
import papermill as pm
import toml
import yaml
from multiprocess import Pool

k_manager = jupyter_client.kernelspec.KernelSpecManager()
avail_kernels = k_manager.find_kernel_specs()

main = click.Group()

here = Path(__file__).parent

NOTEBOOK_DICT = {fl.stem: fl for fl in (here / "notebooks").glob("*.ipynb")}


@main.group(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.option(
    "-k", "--kernel", type=click.Choice(list(avail_kernels.keys())), default="python3"
)
@click.option("-f", "--formats", type=str, multiple=True, default=["html"])
@click.option("--ipynb/--no-ipynb", default=True)
@click.option("-o", "--output", type=str, default=None)
@click.option(
    "--output-dir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    default="output-notebooks",
)
@click.option("--convert-args", type=str, default="")
@click.option(
    "--toml", type=click.Path(exists=True, dir_okay=False, file_okay=True), default=None
)
@click.option("--days", type=str, multiple=True)
@click.option("--threads", default=1)
@click.pass_context
def run(
    ctx, kernel, formats, ipynb, output, output_dir, convert_args, toml, days, threads
):
    """Use papermill to run a hera-templates notebook."""
    ctx.ensure_object(dict)

    ctx.obj["kernel"] = kernel
    ctx.obj["formats"] = formats
    ctx.obj["ipynb"] = ipynb
    ctx.obj["output_dir"] = output_dir
    ctx.obj["convert_args"] = convert_args
    ctx.obj["toml"] = toml
    ctx.obj["days"] = sorted(
        set(
            sum(
                [list(range(*tuple(int(n) for n in d.split("~")))) for d in days],
                start=[],
            )
        )
    )
    ctx.obj["threads"] = threads


def run_notebook_factory(notebook):
    @click.option("-o", "--basename", type=str, default=None)
    @click.pass_context
    def runfunc(ctx, basename, **kwargs):
        nbfile = NOTEBOOK_DICT[notebook]

        if basename is None:
            basename = notebook

        if (pfile := ctx.obj["toml"]) is not None:
            if pfile.endswith(".toml"):
                kwargs.update(toml.load(pfile))
            elif pfile.endswith(".yaml"):
                with open(pfile) as fl:
                    kwargs.update(yaml.load(fl))
            else:
                raise ValueError(f"Unkown extension on --toml input: {ctx.obj['toml']}")

        kwargs["papermill_input_path"] = str(nbfile)

        pool = Pool(ctx.obj["threads"])

        def execute_for_a_day(day):
            kw = {**kwargs, "day": day}
            output_path = Path(ctx.obj["output_dir"]) / f"{basename}-{day}.ipynb"
            print(f"Executing Notebook and saving to {output_path}")
            print(f"Got notebook params: '{kwargs}'")

            kwargs["papermill_output_path"] = str(output_path)

            pm.execute_notebook(
                str(nbfile),
                output_path=output_path,
                kernel_name=ctx.obj["kernel"],
                parameters=kw,
            )

        pool.map(execute_for_a_day, ctx.obj["days"])

        for fmt in ctx.obj["formats"]:
            for day in ctx.obj["days"]:
                print(f"Converting executed notebook to {fmt}...")
                output_path = Path(ctx.obj["output_dir"]) / f"{basename}-{day}.ipynb"

                sbp.run(
                    [
                        "jupyter",
                        "nbconvert",
                        "--output",
                        f"{basename}-{day}.{fmt}",
                        "--output-dir",
                        str(output_path.parent),
                        "--to",
                        fmt,
                        ctx.obj["convert_args"],
                        str(output_path),
                    ],
                    check=True,
                )

        if not ctx.obj["ipynb"]:
            output_path.unlink()

    infer = pm.inspect_notebook(str(NOTEBOOK_DICT[notebook]))
    tps = {
        "str": str,
        "int": int,
        "float": float,
        "bool": bool,
        None: None,
    }
    params = [
        click.option(
            f"--{param.replace('_', '-')}",
            type=tps[v["inferred_type_name"]],
            default=eval(v["default"]),
            help=v["help"],
            show_default=True,
        )
        if v["inferred_type_name"] != "bool"
        else click.option(
            f"--{param.replace('_', '-')}/--no-{param.replace('_', '-')}",
            help=v["help"],
            default=eval(v["default"]),
        )
        for param, v in infer.items()
        if v["inferred_type_name"] in tps
    ]

    # Add all the parameters:
    for param in params:
        runfunc = param(runfunc)

    return click.command(name=notebook)(runfunc)


for nb in NOTEBOOK_DICT:
    run.add_command(run_notebook_factory(nb))

if __name__ == "__main__":
    main()
