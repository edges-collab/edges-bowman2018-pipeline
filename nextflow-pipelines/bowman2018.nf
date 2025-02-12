params.kernel = "prefect-pipeline"

params.cfgdir = "/data7/smurray/edges/projects_with_nive/edges-bowman2018-pipeline/configs"
params.case = "bowman2018repro-injectbeam-doxrfi"

params.rcvcal_cfg = "$params.cfgdir/$params.case/rcvcal.yaml"
params.ants11_cfg = "$params.cfgdir/$params.case/ants11.yaml"
params.dayavg_cfg = "$params.cfgdir/$params.case/dayavg.yaml"
params.lstavg_cfg = "$params.cfgdir/$params.case/lstavg.yaml"
params.interp_cfg = "$params.cfgdir/$params.case/interp.yaml"
params.beamfac_cfg = "$params.cfgdir/$params.case/beamfac.yaml"
params.daycal_cfg = "$params.cfgdir/$params.case/daycal.yaml"

params.min_year = 2016
params.min_day = 250
params.max_year = 2017
params.max_day = 95

process get_ydays {
    output:
    stdout

    script:
    """
    epipe get-ydays $params.min_year $params.min_day $params.max_year $params.max_day
    """
}

process get_calibration {
    output: 
    path "specal.txt", emit: data
    path "receiver-calibration.html", emit: html
    path "receiver-calibration.ipynb", emit: ipynb

    publishDir(
        path: "results/$params.case/",
        pattern: "*.txt",
    )
    publishDir(
        path: "results/$params.case/notebook-html/",
        pattern: "*.html"
    )
    publishDir(
        path: "results/$params.case/notebook-ipynb/",
        pattern: "*.ipynb"
    )

    script: "epipe run receiver-calibration $params.kernel --cfgfile $params.rcvcal_cfg"
}

process get_ants11 {
    output: 
    path "2015_ants11_modelled.h5", emit: data
    path "ants11.html", emit: html
    path "ants11.ipynb", emit: ipynb

    publishDir (
        path: "results/$params.case/",
        pattern: "*.h5"    
    )

    publishDir(
        path: "results/$params.case/notebook-html/",
        pattern: "*.html"
    )
    publishDir(
        path: "results/$params.case/notebook-ipynb/",
        pattern: "*.ipynb"
    )

    script: "epipe run ants11 $params.kernel --cfgfile $params.ants11_cfg"
}

process get_beamfac {
    output: 
    path "beam_factor.hickle", emit: data
    path "high-res-beamfactor.html", emit: html
    path "high-res-beamfactor.ipynb", emit: ipynb

    publishDir (
        path: "results/$params.case/",
        pattern: "*.hickle"    
    )

    publishDir(
        path: "results/$params.case/notebook-html/",
        pattern: "*.html"
    )
    publishDir(
        path: "results/$params.case/notebook-ipynb/",
        pattern: "*.ipynb"
    )

    script: "epipe run high-res-beamfactor $params.kernel --cfgfile $params.beamfac_cfg"
}

process daily_average {
    // There is an issue when trying to launch many jupyter kernels at the same time
    // that they have a race condition and die. This retry should alleviate this,
    // though it will also mean extra time when the error is something else...
    errorStrategy 'retry'

    maxForks 30

    input:
    val yday

    // We expect an *.averaged.gsh5 output, but in some cases while there might be 
    // data on the day in general, there might be no data in the LST range, in which
    // case the notebook exits early (without error) but does not create a file.
    // Marking the output as 'optional' means the process will not fail, and this day
    // will effectively be ignored for the rest of the pipeline.
    output:
    path "${yday}.averaged.gsh5", optional: true, emit: data
    path "${yday}.lsts.txt", optional: true, emit: lsts
    path "single-day-avg-${yday}.html", emit: html
    path "single-day-avg-${yday}.ipynb", emit: ipynb

    publishDir (
        path: "results/$params.case/day-averaged-data/",
        pattern: "*.gsh5"    
    )
    publishDir(
        path: "results/$params.case/notebook-html/",
        pattern: "*.html"
    )
    publishDir(
        path: "results/$params.case/notebook-ipynb/",
        pattern: "*.ipynb"
    )

    script:
    year = yday.split("-")[0]
    day = yday.split("-")[1]
    """
    epipe run single-day $params.kernel  \
      --cfgfile $params.dayavg_cfg --year $year --day $day \
      --basename single-day-avg-$yday \
      --outdir .
    """
}

process daily_calibration {
    // There is an issue when trying to launch many jupyter kernels at the same time
    // that they have a race condition and die. This retry should alleviate this,
    // though it will also mean extra time when the error is something else...
    errorStrategy 'retry'

    maxForks 30

    input:
    path single_day_avg
    path lstfile
    path calfile
    path beamfacfile
    path ants11file

    output:
    path "${obsname}.finalspec.gsh5", emit: data
    path "single-day-cal-${obsname}.html", emit: html
    path "single-day-cal-${obsname}.ipynb", emit: ipynb

    publishDir (
        path: "results/$params.case/calibrated-data/",
        pattern: "*.gsh5"
    )
    publishDir(
        path: "results/$params.case/notebook-html/",
        pattern: "*.html"
    )
    publishDir(
        path: "results/$params.case/notebook-ipynb/",
        pattern: "*.ipynb"
    )

    script:
    obsname = single_day_avg.getSimpleName()
    tmp = 3
    """
    epipe run single-day-calibration $params.kernel \
      --cfgfile $params.daycal_cfg \
      --dayavgfile $single_day_avg \
      --ants11file $ants11file \
      --calfile $calfile \
      --beamfactorfile $beamfacfile \
      --basename single-day-cal-$obsname \
    """
}

process gather {
    input: 
    path "*.finalspec.gsh5"
    output: 
    path "gathered-days.gsh5"

    publishDir "results/$params.case/"
        
    script: "epipe gather *.finalspec.gsh5 gathered-days.gsh5"
}

process average_over_days {
    input:
    path gatherfile

    output:
    path "averaged_spectrum.gsh5", emit: data
    path "average-over-days.html", emit: html
    path "average-over-days.ipynb", emit: ipynb

    publishDir (
        path: "results/$params.case/",
        pattern: "*.gsh5"
    )
    publishDir(
        path: "results/$params.case/notebook-html/",
        pattern: "*.html"
    )
    publishDir(
        path: "results/$params.case/notebook-ipynb/",
        pattern: "*.ipynb"
    )
    script:
    """
    epipe run average-over-days $params.kernel \
      --cfgfile $params.lstavg_cfg \
      --gathered_days_file $gatherfile \
    """
}

process interpret {
    input:
    path avgfile

    output:
    path "interpret.ipynb"
    path "interpret.html"

    publishDir(
        path: "results/$params.case/notebook-html/",
        pattern: "*.html"
    )
    publishDir(
        path: "results/$params.case/notebook-ipynb/",
        pattern: "*.ipynb"
    )

    script:
    """
    epipe run interpret $params.kernel \
      --cfgfile $params.interp_cfg \
      --avgspec_file $avgfile \
    """
}
workflow {
    get_calibration()
    get_ants11()
    get_beamfac()
    ydays = get_ydays().splitText().map(v -> v.trim())
    
    daily_average(ydays)
    daily_calibration(
        daily_average.out.data, 
        daily_average.out.lsts, 
        get_calibration.out.data, 
        get_beamfac.out.data, 
        get_ants11.out.data
    )
    daily_calibration.out.data.collect(sort: true) | gather | average_over_days
    
    interpret(average_over_days.out.data)
}