// Feature Flags
nextflow.enable.dsl=2
nextflow.preview.output = true

process get_ydays {
    input:
    val min_year
    val min_day
    val max_year
    val max_day

    output:
    stdout

    script:
    """
    epipe get-ydays $min_year $min_day $max_year $max_day
    """
}

process get_calibration {
    input:
    path cfgfile
    path notebook

    output: 
    path "specal.txt", emit: data
    path "${basename}.ipynb", emit: report

    script: 
    basename = "${notebook.baseName}-executed"
    "epipe run-nb $notebook $params.kernel --cfgfile $cfgfile --basename ${basename}"
}

process get_ants11 {
    debug true

    input:
    path cfgfile
    path notebook

    output: 
    path "2015_ants11_modelled.h5", emit: data
    path "${basename}.ipynb", emit: report

    script: 
    basename = "${notebook.baseName}-executed"
    "epipe run-nb $notebook $params.kernel --cfgfile $cfgfile --basename ${basename}"
}

process get_beamfac {
    debug true
    input:
    path cfgfile
    path notebook

    output: 
    path "beam_factor.hickle", emit: data
    path "${basename}.ipynb", emit: report

    script: 
    basename = "${notebook.baseName}-executed"
    "epipe run-nb $notebook $params.kernel --cfgfile $cfgfile --basename ${basename}"
}

process convert_acq {
    /*
    Convert raw acquisition files to gsh5 format. This is done once per day
    and permanently cached to avoid redoing it if the pipeline is re-run.
    */
    tag "$yday"
    maxForks 30
    storeDir "$params.raw_gsh5_cache/$params.telescope-$params.lst_setter/"

    input:
    val yday

    output:
    tuple val(yday), path("${yday}.gsh5")

    script:
    year = yday.split("-")[0]
    day = yday.split("-")[1]
    """
    epipe convert $year $day \
        --outdir . \
        --telescope $params.telescope \
        --lst-setter $params.lst_setter \
        --datadir $params.rawdata_dir
    """

}
process daily_average {
    // There is an issue when trying to launch many jupyter kernels at the same time
    // that they have a race condition and die. This retry should alleviate this,
    // though it will also mean extra time when the error is something else...
    errorStrategy 'retry'
    maxForks 30
    tag "${yday}"
    
    input:
    tuple val(yday), path(datafile)
    path cfgfile
    path notebook

    // We expect an *.averaged.gsh5 output, but in some cases while there might be 
    // data on the day in general, there might be no data in the LST range, in which
    // case the notebook exits early (without error) but does not create a file.
    // Marking the output as 'optional' means the process will not fail, and this day
    // will effectively be ignored for the rest of the pipeline.
    output:
    tuple val(yday), path("${yday}.averaged.gsh5"), path("${yday}.lsts.txt"), path("${yday}.integration-flags.npz"), optional: true, emit: data
    path "${yday}.flag-info.yaml", optional: true, emit: flaginfo
    path "single-day-avg-${yday}.ipynb", emit: report
    path "${yday}.unsmoothed.averaged.gsh5", optional: true, emit: unsmoothed
    
    script:
    """
    epipe run-nb $notebook $params.kernel  \
      --cfgfile $cfgfile \
      --basename single-day-avg-$yday \
      --datafile $datafile \
      --outdir .
    """
}

process daily_calibration {
    // There is an issue when trying to launch many jupyter kernels at the same time
    // that they have a race condition and die. This retry should alleviate this,
    // though it will also mean extra time when the error is something else...
    errorStrategy 'retry'
    cache 'deep'
    maxForks 30
    tag "${yday}"

    input:
    tuple val(yday), path(single_day_avg), path(lstfile), path(intflags)
    path calfile
    path beamfacfile
    path ants11file
    path cfgfile
    path notebook

    output:
    path "${yday}.finalspec.gsh5", emit: data
    path "single-day-cal-${yday}.ipynb", emit: report

    script:
    """
    epipe run-nb $notebook $params.kernel \
      --cfgfile $cfgfile \
      --dayavgfile \$PWD/$single_day_avg \
      --ants11file \$PWD/$ants11file \
      --calfile \$PWD/$calfile \
      --beamfactorfile \$PWD/$beamfacfile \
      --basename single-day-cal-$yday \
    """
}

process gather {
    input: 
    path "*.finalspec.gsh5"

    output: 
    path "gathered-days.gsh5"
        
    script: "epipe gather *.finalspec.gsh5 gathered-days.gsh5"
}

process average_over_days {
    input:
    path gatherfile
    path cfgfile
    path notebook

    output:
    path "averaged_spectrum.gsh5", emit: data
    path "averaged_spectrum_legacy_days.gsh5", emit: alandata
    path "${basename}.ipynb", emit: report

    script:
    basename = "${notebook.baseName}-executed"
    """
    epipe run-nb $notebook $params.kernel \
      --cfgfile $cfgfile \
      --gathered_days_file \$PWD/$gatherfile \
      --basename ${basename}
    """
}

process interpret {
    input:
    path avgfile
    path notebook

    output:
    path "${basename}.ipynb", emit: report

    script:
    basename = "${notebook.baseName}-executed"
    """
    epipe run-nb $notebook $params.kernel \
      --avgspec_file \$PWD/$avgfile \
      --basename ${basename}
    """
}
workflow {
    main:
    ydays = get_ydays(
        params.min_year, 
        params.min_day, 
        params.max_year, 
        params.max_day
    ).splitText().map(v -> v.trim())
    convert_acq(ydays)

    get_calibration(file(params.rcvcal_config), file("notebooks/receiver-calibration.ipynb"))
    get_ants11(file(params.ants11_config), file("notebooks/ants11.ipynb"))
    get_beamfac(file(params.beamfac_config), file("notebooks/high-res-beamfactor.ipynb"))

    daily_average(convert_acq.out, file(params.dayavg_config), file("notebooks/single-day-average.ipynb"))
    daily_calibration(
        daily_average.out.data,
        get_calibration.out.data, 
        get_beamfac.out.data, 
        get_ants11.out.data,
        file(params.daycal_config), 
        file("notebooks/single-day-calibration.ipynb")
    )
    daily_calibration.out.data.collect(sort: true) | gather
    average_over_days(gather.out, file(params.lstavg_config), file("notebooks/average-over-days.ipynb"))
    interpret(average_over_days.out.data, file("notebooks/interpret.ipynb"))

    publish:
    cal_data = get_calibration.out.data
    ants11_data = get_ants11.out.data
    beamfac_data = get_beamfac.out.data
    singleavg_data = daily_average.out.data
    singleavg_flginfo = daily_average.out.flaginfo
    singleavg_unsmoothed = daily_average.out.unsmoothed
    singlecal_data = daily_calibration.out.data
    gathered_days_data = gather.out
    avg_over_days_data = average_over_days.out.data
    avg_over_days_legacy_data = average_over_days.out.alandata

    cal_report = get_calibration.out.report
    ants11_report = get_ants11.out.report
    beamfac_report = get_beamfac.out.report
    singleavg_report = daily_average.out.report
    singlecal_report = daily_calibration.out.report
    avg_over_days_report = average_over_days.out.report
    summary = interpret.out.report
}


output {
    cal_data {
        path "${params.label}/"
    }
    cal_report {
        path "${params.label}/notebooks/"
    }
    ants11_data {
        path "${params.label}/"
    }
    ants11_report {
        path "${params.label}/notebooks/"
    }
    beamfac_data {
        path "${params.label}/"
    }
    beamfac_report {
        path "${params.label}/notebooks/"
    }
    singleavg_data {
        path "${params.label}/day-averaged-data/"
    }
    singleavg_unsmoothed {
        path "${params.label}/day-averaged-data/"
    }
    singleavg_flginfo {
        path "${params.label}/day-averaged-data/"
    }
    singleavg_report {
        path "${params.label}/notebooks/single-day-avg/"
    }
    singlecal_data {
        path "${params.label}/calibrated-data/"
    }
    singlecal_report {
        path "${params.label}/notebooks/single-day-cal/"
    }
    gathered_days_data {
        path "${params.label}/"
    }
    avg_over_days_data {
        path "${params.label}/"
    }
    avg_over_days_legacy_data {
        path "${params.label}/"
    }
    avg_over_days_report {
        path "${params.label}/notebooks/"
    }
    summary {
        path "${params.label}/notebooks/"
    }
}