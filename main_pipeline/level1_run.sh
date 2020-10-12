for tt in /data5/edges/data/2014_February_Boolardy/mro/low/2016/2016_29*.acq; do
	edges-analysis calibrate ./settings/level1_low1_settings.yaml $tt -m "NP-1st"

done
wait
