#!/bin/bash

task="CARIT"
acq=("PA" "AP")
prefix="_"
outfile="${task}-l1-list"
datadir="/ncf/hcp/data/HCD-tfMRI-MultiRunFix"
participantlist=($( ls -d "$datadir/HCD"* | sort ))

outfiles=( "${outfile}_1run.txt"  "${outfile}_2run.txt" )
for of in ${outfiles[@]}; do
  [ -f "${of}" ] && rm "${of}"
  touch "${of}"
done

echo "Writing task list..."
for p in ${participantlist[@]}; do
  scanlist=()
  for a in ${acq[@]}; do
    p=$( basename $p )
    t="tfMRI_${task}_${a}"
    datafile="${datadir}/${p}/MNINonLinear/Results/${t}/${t}_Atlas_hp0_clean.dtseries.nii"
    [ -f ${datafile} ] && scanlist+=( "${t}" )
  done
  IFS=\@ eval 'scans="${scanlist[*]}"'
  echo "${p}: ${#scanlist[@]}"
  if [ ${#scanlist[@]} -eq 1 ]; then
    echo "${p} ${scans} tfMRI_${task}" >> "${outfiles[0]}"
  elif [ ${#scanlist[@]} -eq 2 ]; then
    echo "${p} ${scans} tfMRI_${task}" >> "${outfiles[1]}"
  fi
done
