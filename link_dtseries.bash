#!/bin/bash

datadir="/ncf/hcp/data/HCD-tfMRI-MultiRunFix/"
intradbdir="/ncf/hcp/data/intradb_multiprocfix/"
hcd_dirs=($(ls "${datadir}"))

for thisdir in ${hcd_dirs[@]}; do
	echo "Checking ${thisdir} for expected DT files... "
	taskdirloc="${datadir}/${thisdir}/MNINonLinear/Results"
	taskdirs=($(ls -d ${taskdirloc}/tfMRI*))
	for taskdir in ${taskdirs[@]}; do
		taskname="$(basename ${taskdir})"
		dtfile="$(basename ${taskdir})_Atlas_hp0_clean.dtseries.nii"
		if [ ! -f "${taskdir}/${dtfile}" ]; then
			echo "DT series not found in ${taskdir}"
			echo "Checking intradb download..."
			intradbddtdir="${intradbdir}/${thisdir}/MultiRunIcaFix_proc/${thisdir}/MNINonLinear/Results/${taskname}"
			intradbdtfile="${intradbddtdir}/${dtfile}"
			if [ -f "${intradbdtfile}" ]; then
				echo "Found DT series file."
				echo "Linking to data analysis dir."
				ln -sv ${intradbddtdir}/* "${taskdir}"
			else
				echo "Cannot find DT series file: "
				echo "${intradbdtfile}"
			fi
		fi
	done
done
