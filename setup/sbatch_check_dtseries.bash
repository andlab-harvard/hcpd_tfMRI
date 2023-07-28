#!/bin/bash
#SBATCH -p fasse
#SBATCH -c 1
#SBATCH --mem 1000
#SBATCH -t 12:00:00
#SBATCH --account=somerville_lab

datadir="/ncf/hcp/data/HCD-tfMRI-MultiRunFix"
intradbdir="/ncf/hcp/data/intradb_multiprocfix"
hcd_dirs=( $(ls -d "${intradbdir}"/HCD*) )
#hcd_dirs=( "$*" )

checkfile="dtseries_check.txt"
errfn="dtseries_err.txt"

echo "file" > ${checkfile}
echo "file" > ${errfn}
for thisdir in ${hcd_dirs[@]}; do
	HCDID=$( basename ${thisdir} )
	echo ""
	echo "${HCDID}"
	taskdirloc="${thisdir}/MultiRunIcaFix_proc/${HCDID}/MNINonLinear/Results"
	taskdirs=($(ls -d ${taskdirloc}/tfMRI*))
	for taskdir in ${taskdirs[@]}; do
		dtfile="$(basename ${taskdir})_Atlas_hp0_clean.dtseries.nii"
		taskname="$(basename ${taskdir})"
		datadtdir="${datadir}/${HCDID}/MNINonLinear/Results/${taskname}"
		datadtfile="${datadtdir}/${dtfile}"
		if [ -f "${datadtfile}" ]; then
			echo "${datadtfile}" >> ${checkfile}
		else
			echo "${datadtfile}" >> ${errfn}
		fi
	done
done
