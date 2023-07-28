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

for thisdir in ${hcd_dirs[@]}; do
	HCDID=$( basename ${thisdir} )
	echo ""
	echo "${HCDID}"
	echo "Checking ${thisdir} for expected DT files... "
	taskdirloc="${thisdir}/MultiRunIcaFix_proc/${HCDID}/MNINonLinear/Results"
	taskdirs=($(ls -d ${taskdirloc}/tfMRI*))
	for taskdir in ${taskdirs[@]}; do
		taskname="$(basename ${taskdir})"
		dtfile="$(basename ${taskdir})_Atlas_hp0_clean.dtseries.nii"
		if [ -f "${taskdir}/${dtfile}" ]; then
			echo "DT series found in ${taskdir}"
			echo "Checking data directory..."
			datadtdir="${datadir}/${HCDID}/MNINonLinear/Results/${taskname}"
			datadtfile="${datadtdir}/${dtfile}"
			if [ ! -f "${datadtfile}" ]; then
				echo "DT series file not linked in data dir"
				echo "Linking to data analysis dir."
				echo "Checking if ${datadtdir} exists..."
				if [ ! -d ${datadtdir} ]; then
				  echo "Creating target dir"
				  mkdir -pv ${datadtdir}
				else
				  echo "Target dir exists"
				fi
				echo "****LINKING DATA FILES****"
				ln -sv ${taskdir}/* "${datadtdir}"
				echo "********"
			else
				echo "Data file already exists: "
				echo "${datadtfile}"
			fi
		fi
	done
done
