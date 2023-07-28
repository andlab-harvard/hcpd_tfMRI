#!/bin/bash
#SBATCH -J CLEAN
#SBATCH -p fasse
#SBATCH -c 1
#SBATCH --mem 1000
#SBATCH -t 12:00:00
#SBATCH --account=somerville_lab

targetdir="/ncf/hcp/data/HCD-tfMRI-MultiRunFix/"
targettask=("CARIT" "GUESSING")
hcd_dirs=( $(ls -d "${targetdir}"/HCD*) )

for thisdir in ${hcd_dirs[@]}; do
	HCDID=$( basename ${thisdir} )
	echo ""
	echo "${HCDID}"
	taskdirloc="${thisdir}/MNINonLinear/Results"
	for task in ${targettask[@]}; do
		featdirs=($(find -H ${taskdirloc} -maxdepth 2 -type d -regextype posix-extended -regex ".*/tfMRI_${task}_(AP|PA)/tfMRI_${task}.*.feat$"))
		for adir in ${featdirs[@]}; do
			rm -rfv "${adir}"
		done
		fsffiles=($(find -H ${taskdirloc} -maxdepth 2 -type f -regextype posix-extended -regex ".*/tfMRI_${task}_(AP|PA)/tfMRI_${task}.*.fsf$"))
		for afsf in ${fsffiles}; do
			rm -fv "${afsf}"
		done
	done
done
