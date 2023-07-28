#!/bin/bash
#SBATCH -p fasse
#SBATCH -c 1
#SBATCH --mem 1000
#SBATCH -t 12:00:00
#SBATCH --account=somerville_lab

datadir="/ncf/hcp/data/HCD-tfMRI-MultiRunFix/"
hcd_dirs=($(ls "${datadir}"))
checkfn="struct_check.txt"
errfn="struct_err.txt"
echo "file" > ${checkfn}
echo "file" > ${errfn}
for thisdir in ${hcd_dirs[@]}; do
	structdir="/ncf/hcp/data/intradb/${thisdir}/Structural_preproc/${thisdir}/MNINonLinear/fsaverage_LR32k"
	roidir="/ncf/hcp/data/intradb/${thisdir}/Structural_preproc/${thisdir}/MNINonLinear/ROIs"
	if [ -d "${datadir}/${thisdir}/MNINonLinear/fsaverage_LR32k" ]; then
		echo "${datadir}/${thisdir}/MNINonLinear/fsaverage_LR32k" >> ${checkfn}
	else
		echo "${datadir}/${thisdir}/MNINonLinear/fsaverage_LR32k" >> ${errfn}
	fi
	if [ -d "${datadir}/${thisdir}/MNINonLinear/ROIs" ]; then
		echo "${datadir}/${thisdir}/MNINonLinear/ROIs" >> ${checkfn}
	else
		echo "${datadir}/${thisdir}/MNINonLinear/ROIs" >> ${errfn}
	fi
done

