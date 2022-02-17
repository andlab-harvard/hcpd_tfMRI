#!/bin/bash

datadir="/ncf/hcp/data/HCD-tfMRI-MultiRunFix/"
hcd_dirs=($(ls "${datadir}"))

for thisdir in ${hcd_dirs[@]}; do
	structdir="/ncf/hcp/data/intradb/${thisdir}/Structural_preproc/${thisdir}/MNINonLinear/fsaverage_LR32k"
	roidir="/ncf/hcp/data/intradb/${thisdir}/Structural_preproc/${thisdir}/MNINonLinear/ROIs"
	if [ -d ${structdir} ]; then
		if [ ! -d "${datadir}/${thisdir}/MNINonLinear/fsaverage_LR32k" ]; then
			ln -v -s "${structdir}" "${datadir}/${thisdir}/MNINonLinear"
		else
			echo "${thisdir}/MNINonLinear/fsaverage_LR32k exists..."
		fi
	else
		echo ">>> No Structural dir for ${thisdir}"
	fi
	if [ -d ${roidir} ]; then
		if [ ! -d "${datadir}/${thisdir}/MNINonLinear/ROIs" ]; then
			ln -v -s "${roidir}" "${datadir}/${thisdir}/MNINonLinear"
		else
			echo "${thisdir}/MNINonLinear/ROIs exists..."
		fi
	else
		echo ">>> No ROIs dir for ${thisdir}"
	fi
done

