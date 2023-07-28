#!/bin/bash
#SBATCH -p fasse
#SBATCH -c 1
#SBATCH --mem 1000
#SBATCH -t 12:00:00
#SBATCH --account=somerville_lab

datadir="/ncf/hcp/data/HCD-tfMRI-MultiRunFix/"
hcd_dirs=($(ls "${datadir}"))
errlog="Structural_preproc.err"
echo "" > ${errlog}
for thisdir in ${hcd_dirs[@]}; do
	structdir="/ncf/hcp/data/intradb/${thisdir}/Structural_preproc/${thisdir}/MNINonLinear/fsaverage_LR32k"
	roidir="/ncf/hcp/data/intradb/${thisdir}/Structural_preproc/${thisdir}/MNINonLinear/ROIs"
	if [ -d ${structdir} ]; then
		if [ ! -d "${datadir}/${thisdir}/MNINonLinear/fsaverage_LR32k" ]; then
			ln -v -s "${structdir}" "${datadir}/${thisdir}/MNINonLinear"
		else
			echo "${thisdir}/MNINonLinear/fsaverage_LR32k exists..."
		fi
		midthickL="${thisdir}.L.midthickness.32k_fs_LR.surf.gii"
		midthickR="${thisdir}.R.midthickness.32k_fs_LR.surf.gii"
		if [ ! -f "${structdir}/${midthickL}" ]; then
			echo "ERROR: ${thisdir} missing ${midthickL}"
			echo "ERROR: ${thisdir} missing ${midthickL}" >> ${errlog}
		fi
		if [ ! -f "${structdir}/${midthickR}" ]; then
			echo "ERROR: ${thisdir} missing ${midthickR}"
                        echo "ERROR: ${thisdir} missing ${midthickR}" >> ${errlog}
                fi
	else
		echo ">>> No Structural dir for ${thisdir}"
		echo "ERROR: ${thisdir} missing Structural_preproc" >> ${errlog}
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

