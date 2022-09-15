#!/bin/bash
#SBATCH -p fasse
#SBATCH -J concat
#SBATCH -c 1
#SBATCH --mem=2G
#SBATCH -o concat.log
#SBATCH --account=somerville_lab
#SBATCH -t 04:00:00

output="/ncf/mclaughlin/users/jflournoy/code/hcpd_tfMRI/group_level_roi/roi_data.csv"
echo "id,scan,file,value" > "${output}"
HCDdir="/ncf/hcp/data/HCD-tfMRI-MultiRunFix"
pdirs=($(ls "${HCDdir}"))
scandirs=(tfMRI_CARIT_AP  tfMRI_CARIT_PA  tfMRI_GUESSING_AP  tfMRI_GUESSING_PA)

for pdir in "${pdirs[@]}"; do
	for scan in "${scandirs[@]}"; do
		datadir="$HCDdir/${pdir}/MNINonLinear/Results/${scan}/${scan}_hp200_s4_level1_hp0_clean_ColeAnticevic.feat/ParcellatedStats/"
		if [ -d "${datadir}" ]; then
			txtfiles=($(ls ${datadir}/*cope*txt))
			if [ ! -z "${txtfiles}" ]; then
				idrun=$(sed -r -e 's/.*\/(HCD[0-9]+_V[0-9]_MR)\/.*\/(tfMRI.*[AP][PA])\/.*/\1,\2/' <<< "${datadir}")
				for txtfile in ${txtfiles[@]}; do
					fname=$(basename "${txtfile}")
					fname=$(sed -r -e 's/(.*cope[0-9]{1,2})\..*/\1/' <<< "${fname}")
					echo $fname
					echo $idrun
					awk -v idrun="$idrun" -v fname="$fname" '{print idrun","fname","$0}' $txtfile >> $output
				done
			fi
		fi
	done
done
