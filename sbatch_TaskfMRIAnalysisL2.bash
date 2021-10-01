#!/bin/bash
#SBATCH -J hcp1st
#SBATCH --time=0-05:00:00
#SBATCH -n 1
#SBATCH --cpus-per-task=1
#SBATCH --mem=12G
#SBATCH -p ncf
#SBATCH --account=somerville_lab
# Outputs ----------------------------------
#SBATCH -o log/%x-%A_%a.out
###
# Usage:
# sbatch --array=<range> <this script name> <input file> [1st|2nd]
#
# <range> should be lines of <input file>, numbered starting from 0
# <input file> lines should be of the following format:
#
#HCD2156344_V1_MR tfMRI_CARIT_PA@tfMRI_CARIT_AP tfMRI_CARIT
#
# where the first field is the subject directory, the second field
# is a "@" separated list of level 1 directories, and the third field
# is the level 2 directory name.
# [1st|2nd] is an optional argument that specifies to run just 1st or  
# also run 2nd level  models.
##

set -eou pipefail

source /users/jflournoy/code/FSL-6.0.4_workbench-1.0.txt
source /users/jflournoy/code/R_3.5.1_modules.bash
export HCPPIPEDIR="/ncf/mclaughlin/users/jflournoy/code/HCPpipelines/"
source SetUpHCPPipeline.sh

i=$SLURM_ARRAY_TASK_ID
TaskAnalysisiInput=$1
L="${2}"
if [ ! -z "${L}" ] && [ "${L}" != "2nd" ] && [ "${L}" != "1st" ]; then
	echo "Argument 2 not understood: ${2}"
	exit 1
fi

DTFILE="../tfMRI_CARIT_AP_Atlas_hp0_clean.dtseries.nii"
TaskfMRIAnalysis="${HCPPIPEDIR}/TaskfMRIAnalysis/TaskfMRIAnalysis.sh"
STUDYFOLDER="/net/holynfs01/srv/export/ncf_hcp/share_root/data/HCD-tfMRI-MultiRunFix"
SUBJECTIDS=($(awk '{ print $1 }' ${TaskAnalysisiInput}))
TASKIDS=($(awk '{ print $2 }' ${TaskAnalysisiInput}))
TASKIDSL2=($(awk '{ print $3 }' ${TaskAnalysisiInput}))

SUBJECTID="${SUBJECTIDS[${i}]}"
TASKID="${TASKIDS[${i}]}"
TASKIDL2="${TASKIDSL2[${i}]}"

IFS="@" read -a TASKARRAY <<< $TASKID
for TASK in ${TASKARRAY[@]}; do
	L1DIR="${STUDYFOLDER}/${SUBJECTID}/MNINonLinear/Results/${TASK}"
	L1TEMPLATE="${L1DIR}/${TASK}_hp200_s4_level1.fsf"
	cp -v template.fsf ${L1TEMPLATE}
	NEWDTFILE="../${TASK}${DTFILE#../tfMRI_CARIT_AP}"
	sed -i -e "s|${DTFILE}|${NEWDTFILE}|" ${L1TEMPLATE}
	#check for EVs directory
	EVDIR="${L1DIR}/EVs"
	if [ ! -d "${EVDIR}" ]; then
		CSVFILEBASE="/ncf/hcp/data/CCF_HCD_STG_PsychoPy_files/"
		CSVID="${SUBJECTID%_*_*}"
		TASKSHORT=${TASK%_*}
		TASKSHORT=${TASKSHORT#*_}
		CSVFN="${TASKSHORT}_${CSVID}_*.csv"
		CSVFILE=$(ls ${CSVFILEBASE}/${CSVID}/${TASK}/${CSVFN})
		Rscript create_EVs.R --evdir ${EVDIR} ${CSVFILE} 
	fi
done


if [ ! -z "${L}" ] && [ "${L}" == "2nd" ]; then
	L2DIR="${STUDYFOLDER}/${SUBJECTID}/MNINonLinear/Results/${TASKIDL2}"
	L2TEMPLATE="${L2DIR}/${TASKIDL2}_hp200_s4_level2.fsf"

	if [ ! -d ${L2DIR} ]; then mkdir ${L2DIR}; fi
	cp -v template_l2.fsf ${L2TEMPLATE}
	runme="srun -c 1 bash ${TaskfMRIAnalysis} --study-folder=${STUDYFOLDER} \
		--subject=${SUBJECTID} \
		--lvl1tasks=${TASKID} \
		--lvl2task=${TASKIDL2} \
		--procstring=hp0_clean \
		--finalsmoothingFWHM=4 \
		--highpassfilter=200"
elif [ ! -z "${2}" ] && [ "${L}" == "1st" ]; then
	runme="srun -c 1 bash ${TaskfMRIAnalysis} --study-folder=${STUDYFOLDER} \
		--subject=${SUBJECTID} \
		--lvl1tasks=${TASKID} \
		--procstring=hp0_clean \
		--finalsmoothingFWHM=4 \
		--highpassfilter=200"
fi
	
eval "${runme}"
