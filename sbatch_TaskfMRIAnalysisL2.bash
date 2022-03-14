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
# sbatch --array=<range> <this script name> <input file> <template.fsf> [GUESSING|CARIT] [1st|2nd] [parcellated]
#
# <range> should be lines of <input file>, numbered starting from 0
# <input file> lines should be of the following format:
#
#HCD2156344_V1_MR tfMRI_CARIT_PA@tfMRI_CARIT_AP tfMRI_CARIT
#
# where the first field is the subject directory, the second field
# is a "@" separated list of level 1 directories, and the third field
# is the level 2 directory name.
# <template.fsf> is the template fsf file that will be coppied into the
# relevant directories.
# [1st|2nd] is an optional argument that specifies to run just 1st or  
# also run 2nd level  models.
# [parcellated] tells the script to run using parcellated data. For now
# the parcellation is hard-coded.
##

set -eoux pipefail

source /users/jflournoy/code/FSL-6.0.4_workbench-1.0.txt
source /users/jflournoy/code/R_3.5.1_modules.bash
export HCPPIPEDIR="/ncf/mclaughlin/users/jflournoy/code/HCPpipelines/"
source SetUpHCPPipeline.sh

thisdir=$(pwd -P)
i=$SLURM_ARRAY_TASK_ID
TaskAnalysisiInput=$1
TemplateFSF=$2
#In future, we might be able to get the taskname from the input file.
TaskName=$3
L="${4}"

if [ ! -z "${5-}" ] && [ "${5}" = "parcellated" ]; then
	parcellated=1
	parcellation_file="${thisdir}/first_level/CortexSubcortex_ColeAnticevic_NetPartition_wSubcorGSR_parcels_LR.dlabel.nii"
	parcellation_name="ColeAnticevic"
	echo "Running parcellated analysis"
	if [ ! -f "${parcellation_file}" ]; then
		echo "Parcellation file does not exist: ${parcellation_file}"
		exit 1
	fi
else
	parcellated=0
fi

if [ ! -z "${L}" ] && [ "${L}" != "2nd" ] && [ "${L}" != "1st" ]; then
	echo "Argument 4 not understood: ${L}"
	exit 1
fi
if [ ! -z "${TaskName}" ] && [ "${TaskName}" != "CARIT" ] && [ "${TaskName}" != "GUESSING" ]; then
	echo "Argument 3 not understood: ${TaskName}"
	exit 1
fi

DTFILE="../tfMRI_${TaskName}_AP_Atlas_hp0_clean.dtseries.nii"
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
	cp -v ${TemplateFSF} ${L1TEMPLATE}
	NEWDTFILE="../${TASK}${DTFILE#../tfMRI_${TaskName}_AP}"
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
elif [ ! -z "${L}" ] && [ "${L}" == "1st" ]; then
	runme="srun -c 1 bash ${TaskfMRIAnalysis} --study-folder=${STUDYFOLDER} \
		--subject=${SUBJECTID} \
		--lvl1tasks=${TASKID} \
		--procstring=hp0_clean \
		--finalsmoothingFWHM=4 \
		--highpassfilter=200"
	if [ ${parcellated} = 1 ]; then 
		runme="${runme} \
			--parcellation=${parcellation_name} \
			--parcellationfile=${parcellation_file}"
	fi
fi
	
eval "${runme}"
