#!/bin/bash
#SBATCH -J hcp1st
#SBATCH --time=0-05:00:00
#SBATCH -n 1
#SBATCH --cpus-per-task=1
#SBATCH --mem=12G
#SBATCH -p fasse
#SBATCH --account=somerville_lab
# Outputs ----------------------------------
#SBATCH -o log/%x-%A_%a.out
###
# Usage:
# sbatch --array=<range> <this script name> <input file> [GUESSING|CARIT] [parcellated]
#
# <range> should be lines of <input file>, numbered starting from 0
# <input file> lines should be of the following format:
#
#HCD2156344_V1_MR tfMRI_CARIT_PA@tfMRI_CARIT_AP tfMRI_CARIT_PA_PREPOT@tfMRI_CARIT_AP_PREPOT
#
# where the first field is the subject directory, the second field
# is a "@" separated list of level 1 directories, and the third field
# is a "@" spearated list of FSF template files.
# [parcellated] tells the script to run using parcellated data. For now
# the parcellation is hard-coded.
##

set -eoux pipefail

source /ncf/mclaughlin/users/jflournoy/code/FSL-6.0.4_workbench-1.3.2.txt
source /ncf/mclaughlin/users/jflournoy/code/R_3.5.1_modules.bash
export HCPPIPEDIR="/ncf/mclaughlin/users/jflournoy/code/HCPpipelines/"
source SetUpHCPPipeline.sh

thisdir=$(pwd -P)
#i=0
#TaskAnalysisInput=first_level/CARIT-PREVCOD-l1-list_2run.txt
#TaskName=CARIT
#iTASKARRAY=0
i=$SLURM_ARRAY_TASK_ID
TaskAnalysisInput=$1
#In future, we might be able to get the taskname from the input file.
TaskName=$2

if [ ! -z "${3-}" ] && [ "${3}" = "parcellated" ]; then
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

if [ ! -z "${TaskName}" ] && [ "${TaskName}" != "CARIT" ] && [ "${TaskName}" != "GUESSING" ]; then
    echo "Argument 2 not understood: ${TaskName}"
    exit 1
fi

DTFILE="../tfMRI_${TaskName}_AP_Atlas_hp0_clean.dtseries.nii"
TaskfMRIAnalysis="${HCPPIPEDIR}/TaskfMRIAnalysis/TaskfMRIAnalysis.sh"
STUDYFOLDER="/ncf/hcp/data/HCD-tfMRI-MultiRunFix"
SUBJECTIDS=($(awk '{ print $1 }' ${TaskAnalysisInput}))
TASKIDS=($(awk '{ print $2 }' ${TaskAnalysisInput}))
FSFFILES=($(awk '{ print $3 }' ${TaskAnalysisInput}))

SUBJECTID="${SUBJECTIDS[${i}]}"
TASKID="${TASKIDS[${i}]}"
FSFFILE="${FSFFILES[${i}]}"


IFS="@" read -a TASKARRAY <<< $TASKID
IFS="@" read -a FSFARRAY <<< $FSFFILE
LENTASKARRAY=$( seq 0 $(( ${#TASKARRAY[@]} - 1 )) )
for iTASKARRAY in ${LENTASKARRAY[@]}; do
    FSFTEMPLATE=${FSFARRAY[${iTASKARRAY}]}
    TASK=${TASKARRAY[${iTASKARRAY}]}
    L1DIR="${STUDYFOLDER}/${SUBJECTID}/MNINonLinear/Results/${TASK}"
    re='.*_(AP|PA)$'
    if [[ ${TASK} =~ ${re} ]]; then
        DIRECTION=${BASH_REMATCH[1]}
        L1TEMPLATE="${L1DIR}/${FSFTEMPLATE}_${DIRECTION}_hp200_s4_level1.fsf"
    else
        echo "Can't extract direction from TASK, exiting"
        exit 1
    fi
    cp -v ${FSFTEMPLATE}.fsf ${L1TEMPLATE}
    NEWDTFILE="../${TASK}${DTFILE#../tfMRI_${TaskName}_AP}"
    sed -i -e "s|${DTFILE}|${NEWDTFILE}|" ${L1TEMPLATE}
    
    EVDIR="${L1DIR}/EVs"
    OTHERARGS=""
    prevcondre='.*PREVCOND.*'
    if [[ $FSFTEMPLATE =~ ${prevcondre} ]]; then
        OTHERARGS+=" --carit prevcond"
    fi
    CSVFILEBASE="/ncf/hcp/data/intradb_multiprocfix/"
    CSVID="${SUBJECTID}"
    TASKSHORT=${TASK%_*}
    TASKSHORT=${TASKSHORT#*_}
    CSVFN="${TASKSHORT}_${CSVID%_*_*}*_wide.csv"
    CSVFILE=$(ls ${CSVFILEBASE}/${CSVID}/${TASK}/LINKED_DATA/PSYCHOPY/${CSVFN})
    Rscript first_level/create_EVs.R ${OTHERARGS} --evdir ${EVDIR} ${CSVFILE} 
    
    FSFARRAY[${iTASKARRAY}]=${FSFARRAY[${iTASKARRAY}]}_${DIRECTION}
done
FSFFILE=$(IFS=@; echo "${FSFARRAY[*]}")

runme="srun -c 1 bash ${TaskfMRIAnalysis} --study-folder=${STUDYFOLDER} \
    --subject=${SUBJECTID} \
    --lvl1tasks=${TASKID} \
    --lvl1fsfs=${FSFFILE} \
    --procstring=hp0_clean \
    --finalsmoothingFWHM=4 \
    --highpassfilter=200"
if [ ${parcellated} = 1 ]; then 
    runme="${runme} \
        --parcellation=${parcellation_name} \
        --parcellationfile=${parcellation_file}"
fi
    
eval "${runme}"
