#!/bin/bash
#SBATCH -p fasse
#SBATCH -J roifit
#SBATCH -c 4
#SBATCH --mem=12G
#SBATCH -t 1-00:00:00
#SBATCH -o logs/%x-%A_%a.log
#SBATCH --account=somerville_lab

if [ $# -lt 1 ]; then
  echo 1>&2 "$0: not enough arguments"
  exit 2
elif [ $# -gt 1 ]; then
  echo 1>&2 "$0: too many arguments"
  exit 2
fi

echo "singularity version $(singularity version)"

OVERLAY="$SCRATCH/LABS/mclaughlin_lab/Users/jflournoy/$SLURM_JOBID/$(uuidgen).img"
mkdir -p $(dirname ${OVERLAY})
if [ -d $(dirname ${OVERLAY}) ]; then
  echo "Creating overlay:"
  echo "  ${OVERLAY}"
  echo "  ..."
  singularity overlay create --size 2512 "${OVERLAY}"
else
  echo "No dir: $(dirname ${OVERLAY})"
  exit 2
fi

if [ -f ${OVERLAY} ]; then
  echo "Running R script..."
  
  model="${1}"
  roi="${SLURM_ARRAY_TASK_ID}"
  cmd="srun -c $SLURM_CPUS_PER_TASK --mem=$SLURM_MEM_PER_NODE -o logs/%x_${model}-%A_%a.log"
  cmd+=" singularity exec --overlay ${OVERLAY} /n/home_fasse/jflournoy/data/containers/verse-cmdstan-ggseg-libs.simg"
  cmd+=" Rscript --no-save --no-restore roi_model.R"
  cmd+=" --model ${model} --ncpus $SLURM_CPUS_PER_TASK"
  cmd+=" --roi ${roi}"
  
  echo "Command: $cmd"
  exec $cmd
  
  rm -v ${OVERLAY}
else
  echo "No overlay found"
  exit 2
fi