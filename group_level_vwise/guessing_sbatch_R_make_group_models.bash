#!/bin/bash
#SBATCH -J GUESSING_GROUP
#SBATCH -n 1
#SBATCH -p fasse
#SBATCH -c 1
#SBATCH --mem 8G
#SBATCH -t 04:00:00
#SBATCH --signal=USR2
#SBATCH --account=somerville_lab
# Outputs ----------------------------------
#SBATCH --output=%x-%A.log
container="verse-cmdstan-ggseg-libs.simg"

echo "singularity version $(singularity version)"

OVERLAY="$SCRATCH/LABS/mclaughlin_lab/Users/jflournoy/$(uuidgen).img"
srun -c 1 singularity overlay create --size 2512 "${OVERLAY}"

contrasts=(TASK CUE_AVG CUE_HIGH CUE_LOW GUESS FEEDBACK_AVG FEEDBACK_AVG_WIN FEEDBACK_AVG_LOSE FEEDBACK_AVG_WIN-LOSE FEEDBACK_HIGH_WIN FEEDBACK_HIGH_LOSE FEEDBACK_LOW_WIN FEEDBACK_LOW_LOSE FEEDBACK_HIGH-LOW_WIN FEEDBACK_HIGH-LOW_LOSE FEEDBACK-CUE_AVG)

for contrast in ${contrasts[@]}; do
	contrastname="${contrast//-/_m_}"
	srun -c $SLURM_CPUS_PER_TASK singularity exec --bind /ncf \
	  --overlay ${OVERLAY} \
	  /ncf/mclaughlin/users/jflournoy/data/containers/${container} \
	  Rscript --no-save --no-restore \
	    /ncf/mclaughlin/users/jflournoy/code/hcpd_task_behavior/guessing_make_group_model.R \
	      ++name ${contrastname} \
	      ++simple-design ${contrast} \
	      ++outdir /ncf/mclaughlin/users/jflournoy/code/hcpd_tfMRI/group_level_vwise \
	      ++design simple \
	      ++covariates scanner RelativeRMS_mean_c
done
rm ${OVERLAY}
