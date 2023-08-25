#!/bin/bash
#SBATCH -J CARIT_GROUP
#SBATCH -n 1
#SBATCH -p fasse
#SBATCH -c 1
#SBATCH --mem 8G
#SBATCH -t 04:00:00
#SBATCH --signal=USR2
#SBATCH --account=somerville_lab
# Outputs ----------------------------------

container="verse-cmdstan-ggseg-libs.simg"

echo "singularity version $(singularity version)"

OVERLAY="$SCRATCH/LABS/mclaughlin_lab/Users/jflournoy/$(uuidgen).img"
srun -c 1 singularity overlay create --size 2512 "${OVERLAY}"

srun -c $SLURM_CPUS_PER_TASK singularity exec --bind /ncf \
  --overlay ${OVERLAY} \
  /ncf/mclaughlin/users/jflournoy/data/containers/${container} \
  Rscript --no-save --no-restore \
    /ncf/mclaughlin/users/jflournoy/code/hcpd_task_behavior/carit_make_group_model.R \
      ++name cell_coded \
      ++outdir /ncf/mclaughlin/users/jflournoy/code/hcpd_tfMRI/group_level_vwise \
      ++design maineffects \
      ++covariates scanner RelativeRMS_mean_c

srun -c $SLURM_CPUS_PER_TASK singularity exec --bind /ncf \
  --overlay ${OVERLAY} \
  /ncf/mclaughlin/users/jflournoy/data/containers/${container} \
  Rscript --no-save --no-restore \
    /ncf/mclaughlin/users/jflournoy/code/hcpd_task_behavior/carit_make_group_model.R \
      ++name factor_coded \
      ++outdir /ncf/mclaughlin/users/jflournoy/code/hcpd_tfMRI/group_level_vwise \
      ++design interaction \
      ++covariates scanner RelativeRMS_mean_c

rm ${OVERLAY}
