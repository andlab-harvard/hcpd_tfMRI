#!/bin/bash
#SBATCH -p fasse
#SBATCH -J matlabjob
#SBATCH --mem=16G
#SBATCH -c 1
#SBATCH -t 5-00:00:00
#SBATCH -o logs/%x-%A.log
#SBATCH --account=somerville_lab

module load matlab/R2022a-fasrc01
matlab -nodisplay -nosplash -nodesktop -r "run('/ncf/mclaughlin/users/jflournoy/code/hcpd_tfMRI/group_level/CR_m_Go/SwE_contrast_job_run_CR_m_Go.m'); exit"
