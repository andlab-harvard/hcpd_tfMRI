#!/bin/bash
#SBATCH -p fasse
#SBATCH -J matlabjob
#SBATCH --mem=32G
#SBATCH -c 1
#SBATCH -t 1-00:00:00
#SBATCH -o logs/%x-%A.log
#SBATCH --account=somerville_lab

module load matlab/R2021a-fasrc01
matlab -nodisplay -nosplash -nodesktop -r "run('/ncf/mclaughlin/users/jflournoy/code/hcpd_tfMRI/group_level_vwise/CUE_LOW/SwE_contrast_job_run_CUE_LOW.m'); exit"