#!/bin/bash
#SBATCH -p fasse
#SBATCH -J matlabjob
#SBATCH --mem=32G
#SBATCH -c 1
#SBATCH -t 1-00:00:00
#SBATCH -o logs/%x-%A.log
#SBATCH --account=somerville_lab

module load matlab/R2021a-fasrc01
matlab -nodisplay -nosplash -nodesktop -r "run('___RUNFILE___'); exit"
