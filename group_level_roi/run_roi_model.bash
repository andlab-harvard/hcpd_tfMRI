#!/bin/bash

#models=('m0' 'm0_lin' 'm0_spline')
models=('m0_spline')
#'trt' 'trt-dt'
roimax=1
chains=(1 2 3 4)

for model in ${models[@]}; do
  echo "%%%%%%%%"
  echo "Model: $model"
  for roi in $(seq 1 ${roimax}); do
    echo ".."
    echo "ROI: ${roi}"
    for chain in ${chains[@]}; do
      cmd="sbatch -J $model-$roi-c$chain -c 16 --mem=16G -t 1-00:00:00 -o logs/%x-%A.log "
      cmd+="/ncf/mclaughlin/users/jflournoy/data/containers/sbatch_R_command_som.bash "
      cmd+="verse-cmdstan-cuda.simg roi_model.R "
      cmd+="--model $model --ncpus 16 --threads 16 --chainid ${chain} --sampleprior yes --kfold --nfolds 5 --foldid 1 "
      cmd+="--roi ${roi}"
      echo "Command: $cmd"
      exec $cmd &
    done
  done
done
