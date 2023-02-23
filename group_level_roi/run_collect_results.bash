#!/bin/bash

sbatch -c 4 --mem=32G -t 1-00:00:00 --account=somerville_lab ~/data/containers/sbatch_R_command.bash \
	verse-cmdstan-ggseg-libs.simg \
	collect_results.R
