addpath('/ncf/mclaughlin/users/jflournoy/code/spm12')
% List of open inputs
jobfile = {'/ncf/mclaughlin/users/jflournoy/code/hcpd_tfMRI/group_level_vwise/GUESSING/FEEDBACK_AVG/SwE_contrast_job_FEEDBACK_AVG.m'};
jobs = repmat(jobfile, 1, 1);
inputs = cell(0, 1);
spm('defaults', 'FMRI');
spm_jobman('run', jobs, inputs{:});
