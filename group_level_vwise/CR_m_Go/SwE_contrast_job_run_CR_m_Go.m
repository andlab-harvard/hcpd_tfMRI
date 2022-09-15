addpath('/ncf/mclaughlin/users/jflournoy/code/spm12')
% List of open inputs
jobfile = {'/ncf/mclaughlin/users/jflournoy/code/hcpd_tfMRI/group_level/CR_m_Go/SwE_contrast_job_CR_m_Go.m'};
jobs = repmat(jobfile, 1, 1);
inputs = cell(0, 1);
spm('defaults', 'FMRI');
spm_jobman('run', jobs, inputs{:});
