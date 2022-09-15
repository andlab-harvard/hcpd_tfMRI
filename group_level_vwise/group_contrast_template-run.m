addpath('/ncf/mclaughlin/users/jflournoy/code/spm12')
% List of open inputs
jobfile = {'___JOBFILE___'};
jobs = repmat(jobfile, 1, 1);
inputs = cell(0, 1);
spm('defaults', 'FMRI');
spm_jobman('run', jobs, inputs{:});
