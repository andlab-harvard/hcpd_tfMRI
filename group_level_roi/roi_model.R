TEST <- FALSE
#TEST <- TRUE
.libPaths(c('/ncf/mclaughlin/users/jflournoy/R/x86_64-pc-linux-gnu-library/verse-4.3.1', .libPaths()))
library(stringi)
library(data.table)
library(brms)
library(argparse)

ITER <- ifelse(TEST, 100, 3000)
WARMUP <- ifelse(TEST, 50, 2000)

parse_cope_names <- function(x, cope_lookup, ROI_ARG, this_dir = '.'){
  roi_labels <- fread(file.path(this_dir, 'CortexSubcortex_ColeAnticevic_NetPartition_wSubcorGSR_parcels_LR_LabelKey.txt'))
  roi_labels <- roi_labels[, c('INDEX', 'LABEL', 'NETWORKSORTEDORDER')]
  regex <- '([\\w-]+)-\\d{2}_([LR])-(\\w+)'
  roi_labels[, c('LABEL', 'network', 'hemi', 'anat') := transpose(stri_match_all_regex(LABEL, regex))]
  setnames(roi_labels, 'INDEX', 'roi_num')
  subcort <- na.omit(unique(roi_labels[anat != "Ctx", anat]))
  get_these_rois <- ROI_ARG
  if (ROI_ARG > 360){
    subcort_roi <- ROI_ARG - 360
    subcort_region <- subcort[subcort_roi]
    get_these_rois <- roi_labels[anat %in% subcort_region, roi_num]
  }
    
  x <- melt(x[roi %in% get_these_rois], id.vars = c('id', 'roi', 'direction', 'session'))
  
  x[, c('variable', 'var', 'condition') := transpose(stri_match_all_regex(variable, '(var)*(cope\\d+)'))]
  x[, condition := cope_lookup[condition]]
  
  #The sqrt of the varcope is the standard error, which we need for our brms formulation
  #From https://fsl.fmrib.ox.ac.uk/fsl/fslwiki/FEAT/UserGuide
  #stats/tstat1 the T statistic image for contrast 1 (=cope/sqrt(varcope)).
  #stats/varcope1 the variance (error) image for contrast 1. 
  
  x[, c('value', 'var') := 
      list(fifelse(is.na(var), value, sqrt(value)),
           fifelse(is.na(var), 'Est', 'SE'))]
  x[, variable := NULL]
  x <- dcast(x, ... ~ var, value.var = 'value')
  x <- x[Est != 0 & SE != 0]
  x[, roi_fac := factor(roi)]
  return(x)
}

# create parser object
parser <- ArgumentParser()

# specify our desired options 
# by default ArgumentParser will add a help option 
parser$add_argument('--ncpus', type = 'integer', help = 'Number of CPUs allocated to job.')
parser$add_argument(
  "--model", type = "character", default = 'm0',
  help = "Which model to fit. Options are [M0|M_lin|M_spline].")
parser$add_argument(
  "--task", type = "character", default = 'CARIT_PREPOT',
  help = "Which fMRI task. Options are [CARIT_PREPOT|CARIT_PREVCOND|GUESSING].")
parser$add_argument('--threads', type = "integer", default = NULL, 
                    help = 'Number of threads per chain. `nthreads` x 4 should be equal to the number of CPUs allocated, unless `chainid` is specified.')
parser$add_argument('--roi', type = "integer", default = NULL, 
                    help = 'ROI number in 1-718')
parser$add_argument('--network', type = "integer", default = NULL, 
                    help = 'Not implemented')
parser$add_argument('--chainid', type = "integer", default = NULL, 
                    help = 'If splitting chains across nodes, this is the chain ID.')
parser$add_argument('--sampleprior', type = "character", default = 'yes', 
                    help = 'Sample prior? Default is "yes". Options are "only", "yes", "no"')
parser$add_argument("--kfold", action = "store_true", 
                    help = "Run k-fold CV using participant clusters.")
parser$add_argument("--refit", action = "store_true", 
                    help = "Force overwrite of previous model.")
parser$add_argument('--nfolds', type = "integer", default = 5, help = 'Number of folds')
parser$add_argument('--foldid', type = "integer", default = 1, help = 'Fold ID')
parser$add_argument("--long", action = "store_true", 
                    help = "Use longitudinal data.")
parser$add_argument("--onlylong", action = "store_true", 
                    help = "Use _only_ longitudinal data.")
#ADD KFOLDS, NFOLDS, FOLDID

if(TEST){
  args <- parser$parse_args(c('--model', 'm0_spline',
                              '--task', 'GUESSING',
                              '--roi', '361',
                              '--threads', '4', 
                              '--ncpus', '4', 
                              '--chainid', '1'))
  this_dir <- 'group_level_roi'
} else {
  args <- parser$parse_args()
  this_dir <- '.'
}

MODEL <- args$model
TASK <- args$task
ROI_ARG <- args$roi
NETWORK <- args$network
THREADS <- args$threads
NCPUS <- args$ncpus
KFOLD <- args$kfold
FOLDID <- args$foldid
NFOLDS <- args$nfolds
REFIT <- args$refit
LONG <- args$long | args$onlylong
ONLYLONG <- args$onlylong

if(is.null(args$chainid)){
  CHAINS <- 4
  CHAINID <- NULL
} else {
  CHAINS <- 1  
  CHAINID <- args$chainid
}
if(is.null(ROI_ARG) & !is.null(NETWORK)){
  USE_NETWORK <- TRUE
  number_report <- sprintf('Network number: %s', NETWORK)
  fn_number <- sprintf('N%02d', NETWORK)
} else if(!is.null(ROI_ARG) & is.null(NETWORK)){
  USE_NETWORK <- FALSE
  number_report <- sprintf('ROI number: %s', ROI_ARG)
  fn_number <- sprintf('%03d', ROI_ARG)
} else {
  stop('Incompatible ROI number and network number (you can only specify one)')
}
fn <- sprintf('%s%s-%s-%s%s-c%d', 
              ifelse(TEST, 'test-', ''),
              TASK,
              MODEL, 
              fn_number, 
              ifelse(KFOLD, sprintf('-k%02d', FOLDID), ''), 
              ifelse(is.null(CHAINID), 1234, CHAINID))

if(grepl('group_level_roi', getwd())){
  source('load_data.R')
  fn <- file.path('fits', fn)
} else {
  source('group_level_roi/load_data.R')
  fn <- file.path('group_level_roi', 'fits', fn)
}

dem_data <- fread(file.path(this_dir, '..', 'hcpd_task_behavior', 'HCD_Inventory_2022-04-12.csv'), 
               select = c('subject', 'site',
                          'event_age', 'redcap_event'))
setnames(dem_data, 'subject', 'id')
setnames(dem_data, 'event_age', 'age')
setnames(dem_data, 'redcap_event', 'session')
dem_data[, age_c10 := age - 10]

if(TASK %in% c('CARIT_PREPOT', 'CARIT_PREVCOND')){
  carit_fmri_data <- load_hcpd_data('CARIT_PREPOT', nthreads = 1)
  carit_cope_lookup <- c('cope1' =  'Hit_1go',
                         'cope2' =  'Hit_2go',
                         'cope3' =  'Hit_3go',
                         'cope4' =  'Hit_4go',
                         'cope5' =  'Miss',
                         'cope6' =  'CR_2go',
                         'cope7' =  'CR_3go',
                         'cope8' =  'CR_4go',
                         'cope9' =  'FA_2go',
                         'cope10' = 'FA_3go',
                         'cope11' = 'FA_4go')
  
  carit_fmri_data <- parse_cope_names(carit_fmri_data, cope_lookup = carit_cope_lookup, ROI_ARG = ROI_ARG, this_dir = this_dir)
  
  carit_fmri_data[, c('resp_type', 'n_go') := tstrsplit(condition, '_')]
  carit_fmri_data[, resp_type_fac := C(factor(resp_type, levels = c('Hit', 'CR')), contr = 'contr.sum')]
  carit_fmri_data[, condition_fac := C(factor(condition), contr = 'contr.sum')]
  fmri_data <- carit_fmri_data[resp_type %in% c('Hit', 'CR')]

  
  N_roi <- length(unique(fmri_data$roi))
  
  if(N_roi > 1){
    brm_model_options <- list(
      m0 = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac + 
                                (1 | id/direction) + (1 | roi_fac), 
                              sigma ~ 1 + (1 | id), 
                              family = 'student')),
      m0_lin = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac * age_c10 + 
                                    (1 | id/direction) + (1 + age_c10 | roi_fac),  
                                  sigma ~ 1 + (1 | id),
                                  family = 'student')),
      m0_spline = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac + 
                                       t2(age_c10, by = condition_fac, bs = 'tp', k = 10) +
                                       t2(roi_fac, age_c10, bs = 'sz', k = 10) +
                                       (1 | id/direction), 
                                     sigma ~ 1 + (1 | id),
                                     family = 'student')))[[MODEL]]
  } else {
    brm_model_options <- list(
      m0 = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac + (1 | id/direction), 
                              sigma ~ 1 + (1 | id), 
                              family = 'student')),
      m0_lin = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac * age_c10 + (1 | id/direction),  
                                  sigma ~ 1 + (1 | id),
                                  family = 'student')),
      m0_spline = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac + 
                                       t2(age_c10, by = condition_fac, bs = 'tp', k = 10) +
                                       (1 | id/direction), 
                                     sigma ~ 1 + (1 | id),
                                     family = 'student')))[[MODEL]]
  }
} else if(TASK == 'GUESSING') {
  guessing_fmri_data <- load_hcpd_data('GUESSING', nthreads = 1)  
  guessing_cope_lookup <- c('cope1' =  'TASK',
                         'cope2' =  'CUE_AVG',
                         'cope3' =  'CUE_HIGH',
                         'cope4' =  'CUE_LOW',
                         'cope5' =  'GUESS',
                         'cope6' =  'FEEDBACK_AVG',
                         'cope7' =  'FEEDBACK_AVG_WIN',
                         'cope8' =  'FEEDBACK_AVG_LOSE',
                         'cope9' =  'FEEDBACK_AVG_WIN-LOSE',
                         'cope10' = 'FEEDBACK_HIGH_WIN',
                         'cope11' = 'FEEDBACK_HIGH_LOSE',
                         'cope12' = 'FEEDBACK_LOW_WIN',
                         'cope13' = 'FEEDBACK_LOW_LOSE',
                         'cope14' = 'FEEBACK_HIGH-LOW_WIN',
                         'cope15' = 'FEEDBACK_HIGH-LOW_LOSE',
                         'cope16' = 'FEEDBACK-CUE_AVG')
  guessing_fmri_data <- parse_cope_names(x = guessing_fmri_data, cope_lookup = guessing_cope_lookup, ROI_ARG = ROI_ARG, this_dir = this_dir)
  guessing_fmri_data <- guessing_fmri_data[
    condition %in% c('CUE_HIGH', 'CUE_LOW', 
                     'GUESS',
                     'FEEDBACK_HIGH_WIN',
                     'FEEDBACK_HIGH_LOSE',
                     'FEEDBACK_LOW_WIN',
                     'FEEDBACK_LOW_LOSE')
  ]
  guessing_fmri_data[, c('magnitude', 'feedback') :=
                        transpose(stri_match_all_regex(condition, '(?:FEEDBACK|CUE)_(HIGH|LOW)_*(WIN|LOSE)*'))[2:3]]
  guessing_fmri_data[, condition_fac := C(factor(condition), contr = 'contr.sum')]
  fmri_data <- guessing_fmri_data
  
  N_roi <- length(unique(fmri_data$roi))
  
  if(N_roi > 1){
    brm_model_options <- list(
      m0 = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac + 
                                (1 | id/direction) + (1 | roi_fac), 
                              sigma ~ 1 + (1 | id), 
                              family = 'student')),
      m0_lin = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac * age_c10 + 
                                    (1 | id/direction) + (1 + age_c10 | roi_fac),  
                                  sigma ~ 1 + (1 | id),
                                  family = 'student')),
      m0_spline = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac + 
                                       t2(age_c10, by = condition_fac, bs = 'tp', k = 10) +
                                       t2(roi_fac, age_c10, bs = 'sz', k = 10) +
                                       (1 | id/direction), 
                                     sigma ~ 1 + (1 | id),
                                     family = 'student')))[[MODEL]]
  } else {
    brm_model_options <- list(
      m0 = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac + (1 | id/direction), 
                              sigma ~ 1 + (1 | id), 
                              family = 'student')),
      m0_lin = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac * age_c10 + (1 | id/direction),  
                                  sigma ~ 1 + (1 | id),
                                  family = 'student')),
      m0_spline = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition_fac + 
                                       t2(age_c10, by = condition_fac, bs = 'tp', k = 10) +
                                       (1 | id/direction), 
                                     sigma ~ 1 + (1 | id),
                                     family = 'student')))[[MODEL]]
  }
} else {
  stop("No valid task")
}

if(KFOLD){
  set.seed(92105)
  folds <- loo::kfold_split_grouped(K = NFOLDS, x = as.character(model_data$id))
  omitted <- predicted <- which(folds == FOLDID)
  full_data <- model_data
  model_data <- model_data[-omitted]
}

model_data <- dem_data[fmri_data, on = c('id', 'session')]

if(!LONG){
  model_data <- model_data[session == 'V1']
} else if(ONLYLONG){
  long_pid <- model_data[, .(nwaves = length(unique(session))), by = 'id'][nwaves > 1, 'id']
  model_data <- model_data[long_pid, on = 'id']
} 

Est_sd <- sd(model_data$Est)
model_prior <- brms::get_prior(formula = brm_model_options[['formula']], data = model_data)
coef_names <- unique(model_prior$coef[model_prior$class == 'b' & model_prior$coef != ""])
sds_names <- unique(model_prior$coef[model_prior$class == 'sds' & model_prior$coef != ""])
conditions_names <- coef_names[!grepl('(age|Intercept)', coef_names)]
age_names <- coef_names[grepl('age', coef_names)]

prior <- c(set_prior('gamma(2, 0.25)', class = 'nu', lb = 1))
if(!length(conditions_names) == 0){
  prior <- c(prior, set_prior(sprintf('student_t(3, 0, %0.1f)', Est_sd / 2), class = 'b', coef = conditions_names))
}
if(!length(age_names) == 0){
  prior <- c(prior, set_prior(sprintf('student_t(3, 0, %0.1f)', Est_sd / 4), class = 'b', coef = age_names))
}
if(!length(sds_names) == 0){
  prior <- c(prior, set_prior(sprintf('student_t(3, %0.1f, %0.1f)', Est_sd/8, Est_sd / 2), class = 'sds', coef = sds_names))
}

cat('\nDefault Priors:\n\n')
model_prior
cat('\nOur Priors:\n\n')
prior

brm_options <- c(brm_model_options, 
                 list(prior = prior,
                      data = model_data,
                      file = fn,
                      file_refit = ifelse(REFIT, 'always', 'on_change'),
                      backend = 'cmdstanr',
                      iter = ITER, warmup = WARMUP, chains = CHAINS, cores = CHAINS,
                      control = list(adapt_delta = .9999, max_treedepth = 12)))
if(!is.null(THREADS)){
  brm_options <- c(brm_options, list(threads = THREADS))
}
if(!is.null(CHAINID)){
  brm_options <- c(brm_options, list(chain_ids = CHAINID))
}
brm_options$sample_prior <- args$sampleprior
brm_options$stan_model_args <- list(stanc_options = list('O1')) 
brm_options$silent <- 0

cat('\nData structure:\n\n')
cat(sprintf('N: %d\n', length(unique(model_data$id))))
cat(sprintf('Obs: %d\n', dim(unique(model_data[, c('id', 'direction', 'session')]))[[1]]))
cat(sprintf('Waves: %d\n', dim(unique(model_data[, c('session')]))[[1]]))
cat(sprintf('Coefs: \n%s', paste(coef_names, collapse = '  \n')))
cat('\nOptions:\n\n')
brm_options

fit <- do.call(brm, brm_options)
summary(fit)

if(KFOLD){
  fit$manual_kfold <- brms:::nlist(full_data, omitted, predicted, folds)
  saveRDS(fit, sprintf('%s.rds', brm_options$file))
}
#pp_check(fit)
