TEST <- FALSE
#TEST <- TRUE
.libPaths(c('/ncf/mclaughlin/users/jflournoy/R/x86_64-pc-linux-gnu-library/verse-4.2.1', .libPaths()))
library(stringi)
library(data.table)
library(brms)
library(argparse)

ITER <- ifelse(TEST, 100, 5000)
WARMUP <- ifelse(TEST, 50, 1500)

# create parser object
parser <- ArgumentParser()

# specify our desired options 
# by default ArgumentParser will add a help option 
parser$add_argument('--ncpus', type = 'integer', help = 'Number of CPUs allocated to job.')
parser$add_argument(
  "--model", type = "character", default = 'm0',
  help = "Which model to fit. Options are [M0|M_lin|M_spline].")
parser$add_argument('--threads', type = "integer", default = NULL, 
                    help = 'Number of threads per chain. `nthreads` x 4 should be equal to the number of CPUs allocated, unless `chainid` is specified.')
parser$add_argument('--roi', type = "integer", default = NULL, 
                    help = 'ROI number in 1-718')
parser$add_argument('--network', type = "integer", default = NULL, 
                    help = 'Not implemented')
parser$add_argument('--chainid', type = "integer", default = NULL, 
                    help = 'If splitting chains across nodes, this is the chain ID.')

if(TEST){
  args <- parser$parse_args(c('--model', 'm0_spline',
                              '--roi', '1',
                              '--threads', '4', 
                              '--ncpus', '4', 
                              '--chainid', '1'))
} else {
  args <- parser$parse_args()
}

MODEL <- args$model
ROI_ARG <- args$roi
NETWORK <- args$network
THREADS <- args$threads
NCPUS <- args$ncpus
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
if(is.null(CHAINID)){
  fn <- sprintf('%s-%s', MODEL, fn_number)
} else {
  fn <- sprintf('%s-%s-c%d', MODEL, fn_number, CHAINID)
}

if(grepl('group_level_roi', getwd())){
  source('load_data.R')
  fn <- file.path('fits', fn)
} else {
  source('group_level_roi/load_data.R')
  fn <- file.path('group_level_roi', 'fits', fn)
}
carit_dem_data <- fread('~/code/hcpd_task_behavior/HCPD_staged_and_pr_w_demogs.csv', select = c('sID', 'age'))
carit_dem_data[, age_c10 := age - 10]

carit_fmri_data <- load_hcpd_data('carit', nthreads = 4)
roi_range <- range(carit_fmri_data$roi)
cat(sprintf('Choosing ROI %d out of %d-%d.', ROI_ARG, roi_range[[1]], roi_range[[2]]))

cope_lookup <- c('cope1' =  'Hit_1go',
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

carit_fmri_data <- melt(carit_fmri_data[roi == ROI_ARG], id.vars = c('id', 'roi', 'direction'))
carit_fmri_data[, c('variable', 'var', 'condition') := transpose(stri_match_all_regex(variable, '(var)*(cope\\d+)'))]
carit_fmri_data[, condition := cope_lookup[condition]]
carit_fmri_data[, c('value', 'var') := 
             list(fifelse(is.na(var), value, sqrt(value)),
                  fifelse(is.na(var), 'Est', 'SE'))]
carit_fmri_data[, variable := NULL]
carit_fmri_data <- dcast(carit_fmri_data, ... ~ var, value.var = 'value')
carit_fmri_data <- carit_fmri_data[Est != 0 & SE != 0]
carit_fmri_data[, c('resp_type', 'n_go') := tstrsplit(condition, '_')]
carit_fmri_data[, sID := gsub('(HCD\\d+)_\\w+_\\w+', '\\1', id)]
carit_fmri_data <- carit_fmri_data[resp_type %in% c('Hit', 'CR')]

model_data <- carit_dem_data[carit_fmri_data, on = 'sID']


brm_model_options <- list(
  m0 = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition + (1 | id/direction), 
                          family = 'student')),
  m0_lin = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition * age_c10 + (1 | id/direction), 
                               family = 'student')),
  m0_spline = list(formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition + s(age_c10, by = condition, bs = 'tp', k = 10) +
                                 (1 | id/direction), 
                                 family = 'student')))[[MODEL]]

Est_sd <- sd(carit_fmri_data$Est)
model_prior <- brms::get_prior(formula = brm_model_options[['formula']], data = model_data)
coef_names <- unique(model_prior$coef[model_prior$class == 'b' & model_prior$coef != ""])
conditions_names <- coef_names[!grepl('(age|Intercept)', coef_names)]
age_names <- coef_names[grepl('age', coef_names)]
prior <- c(set_prior('gamma(2, 0.1)', class = 'nu', lb = 1))
if(!length(conditions_names) == 0){
  prior <- c(prior, set_prior(sprintf('normal(0,%0.1f)', Est_sd / 2), class = 'b', coef = conditions_names))
}
if(!length(age_names) == 0){
  prior <- c(prior, set_prior(sprintf('normal(0,%0.1f)', Est_sd / 4), class = 'b', coef = age_names))
}

cat('\nDefault Priors:\n\n')
model_prior
cat('\nOur Priors:\n\n')
prior

brm_options <- c(brm_model_options, 
                 list(prior = prior,
                      data = model_data,
                      file = fn,
                      file_refit = 'on_change',
                      backend = 'cmdstanr',
                      iter = ITER, warmup = WARMUP, chains = CHAINS, cores = CHAINS,
                      control = list(adapt_delta = .9999999999, max_treedepth = 20)))
if(!is.null(THREADS)){
  brm_options <- c(brm_options, list(threads = THREADS))
}
if(!is.null(CHAINID)){
  brm_options <- c(brm_options, list(chain_ids = CHAINID))
}

cat('\nData structure:\n\n')
unique(model_data[, c('n_go', 'resp_type')])
cat(sprintf('N: %d\n', length(unique(model_data$sID))))
cat(sprintf('Obs: %d\n', dim(unique(model_data[, c('sID', 'direction')]))[[1]]))
cat(sprintf('Coefs: \n%s', paste(coef_names, collapse = '  \n')))
cat('\nOptions:\n\n')
brm_options

fit <- do.call(brm, brm_options)
summary(fit)
