library(stringi)
library(data.table)
library(brms)
source('group_level_roi/load_data.R')
setDTthreads(4)
carit_dem_data <- fread('~/code/hcpd_task_behavior/HCPD_staged_and_pr_w_demogs.csv')
carit_dem_data[, age_c10 := age - 10]

carit_fmri_data <- load_hcpd_data('carit', nthreads = 4)

dim(carit_fmri_data)
head(carit_fmri_data)

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

carit_fmri_data <- melt(carit_fmri_data, id.vars = c('id', 'roi', 'direction'))
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
carit_fmri_data_l <- melt(carit_fmri_data, measure.vars = c('Est', 'SE'))
carit_fmri_data_l[, condition := paste(variable, condition, sep = '_')]
carit_fmri_data_w <- dcast(carit_fmri_data_l[, -c('resp_type', 'n_go', 'variable')], ... ~ condition)
saveRDS(carit_fmri_data, '/ncf/hcp/data/analyses/tfMRI/roi/data/roi_data_carit_long.rds')
saveRDS(carit_fmri_data_w, '/ncf/hcp/data/analyses/tfMRI/roi/data/roi_data_carit_wide.rds')

carit_fmri_data <- carit_fmri_data[resp_type %in% c('Hit', 'CR')]

## Output fully long brain data with good names for conditions

model_data <- carit_dem_data[carit_fmri_data, on = 'sID']

m0_spline_formula = bf( Est | se(SE, sigma = TRUE) ~ 1 + condition + s(age_c10, by = condition, bs = 'tp', k = 10) +
                          (1 | id/direction),
                        family = student())

Est_sd <- sd(carit_fmri_data$Est)
(model_prior <- brms::get_prior(formula = m0_spline_formula, data = model_data))
coef_names <- unique(model_prior$coef[model_prior$class == 'b' & model_prior$coef != ""])
conditions_names <- coef_names[!grepl('(age|Intercept)', coef_names)]
age_names <- coef_names[grepl('age', coef_names)]
m0_spline_prior <- c(prior('gamma(2, 0.1)', class = 'nu', lb = 1))
if(!length(conditions_names) == 0){
  m0_spline_prior <- c(m0_spline_prior, set_prior(sprintf('normal(0,%0.1f)', Est_sd / 2), class = 'b', coef = conditions_names))
}
if(!length(age_names) == 0){
  m0_spline_prior <- c(m0_spline_prior, set_prior(sprintf('normal(0,%0.1f)', Est_sd / 4), class = 'b', coef = age_names))
}

m0_spline_prior
set.seed(1)
sID_sample <- sample(unique(model_data$sID), size = 150)
model_data_subset <- model_data[sID %in% sID_sample]

m0_spline_fit <- brm(formula = m0_spline_formula, 
                     prior = m0_spline_prior,
                     data = model_data_subset,
                     file = 'example_spline_model',
                     file_refit = 'never',
                     backend = 'cmdstanr', # you can leave this out
                     iter = 2000, warmup = 1000, chains = 4, cores = 4,
                     control = list(adapt_delta = .99, max_treedepth = 20))

# All 4 chains finished successfully.
# Mean chain execution time: 187.8 seconds.
# Total execution time: 194.3 seconds.

summary(m0_spline_fit)
conditional_effects(m0_spline_fit)

make_newdata_carit <- function(fit){
  require(data.table)
  age_range <- range(fit$data$age_c10)
  age_seq <- seq(from = age_range[[1]], to = age_range[[2]], length.out = 50)
  conditions <- unique(fit$data$condition)
  newdata <- as.data.table(expand.grid(age_c10 = age_seq, condition = conditions, SE = 0))
  return(newdata)
}

newdata <- make_newdata_carit(m0_spline_fit)

epred <- as.data.table(brms::posterior_epred(m0_spline_fit, newdata = newdata, re_formula = NA))
epred[, draw_id := 1:.N]
epred <- melt(epred, variable.name = 'index', id.vars = 'draw_id')
newdata[, index := sprintf('V%d', 1:.N)]
newdata_epred <- merge(epred, newdata, by = 'index', all.x = TRUE)
newdata_epred_w <- dcast(newdata_epred[, -'index'], ... ~ condition)

newdata_epred_w[, CRmHit := (1/3)*(CR_2go + CR_3go + CR_4go) - (1/4)*(Hit_1go + Hit_2go + Hit_3go + Hit_4go)]

newdata_epred_post_summary <-  
  newdata_epred_w[, as.data.table(posterior_summary(CRmHit, robust = TRUE)), by = 'age_c10']

library(ggplot2)
ggplot(newdata_epred_post_summary, aes(x = age_c10, y = Estimate)) + 
  geom_ribbon(aes(ymin = Q2.5, ymax = Q97.5), alpha = .2) + 
  geom_line() +
  geom_hline(yintercept = 0) +
  theme_minimal() +
  theme(strip.text = element_blank()) + 
  labs(x = 'Age', y = expression('Correct Rejections ' ~ - ~ 'Hits'))


# Predict Behavior -----

carit_behavior <- fread('~/code/hcpd_task_behavior/CARIT_allRaw.csv')
carit_behavior[, direction := gsub('.*CARIT_(AP|PA).*', '\\1', filename)]
unique(carit_behavior$corrRespTrialType)
carit_behavior_summary <- carit_behavior[corrRespTrialType %in% c('corReject', 'falseAlarm'), .(N_correct = sum(corrRespTrialType == 'corReject'), N_trials = .N), by = c('sessionID', 'direction')]


model_data_subset_summary <- model_data_subset[, .(CRmHit = mean(Est[grepl('CR_', condition)]) - mean(Est[grepl('Hit_', condition)])), by = c('sessionID', 'direction', 'age_c10')]
model_data_subset_behav_summary <- carit_behavior_summary[model_data_subset_summary, on = c('sessionID', 'direction')]

behav_formula = bf( N_correct | trials(N_trials) ~ 1 + s(age_c10) + CRmHit + (1 | sessionID), 
                        family = 'binomial')

behav_fit <- brm(formula = behav_formula, 
                 data = model_data_subset_behav_summary,
                 file = 'example_behav_model',
                 file_refit = 'never',
                 backend = 'cmdstanr', # you can leave this out
                 iter = 2000, warmup = 1000, chains = 4, cores = 4,
                 control = list(adapt_delta = .99, max_treedepth = 20))
summary(behav_fit)
