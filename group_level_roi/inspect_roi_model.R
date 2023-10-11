library(brms)
library(bayesplot)
library(patchwork)
library(priorsense)

read_rds_file <- function(x){
  require(brms)
  fits <- lapply(x, readRDS)
  cfit <- brms::combine_models(mlist = fits)
  return(cfit)
}

fit <- read_rds_file(dir('group_level_roi/fits', pattern = 'm0_spline-001-c.*rds', full.names = TRUE))
summary(fit)

pp_check(fit, ndraws = 15)
do.call(patchwork::wrap_plots, plot(conditional_effects(fit), ask = FALSE))

np <- nuts_params(fit)

length(variables(fit))
variables(fit)[c(1:11,3221:(3249 - 13))]

#ps_pss <- priorsense::powerscale_sensitivity(fit, variable = variables(fit)[c(1)])

mcmc_parcoord(fit, regex_pars = c('^b_.*', '^sd_.*', '^sigma', '^bs_.*', '^sds_.*'), 
              np = np, transformations = 'scale') + 
  ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 65, hjust = 1))

mcmc_parcoord(fit, regex_pars = c('^s_t2age_c10.*', 'nu'), 
              np = np, transformations = list(nu = \(x) scale(x)*100)) + 
  ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 360-45, hjust = 0))

mcmc_parcoord(fit, regex_pars = c('^bs_t2age_c10.*', 'nu'), 
              np = np, transformations = list(nu = \(x) scale(x)*100)) + 
  ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 360-45, hjust = 0))

mcmc_parcoord(fit, regex_pars = c('s_t2age_c10resp_type_ofacCR_1.*', 'nu'), 
              np = np, transformations = list(nu = function(x) scale(x)*100)) + 
  ggplot2::theme(axis.text.x = ggplot2::element_text(angle = 360-45, hjust = 0))

mcmc_pairs(fit, regex_pars = c('^s_t2age_c10.*'), np = np)

mcmc_pairs(fit, regex_pars = c('^b_.*', '^sd_.*', '^sigma', '^bs_.*', '^sds_.*'), np = np)
