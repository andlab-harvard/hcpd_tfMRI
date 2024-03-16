library(data.table)
library(brms)
library(ggplot2)
library(ggseg)
library(ggsegGlasser)
library(argparse)
library(marginaleffects)
library(tidybayes)
library(modelr)
library(data.table)
library(rlang)
library(viridis)

# Create argument parser
parser <- ArgumentParser(description = "Create plots for contrasts using parcel-level models")

# Add arguments
parser$add_argument("--task", type = "character", help = "Task variable")
parser$add_argument("--contrast", type = "character", help = "Contrast variable")
parser$add_argument("--clust_csv", type = "character", help = "Path to csv with cluster-parcel info")

# Parse arguments
args <- parser$parse_args()

# Check if arguments are empty (interactive mode)
if (all(as.logical(lapply(args, is.null)))) {
  # Create test arguments
  args <- list(
    task = "GUESSING",
    contrast = "FEEDBACK_AVG_WIN_LOSE",
    clust_csv = "group_level_vwise/GUESSING/FEEDBACK_AVG_WIN_LOSE/swe_dpx_zTstat_c01_clust.dtseries.csv"
  )
  cat("Running in interactive mode with test arguments:\n")
  print(args)
}

# Access argument values
task <- args$task
contrast <- args$contrast
clust_csv <- args$clust_csv

clust_info <- fread(clust_csv)
clust_info <- clust_info[cluster != 0 & prop > .5]


read_model_files <- function(x){
  require(brms)
  fits <- lapply(x, readRDS)
  cfit <- brms::combine_models(mlist = fits)
  return(cfit)
}


parcel_file_names <- function(parcel, task){
  fns <- sprintf('group_level_roi/fits/%s-m0_spline-%03d-c%d.rds', task, parcel, 1:4)
  fns_exist <- fns[file.exists(fns)]
  if (length(fns_exist) < 4) {
    warning(sprintf('%d of 4 files found for task %s, parcel %d', length(fns_exist), task, parcel))
  }
  return(fns_exist)
}

eval_contrast_expr <- function(expr, dt){
  eval(parse(text = expr), envir = as.list(dt))
}

format_contrast_string <- function(contrast_value){
  contrast_value <- gsub('-', '_', contrast_value)
  contrast_dt <- rbindlist(
    list(
      data.table(contrast = 'FEEDBACK_AVG_WIN_LOSE',
                 columns = list(c('FEEDBACK_HIGH_WIN', 'FEEDBACK_LOW_WIN', 'FEEDBACK_HIGH_LOSE', 'FEEDBACK_LOW_LOSE')),
                 contrast_expression = '(FEEDBACK_HIGH_WIN + FEEDBACK_LOW_WIN)/2 - (FEEDBACK_HIGH_LOSE + FEEDBACK_LOW_LOSE)/2'),
      data.table(contrast = 'FEEDBACK_HIGH_LOW_WIN', 
                 columns = list(c('FEEDBACK_HIGH_WIN', 'FEEDBACK_LOW_WIN')),
                 contrast_expression = 'FEEDBACK_HIGH_WIN - FEEDBACK_LOW_WIN'),
      data.table(contrast = 'FEEDBACK_HIGH_LOW_LOSE', 
                 columns = list(c('FEEDBACK_HIGH_LOSE', 'FEEDBACK_LOW_LOSE')),
                 contrast_expression = 'FEEDBACK_HIGH_LOSE - FEEDBACK_LOW_LOSE'),
      data.table(contrast = 'CUE_HIGH',
                 columns = list(c('CUE_HIGH')),
                 contrast_expression = 'CUE_HIGH')
    )
  )
  return(contrast_dt[contrast == contrast_value])
}

get_parcel_predictions <- function(parcel, contrast, task, xvar = 'age_c10'){
  fns <- parcel_file_names(parcel, task)
  parcel_model <- read_model_files(fns)
  contrast_dt <- format_contrast_string(contrast)
  
  contrast_expr <- contrast_dt[,contrast_expression]
  contrast_cols <- contrast_dt[, columns][[1]]
  
  newdata <- data_grid(parcel_model$data, 
                       !! sym(xvar) := seq_range(age_c10, 100), 
                       condition_fac = contrast_cols,
                       id = id[[1]],
                       direction = direction[[1]],
                       SE = 0)
  parcel_model_pred_draws <- as.data.table(add_epred_draws(newdata = newdata, 
                                                           object = parcel_model,
                                                           re_formula = NA))
  
  parcel_model_pred_draws_w <- dcast(parcel_model_pred_draws[, -c('.row')], ... ~ condition_fac, value.var = '.epred')
  
  parcel_model_pred_draws_w[, contrast_eval := eval_contrast_expr(contrast_expr, .SD)]
  parcel_model_pred_summary <- parcel_model_pred_draws_w[
    ,
    tidybayes::point_interval(contrast_eval, .point = median, .interval = qi, width = .95),
    by = c(xvar)
  ]
  parcel_model_pred_summary[, parcel := parcel]
  return(parcel_model_pred_summary)
}

get_predictions <- function(parcels, contrast, task, xvar = 'age_c10'){
  post_summary <- rbindlist(
    lapply(parcels, 
           get_parcel_predictions, 
           contrast = contrast, 
           task = task, 
           xvar = 'age_c10')
    )
  return(post_summary)
}

clust_dt <- fread(clust_csv)
clust_dt <- clust_dt[cluster != 0 & prop > .5]

a_clust_net_dt <- clust_dt[cluster == cluster[[2]] & NETWORKKEY == NETWORKKEY[[2]]]

post_summary <- get_predictions(parcels = a_clust_net_dt[, parcel], contrast = contrast, task = task)
post_summary[, parcel_fac := factor(parcel, 
                                    levels = a_clust_net_dt[, parcel], 
                                    labels = a_clust_net_dt[, GLASSERLABELNAME])]
ggplot(post_summary, aes(x = age_c10, y = y, group = parcel_fac)) + 
  geom_ribbon(aes(ymin = ymin, ymax = ymax, fill = parcel_fac), alpha = .2) + 
  geom_line(aes(color = parcel_fac)) + 
  scale_color_viridis_d(aesthetics = c('color', 'fill'))

