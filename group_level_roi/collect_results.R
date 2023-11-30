.libPaths(new = c('/ncf/mclaughlin/users/jflournoy/R/x86_64-pc-linux-gnu-library/verse-4.3.1', .libPaths()))
options(future.globals.onReference = NULL)
Sys.setenv(R_PROGRESSR_ENABLE=TRUE)
library(data.table)
library(brms)
library(future.apply)
library(progressr)
handlers(global = TRUE)
handlers(handler_progress(format = "[:bar] :message"))
CPUS_DEFAULT <- 4
cpus_total <- as.numeric(Sys.getenv('SLURM_CPUS_PER_TASK'))
if(is.na(cpus_total)){
  cpus_total <- CPUS_DEFAULT
  message(sprintf('Using %d CPUs...', cpus_total))
}

parse_filenames <- function(x, regex, colnames){
  require(stringi)
  require(data.table)
  
  parsed_dt <- as.data.table(transpose(stri_match_all_regex(x, regex)))
  setnames(parsed_dt, c('file', colnames))
  return(parsed_dt)
}
read_rds_file <- function(x){
  require(brms)
  fits <- lapply(x, readRDS)
  cfit <- brms::combine_models(mlist = fits)
  return(cfit)
}
make_newdata_carit <- function(fit){
  require(data.table)
  age_range <- range(fit$data$age_c10)
  age_seq <- seq(from = age_range[[1]], to = age_range[[2]], length.out = 50)
  conditions <- unique(fit$data$condition_fac)
  newdata <- as.data.table(expand.grid(age_c10 = age_seq, condition_fac = conditions, SE = 0))
  return(newdata)
}
make_newdata_guessing <- function(fit){
  #fit <- read_rds_file(fit_files[1:4, file])
  age_range <- range(fit$data$age_c10)
  age_seq <- seq(from = age_range[[1]], to = age_range[[2]], length.out = 50)
  conditions <- unique(fit$data$condition_fac)
  newdata <- as.data.table(expand.grid(age_c10 = age_seq, condition_fac = conditions, SE = 0))
  return(newdata)
}

make_get_r2 <- function(){
  get_r2 <- function(x){
    require(brms)
    r2 <- as.data.table(brms::bayes_R2(x))
    return(r2)
  }
  return(get_r2)
}

prediction_posteriors <- function(newdata_function, contrasts, dcast_col = 'condition_fac', posterior_by_col = 'age_c10'){
  newdata_function <- newdata_function
  ret_func <- function(x){
    colnames_from_contrast <- function(text) {
      require(stringi)
      pattern <- "\\b(?=[^\\d])\\w+(_\\w+)*?\\b"
      matches <- unlist(stri_extract_all_regex(text, pattern))
      return(matches)
    }
    #x <- fit
    # newdata_function <- make_newdata_carit
    # contrasts <- list(
    #   'CR' = '(1/3)*(CR_2go + CR_3go + CR_4go)',
    #   'Hit' = '(1/4)*(Hit_1go + Hit_2go + Hit_3go + Hit_4go)',
    #   'CRmHit' = '(1/3)*(CR_2go + CR_3go + CR_4go) - (1/4)*(Hit_1go + Hit_2go + Hit_3go + Hit_4go)',
    #   'GoxPrepot' = '(1/3)*(Hit_1go + Hit_2go + Hit_3go) - Hit_4go',
    #   'CRxPrepot' = '(1/2)*(CR_2go + CR_3go) - CR_4go')
    
    require(brms)
    require(data.table)
    
    message('Making new data..')
    newdata_function <- newdata_function
    newdata <- newdata_function(x)
    message('Getting predictions...')
    epred <- as.data.table(brms::posterior_epred(x, newdata = newdata, re_formula = NA))
    epred[, draw_id := 1:.N]
    epred <- melt(epred, variable.name = 'index', id.vars = 'draw_id')
    newdata[, index := sprintf('V%d', 1:.N)]
    message('Merging data and predictions...')
    newdata_epred <- merge(epred, newdata, by = 'index', all.x = TRUE)
    dcast_formula <- formula(sprintf('... ~ %s', dcast_col))
    newdata_epred_w <- dcast(newdata_epred[, -'index'], dcast_formula)
    newdata_epred_w_cols <- names(newdata_epred_w)
    newdata_epred_w[, names(contrasts) := lapply(contrasts, \(x){ 
      message("Applying contrast: ", x)
      con_cols <- colnames_from_contrast(x)
      for (col in con_cols) {
        message(col)
      }
      if (!all(con_cols %in% newdata_epred_w_cols)){
        err_cols <- con_cols[!con_cols %in% newdata_epred_w_cols]
        message("ERROR: Columns not in data. Error columns: ")
        for (err_col in err_cols){
          message(err_col)
        }
        stop("error")
      }
      eval(parse(text = x)) 
    })]
    newdata_epred_post_summary <-  
      newdata_epred_w[, as.data.table(posterior_summary(.SD, robust = TRUE), keep.rownames = 'param'), 
                      .SDcols = names(contrasts), by = c(posterior_by_col)]
    sample_size <- as.data.table(ngrps(x))
    newdata_epred_post_summary <- cbind(newdata_epred_post_summary, sample_size)
    return(newdata_epred_post_summary)
  }
  return(ret_func)
}

run_process_data_function <- function(x, process_data_function, p){
  #x <- some_fns[[1]]
  #x <- fit_files[1:4, ]
  message('Reading fits from files:')
  for (file in x$file){
    message(file)
  }
  fit <- read_rds_file(x$file)
  message('Processing data')
  out_data_list <- lapply(process_data_function, \(f){
    tryCatch({
      out_data <- f(fit)
      out_data <- cbind(x[1], out_data)},
      error = {out_data <- x[1]})
    return(out_data)
  })
  p(message = sprintf('%s', x$file[[1]]))
  return(out_data_list)
}

parallel_process_data_file <- function(x, process_data_function, split_cols, cpus_total, p){
  #x <- x[1:8, ]
  require(future.apply)
  message('Setting up parallel processes..., cpus_total = ', cpus_total)
  plan('multisession', workers = cpus_total)
  
  # cl <- parallel::makeCluster(cpus_total)
  # 
  # parallel::clusterExport(cl, ls(.GlobalEnv), envir = .GlobalEnv)
  # parallel::clusterExport(cl, ls(), envir = environment())
  # nada <- parallel::clusterEvalQ(cl, {
  #   library(data.table)
  #   library(brms)
  #   setDTthreads(1)
  #   .libPaths(new = c('/ncf/mclaughlin/users/jflournoy/R/x86_64-pc-linux-gnu-library/verse-4.2.1', .libPaths()))
  # })
  system.time({
    message('Submitting to parallel processes...')
    filename_list <- split(
      split(x, by = split_cols, drop = TRUE), 
      1:cpus_total)
    data_out_list <<- future.apply::future_lapply(filename_list, function(some_fns){
      #some_fns = filename_list[[1]]
      lapply(some_fns, run_process_data_function, process_data_function = process_data_function, p = p)
    }, future.seed=TRUE)
  })
  
  # parallel::stopCluster(cl)
  return(data_out_list)
}

if(grepl('group_level_roi', getwd())){
  basepath <- '.'
} else {
  basepath <- 'group_level_roi'
}
fit_dir <- file.path(basepath, 'fits')

collect_data_list <- list(
  carit_spline = list(
    data_fn = file.path(basepath, 'carit-prevcond_spline_contrasts.rds'),
    pattern = '^m0_spline-\\d{3}-c[1234].*\\.rds',
    regex = file.path(fit_dir, 'm0_(spline)-(\\d{3})\\-c[1234].rds'),
    colnames = c('model', 'roi'),
    process_data_function = 
      list(prediction_posteriors(newdata_function = make_newdata_carit,
                                 contrasts = list(
                                   CR = '(1/3)*(CR_2go + CR_3go + CR_4go)',
                                   Hit = '(1/4)*(Hit_1go + Hit_2go + Hit_3go + Hit_4go)',
                                   CRmHit = '(1/3)*(CR_2go + CR_3go + CR_4go) - (1/4)*(Hit_1go + Hit_2go + Hit_3go + Hit_4go)',
                                   GoxPrepot = '(1/3)*(Hit_1go + Hit_2go + Hit_3go) - Hit_4go',
                                   CRxPrepot = '(1/2)*(CR_2go + CR_3go) - CR_4go')),
           r2 = make_get_r2())
  ),
  guessing_spline = list(
    data_fn = file.path(basepath, 'guessing_spline_contrasts.rds'),
    pattern = '^GUESSING-m0_spline-\\d{3}-c[1234].*\\.rds',
    regex = file.path(fit_dir, 'GUESSING-m0_(spline)-(\\d{3})\\-c[1234].rds'),
    colnames = c('model', 'roi'),
    process_data_function = 
      list(age_pred = prediction_posteriors(newdata_function = make_newdata_guessing,
                                            contrasts = list(
                                              CueHighmCueLow = 'CUE_HIGH - CUE_LOW',
                                              FBWinmFBLoss = '(1/2)*(FEEDBACK_HIGH_WIN + FEEDBACK_LOW_WIN) - (1/2)*(FEEDBACK_HIGH_LOSE + FEEDBACK_LOW_LOSE)'
                                            )), 
           r2 = make_get_r2())
  )
)

# collect_data_list=collect_data_list[2]
# x <- collect_data_list[[2]]
rez_list <- lapply(collect_data_list, \(x){
  if(!file.exists(x$data_fn)){
    
    message(sprintf('Collecting filenames from %s', file.path(fit_dir, x$pattern)))
    fit_files <- parse_filenames(dir(fit_dir, pattern = x$pattern, full.names = TRUE),
                                 regex = x$regex,
                                 colnames = x$colnames)
    # fit_files <- fit_files[1:32,]
    split_cols <- x$colnames
    with_progress({ p <- progressr::progressor(along = fit_files$file)
    rez_data <- parallel_process_data_file(x = fit_files, 
                                           process_data_function = x$process_data_function, 
                                           split_cols = split_cols, 
                                           cpus_total = cpus_total,
                                           p = p)
    })
    proc_func_names <- names(x$process_data_function)
    names(proc_func_names) <- proc_func_names
    if(length(proc_func_names) > 1){
      proc_rez_data <- lapply(proc_func_names, \(pname){
        outer_list <- lapply(rez_data, \(outer_item){
          inner_list <- lapply(outer_item, \(inner_item){
            inner_item[[pname]]
          })
        })
        return(unlist(outer_list, recursive = FALSE))
      })
    }
    message(sprintf('Done. Writing data to %s', x$data_fn))
    saveRDS(proc_rez_data, file = x$data_fn)
  } else {
    message(sprintf('Reading data from %s', x$data_fn))
    proc_rez_data <- readRDS(file = x$data_fn)
  }
  return(proc_rez_data)
})


