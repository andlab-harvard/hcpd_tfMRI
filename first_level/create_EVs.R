###
#  Usage:
#  
#    Rscript create_EVs.R <file>
#  
#  where <file> is the path to the csv file containing behavioral output.
#  
#  For more information, run:
#
#    Rscript create_EVs.R -h
##

if(!require(argparse)){
  warning('Installing required argparse package...')
  install.packages('argparse', repos = 'http://cran.wustl.edu/')
}
if(!require(data.table)){
  warning('Installing required data.table package...')
  install.packages('data.table', repos = 'http://cran.wustl.edu/')
}
if(!require(stringr)){
  warning('Installing required stringr package...')
  install.packages('stringr', repos = 'http://cran.wustl.edu/')
}

library(argparse)
library(data.table)
library(stringr)
#Change this if you want to use more for some reason. DT can optimize some
#operations over multiple threads.
setDTthreads(1)

num_fac <- function(x, levels = NULL){
  if(is.null(levels)){
    as.numeric(factor(x))  
  } else { 
    as.numeric(factor(x, levels = levels))
  }
}
lag1_num_fac_diff <- function(x, levels = NULL){
  y <- abs(num_fac(x, levels = levels) - num_fac(shift(x, fill = x[[1]], type = 'lag'), levels = levels))
  return(y)
}
save_evs <- function(x, EV_dir){
  evfn <- file.path(EV_dir, paste0(unique(x[,EVtrialType]), '.txt'))
  if(dim(na.omit(x[, c('evtime', 'dur', 'amp')]))[[1]] == 0){
    if(file.exists(evfn)){
      file.remove(evfn)
    }
    message('Creating empty ', evfn)
    file.create(evfn)
  } else {
    data.table::fwrite(x = x[, c('evtime', 'dur', 'amp')], file = evfn, sep = '\t', col.names = FALSE, na = '')
  }
  return(evfn)
}

process_carit_evs <- function(d){
  d[, evtime := round(shapeStartTime - 8, 6)]
  
  setorder(d, trialNum)
  setnames(d, 'corrAns', 'trialType')
  d[, prepotency := factor(prepotency,levels=c("2","3","4"),labels=c("2go","3go","4go"))]
  
  d[, trial_type_diff := lag1_num_fac_diff(trialType, levels = c('go', 'nogo'))]
  d[, chunkID := cumsum(trial_type_diff)]
  d[, N_of_trialType := 1:.N, by = 'chunkID']
  d[trialType == 'go', ppgo := paste0(N_of_trialType, 'go')]
  d[, EVpp := fifelse(is.na(prepotency), ppgo, as.character(prepotency))]
  d[, EVtrialType := fifelse(corrRespTrialType == 'Miss', 
                             as.character(corrRespTrialType), 
                             paste(corrRespTrialType, EVpp, sep = '_'))]
  
  d <- d[! EVtrialType %in% c('_1go', '_2go', '_3go', '_4go')]
  
  all_trialtype <- data.table(EVtrialType = c(
    'Hit_1go',
    'Hit_2go',
    'Hit_3go',
    'Hit_4go',
    'corReject_2go',
    'corReject_3go',
    'corReject_4go',
    'falseAlarm_2go',
    'falseAlarm_3go',
    'falseAlarm_4go',
    'Miss'))
  
  d_all_trialtype <- d[all_trialtype, on = c('EVtrialType')]
  d_all_trialtype$dur <- NA_real_
  d_all_trialtype$amp <- NA_real_
  d_all_trialtype[!is.na(evtime), dur := 0.6]
  d_all_trialtype[!is.na(evtime), amp := 1]
  setorder(d_all_trialtype, EVtrialType, trialNum)
  d_all_trialtype_split <- split(d_all_trialtype, by = c('EVtrialType'))
  
  if(length(d_all_trialtype_split) != 11){
    stop("Number of conditions is not 11.")
  }
  
  return(d_all_trialtype_split)
}

process_guessing_evs <- function(d){
  dur_amp <- data.table(
    variable = c('cue', 'feedback', 'guess'),
    dur = c(1.5, 2, 2),
    amp = rep(1, 3)
  )
  d_l <- melt(d, id.vars = c('trialNum', 'feedbackName', 'valueCondition'))[!is.na(value)]
  d_l[, c('valueCondition', 'variable', 'evtime') := 
        list(str_to_title(valueCondition),
             gsub('StartTime', '', variable),
             round(value - 8, 6))]
  d_l <- merge(d_l, dur_amp, on = 'variable', all = TRUE)
  d_l[, c('variable', 'feedbackName', 'valueCondition', 'value') := 
        list(fifelse(variable == 'cue', paste0(variable, valueCondition),
                     fifelse(variable == 'feedback', paste0(variable, valueCondition, feedbackName),
                             variable)),
             NULL, NULL, NULL)]
  setorder(d_l, variable, trialNum)
  setnames(d_l, 'variable', 'EVtrialType')
  
  d_all_trialtype_split <- split(d_l, by = c('EVtrialType'))
  
  if(length(d_all_trialtype_split) != 7){
    stop("Number of conditions is not 7.")
  }
  return(d_all_trialtype_split)
}

parser <- argparse::ArgumentParser()
parser$add_argument('csv_file', type = 'character', help = 'Path to the csv file containing behavior')
parser$add_argument('--evdir', type = 'character', default = 'EVs', 
                    help = 'Path to the folder to save the EV text files. Default is to store it in "EVs" under the directory where the csv file is.')
parser$add_argument('--task', type = 'character', 
                    help = 'GUESSING or CARIT. If not supplied, script will guess from the name of the input file.')
# parser$parse_args('-h')
# args <- parser$parse_args('/ncf/hcp/data/CCF_HCD_STG_PsychoPy_files/HCD0001305/tfMRI_GUESSING_AP/GUESSING_HCD0001305_V1_A_run2_wide.csv')
# args <- parser$parse_args('/ncf/hcp/data/CCF_HCD_STG_PsychoPy_files/HCD2156344/tfMRI_CARIT_AP/CARIT_HCD2156344_V1_A_run2_wide.csv')
args <- parser$parse_args()

if(args$evdir == 'EVs'){
  EV_dir <- file.path(dirname(args$csv_file), args$evdir)
} else {
  EV_dir <- args$evdir
}

if(is.null(args$task)){
  if(grepl('tfMRI_GUESSING', args$csv_file)){
    task <- 'GUESSING'
  } else if(grepl('tfMRI_CARIT', args$csv_file)){
    task <- 'CARIT'
  }
} else {
  task <- args$task
}

if(!task %in% c('GUESSING', 'CARIT')){
  stop('Either --task is not either "GUESSING" or "CARIT" or cannot guess correct task from behavior filename.')
} else if(task == 'GUESSING') { 
  col_select <- c('trialNum', 
                  'cueStartTime', 
                  'guessStartTime', 
                  'feedbackStartTime', 
                  'feedbackName', 
                  'valueCondition')
} else if(task == 'CARIT') {
  col_select <- c('trialNum', 
                  'corrAns', 
                  'shapeStartTime',
                  'prepotency',
                  'corrRespTrialType')
}

message("Reading data from ", args$csv_file)

d <- fread(args$csv_file, select = col_select)[trialNum != 0]
if(!all(col_select %in% names(d))) {
  stop('Data file does not have expected column names.')
}

if(task == 'GUESSING'){
  d_all_trialtype_split <- process_guessing_evs(d)  
}
if(task == 'CARIT'){
  d_all_trialtype_split <- process_carit_evs(d)  
}

if(!dir.exists(EV_dir)){
  dir.create(EV_dir)
}
fin <- lapply(d_all_trialtype_split, save_evs, EV_dir = EV_dir)
message('Output ', length(d_all_trialtype_split), ' EVs files:\n', paste(fin, collapse = '\n'))
