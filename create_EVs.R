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

library(argparse)
library(data.table)
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
  x$dur <- NA_real_
  x$amp <- NA_real_
  x[!is.na(evtime), dur := 0.6]
  x[!is.na(evtime), amp := 1]
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

parser <- argparse::ArgumentParser()
parser$add_argument('csv_file', type = 'character', help = 'Path to the csv file containing behavior')
parser$add_argument('--evdir', type = 'character', default = 'EVs', 
                    help = 'Path to the folder to save the EV text files. Default is to store it in "EVs" under the directory where the csv file is.')
# parser$parse_args('-h')
# args <- parser$parse_args('/ncf/hcp/data/CCF_HCD_STG_PsychoPy_files/HCD2156344/tfMRI_CARIT_AP/CARIT_HCD2156344_V1_A_run2_wide.csv')
args <- parser$parse_args()


if(args$evdir == 'EVs'){
  EV_dir <- file.path(dirname(args$csv_file), args$evdir)
} else {
  EV_dir <- args$evdir
}

col_select <- c('trialNum', 
                'corrAns', 
                'shapeStartTime',
                'prepotency',
                'corrRespTrialType')

message("Reading data from ", args$csv_file)

d <- fread(args$csv_file, select = col_select)[trialNum != 0]
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
setorder(d_all_trialtype, EVtrialType, trialNum)
d_all_trialtype_split <- split(d_all_trialtype, by = c('EVtrialType'))

if(length(d_all_trialtype_split) != 11){
  stop("Number of conditions is not 11.")
}

if(!dir.exists(EV_dir)){
  dir.create(EV_dir)
}
fin <- lapply(d_all_trialtype_split, save_evs, EV_dir = EV_dir)
message('Output ', length(d_all_trialtype_split), ' EVs files:\n', paste(fin, collapse = '\n'))
