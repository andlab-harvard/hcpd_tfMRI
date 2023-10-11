read_rds_file <- function(x){
  require(brms)
  fits <- lapply(x, readRDS)
  cfit <- brms::combine_models(mlist = fits)
  return(cfit)
}