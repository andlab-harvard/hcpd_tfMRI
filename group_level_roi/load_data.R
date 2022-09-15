load_hcpd_data <- function(task = 'carit', nthreads = 1){
  require(data.table)
  
  setDTthreads(nthreads)
  
  fname <- sprintf('group_level_roi/roi_data_%s_w.rds', task)
  
  message(sprintf('Retrieving %s data...', task))
  if(!file.exists(fname)){
    message('RDS data not found, loading from csv (and saving result as RDS for future speedup)')
    require(stringi)
    roi_data <- fread(file = 'group_level_roi/roi_data.csv',
                      sep = ',',
                      quote = '',
                      header = TRUE,
                      stringsAsFactors = FALSE,
                      strip.white = FALSE,
                      fill = FALSE,
                      skip = 0,
                      check.names = FALSE,
                      col.names = c('id', 'scan', 'file', 'value'),
                      select = list(character = c('id', 'scan', 'file'), numeric = 'value'),
                      verbose = TRUE,
                      na.strings = "NA",
                      key = c('id', 'scan'),
                      showProgress = TRUE, 
                      data.table = TRUE)
    
    roi_data[, roi := 1:.N, by = c('id', 'scan', 'file')]
    
    roi_data[, direction := stringi::stri_sub(scan, from = -2, length = 2)]
    
    scan_names <- sprintf('tfMRI_%s_%s', toupper(task), c('AP', 'PA'))
    
    roi_data_scans <- roi_data[scan %in% scan_names]
    rm(roi_data)
    roi_data_scans_w <- dcast(roi_data_scans, ... ~ file, value.var = 'value')
    rm(roi_data_scans)
    
    roi_data_scans_w[, scan := NULL]
    
    saveRDS(roi_data_scans_w, file = fname, compress = TRUE)
  } else {
    message('Loading data from RDS file...')
    roi_data_scans_w <- readRDS(fname)
  }
  return(roi_data_scans_w)
}

carit_scans <- load_hcpd_data(task = 'carit', nthreads = 4)
guessing_scans <- load_hcpd_data(task = 'guessing', nthreads = 4)
