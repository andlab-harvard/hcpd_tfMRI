load_hcpd_data <- function(task = 'GUESSING', nthreads = 4){
  if (!task %in% c('GUESSING', 'CARIT_PREPOT', 'CARIT_PREVCOND')){
    stop("task is not 'GUESSING', 'CARIT_PREPOT', or 'CARIT_PREVCOND'")
  }
  require(data.table)
  
  setDTthreads(nthreads)
  
  if(grepl('group_level_roi', getwd())){
    fname_dir <-  '.'
  } else {
    fname_dir <-  'group_level_roi'
  }
  fname <- file.path(fname_dir, sprintf('roi_data_%s_w.rds', task))
  
  message(sprintf('Retrieving %s data...', task))
  if(!file.exists(fname)){
    message('RDS data not found, loading from feather (and saving result as RDS for future speedup)')
    require(stringi)
    require(arrow)
    if(grepl('group_level_roi', getwd())){
      arrow_fname_dir <-  '..'
    } else {
      arrow_fname_dir <-  '.'
    }
    
    roi_data <- as.data.table(
      read_feather(
        file.path(arrow_fname_dir, sprintf('parcellated-data_%s.feather', task))))
    roi_data[, roi := 1:.N, by = c('id', 'session', 'scan', 'direction', 'file')]
    
    message(sprintf('Object size is %s', format(object.size(roi_data), units = 'MB')))
    
    roi_data_scans <- roi_data[scan %in% task]
    rm(roi_data)
    roi_data_scans_w <- dcast(roi_data_scans, ... ~ file, value.var = 'value')
    rm(roi_data_scans)
    
    roi_data_scans_w[, scan := NULL]
    
    message(sprintf('Final object size is %s', format(object.size(roi_data_scans_w), units = 'MB')))
    
    saveRDS(roi_data_scans_w, file = fname, compress = TRUE)
  } else {
    message('Loading data from RDS file')
    roi_data_scans_w <- readRDS(fname)
  }
  return(roi_data_scans_w)
}
