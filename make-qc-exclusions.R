library(data.table)
library(ggplot2)
library(patchwork)

auto_qc_cols <- c('PctBrainCoverage' = '< 98',
                  'tSNR' = '< 15',
                  'REL_RMS_0_5' = '> 30',
                  'DVAR_SD' = '> 50')
manual_qc_cols <- c('WishartProb' = '== 1',
                    'DCRating' = '>= 2',
                    'PctCerebMiss' = '>= 50',
                    'SBRefCorrupt' = '== "Y"',
                    'ExcludeOther' = '== 1')
                    
# ,
#                   'DVAR_SD' = '> 50') 
id_cols <- c('MR_ID', 'DB_SeriesDesc')

qc_2021 <- fread('~/code/hcpd_tfMRI/qc/fMRI_QC_Meta_2021.03.16.xlsx - fmriqcmeta.csv')
qc_2021 <- qc_2021[Project == 'HCD']
sort(names(qc_2021))

qc_2023 <- fread('~/code/hcpd_tfMRI/qc/HCD_fMRI_QC_2023.11.07.csv')
qc_2023[, DVAR_SD := sqrt(DVAR_2ND_MOMENT)]
finalmask_2023 <- fread('~/code/hcpd_tfMRI/qc/HCP-D_fMRI_finalmask.stats_2023.11.06.csv')

setnames(qc_2023, c('MR ID', 'DB SeriesDesc', 'MED_tSNR', 'REL_RMS_0.5'), c('MR_ID', 'DB_SeriesDesc', 'tSNR', 'REL_RMS_0_5'))
qc_2021[, MR_ID := gsub('(HCD\\d+_V[123])_.*$', '\\1', MR_ID)]
qc_2023[, MR_ID := gsub('(HCD\\d+_V[123])_.*$', '\\1', MR_ID)]
finalmask_2023[, MR_ID := gsub('(HCD\\d+_V[123])_.*$', '\\1', MR_ID)]

qc_2023 <- merge(qc_2023[DB_SeriesDesc != ''], finalmask_2023, by = id_cols, all = TRUE)
cols_i_want <- c(id_cols, names(auto_qc_cols))

qc_2023_subset <- qc_2023[, ..cols_i_want]
qc_2021_subset <- qc_2021[, ..cols_i_want]
updated <- qc_2023_subset[!qc_2021_subset]

qc_combined <- rbindlist(list(`2021` = qc_2021_subset, `2023` = updated), idcol = 'version')
qc_combined_l <- melt(qc_combined, id.vars = c(id_cols, 'version'))
qc_combined_l_plot <- copy(qc_combined_l)
qc_combined_l_plot[, value := fifelse(variable == 'tSNR', value,
                                fifelse(variable == 'REL_RMS_0_5', log(.25 + value),
                                        fifelse(variable == 'PctBrainCoverage', exp(qlogis((value - .1) / 100)),
                                                fifelse(variable == 'DVAR_SD', log(value),
                                                        NA_real_))))]

scale_definitions <- list(
  PctBrainCoverage = list(labels = c(60, 98, 99, 100), breaks = exp(qlogis((c(60, 98, 99, 100) - .1) / 100)), cut = exp(qlogis((98 - .1) / 100))),
  tSNR = list(labels = c(10, 15, 20, 30), breaks = c(10, 15, 20, 30), cut = 15),
  REL_RMS_0_5 = list(labels = c(0, 30, 60), breaks = log(.25 + c(0, 30, 60)), cut = log(.25 + 30)),
  DVAR_SD = list(labels = c(5, 20, 60), breaks = log(c(5, 20, 60)), cut = log(60))
)

patchwork::wrap_plots(lapply(unique(qc_combined_l[, variable]), \(varname){
  ggplot(qc_combined_l_plot[variable == varname], aes(x = value)) + 
    geom_density(aes(group = version, fill = version, color = version), alpha = .5, position = position_identity()) + 
    geom_vline(xintercept = scale_definitions[[varname]]$cut) + 
    labs(x = varname) + 
    scale_x_continuous(labels = scale_definitions[[varname]]$labels,
                       breaks = scale_definitions[[varname]]$breaks) + 
    theme_minimal()
})) + plot_layout(ncol = 1)


is_excluded <- Vectorize(vectorize.args = c('variable', 'value'), FUN = function(variable, value, criteria){
  comparison_string <- paste(value, criteria[[variable]], sep = ' ')
  return(eval(str2expression(comparison_string)))
})
qc_combined_l[, exclude := is_excluded(variable, value, ..auto_qc_cols)]
qc_combined_exclude_summary <- qc_combined_l[, .(exclude = sum(exclude) > 0), by = c(id_cols, 'version')]
qc_combined_exclude_summary_overview <- qc_combined_exclude_summary[, .(N = .N), by = c('version', 'exclude')]
overview <- dcast(qc_combined_exclude_summary_overview, ... ~ exclude)
overview[, prop := round(`TRUE` / (`FALSE` + `TRUE` + `NA`), 2)]
qc_combined_exclude_summary_for_overview_by_scan <- copy(qc_combined_exclude_summary)
qc_combined_exclude_summary_for_overview_by_scan[, DB_SeriesDesc := gsub('(.*?)(?:[123])*(?:[ab])*(_[PA][AP])', '\\1\\2', DB_SeriesDesc)]
qc_combined_exclude_summary_overview_by_scan <- qc_combined_exclude_summary_for_overview_by_scan[, .(N = .N), by = c('version', 'exclude', 'DB_SeriesDesc')]
overview_by_scan <- dcast(qc_combined_exclude_summary_overview_by_scan, ... ~ exclude)
overview_by_scan[, prop := round(`TRUE` / (`FALSE` + `TRUE` + `NA`), 2)]

overview
overview_by_scan[grepl('tfMRI', DB_SeriesDesc)]

manual_col_names_i_want <- c(id_cols, names(manual_qc_cols))
qc_2021_manual_qc <- qc_2021[, ..manual_col_names_i_want]
qc_2021_manual_qc_l <- melt(qc_2021_manual_qc, id.vars = id_cols)
qc_2021_manual_qc_l[, value := fifelse(is.na(value), '""', paste0('"', value, '"'))]
qc_2021_manual_qc_l[, exclude := is_excluded(variable, value, ..manual_qc_cols)]
qc_2021_manual_qc_l <- qc_2021_manual_qc_l[, .(manual_exclude = sum(exclude) > 0), by = id_cols]
qc_2021_manual_qc_l[, version := '2021']
qc_manual_auto_l <- merge(qc_combined_exclude_summary, qc_2021_manual_qc_l, all = TRUE, by = c(id_cols, 'version'))
qc_manual_auto_l[, manual_exclude := fifelse(is.na(manual_exclude), FALSE, manual_exclude)]
qc_manual_auto_l[, excluded_by := fifelse(exclude & manual_exclude, 'auto or both',
                                             fifelse(!exclude & !manual_exclude, 'neither',
                                                     fifelse(exclude | manual_exclude, fifelse(manual_exclude, 'only manual', 'auto or both'), 
                                                             NA_character_)))]

qc_manual_auto_l_summary <- qc_manual_auto_l[, .(N=.N), by = c('version', 'excluded_by')]
qc_manual_auto_l_summary[, prop := round(N / sum(N), 3), by = c('version')]
qc_manual_auto_l_summary_overview_N <- dcast(qc_manual_auto_l_summary[, -'prop'], ...~excluded_by, value.var = 'N')
qc_manual_auto_l_summary_overview_prop <- dcast(qc_manual_auto_l_summary[, -'N'], ...~excluded_by, value.var = 'prop')


qc_manual_auto_l_overview <- qc_manual_auto_l[, .(total = .N, excluded = sum(exclude | manual_exclude, na.rm = TRUE)), by = c('version')]
qc_manual_auto_l_overview[, prop := round(excluded / total, 3)]

qc_manual_auto_l_summary_overview_N[, c('version', 'neither', 'auto or both', 'only manual')]
qc_manual_auto_l_summary_overview_prop[, c('version', 'neither', 'auto or both', 'only manual')]
qc_manual_auto_l_overview


setnames(qc_manual_auto_l, 'exclude', 'auto_exclude')

fwrite(qc_manual_auto_l[, -c('version', 'excluded_by')], '~/code/hcpd_tfMRI/qc/HCPD-exclusions.csv')
