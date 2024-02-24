library(data.table)
library(ggplot2)
library(ggsankey)
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
                    
demos <- fread('~/code/hcpd_tfMRI/hcpd_task_behavior/HCD_Inventory_2022-04-12.csv')[grepl('V[123]', redcap_event), c('subject', 'event_age', 'redcap_event')]
demos[, MR_ID := sprintf('%s_%s', subject, redcap_event)]

id_cols <- c('MR_ID', 'DB_SeriesDesc')

qc_2021 <- fread('~/code/hcpd_tfMRI/qc/fMRI_QC_Meta_2021.03.16.xlsx - fmriqcmeta.csv')
qc_2021 <- qc_2021[Project == 'HCD' & grepl('GUESS|CARIT', DB_SeriesDesc)]
sort(names(qc_2021))

qc_2023 <- fread('~/code/hcpd_tfMRI/qc/HCD_fMRI_QC_2023.11.07.csv')
qc_2023[, DVAR_SD := sqrt(DVAR_2ND_MOMENT)]
finalmask_2023 <- fread('~/code/hcpd_tfMRI/qc/HCP-D_fMRI_finalmask.stats_2023.11.06.csv')

setnames(qc_2023, c('MR ID', 'DB SeriesDesc', 'MED_tSNR', 'REL_RMS_0.5'), c('MR_ID', 'DB_SeriesDesc', 'tSNR', 'REL_RMS_0_5'))
qc_2021[, MR_ID := gsub('(HCD\\d+_V[123])_.*$', '\\1', MR_ID)]
qc_2023[, MR_ID := gsub('(HCD\\d+_V[123])_.*$', '\\1', MR_ID)]
finalmask_2023[, MR_ID := gsub('(HCD\\d+_V[123])_.*$', '\\1', MR_ID)]

qc_2023 <- merge(qc_2023[DB_SeriesDesc != ''], finalmask_2023, by = id_cols, all = TRUE)
qc_2023 <- qc_2023[grepl('GUESS|CARIT', DB_SeriesDesc)]
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
qc_combined_exclude_summary_for_overview_by_scan_simple <- copy(qc_combined_exclude_summary)
qc_combined_exclude_summary_for_overview_by_scan_simple[, scan := gsub('tfMRI_(.*?)(?:[123])*(?:[ab])*(_[PA][AP])', '\\1', DB_SeriesDesc)]
qc_combined_exclude_summary_overview_by_scan_simple <- qc_combined_exclude_summary_for_overview_by_scan_simple[, .(N = .N), by = c('version', 'exclude', 'scan')]
overview_by_scan_simple <- dcast(qc_combined_exclude_summary_overview_by_scan_simple, ... ~ exclude)
overview_by_scan_simple[, prop := round(`TRUE` / (`FALSE` + `TRUE` + `NA`), 2)]
setnames(overview_by_scan_simple, c('FALSE', 'TRUE', 'prop'), c('Included', 'Excluded', 'Prop. Exlc.'))

overview
overview_by_scan
overview_by_scan_simple[, -'NA']

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

qc_combined_demos_l <- qc_combined_l[demos, on = 'MR_ID']
qc_combined_demos_l[, age_bin := floor(event_age)]
qc_combined_demos_l[, N_age_bin := .N, by = 'age_bin']
qc_combined_demos_l[, scan := gsub('.*(CARIT|GUESSING).*', '\\1', DB_SeriesDesc)]
setorder(qc_combined_demos_l, 'MR_ID', 'DB_SeriesDesc', 'variable')
exclude_reason_summary <- qc_combined_demos_l[!is.na(exclude) & exclude == TRUE]
exclude_reason_summary[, N_exclusions := factor(.N, levels = 4:1), by = c('MR_ID', 'DB_SeriesDesc')]

exclude_reason_short_summary <- qc_combined_demos_l[
  !is.na(scan), 
  .(excluded = any(exclude),
      excluded_by_N = factor(sum(exclude, na.rm = TRUE), levels = 4:0)), 
  by = c('MR_ID', 'DB_SeriesDesc', 'age_bin', 'scan')]
exclude_reason_short_summary[, N := .N, by = c('scan', 'age_bin')]
exclude_reason_short_summary <- exclude_reason_short_summary[
    , .(N = unique(N), N_exclusions = sum(excluded, na.rm = TRUE)), 
    by = c('scan', 'age_bin', 'excluded_by_N')]
exclude_reason_short_summary[, prop_excluded := N_exclusions / N]

N_total_auto_exclusions <- unique(exclude_reason_summary[, c('DB_SeriesDesc', 'MR_ID', 'scan')])[, .N, by = 'scan']

#1
ggplot(exclude_reason_summary, aes(x = variable)) + 
  geom_hline(data = N_total_auto_exclusions, aes(yintercept = N)) + 
  geom_bar(aes(group = N_exclusions, fill = N_exclusions)) + 
  theme_minimal() + 
  scale_fill_viridis_d(begin = 1, end = 0, name = 'Number of failed criteria') +
  facet_wrap(~scan) + 
  labs(x = 'Excluded on') + 
  #2
  ggplot(exclude_reason_short_summary[!excluded_by_N %in% 0], aes(x = age_bin, y = N_exclusions)) + 
  geom_col(aes(group = excluded_by_N, fill = excluded_by_N)) + 
  theme_minimal() + 
  scale_fill_viridis_d(begin = 1, end = 0, name = 'Number of failed criteria') +
  facet_wrap(~scan) + 
  labs(x = 'Age', y = 'Number of scans excluded') + 
  #3
  ggplot(exclude_reason_short_summary[!excluded_by_N %in% 0], aes(x = age_bin, y = prop_excluded)) + 
  geom_col(aes(group = excluded_by_N, fill = excluded_by_N)) + 
  theme_minimal() + 
  scale_fill_viridis_d(begin = 1, end = 0, name = 'Number of failed criteria') +
  coord_cartesian(y = c(0, .5)) + 
  facet_wrap(~scan) + 
  labs(x = 'Age', y = 'proportion of scans excluded') + 
  plot_layout(guides = 'collect', design = c('AA\nBC'))

setnames(qc_manual_auto_l, 'exclude', 'auto_exclude')

fwrite(qc_manual_auto_l[, -c('version', 'excluded_by')], '~/code/hcpd_tfMRI/qc/HCPD-exclusions.csv')

exclude_reason_summary[
  , c('MR_ID', 'DB_SeriesDesc', 'variable', 'exclude', 'scan')]

wide_auto_exclude <- dcast(exclude_reason_summary[
  , c('MR_ID', 'DB_SeriesDesc', 'variable', 'exclude', 'scan')], ... ~ variable, value.var = 'exclude', fill = FALSE)
wide_auto_exclude[, any := TRUE]

wide_auto_exclude[, c('first', 'second', 'third', 'fourth') := 
                    .(fifelse(DVAR_SD, 'DVAR_SD',
                              fifelse(tSNR, 'tSNR',
                                      fifelse(PctBrainCoverage, 'PctBrainCoverage',
                                              fifelse(REL_RMS_0_5, 'REL_RMS_0_5', NA_character_)))),
                      fifelse(DVAR_SD & tSNR, 'tSNR',
                              fifelse(tSNR & PctBrainCoverage, 'PctBrainCoverage',
                                      fifelse(PctBrainCoverage & REL_RMS_0_5, 'REL_RMS_0_5', 'None'))),
                      fifelse(DVAR_SD & tSNR & PctBrainCoverage, 'PctBrainCoverage',
                              fifelse(tSNR & PctBrainCoverage & REL_RMS_0_5, 'REL_RMS_0_5', 'None')),
                      fifelse(DVAR_SD & tSNR & PctBrainCoverage & REL_RMS_0_5, 'REL_RMS_0_5', 'None'))]


# sankey_long_carit <- make_long(wide_auto_exclude[scan == 'CARIT'], REL_RMS_0_5, PctBrainCoverage, tSNR, DVAR_SD, any)
# sankey_long_guessing <- make_long(wide_auto_exclude[scan == 'GUESSING'], REL_RMS_0_5, PctBrainCoverage, tSNR, DVAR_SD, any)
sankey_long_carit_alt <- as.data.table(make_long(wide_auto_exclude[scan == 'CARIT'], first, second, third, fourth))
sankey_long_guessing_alt <- as.data.table(make_long(wide_auto_exclude[scan == 'GUESSING'], first, second, third, fourth))

sankey_long_carit_alt[, node := factor(node, levels = c('DVAR_SD',
                                                        'tSNR',
                                                        'PctBrainCoverage',
                                                        'REL_RMS_0_5',
                                                        'None'))]
sankey_long_guessing_alt[, node := factor(node, levels = c('DVAR_SD',
                                                        'tSNR',
                                                        'PctBrainCoverage',
                                                        'REL_RMS_0_5',
                                                        'None'))]

plot_sankey <- function(df, title, breaks = c(TRUE, FALSE), labels = c('Excluded', 'Not excluded')){
  ggplot(df, aes(x = x, 
                 next_x = next_x, 
                 node = node, 
                 next_node = next_node,
                 fill = factor(node),
                 label = node)) +
    geom_sankey(flow.alpha = .8) +
    # geom_sankey_label() + 
    theme_sankey() + 
    labs(x = 'Rank of exclusion criterion', y = 'Number of scans', fill = 'Exclusion criterion', title = title) + 
    scale_fill_viridis_d(breaks = breaks, labels = labels, option = 'inferno', begin = .25, end = 1) + 
    coord_cartesian(y = c(-100, 100))
}


#1
(ggplot(exclude_reason_summary, aes(x = variable)) + 
  geom_hline(data = N_total_auto_exclusions, aes(yintercept = N)) + 
  geom_bar(aes(group = N_exclusions, fill = N_exclusions)) + 
  theme_minimal() + 
  scale_fill_viridis_d(begin = 1, end = 0, name = 'Number of failed criteria') +
  facet_wrap(~scan) + 
  labs(x = 'Excluded on') + 
  #2
  ggplot(exclude_reason_short_summary[!excluded_by_N %in% 0], aes(x = age_bin, y = N_exclusions)) + 
  geom_col(aes(group = excluded_by_N, fill = excluded_by_N)) + 
  theme_minimal() + 
  scale_fill_viridis_d(begin = 1, end = 0, name = 'Number of failed criteria') +
  facet_wrap(~scan) + 
  labs(x = 'Age', y = 'Number of scans excluded') + 
  #3
  ggplot(exclude_reason_short_summary[!excluded_by_N %in% 0], aes(x = age_bin, y = prop_excluded)) + 
  geom_col(aes(group = excluded_by_N, fill = excluded_by_N)) + 
  theme_minimal() + 
  scale_fill_viridis_d(begin = 1, end = 0, name = 'Number of failed criteria') +
  coord_cartesian(y = c(0, .5)) + 
  facet_wrap(~scan) + 
  labs(x = 'Age', y = 'proportion of scans excluded') + 
  plot_layout(guides = 'collect')) +
  (plot_sankey(sankey_long_carit_alt, 'CARIT', 
              breaks = c('DVAR_SD',
                         'tSNR',
                         'PctBrainCoverage',
                         'REL_RMS_0_5',
                         'None'),
              labels = c('DVAR_SD',
                         'tSNR',
                         'PctBrainCoverage',
                         'REL_RMS_0_5',
                         'None')) + 
  plot_sankey(sankey_long_guessing_alt, 'GUESSING', 
              breaks = c('DVAR_SD',
                         'tSNR',
                         'PctBrainCoverage',
                         'REL_RMS_0_5',
                         'None'),
              labels = c('DVAR_SD',
                         'tSNR',
                         'PctBrainCoverage',
                         'REL_RMS_0_5',
                         'None')) + 
  plot_layout(guides = 'collect')) + 
  plot_layout(design = 'AA\nBC\nEE')


# Age model exclusion criteria
library(brms)


qc_combined_demos_l_model <- qc_combined_demos_l[, c('subject', 'redcap_event', 'DB_SeriesDesc', 'event_age',
                                                     'variable', 'value')]
age_qc_model_d <- dcast(qc_combined_demos_l_model[!is.na(variable)], ... ~ variable)
age_qc_model_d[, PctBrainCoverage_bin := round(PctBrainCoverage*100)]
age_qc_model_d[, PctBrainCoverage_trials := 100*100]

## pctbrain
age_qc_bcovrg_form <- bf(PctBrainCoverage_bin | trials(PctBrainCoverage_trials) ~ 
                           1 + redcap_event + DB_SeriesDesc + 
                           s(event_age, k = 3) + 
                           (1 | subject),
                         family = binomial)
age_qc_bcovrg_priors_def <- get_prior(age_qc_bcovrg_form, data = age_qc_model_d)
age_qc_bcovrg_priors <- c(prior('normal(0, .25)', class = 'b'),
                          prior('student_t(3, .5, .5)', class = 'Intercept'),
                          prior('student_t(3, 0, .5)', class = 'sds'),
                          prior('student_t(3, 0, .5)', class = 'sd'))
age_qc_bcovrg_fit_ponly <- brm(age_qc_bcovrg_form, prior = age_qc_bcovrg_priors,
                               data = age_qc_model_d,
                               cores = 4, chains = 4, warmup = 100, iter = 500,
                               backend = 'cmdstanr', file = 'qc/age_qc_bcovrg_ponly', file_refit = 'never', 
                               silent = 0, sample_prior = 'only')

age_qc_bcovrg_pp <- posterior_predict(age_qc_bcovrg_fit_ponly, newdata = age_qc_model_d, allow_new_levels = TRUE)
hist(age_qc_bcovrg_pp)
plot(age_qc_bcovrg_fit_ponly, ask = FALSE)
plot(conditional_effects(age_qc_bcovrg_fit_ponly), ask = FALSE)

age_qc_bcovrg_fit <- brm(age_qc_bcovrg_form, prior = age_qc_bcovrg_priors,
                         data = age_qc_model_d,
                         cores = 4, chains = 4, warmup = 1000, iter = 2000,
                         backend = 'cmdstanr', file = 'qc/age_qc_bcovrg', file_refit = 'never', 
                         silent = 0, sample_prior = TRUE)
age_qc_bcovrg_ceffs <- conditional_effects(age_qc_bcovrg_fit, effects = 'event_age',
                                           conditions = data.frame(PctBrainCoverage_trials = 100*100))

ggplot(age_qc_bcovrg_ceffs$event_age,
       aes(x = event_age, y = estimate__/10000)) + 
  # geom_point(data = age_qc_model_d, aes(y = PctBrainCoverage_bin/10000), alpha = .2) + 
  geom_hex(data = age_qc_model_d, aes(y = PctBrainCoverage_bin/10000), binwidth = c(2, .001)) + 
  geom_ribbon(aes(ymax = upper__/10000, ymin = lower__/10000), fill = 'blue', alpha = .8) + 
  geom_line(color = 'darkblue') +
  scale_color_gradient(high = '#aaaaaa99', low = '#f9f9f999', aesthetics = c('color', 'fill'))+
  coord_trans(y = 'log10', ylim = c(.99, 1)) + 
  theme_minimal()

## tsnr
par(mfcol = c(1,2))
hist(age_qc_model_d$tSNR)
plot(x <- seq(0,35,length.out = 100), dweibull(x, shape = 5, scale = 23), type = 'l')
hist(exp(rnorm(1e6, mean = log(5), sd = .25)))
hist((rnorm(1e6, mean = log(5), sd = .25)))
hist(exp(rnorm(1e6, mean = log(23), sd = .25)))
hist((rnorm(1e6, mean = log(23), sd = .25)))
par(mfcol = c(1,1))
age_qc_tsnr_form <- bf(tSNR ~ 1 + 
                        1 + redcap_event + DB_SeriesDesc + 
                        s(event_age, k = 3) + 
                         (1 | subject),
                       shape ~ 1,
                       family = brms::weibull())
age_qc_tsnr_priors_def <- get_prior(age_qc_tsnr_form, data = age_qc_model_d)
age_qc_tsnr_priors <- c(set_prior('normal(0, .25)', class = 'b'),
                        set_prior(sprintf('student_t(3, %0.2f, .25)', log(23)), class = 'Intercept'),
                        set_prior(sprintf('student_t(3, %0.2f, .25)', log(5)), class = 'Intercept', dpar = 'shape'),
                        set_prior('student_t(3, 0, .1)', class = 'sd'),
                        set_prior('student_t(3, 0, .1)', class = 'sds'))
age_qc_tsnr_fit_ponly <- brm(age_qc_tsnr_form, prior = age_qc_tsnr_priors,
                             data = age_qc_model_d,
                             cores = 4, chains = 4, warmup = 100, iter = 500,
                             backend = 'cmdstanr', file = 'qc/age_qc_tsnr_ponly', file_refit = 'never', 
                             silent = 0, sample_prior = 'only')

age_qc_tsnr_pp <- posterior_predict(age_qc_tsnr_fit_ponly, newdata = age_qc_model_d, allow_new_levels = TRUE)
ggplot(data.frame(x = as.vector(age_qc_tsnr_pp)), aes(x = x)) + geom_histogram(bins = 500) + coord_cartesian(xlim = c(0,100))
plot(age_qc_tsnr_fit_ponly, ask = FALSE)
plot(conditional_effects(age_qc_tsnr_fit_ponly), ask = FALSE)

age_qc_tsnr_fit <- brm(age_qc_tsnr_form, prior = age_qc_tsnr_priors,
                         data = age_qc_model_d,
                         cores = 4, chains = 4, warmup = 1500, iter = 2500, threads = 2,
                         backend = 'cmdstanr', file = 'qc/age_qc_tsnr', file_refit = 'never', 
                         silent = 0, sample_prior = TRUE,
                       control = list(adapt_delta = .8, max_treedepth = 10))
summary(age_qc_tsnr_fit)

age_qc_tsnr_ceffs <- conditional_effects(age_qc_tsnr_fit, effects = 'event_age')
plot(conditional_effects(age_qc_tsnr_fit), ask = FALSE)
pp_check(age_qc_tsnr_fit)
ggplot(age_qc_tsnr_ceffs$event_age,
       aes(x = event_age, y = estimate__)) + 
  # geom_point(data = age_qc_model_d, aes(y = PctBrainCoverage_bin/10000), alpha = .2) + 
  geom_hex(data = age_qc_model_d, aes(y = tSNR)) + 
  geom_ribbon(aes(ymax = upper__, ymin = lower__), fill = 'blue', alpha = .8) + 
  geom_line(color = 'darkblue') +
  scale_color_gradient(high = '#90909099', low = '#f9f9f999', aesthetics = c('color', 'fill'))+
  theme_minimal()

