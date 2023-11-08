library(data.table)
library(ggplot2)
library(viridis)
library(stringi)
library(patchwork)

CA_ids <- fread('group_level_roi/CortexSubcortex_ColeAnticevic_NetPartition_wSubcorGSR_parcels_LR_LabelKey.txt')
CA_label_ids <- CA_ids[, c('INDEX', 'LABEL', 'NETWORKSORTEDORDER')]
regex <- '([\\w-]+)-\\d{2}_([LR])-(\\w+)'
CA_label_ids[, c('LABEL', 'network', 'hemi', 'anat') := transpose(stri_match_all_regex(LABEL, regex))]
setnames(CA_label_ids, 'INDEX', 'roi_num')

subcort <- unique(CA_label_ids[anat != "Ctx", c('anat')])
subcort[, roi_num := 360 + 1:.N]
CA_label_ids <- rbindlist(list(CA_label_ids[roi_num <= 360], subcort), fill = TRUE)

traj_data_l <- readRDS('group_level_roi/guessing_spline_contrasts.rds')
traj_data <- rbindlist(unlist(unlist(traj_data_l, recursive = FALSE), recursive = FALSE))
traj_data[, roi_num := as.integer(roi)]
traj_data <- merge(traj_data, CA_label_ids, by = 'roi_num', all.x = TRUE)
setorder(traj_data, 'NETWORKSORTEDORDER', 'roi_num')
traj_data[, netsortfac := factor(roi_num, levels = unique(roi_num))]
unique(traj_data[, c('roi_num', 'NETWORKSORTEDORDER', 'netsortfac')])

xbreaks <- c(-5, 0, 5, 10)
make_plots <- function(x, selected_contrast){
  plots <- unlist(recursive = FALSE, x = lapply(c('L', 'R'), \(LR){
    lapply(c('cortex', 'subcortex'), \(anat){
      if(anat == 'cortex'){
        x <- x[param == selected_contrast & 
                         hemi == LR & 
                         anat == 'Ctx']
        
      } else {
        x <- x[param == selected_contrast & 
                         hemi == LR & 
                         anat != 'Ctx']
        title <- sprintf('%s subcortex', LR)
      }
      
      
      title <- sprintf('%s %s', LR, anat)
      fn <- sprintf('ROI-age-plot_%s-%s.png', LR, anat)
      
      # min_y <- min(unlist(traj_data[, c('Q2.5', 'Q97.5')]))
      # max_y <- max(unlist(traj_data[, c('Q2.5', 'Q97.5')]))
      min_y <- min(unlist(traj_data[, Estimate]))
      max_y <- max(unlist(traj_data[, Estimate]))
      
      p <- ggplot(x, aes(x = age_c10, y = Estimate)) + 
        geom_hline(yintercept = 0, alpha = .8) +
        geom_ribbon(aes(ymin = Q2.5, ymax = Q97.5, group = netsortfac, fill = network), alpha = .1) + 
        geom_line(aes(group = netsortfac, color = network), alpha = .5, linewidth = .2) +
        scale_color_viridis_d(option = 'magma', na.value = 'gray', end = .7) + 
        scale_fill_viridis_d(option = 'magma', na.value = 'gray', end = .7) + 
        scale_x_continuous(breaks = xbreaks, labels = xbreaks + 10) + 
        theme_minimal() + 
        coord_cartesian(y = c(min_y, max_y)) + 
        labs(x = 'Age', y = selected_contrast, title = title)
      if(anat == 'cortex'){
        p <- p + 
          facet_wrap(~ network, ncol = 6) +
          theme(strip.text = element_text(size = 7),
                axis.text = element_blank(),
                panel.grid = element_blank(),
                legend.position = 'none') 
      } else {
        p <- p + 
          facet_wrap(~ anat, ncol = 5) +
          theme(strip.text = element_text(size = 8),
                axis.text = element_blank(),
                panel.grid = element_blank(),
                legend.position = 'none')
      }
      # ggsave(filename = fn, plot = p,
      #        width = 10, height = 8, dpi = 300, units = 'in')
      if (dim(x)[[1]] == 0){
        p <- NULL
      }
      return(p)
    })
  }))
  return(plots[!unlist(lapply(plots, is.null))])
}

unique(traj_data$param)

plots <- make_plots(traj_data, 'FBWinmFBLoss')

roi_plot <- wrap_plots(plots, design = c('AC\nBD'))
ggsave(filename = 'ROI-age-plot_Guessing.png', plot = roi_plot,
       width = 12, height = 8, dpi = 300, units = 'in')
