library(data.table)
library(ggplot2)
traj_data_l <- readRDS('group_level_roi/spline_contrasts.rds')

traj_data <- rbindlist(unlist(unlist(traj_data, recursive = FALSE), recursive = FALSE))

xbreaks <- c(-5, 0, 5, 10)
ggplot(traj_data[param == 'CRmHit'], aes(x = age_c10, y = Estimate)) + 
  geom_ribbon(aes(ymin = Q2.5, ymax = Q97.5), alpha = .2) + 
  geom_line(aes(group = roi)) +
  geom_hline(yintercept = 0) +
  scale_x_continuous(breaks = xbreaks, labels = xbreaks + 10) + 
  facet_wrap(~ roi, ncol = 26) + 
  theme_minimal() +
  theme(strip.text = element_blank()) + 
  labs(x = 'Age', y = expression('Correct Rejections ' ~ - ~ 'Hits'))
