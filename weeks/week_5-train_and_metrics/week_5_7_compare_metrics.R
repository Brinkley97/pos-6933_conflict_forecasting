# =============================================================================
# Week 5 - Step 7: Compare Metrics (MSE, RMSE, CRPS_det, CRPS_prob)
# =============================================================================



# install and load packages
needed_packages <- c("dplyr", "readr", "ggplot2", "tidyr", "scoringRules", "scales")
for (pkg in needed_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
  }
}

library(dplyr)
library(readr)
library(ggplot2)
library(tidyr)
library(scoringRules)
library(scales)

resolve_week5_dir <- function() {
  candidates <- c(".", "..", "Week_5", file.path("..", "Week_5"))

  for (cand in candidates) {
    if (file.exists(file.path(cand, "README.md")) && dir.exists(file.path(cand, "scripts"))) {
      return(normalizePath(cand, mustWork = TRUE))
    }
  }

  stop("Could not locate the Week_5 project directory. Run this script from Week_5 or its scripts/ directory.")
}


# paths
project_root <- resolve_week5_dir()
week5_dir <- file.path(project_root, "outputs", "week5")

lag_dir <- file.path(week5_dir, "model_rf_lag123")
roll_dir <- file.path(week5_dir, "model_rf_roll3")
compare_dir <- file.path(week5_dir, "model_comparison")
dir.create(compare_dir, recursive = TRUE, showWarnings = FALSE)

lag_pred_path <- file.path(lag_dir, "predictions_log_dv.csv")
roll_pred_path <- file.path(roll_dir, "predictions_log_dv.csv")
lag_metrics_path <- file.path(lag_dir, "metrics_log_dv.csv")
roll_metrics_path <- file.path(roll_dir, "metrics_log_dv.csv")
lag_tune_path <- file.path(lag_dir, "tuning_log_dv.csv")
roll_tune_path <- file.path(roll_dir, "tuning_log_dv.csv")

required_files <- c(
  lag_pred_path, roll_pred_path,
  lag_metrics_path, roll_metrics_path,
  lag_tune_path, roll_tune_path
)

missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop("Missing files:\n", paste(missing_files, collapse = "\n"))
}


# load base inputs
lag_pred <- read_csv(lag_pred_path, show_col_types = FALSE) %>%
  mutate(model = ifelse(is.na(model) | model == "", "rf_lag123_caret_cv", model)) %>%
  filter(!is.na(horizon), !is.na(actual_log), !is.na(pred_log))

roll_pred <- read_csv(roll_pred_path, show_col_types = FALSE) %>%
  mutate(model = ifelse(is.na(model) | model == "", "rf_roll3_caret_cv", model)) %>%
  filter(!is.na(horizon), !is.na(actual_log), !is.na(pred_log))

all_pred <- bind_rows(lag_pred, roll_pred)

lag_metrics <- read_csv(lag_metrics_path, show_col_types = FALSE)
roll_metrics <- read_csv(roll_metrics_path, show_col_types = FALSE)
all_metrics_base <- bind_rows(lag_metrics, roll_metrics)

lag_tune <- read_csv(lag_tune_path, show_col_types = FALSE) %>%
  select(horizon, model, cv_rmse)

roll_tune <- read_csv(roll_tune_path, show_col_types = FALSE) %>%
  select(horizon, model, cv_rmse)

all_tune <- bind_rows(lag_tune, roll_tune)


#  deterministic CRPS (point forecast treated as one-value sample)
det_crps_tbl <- all_pred %>%
  mutate(crps_det = scoringRules::crps_sample(y = actual_log, dat = matrix(pred_log, ncol = 1))) %>%
  group_by(horizon, model) %>%
  summarise(
    n = n(),
    crps_det = mean(crps_det),
    .groups = "drop"
  ) %>%
  arrange(horizon)

write_csv(
  det_crps_tbl %>% filter(model == "rf_lag123_caret_cv") %>% select(horizon, model, n, crps = crps_det),
  file.path(lag_dir, "metrics_crps_log_dv.csv")
)

write_csv(
  det_crps_tbl %>% filter(model == "rf_roll3_caret_cv") %>% select(horizon, model, n, crps = crps_det),
  file.path(roll_dir, "metrics_crps_log_dv.csv")
)

compare_det_tbl <- all_metrics_base %>%
  left_join(det_crps_tbl %>% select(horizon, model, crps = crps_det), by = c("horizon", "model")) %>%
  arrange(horizon, crps)

best_det_tbl <- compare_det_tbl %>%
  group_by(horizon) %>%
  slice_min(order_by = crps, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  rename(
    best_model = model,
    best_mse = mse,
    best_rmse = rmse,
    best_crps = crps
  )

write_csv(compare_det_tbl, file.path(compare_dir, "comparison_metrics_log_dv_with_crps.csv"))
write_csv(best_det_tbl, file.path(compare_dir, "best_model_by_horizon_crps_log_dv.csv"))


# probabilistic CRPS (Normal(mean=pred_log, sd=cv_rmse))


prob_tbl <- all_pred %>%
  left_join(all_tune, by = c("horizon", "model")) %>%
  mutate(sd_pred = cv_rmse)

if (any(is.na(prob_tbl$sd_pred))) {
  stop("Missing cv_rmse after merge. Check horizon/model names in tuning files.")
}
if (any(prob_tbl$sd_pred <= 0)) {
  stop("Non-positive sd found. cv_rmse must be > 0.")
}

prob_metrics_tbl <- prob_tbl %>%
  mutate(
    crps_prob = scoringRules::crps_norm(y = actual_log, mean = pred_log, sd = sd_pred),
    lower80 = qnorm(0.10, mean = pred_log, sd = sd_pred),
    upper80 = qnorm(0.90, mean = pred_log, sd = sd_pred),
    covered80 = as.integer(actual_log >= lower80 & actual_log <= upper80),
    width80 = upper80 - lower80,
    lower95 = qnorm(0.025, mean = pred_log, sd = sd_pred),
    upper95 = qnorm(0.975, mean = pred_log, sd = sd_pred),
    covered95 = as.integer(actual_log >= lower95 & actual_log <= upper95),
    width95 = upper95 - lower95
  ) %>%
  group_by(horizon, model) %>%
  summarise(
    n = n(),
    crps_prob = mean(crps_prob),
    coverage80 = mean(covered80),
    coverage95 = mean(covered95),
    calibration_error80 = abs(coverage80 - 0.80),
    calibration_error95 = abs(coverage95 - 0.95),
    sharpness_sd = mean(sd_pred),
    sharpness_width80 = mean(width80),
    sharpness_width95 = mean(width95),
    .groups = "drop"
  ) %>%
  mutate(
    scorecard = case_when(
      calibration_error80 <= 0.03 & calibration_error95 <= 0.02 ~ "well_calibrated",
      coverage80 < 0.80 & coverage95 < 0.95 ~ "over_confident_too_narrow",
      coverage80 > 0.80 & coverage95 > 0.95 ~ "under_confident_too_wide",
      TRUE ~ "mixed_calibration"
    )
  ) %>%
  arrange(horizon, crps_prob)

write_csv(
  prob_metrics_tbl %>% filter(model == "rf_lag123_caret_cv"),
  file.path(lag_dir, "metrics_probabilistic_log_dv.csv")
)

write_csv(
  prob_metrics_tbl %>% filter(model == "rf_roll3_caret_cv"),
  file.path(roll_dir, "metrics_probabilistic_log_dv.csv")
)

best_prob_tbl <- prob_metrics_tbl %>%
  group_by(horizon) %>%
  slice_min(order_by = crps_prob, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  rename(
    best_model = model,
    best_crps_prob = crps_prob
  )

write_csv(prob_metrics_tbl, file.path(compare_dir, "comparison_probabilistic_metrics_log_dv.csv"))
write_csv(best_prob_tbl, file.path(compare_dir, "best_model_by_horizon_prob_crps_log_dv.csv"))

# combine all four metrics in one table

all_four_tbl <- all_metrics_base %>%
  left_join(det_crps_tbl %>% select(horizon, model, crps_det), by = c("horizon", "model")) %>%
  left_join(prob_metrics_tbl %>% select(horizon, model, crps_prob, coverage80, coverage95, scorecard), by = c("horizon", "model")) %>%
  arrange(horizon, rmse)

if (any(is.na(all_four_tbl$crps_det)) || any(is.na(all_four_tbl$crps_prob))) {
  stop("Missing CRPS metrics after merge in all_four_tbl.")
}

write_csv(all_four_tbl, file.path(compare_dir, "comparison_all_four_metrics_log_dv.csv"))

# best model by horizon for each metric
all_four_long <- all_four_tbl %>%
  select(horizon, model, mse, rmse, crps_det, crps_prob) %>%
  pivot_longer(
    cols = c(mse, rmse, crps_det, crps_prob),
    names_to = "metric",
    values_to = "value"
  )

best_by_metric_tbl <- all_four_long %>%
  group_by(horizon, metric) %>%
  slice_min(order_by = value, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  rename(best_model = model, best_value = value) %>%
  arrange(metric, horizon)

write_csv(best_by_metric_tbl, file.path(compare_dir, "best_model_by_horizon_each_metric_log_dv.csv"))

# visualizations for all four metrics
metric_labels <- c(
  mse = "MSE",
  rmse = "RMSE",
  crps_det = "CRPS (Deterministic)",
  crps_prob = "CRPS (Probabilistic)"
)

model_labels <- c(
  rf_lag123_caret_cv = "Lag123 (RF)",
  rf_roll3_caret_cv = "Roll3 (RF)"
)

model_labels_short <- c(
  rf_lag123_caret_cv = "Lag123",
  rf_roll3_caret_cv = "Roll3"
)

model_palette <- c(
  rf_lag123_caret_cv = "#1B4965",
  rf_roll3_caret_cv = "#CA6702"
)

plot_tbl <- all_four_long %>%
  mutate(metric = factor(metric, levels = names(metric_labels), labels = unname(metric_labels)))

# Plot A: all four metrics, faceted
plot_a <- plot_tbl %>%
  ggplot(aes(x = horizon, y = value, color = model)) +
  geom_line(linewidth = 1.1) +
  geom_point(size = 2.3) +
  scale_color_manual(values = model_palette, labels = model_labels) +
  scale_x_continuous(breaks = 1:7, expand = expansion(mult = c(0.02, 0.04))) +
  facet_wrap(~ metric, scales = "free_y", ncol = 2) +
  labs(
    title = "Week 5 Benchmark: Four Metrics by Horizon",
    subtitle = "Lower is better for all panels",
    x = "Horizon (months ahead)",
    y = "Metric value",
    color = "Model"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 14),
    plot.subtitle = element_text(color = "gray30"),
    strip.text = element_text(face = "bold"),
    strip.background = element_rect(fill = "#F2E9E4", color = NA),
    panel.grid.minor = element_blank(),
    legend.position = "bottom"
  )

ggsave(
  file.path(compare_dir, "all_four_metrics_by_horizon_log_dv.png"),
  plot_a,
  width = 12,
  height = 8,
  dpi = 320
)

# Plot B: winner heatmap (best model at each horizon for each metric)
winner_heat <- best_by_metric_tbl %>%
  mutate(
    metric = factor(metric, levels = names(metric_labels), labels = unname(metric_labels)),
    best_model = factor(best_model, levels = names(model_labels)),
    best_model_label = recode(best_model, !!!model_labels_short)
  ) %>%
  ggplot(aes(x = factor(horizon), y = metric, fill = best_model)) +
  geom_tile(color = "white", linewidth = 0.8) +
  geom_text(aes(label = best_model_label), size = 3.1, color = "black") +
  scale_fill_manual(values = model_palette, labels = model_labels) +
  labs(
    title = "Best Model by Horizon and Metric",
    x = "Horizon (months ahead)",
    y = "Metric",
    fill = "Best model"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    panel.grid = element_blank(),
    legend.position = "none"
  )

ggsave(
  file.path(compare_dir, "best_model_heatmap_all_four_metrics_log_dv.png"),
  winner_heat,
  width = 12,
  height = 5.8,
  dpi = 320
)

# Plot C: percent improvement of lag123 over roll3 (positive means lag123 better)
improve_tbl <- all_four_long %>%
  pivot_wider(names_from = model, values_from = value)

if (!all(c("rf_lag123_caret_cv", "rf_roll3_caret_cv") %in% names(improve_tbl))) {
  stop("Expected model names not found when building percent-improvement plot.")
}

improve_tbl <- improve_tbl %>%
  mutate(
    pct_improve_lag123 = 100 * (rf_roll3_caret_cv - rf_lag123_caret_cv) / rf_roll3_caret_cv,
    metric = factor(metric, levels = names(metric_labels), labels = unname(metric_labels))
  )

plot_c <- improve_tbl %>%
  ggplot(aes(x = horizon, y = pct_improve_lag123, color = metric)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray35") +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.0) +
  scale_x_continuous(breaks = 1:7) +
  scale_y_continuous(labels = label_number(suffix = "%", accuracy = 0.1)) +
  labs(
    title = "Lag123 Advantage Over Roll3 by Metric",
    subtitle = "Positive values mean lag123 has lower error (better)",
    x = "Horizon (months ahead)",
    y = "Percent improvement",
    color = "Metric"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "bottom"
  )

ggsave(
  file.path(compare_dir, "lag123_advantage_percent_all_four_metrics_log_dv.png"),
  plot_c,
  width = 12,
  height = 6.8,
  dpi = 320
)

det_crps_plot <- compare_det_tbl %>%
  ggplot(aes(x = horizon, y = crps, color = model)) +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.2) +
  scale_color_manual(values = model_palette, labels = model_labels) +
  scale_x_continuous(breaks = 1:7) +
  labs(
    title = "Week 5 Comparison: Deterministic CRPS by Horizon (log DV)",
    x = "Horizon (months ahead)",
    y = "CRPS (deterministic)",
    color = "Model"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    legend.position = "bottom"
  )

ggsave(
  file.path(compare_dir, "crps_comparison_log_dv.png"),
  det_crps_plot,
  width = 10,
  height = 6,
  dpi = 300
)

prob_crps_plot <- prob_metrics_tbl %>%
  ggplot(aes(x = horizon, y = crps_prob, color = model)) +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.2) +
  scale_color_manual(values = model_palette, labels = model_labels) +
  scale_x_continuous(breaks = 1:7) +
  labs(
    title = "Probabilistic CRPS by Horizon (log DV)",
    x = "Horizon (months ahead)",
    y = "CRPS (probabilistic)",
    color = "Model"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold"),
    panel.grid.minor = element_blank(),
    legend.position = "bottom"
  )

ggsave(
  file.path(compare_dir, "probabilistic_crps_by_horizon_log_dv.png"),
  prob_crps_plot,
  width = 10,
  height = 6,
  dpi = 300
)

cat("\nDone. Combined 4-metric outputs created:\n")
cat("- ", file.path(compare_dir, "comparison_all_four_metrics_log_dv.csv"), "\n", sep = "")
cat("- ", file.path(compare_dir, "comparison_metrics_log_dv_with_crps.csv"), "\n", sep = "")
cat("- ", file.path(compare_dir, "comparison_probabilistic_metrics_log_dv.csv"), "\n", sep = "")
cat("- ", file.path(compare_dir, "best_model_by_horizon_crps_log_dv.csv"), "\n", sep = "")
cat("- ", file.path(compare_dir, "best_model_by_horizon_prob_crps_log_dv.csv"), "\n", sep = "")
cat("- ", file.path(compare_dir, "best_model_by_horizon_each_metric_log_dv.csv"), "\n", sep = "")
cat("- ", file.path(compare_dir, "crps_comparison_log_dv.png"), "\n", sep = "")
cat("- ", file.path(compare_dir, "probabilistic_crps_by_horizon_log_dv.png"), "\n", sep = "")
cat("- ", file.path(compare_dir, "all_four_metrics_by_horizon_log_dv.png"), "\n", sep = "")
cat("- ", file.path(compare_dir, "best_model_heatmap_all_four_metrics_log_dv.png"), "\n", sep = "")
cat("- ", file.path(compare_dir, "lag123_advantage_percent_all_four_metrics_log_dv.png"), "\n", sep = "")
