# =============================================================================
# Week 5 - Step 6: Compare Model 1 vs Model 2
# =============================================================================
# Inputs:
# - model_rf_lag123/metrics_log_dv.csv
# - model_rf_roll3/metrics_log_dv.csv
#
# Outputs:
# - Combined comparison table
# - Best model by horizon
# - RMSE and MSE comparison plots
# =============================================================================

# packages
library(dplyr)
library(readr)
library(ggplot2)

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

lag_metrics_path <- file.path(week5_dir, "model_rf_lag123", "metrics_log_dv.csv")
roll_metrics_path <- file.path(week5_dir, "model_rf_roll3", "metrics_log_dv.csv")

compare_dir <- file.path(week5_dir, "model_comparison")
dir.create(compare_dir, recursive = TRUE, showWarnings = FALSE)
unlink(list.files(compare_dir, full.names = TRUE), recursive = TRUE, force = TRUE)

if (!file.exists(lag_metrics_path)) {
  stop("Missing: ", lag_metrics_path, "\nRun week_5_2_train_rf_lag123.R first.")
}
if (!file.exists(roll_metrics_path)) {
  stop("Missing: ", roll_metrics_path, "\nRun week_5_3_train_rf_roll3.R first.")
}

lag_metrics <- read_csv(lag_metrics_path, show_col_types = FALSE)
roll_metrics <- read_csv(roll_metrics_path, show_col_types = FALSE)

compare_tbl <- bind_rows(lag_metrics, roll_metrics) %>%
  arrange(horizon, rmse)

best_tbl <- compare_tbl %>%
  group_by(horizon) %>%
  slice_min(order_by = rmse, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  rename(best_model = model, best_mse = mse, best_rmse = rmse)

# save them
write_csv(compare_tbl, file.path(compare_dir, "comparison_metrics_log_dv.csv"))
write_csv(best_tbl, file.path(compare_dir, "best_model_by_horizon_log_dv.csv"))

# plots
rmse_plot <- compare_tbl %>%
  ggplot(aes(x = horizon, y = rmse, color = model)) +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.2) +
  scale_x_continuous(breaks = 1:7) +
  labs(
    title = "Week 5 Comparison: RMSE by Horizon (log DV)",
    x = "Horizon (months ahead)",
    y = "RMSE"
  ) +
  theme_minimal(base_size = 12)

mse_plot <- compare_tbl %>%
  ggplot(aes(x = horizon, y = mse, color = model)) +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.2) +
  scale_x_continuous(breaks = 1:7) +
  labs(
    title = "Week 5 Comparison: MSE by Horizon (log DV)",
    x = "Horizon (months ahead)",
    y = "MSE"
  ) +
  theme_minimal(base_size = 12)

# save plots
ggsave(file.path(compare_dir, "rmse_comparison_log_dv.png"), rmse_plot, width = 10, height = 6, dpi = 300)
ggsave(file.path(compare_dir, "mse_comparison_log_dv.png"), mse_plot, width = 10, height = 6, dpi = 300)
