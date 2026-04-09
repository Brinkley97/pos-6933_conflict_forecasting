# =============================================================================
# Week 5 - Step 4: Add CRPS for Benchmark Models
# =============================================================================
# =============================================================================



# install packages

needed_packages <- c("dplyr", "readr", "ggplot2", "scoringRules")
for (pkg in needed_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
  }
}

# load packages
library(dplyr)
library(readr)
library(ggplot2)
library(scoringRules)

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

# check that all required files exist before proceeding
required_files <- c(lag_pred_path, roll_pred_path, lag_metrics_path, roll_metrics_path)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop("Missing files:\n", paste(missing_files, collapse = "\n"))
}


# read predictions and compute CRPS

# lag predictions 
lag_pred <- read_csv(lag_pred_path, show_col_types = FALSE) %>%
  mutate(model = ifelse(is.na(model) | model == "", "rf_lag123_caret_cv", model)) %>%
  filter(!is.na(horizon), !is.na(actual_log), !is.na(pred_log))

# rolling predictions
roll_pred <- read_csv(roll_pred_path, show_col_types = FALSE) %>%
  mutate(model = ifelse(is.na(model) | model == "", "rf_roll3_caret_cv", model)) %>%
  filter(!is.na(horizon), !is.na(actual_log), !is.na(pred_log))


# deterministic prediction = one-value predictive sample for CRPS
lag_pred <- lag_pred %>%
  mutate(crps = scoringRules::crps_sample(y = actual_log, dat = matrix(pred_log, ncol = 1)))

roll_pred <- roll_pred %>%
  mutate(crps = scoringRules::crps_sample(y = actual_log, dat = matrix(pred_log, ncol = 1)))

lag_crps <- lag_pred %>%
  group_by(horizon, model) %>%
  summarise(n = n(), crps = mean(crps), .groups = "drop") %>%
  arrange(horizon)

roll_crps <- roll_pred %>%
  group_by(horizon, model) %>%
  summarise(n = n(), crps = mean(crps), .groups = "drop") %>%
  arrange(horizon)

write_csv(lag_crps, file.path(lag_dir, "metrics_crps_log_dv.csv"))
write_csv(roll_crps, file.path(roll_dir, "metrics_crps_log_dv.csv"))




# merge CRPS with existing metrics

lag_metrics <- read_csv(lag_metrics_path, show_col_types = FALSE)
roll_metrics <- read_csv(roll_metrics_path, show_col_types = FALSE)

compare_tbl <- bind_rows(lag_metrics, roll_metrics) %>%
  left_join(
    bind_rows(lag_crps, roll_crps) %>% select(horizon, model, crps),
    by = c("horizon", "model")
  ) %>%
  arrange(horizon, rmse)

if (any(is.na(compare_tbl$crps))) {
  stop("Could not attach CRPS for one or more model-horizon rows.")
}

best_crps_tbl <- compare_tbl %>%
  group_by(horizon) %>%
  slice_min(order_by = crps, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  rename(
    best_model = model,
    best_mse = mse,
    best_rmse = rmse,
    best_crps = crps
  )

write_csv(compare_tbl, file.path(compare_dir, "comparison_metrics_log_dv_with_crps.csv"))
write_csv(best_crps_tbl, file.path(compare_dir, "best_model_by_horizon_crps_log_dv.csv"))


# plot CRPS by horizon


crps_plot <- compare_tbl %>%
  ggplot(aes(x = horizon, y = crps, color = model)) +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.2) +
  scale_x_continuous(breaks = 1:7) +
  labs(
    title = "Week 5 Comparison: Deterministic CRPS by Horizon (log DV)",
    x = "Horizon (months ahead)",
    y = "CRPS (deterministic)"
  ) +
  theme_minimal(base_size = 12)

ggsave(file.path(compare_dir, "crps_comparison_log_dv.png"), crps_plot, width = 10, height = 6, dpi = 300)

cat("\nDone. CRPS outputs created in:\n")
cat("- ", lag_dir, "\n", sep = "")
cat("- ", roll_dir, "\n", sep = "")
cat("- ", compare_dir, "\n", sep = "")
