# =============================================================================
# Week 5 - Step 5: Probabilistic CRPS + Calibration + Sharpness
# =============================================================================




# install and load packages
needed_packages <- c("dplyr", "readr", "ggplot2", "scoringRules")
for (pkg in needed_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    install.packages(pkg, repos = "https://cloud.r-project.org")
  }
}

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
lag_tune_path <- file.path(lag_dir, "tuning_log_dv.csv")
roll_tune_path <- file.path(roll_dir, "tuning_log_dv.csv")

required_files <- c(lag_pred_path, roll_pred_path, lag_tune_path, roll_tune_path)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop("Missing files:\n", paste(missing_files, collapse = "\n"))
}


# read data


lag_pred <- read_csv(lag_pred_path, show_col_types = FALSE) %>%
  mutate(model = ifelse(is.na(model) | model == "", "rf_lag123_caret_cv", model)) %>%
  filter(!is.na(horizon), !is.na(actual_log), !is.na(pred_log))

roll_pred <- read_csv(roll_pred_path, show_col_types = FALSE) %>%
  mutate(model = ifelse(is.na(model) | model == "", "rf_roll3_caret_cv", model)) %>%
  filter(!is.na(horizon), !is.na(actual_log), !is.na(pred_log))

lag_tune <- read_csv(lag_tune_path, show_col_types = FALSE) %>%
  select(horizon, model, cv_rmse)

roll_tune <- read_csv(roll_tune_path, show_col_types = FALSE) %>%
  select(horizon, model, cv_rmse)

all_pred <- bind_rows(lag_pred, roll_pred)
all_tune <- bind_rows(lag_tune, roll_tune)


# build probabilistic forecast and evaluate row-by-row


prob_tbl <- all_pred %>%
  left_join(all_tune, by = c("horizon", "model")) %>%
  mutate(sd_pred = cv_rmse)

if (any(is.na(prob_tbl$sd_pred))) {
  stop("Missing cv_rmse after merge. Check horizon/model names in tuning files.")
}
if (any(prob_tbl$sd_pred <= 0)) {
  stop("Non-positive sd found. cv_rmse must be > 0.")
}

prob_tbl <- prob_tbl %>%
  mutate(
    # Probabilistic CRPS for normal predictive distribution
    crps_prob = scoringRules::crps_norm(y = actual_log, mean = pred_log, sd = sd_pred),

    # 80% predictive interval
    lower80 = qnorm(0.10, mean = pred_log, sd = sd_pred),
    upper80 = qnorm(0.90, mean = pred_log, sd = sd_pred),
    covered80 = as.integer(actual_log >= lower80 & actual_log <= upper80),
    width80 = upper80 - lower80,

    # 95% predictive interval
    lower95 = qnorm(0.025, mean = pred_log, sd = sd_pred),
    upper95 = qnorm(0.975, mean = pred_log, sd = sd_pred),
    covered95 = as.integer(actual_log >= lower95 & actual_log <= upper95),
    width95 = upper95 - lower95
  )


# summarize by horizon and model


summary_tbl <- prob_tbl %>%
  group_by(horizon, model) %>%
  summarise(
    n = n(),
    crps_prob = mean(crps_prob),

    # calibration: empirical coverage should be close to target
    coverage80 = mean(covered80),
    coverage95 = mean(covered95),
    calibration_error80 = abs(coverage80 - 0.80),
    calibration_error95 = abs(coverage95 - 0.95),

    # sharpness: lower width means sharper distribution
    sharpness_sd = mean(sd_pred),
    sharpness_width80 = mean(width80),
    sharpness_width95 = mean(width95),
    .groups = "drop"
  ) %>%
  mutate(
    # simple interpretation label
    scorecard = case_when(
      calibration_error80 <= 0.03 & calibration_error95 <= 0.02 ~ "well_calibrated",
      coverage80 < 0.80 & coverage95 < 0.95 ~ "over_confident_too_narrow",
      coverage80 > 0.80 & coverage95 > 0.95 ~ "under_confident_too_wide",
      TRUE ~ "mixed_calibration"
    )
  ) %>%
  arrange(horizon, crps_prob)

best_prob_tbl <- summary_tbl %>%
  group_by(horizon) %>%
  slice_min(order_by = crps_prob, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  rename(
    best_model = model,
    best_crps_prob = crps_prob
  )

# save model-level tables
write_csv(
  summary_tbl %>% filter(model == "rf_lag123_caret_cv"),
  file.path(lag_dir, "metrics_probabilistic_log_dv.csv")
)

write_csv(
  summary_tbl %>% filter(model == "rf_roll3_caret_cv"),
  file.path(roll_dir, "metrics_probabilistic_log_dv.csv")
)

# Save comparison tables
write_csv(summary_tbl, file.path(compare_dir, "comparison_probabilistic_metrics_log_dv.csv"))
write_csv(best_prob_tbl, file.path(compare_dir, "best_model_by_horizon_prob_crps_log_dv.csv"))


# plots

crps_plot <- summary_tbl %>%
  ggplot(aes(x = horizon, y = crps_prob, color = model)) +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.2) +
  scale_x_continuous(breaks = 1:7) +
  labs(
    title = "Probabilistic CRPS by Horizon (log DV)",
    x = "Horizon (months ahead)",
    y = "CRPS (probabilistic)"
  ) +
  theme_minimal(base_size = 12)

ggsave(file.path(compare_dir, "probabilistic_crps_by_horizon_log_dv.png"), crps_plot, width = 10, height = 6, dpi = 300)

calibration80_plot <- summary_tbl %>%
  ggplot(aes(x = horizon, y = coverage80, color = model)) +
  geom_hline(yintercept = 0.80, linetype = "dashed", color = "gray35") +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.2) +
  scale_x_continuous(breaks = 1:7) +
  labs(
    title = "Calibration Check: 80% Coverage by Horizon",
    x = "Horizon (months ahead)",
    y = "Empirical 80% Coverage"
  ) +
  theme_minimal(base_size = 12)

ggsave(file.path(compare_dir, "calibration80_by_horizon_log_dv.png"), calibration80_plot, width = 10, height = 6, dpi = 300)

sharpness_plot <- summary_tbl %>%
  ggplot(aes(x = horizon, y = sharpness_width80, color = model)) +
  geom_line(linewidth = 1.05) +
  geom_point(size = 2.2) +
  scale_x_continuous(breaks = 1:7) +
  labs(
    title = "Sharpness: Mean 80% Interval Width by Horizon",
    x = "Horizon (months ahead)",
    y = "Mean 80% Width"
  ) +
  theme_minimal(base_size = 12)

ggsave(file.path(compare_dir, "sharpness80_by_horizon_log_dv.png"), sharpness_plot, width = 10, height = 6, dpi = 300)

cat("\nDone. Probabilistic outputs created in:\n")
cat("- ", lag_dir, " / metrics_probabilistic_log_dv.csv\n", sep = "")
cat("- ", roll_dir, " / metrics_probabilistic_log_dv.csv\n", sep = "")
cat("- ", compare_dir, " / comparison_probabilistic_metrics_log_dv.csv\n", sep = "")
cat("- ", compare_dir, " / best_model_by_horizon_prob_crps_log_dv.csv\n", sep = "")
cat("- ", compare_dir, " / probabilistic_crps_by_horizon_log_dv.png\n", sep = "")
cat("- ", compare_dir, " / calibration80_by_horizon_log_dv.png\n", sep = "")
cat("- ", compare_dir, " / sharpness80_by_horizon_log_dv.png\n", sep = "")
