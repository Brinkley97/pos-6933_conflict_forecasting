# =============================================================================
# Week 5 - Step 1: Prepare Data and Forecast Splits
# =============================================================================
# Input:
# - data_processed/ucdp_country_month_panel.csv
#
# Output:
# - Feature file with lag predictors
# - Train/test split files for horizons 1..7
# - Split summary table
# =============================================================================

# packages
library(dplyr)
library(readr)

# for reproducibility
set.seed(6933)

resolve_week5_dir <- function() {
  candidates <- c(".", "..", "Week_5", file.path("..", "Week_5"))

  for (cand in candidates) {
    if (file.exists(file.path(cand, "README.md")) && dir.exists(file.path(cand, "scripts"))) {
      return(normalizePath(cand, mustWork = TRUE))
    }
  }

  stop("Could not locate the Week_5 project directory. Run this script from Week_5 or its scripts/ directory.")
}


# -----------------------------------------------------------------------------
# 1) Paths and parameters
# -----------------------------------------------------------------------------

week5_dir <- resolve_week5_dir()
project_parent <- normalizePath(file.path(week5_dir, ".."), mustWork = TRUE)
proc_dir <- file.path(week5_dir, "data_processed")

setup_dir <- file.path(proc_dir, "week5_setup")
split_dir <- file.path(setup_dir, "splits")
out_dir <- file.path(week5_dir, "outputs", "week5")

panel_path <- file.path(proc_dir, "ucdp_country_month_panel.csv")
legacy_panel_paths <- c(
  file.path(project_parent, "Week_3", "data_processed", "ucdp_country_month_panel.csv"),
  file.path(project_parent, "POS6933_Data", "Data", "data_processed", "ucdp_country_month_panel.csv")
)
features_path <- file.path(setup_dir, "ucdp_country_month_panel_week5_features.csv")
summary_path <- file.path(out_dir, "setup_split_summary.csv")

dir.create(proc_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(setup_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(split_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

if (!file.exists(panel_path)) {
  existing_legacy <- legacy_panel_paths[file.exists(legacy_panel_paths)]
  if (length(existing_legacy)) {
    file.copy(existing_legacy[1], panel_path, overwrite = FALSE)
  }
}
if (!file.exists(panel_path)) {
  stop("Missing input panel: ", panel_path)
}

# clean each run
unlink(list.files(split_dir, full.names = TRUE), recursive = TRUE, force = TRUE)
unlink(list.files(out_dir, full.names = TRUE), recursive = TRUE, force = TRUE)
unlink(features_path, force = TRUE)

# forecast horizons and target month_id ranges
horizons <- 1:7 # 1 to 7 months ahead
last_train_target <- 372   # Dec 2020
first_test_target <- 373   # Jan 2021
last_test_target <- 420    # Dec 2024


# -----------------------------------------------------------------------------
# 2) load and validate panel
# -----------------------------------------------------------------------------

panel <- read_csv(panel_path, show_col_types = FALSE) %>%
  mutate(
    month = as.Date(month),
    month_id = as.integer(month_id),
    country_id = as.integer(country_id)
  ) %>%
  filter(!is.na(country_id), !is.na(month_id)) %>%
  arrange(country_id, month_id)


# -----------------------------------------------------------------------------
# 3) create predictor features
# -----------------------------------------------------------------------------

panel_features <- panel %>%
  group_by(country_id) %>%
  arrange(month_id, .by_group = TRUE) %>%
  mutate(
    lag1 = lag(fatalities_total, 1),
    lag2 = lag(fatalities_total, 2),
    lag3 = lag(fatalities_total, 3),
    roll3 = (lag1 + lag2 + lag3) / 3,
    log_lag1 = log1p(lag1),
    log_lag2 = log1p(lag2),
    log_lag3 = log1p(lag3),
    log_roll3 = log1p(roll3)
  ) %>%
  ungroup()

write_csv(panel_features, features_path)

# -----------------------------------------------------------------------------
# 4) create train/test splits for each horizon
# -----------------------------------------------------------------------------

split_summary <- list() # to store summary info for each horizon

for (h in horizons) {
  target_log <- paste0("log_fatalities_ahead_", h, "m")

  train_df <- panel_features %>%
    filter(month_id >= 1, month_id <= (last_train_target - h)) %>%
    mutate(
      horizon = h,
      origin_month_id = month_id,
      target_month_id = month_id + h
    )

  test_df <- panel_features %>%
    filter(month_id >= (first_test_target - h), month_id <= (last_test_target - h)) %>%
    mutate(
      horizon = h,
      origin_month_id = month_id,
      target_month_id = month_id + h
    )

  if (!(target_log %in% names(train_df))) {
    stop("Missing target column: ", target_log)
  }

  # leakage checks
  if (max(train_df$target_month_id) != last_train_target) {
    stop("H", h, ": train target max mismatch.")
  }
  if (min(test_df$target_month_id) != first_test_target) {
    stop("H", h, ": test target min mismatch.")
  }
  if (max(test_df$target_month_id) != last_test_target) {
    stop("H", h, ": test target max mismatch.")
  }
  if (max(train_df$target_month_id) >= min(test_df$target_month_id)) {
    stop("H", h, ": leakage detected.")
  }

# save splits
write_csv(train_df, file.path(split_dir, paste0("train_horizon_", h, ".csv")))
write_csv(test_df, file.path(split_dir, paste0("test_horizon_", h, ".csv")))

# store summary info
  split_summary[[as.character(h)]] <- data.frame(
    horizon = h,
    train_rows = nrow(train_df),
    test_rows = nrow(test_df),
    train_origin_min = min(train_df$origin_month_id),
    train_origin_max = max(train_df$origin_month_id),
    train_target_min = min(train_df$target_month_id),
    train_target_max = max(train_df$target_month_id),
    test_origin_min = min(test_df$origin_month_id),
    test_origin_max = max(test_df$origin_month_id),
    test_target_min = min(test_df$target_month_id),
    test_target_max = max(test_df$target_month_id),
    leakage_free = max(train_df$target_month_id) < min(test_df$target_month_id)
  )
}

summary_tbl <- bind_rows(split_summary) %>%
  arrange(horizon)

# save summary
write_csv(summary_tbl, summary_path)

# check if leakage-free
print(summary_tbl %>% select(horizon, leakage_free), row.names = FALSE)






