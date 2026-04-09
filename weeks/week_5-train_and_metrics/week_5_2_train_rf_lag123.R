# =============================================================================
# Week 5 - Step 2: Train Model 1 (Autoregressive Random Forest with lag1,lag2,lag3)
# =============================================================================
# Dependent variable used for each horizon:
# - log_fatalities_ahead_1m ... log_fatalities_ahead_7m
#
# Evaluation metrics:
# - MSE # (Mean Squared Error)
# - RMSE # (Root Mean Squared Error)
# =============================================================================

# packages
library(dplyr)
library(readr)
library(ggplot2)
library(caret)
library(ranger)
library(doParallel)

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
# 1) paths and parameters
# -----------------------------------------------------------------------------

week5_dir <- resolve_week5_dir()
proc_dir <- file.path(week5_dir, "data_processed")
split_dir <- file.path(proc_dir, "week5_setup", "splits")
out_dir <- file.path(week5_dir, "outputs", "week5", "model_rf_lag123")

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
unlink(list.files(out_dir, full.names = TRUE), recursive = TRUE, force = TRUE)

# parameters(hyperparameters and Cross validation settings)
horizons <- 1:7
rf_num_trees <- 1000
cv_folds <- 10
cv_valid_window <- 12 # validate on 12 months (1 year) at a time
cv_min_train_months <- 120 # require at least 10 years of training data before the first validation fold


# -----------------------------------------------------------------------------
# 2) parallel computing setup for faster traing
# -----------------------------------------------------------------------------

workers <- suppressWarnings(as.integer(Sys.getenv("WEEK5_PARALLEL_WORKERS", "2")))
if (is.na(workers) || workers < 1) workers <- 2L

parallel_enabled <- FALSE
if (workers > 1) {
  ok <- tryCatch({
    doParallel::registerDoParallel(cores = workers)
    TRUE
  }, error = function(e) FALSE)

  if (ok) {
    parallel_enabled <- TRUE
    workers <- foreach::getDoParWorkers()
  } else {
    foreach::registerDoSEQ()
    workers <- 1L
  }
} else {
  foreach::registerDoSEQ()
}

on.exit({
  if (parallel_enabled) doParallel::stopImplicitCluster()
  foreach::registerDoSEQ()
}, add = TRUE)



# -----------------------------------------------------------------------------
# 3) helper functions
# -----------------------------------------------------------------------------

make_time_cv_folds <- function(train_df, n_folds, valid_window, min_train_months) {
  months <- sort(unique(train_df$month_id))
  max_month <- max(months)

  latest_val_start <- max_month - valid_window + 1
  val_starts <- latest_val_start - (n_folds - 1):0 * valid_window

  min_allowed <- min(months) + min_train_months
  val_starts <- val_starts[val_starts >= min_allowed]
   
  # sanity checks
  if (length(val_starts) < 2) stop("Not enough data to build CV folds.")
  if (length(val_starts) > n_folds) val_starts <- tail(val_starts, n_folds)

  index <- list()
  indexOut <- list()
  fold_info <- list()

  for (i in seq_along(val_starts)) {
    v_start <- val_starts[i]
    v_end <- v_start + valid_window - 1

    tr_idx <- which(train_df$month_id < v_start)
    va_idx <- which(train_df$month_id >= v_start & train_df$month_id <= v_end)

    if (length(tr_idx) == 0 || length(va_idx) == 0) next

    nm <- paste0("Fold", i)
    index[[nm]] <- tr_idx
    indexOut[[nm]] <- va_idx

    fold_info[[nm]] <- data.frame(
      fold = nm,
      train_month_min = min(train_df$month_id[tr_idx]),
      train_month_max = max(train_df$month_id[tr_idx]),
      valid_month_min = min(train_df$month_id[va_idx]),
      valid_month_max = max(train_df$month_id[va_idx]),
      train_rows = length(tr_idx),
      valid_rows = length(va_idx)
    )
  }

  if (length(index) < 2) stop("Failed to create valid CV folds.")

  list(
    index = index,
    indexOut = indexOut,
    n_folds = length(index),
    fold_table = bind_rows(fold_info)
  )
}

# compute metrics for a single horizon/model as a data frame row
metric_row <- function(actual, pred, horizon, model_name) {
  mse <- mean((actual - pred)^2) # MSE on log DV
  data.frame(
    horizon = horizon,
    model = model_name,
    n = length(actual),
    mse = mse,
    rmse = sqrt(mse)
  )
}

# extract variable importance from a fitted caret model and return as a tidy data frame
extract_importance <- function(fit, horizon, model_name) {
  imp <- as.data.frame(varImp(fit, scale = FALSE)$importance)
  imp$feature <- rownames(imp)
  rownames(imp) <- NULL
  score_col <- setdiff(names(imp), "feature")[1]

  imp %>%
    transmute(
      horizon = horizon,
      model = model_name,
      feature = feature,
      importance = .data[[score_col]]
    )
}



# -----------------------------------------------------------------------------
# 4) train model horizon-by-horizon
# -----------------------------------------------------------------------------

# we will store results in lists and combine at the end for efficiency
all_metrics <- list()
all_predictions <- list()
all_tuning <- list()
all_resamples <- list()
all_importance <- list()
all_cv_schedule <- list()

for (h in horizons) {
  cat("\nTraining Model 1 for horizon ", h, "...\n", sep = "")

  train_path <- file.path(split_dir, paste0("train_horizon_", h, ".csv"))
  test_path <- file.path(split_dir, paste0("test_horizon_", h, ".csv"))

  if (!file.exists(train_path) || !file.exists(test_path)) {
    stop("Missing split files for horizon ", h, ". Run week_5_1_setup_data.R first.")
  }

  train_df <- read_csv(train_path, show_col_types = FALSE)
  test_df <- read_csv(test_path, show_col_types = FALSE)

  target_log <- paste0("log_fatalities_ahead_", h, "m")
  needed_cols <- c("log_lag1", "log_lag2", "log_lag3", "month_id", target_log)

  train_model <- train_df %>% filter(if_all(all_of(needed_cols), ~ !is.na(.x)))
  test_model <- test_df %>% filter(if_all(all_of(needed_cols), ~ !is.na(.x)))

  if (nrow(train_model) == 0 || nrow(test_model) == 0) {
    stop("No model-ready rows for horizon ", h)
  }

  cv <- make_time_cv_folds(
    train_df = train_model,
    n_folds = cv_folds,
    valid_window = cv_valid_window,
    min_train_months = cv_min_train_months
  )

  all_cv_schedule[[as.character(h)]] <- cv$fold_table %>%
    mutate(horizon = h, .before = 1)

  # cross validation setup for caret
  ctrl <- trainControl(
    method = "cv",
    number = cv$n_folds,
    index = cv$index,
    indexOut = cv$indexOut,
    savePredictions = "final",
    returnResamp = "all",
    allowParallel = parallel_enabled,
    verboseIter = FALSE
  )
  

  set.seed(7000 + h) # for reproducibility of CV folds and model training
  # train the random forest model using caret with ranger method
  fit <- caret::train(
    form = as.formula(paste0(target_log, " ~ log_lag1 + log_lag2 + log_lag3")),
    data = train_model,
    method = "ranger",
    metric = "RMSE",
    trControl = ctrl,
    tuneGrid = expand.grid(
      mtry = c(1, 2, 3),
      splitrule = "variance",
      min.node.size = 5
    ),
    num.trees = rf_num_trees,
    importance = "permutation",
    num.threads = 1
  )

  pred_log <- as.numeric(predict(fit, newdata = test_model))
  actual_log <- test_model[[target_log]]

  all_metrics[[as.character(h)]] <- metric_row(
    actual = actual_log,
    pred = pred_log,
    horizon = h,
    model_name = "rf_lag123_caret_cv"
  )

  all_predictions[[as.character(h)]] <- test_model %>%
    transmute(
      horizon = h,
      model = "rf_lag123_caret_cv",
      country_name,
      country_id,
      month,
      month_id,
      origin_month_id,
      target_month_id,
      actual_log = .data[[target_log]],
      pred_log = pred_log
    )

  # extract best tuning parameters and corresponding CV results
  best <- fit$bestTune
  all_tuning[[as.character(h)]] <- fit$results %>%
    filter(
      mtry == best$mtry,
      splitrule == best$splitrule,
      min.node.size == best$min.node.size
    ) %>%
    transmute(
      horizon = h,
      model = "rf_lag123_caret_cv",
      best_mtry = mtry,
      splitrule = splitrule,
      min_node_size = min.node.size,
      cv_mse = RMSE^2,
      cv_rmse = RMSE
    )

  all_resamples[[as.character(h)]] <- fit$resample %>%
    mutate(horizon = h, model = "rf_lag123_caret_cv", .before = 1)

  all_importance[[as.character(h)]] <- extract_importance(
    fit = fit,
    horizon = h,
    model_name = "rf_lag123_caret_cv"
  )
}

# -----------------------------------------------------------------------------
# 5) save outputs
# -----------------------------------------------------------------------------

# combine results into data frames and save as CSV files
metrics_tbl <- bind_rows(all_metrics) %>% arrange(horizon)
pred_tbl <- bind_rows(all_predictions) %>% arrange(horizon, country_id, month_id)
tuning_tbl <- bind_rows(all_tuning) %>% arrange(horizon)
resamples_tbl <- bind_rows(all_resamples) %>% arrange(horizon, Resample)
importance_tbl <- bind_rows(all_importance) %>% arrange(horizon, desc(importance))
cv_schedule_tbl <- bind_rows(all_cv_schedule) %>% arrange(horizon, fold)

# save them as CSV files in the output directory
write_csv(metrics_tbl, file.path(out_dir, "metrics_log_dv.csv"))
write_csv(pred_tbl, file.path(out_dir, "predictions_log_dv.csv"))
write_csv(tuning_tbl, file.path(out_dir, "tuning_log_dv.csv"))
write_csv(resamples_tbl, file.path(out_dir, "cv_resamples_log_dv.csv"))
write_csv(importance_tbl, file.path(out_dir, "variable_importance_log_dv.csv"))
write_csv(cv_schedule_tbl, file.path(out_dir, "cv_schedule.csv"))

# plot RMSE and MSE by horizon
rmse_plot <- metrics_tbl %>%
  ggplot(aes(x = horizon, y = rmse)) +
  geom_line(linewidth = 1.0, color = "#1f77b4") +
  geom_point(size = 2.2, color = "#1f77b4") +
  scale_x_continuous(breaks = horizons) +
  labs(
    title = "Model 1 (lag123): Test RMSE by Horizon",
    x = "Horizon (months ahead)",
    y = "RMSE (log DV)"
  ) +
  theme_minimal(base_size = 12)

mse_plot <- metrics_tbl %>%
  ggplot(aes(x = horizon, y = mse)) +
  geom_line(linewidth = 1.0, color = "#d62728") +
  geom_point(size = 2.2, color = "#d62728") +
  scale_x_continuous(breaks = horizons) +
  labs(
    title = "Model 1 (lag123): Test MSE by Horizon",
    x = "Horizon (months ahead)",
    y = "MSE (log DV)"
  ) +
  theme_minimal(base_size = 12)

# save plots as PNG files
ggsave(file.path(out_dir, "rmse_by_horizon_log_dv.png"), rmse_plot, width = 10, height = 6, dpi = 300)
ggsave(file.path(out_dir, "mse_by_horizon_log_dv.png"), mse_plot, width = 10, height = 6, dpi = 300)
