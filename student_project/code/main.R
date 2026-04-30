# =============================================================================
# Model Building Template
# =============================================================================
# Goal:
# Fit two theory-driven models and compare them against the two benchmarks
# =============================================================================

# -----------------------------------------------------------------------------
# Package setup
# -----------------------------------------------------------------------------
required_pkgs <- c(
  "dplyr",
  "readr",
  "ggplot2",
  "tidyr",
  "caret",
  "ranger",
  "gbm",
  "scoringRules",
  "tibble"
)
missing_pkgs <- required_pkgs[!required_pkgs %in% rownames(installed.packages())]
if (length(missing_pkgs) > 0) {
  repos <- getOption("repos")
  if (is.null(repos[["CRAN"]]) || identical(repos[["CRAN"]], "@CRAN@")) {
    options(repos = c(CRAN = "https://cloud.r-project.org"))
  }
  install.packages(missing_pkgs)
}
invisible(lapply(required_pkgs, library, character.only = TRUE))
set.seed(6933)

# -----------------------------------------------------------------------------
# Theory configuration
# -----------------------------------------------------------------------------
theory_name <- "Grievance Model"
theory_slug <- gsub("[^a-z0-9]+", "_", tolower(theory_name))
theory_slug <- gsub("^_|_$", "", theory_slug)

selected_predictors <- c(
    # Freedom House
    "fh_cl",                    # Civil liberties suppression
    "fh_pr",
    # V-Dem — Equality and exclusion dimensions of grievance
    "vdem_v2x_egaldem",        # Unequal distribution of political influence
    "vdem_v2x_egal",           # Overall structural equality
    "vdem_v2xeg_eqprotec",     # Unequal protection under law — core injustice indicator
    "vdem_v2xeg_eqaccess",     # Exclusion from political power
    "vdem_v2xeg_eqdr",         # Resource deprivation
    # V-Dem — Suppression of voice and association
    "vdem_v2x_freexp_altinf",  # Freedom of expression and alternative information
    "vdem_v2x_frassoc_thick",  # Freedom of association
    "vdem_v2x_suffr",          # Breadth of political inclusion via suffrage
    "vdem_v2x_cspart",         # Civil society exclusion
    # V-Dem — Electoral unfairness and injustice
    "vdem_v2elfrfair",         # Free and fair elections
    "vdem_v2elirreg",          # Electoral manipulation and injustice
    "vdem_v2elintim",          # Coercive exclusion through intimidation
    "vdem_v2elvotbuy"          # Corruption of political fairness
)

# Keep 1:7 for the final project.
horizon_ids <- 1:7

# The train/test design follows the course pipeline.
last_train_target  <- 372
first_test_target  <- 373
last_test_target   <- 420

# Modeling settings.
cv_folds           <- 5
cv_valid_window    <- 12
cv_min_train_months <- 120
rf_num_trees       <- 150
gbm_tree_values    <- c(100, 150)

quick_mode <- identical(Sys.getenv("MODEL_QUICK_MODE", unset = "0"), "1")
if (quick_mode) {
  cv_folds            <- 3
  cv_valid_window     <- 6
  cv_min_train_months <- 96
  rf_num_trees        <- 75
  gbm_tree_values     <- c(75, 100)
}

env_horizons <- Sys.getenv("MODEL_HORIZONS", unset = "")
if (nzchar(env_horizons)) {
  parsed_horizons <- suppressWarnings(as.integer(strsplit(env_horizons, ",")[[1]]))
  parsed_horizons <- parsed_horizons[!is.na(parsed_horizons)]
  if (length(parsed_horizons) > 0) horizon_ids <- parsed_horizons
}

env_rf_trees <- suppressWarnings(as.integer(Sys.getenv("MODEL_RF_TREES", unset = "")))
if (!is.na(env_rf_trees) && env_rf_trees > 0) rf_num_trees <- env_rf_trees

env_cv_folds <- suppressWarnings(as.integer(Sys.getenv("MODEL_CV_FOLDS", unset = "")))
if (!is.na(env_cv_folds) && env_cv_folds > 1) cv_folds <- env_cv_folds

env_valid_window <- suppressWarnings(as.integer(Sys.getenv("MODEL_VALID_WINDOW", unset = "")))
if (!is.na(env_valid_window) && env_valid_window > 0) cv_valid_window <- env_valid_window

env_min_train <- suppressWarnings(as.integer(Sys.getenv("MODEL_MIN_TRAIN_MONTHS", unset = "")))
if (!is.na(env_min_train) && env_min_train > 0) cv_min_train_months <- env_min_train

env_gbm_trees <- Sys.getenv("MODEL_GBM_TREES", unset = "")
if (nzchar(env_gbm_trees)) {
  parsed_gbm_trees <- suppressWarnings(as.integer(strsplit(env_gbm_trees, ",")[[1]]))
  parsed_gbm_trees <- parsed_gbm_trees[!is.na(parsed_gbm_trees) & parsed_gbm_trees > 0]
  if (length(parsed_gbm_trees) > 0) gbm_tree_values <- parsed_gbm_trees
}

# -----------------------------------------------------------------------------
# Path helpers
# -----------------------------------------------------------------------------
is_student_root <- function(path) {
  p <- normalizePath(path, winslash = "/", mustWork = FALSE)
  file.exists(file.path(p, "data", "imputed_dataset.rds")) &&
    file.exists(file.path(p, "benchmark_results", "csv", "benchmark_lag123_predictions.csv")) &&
    file.exists(file.path(p, "benchmark_results", "csv", "benchmark_roll3_predictions.csv"))
}

find_student_root_upward <- function(start_dir, max_depth = 10L) {
  cur <- normalizePath(start_dir, winslash = "/", mustWork = FALSE)
  for (i in seq_len(max_depth)) {
    if (is_student_root(cur)) return(normalizePath(cur, winslash = "/", mustWork = TRUE))
    parent <- dirname(cur)
    if (identical(parent, cur)) break
    cur <- parent
  }
  NA_character_
}

find_student_root_child_upward <- function(start_dir, max_depth = 10L) {
  cur <- normalizePath(start_dir, winslash = "/", mustWork = FALSE)
  for (i in seq_len(max_depth)) {
    child <- file.path(cur, "student_project")
    if (is_student_root(child)) return(normalizePath(child, winslash = "/", mustWork = TRUE))
    parent <- dirname(cur)
    if (identical(parent, cur)) break
    cur <- parent
  }
  NA_character_
}

resolve_student_root <- function() {
  env_root <- Sys.getenv("PROJECT_ROOT", unset = "")
  if (nzchar(env_root) && is_student_root(env_root))
    return(normalizePath(env_root, winslash = "/", mustWork = TRUE))
  from_cwd <- find_student_root_upward(getwd())
  if (!is.na(from_cwd)) return(from_cwd)
  from_cwd_child <- find_student_root_child_upward(getwd())
  if (!is.na(from_cwd_child)) return(from_cwd_child)
  args     <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg) > 0) {
    script_path <- sub("^--file=", "", file_arg[1])
    script_dir  <- dirname(normalizePath(script_path, winslash = "/", mustWork = FALSE))
    from_script <- find_student_root_upward(script_dir)
    if (!is.na(from_script)) return(from_script)
    from_script_child <- find_student_root_child_upward(script_dir)
    if (!is.na(from_script_child)) return(from_script_child)
  }
  stop(
    "Could not find the student_project workspace.\n",
    "Prepare the workspace first, then run this script from\n",
    "student_project/ or student_project/code/."
  )
}

student_root      <- resolve_student_root()
data_dir          <- file.path(student_root, "data")
benchmark_csv_dir <- file.path(student_root, "benchmark_results", "csv")
results_root      <- file.path(student_root, "results", theory_slug)
model_output_dir  <- file.path(results_root, "model_outputs")
comparison_dir    <- file.path(results_root, "comparison")
plots_dir         <- file.path(results_root, "plots")
logs_dir          <- file.path(results_root, "logs")

dir.create(results_root,     recursive = TRUE, showWarnings = FALSE)
dir.create(model_output_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(comparison_dir,   recursive = TRUE, showWarnings = FALSE)
dir.create(plots_dir,        recursive = TRUE, showWarnings = FALSE)
dir.create(logs_dir,         recursive = TRUE, showWarnings = FALSE)

spec_path         <- file.path(results_root, "model_specification.txt")
results_readme_path <- file.path(results_root, "read_me_please.txt")
run_summary_path  <- file.path(results_root, "run_summary.txt")

# -----------------------------------------------------------------------------
# Helper functions
# -----------------------------------------------------------------------------
recode_model_codes <- function(x) {
  dplyr::recode(
    x,
    rf_lag123_caret_cv = "benchmark_lag123",
    rf_roll3_caret_cv  = "benchmark_roll3",
    .default = x
  )
}

make_time_cv_folds <- function(train_data, n_folds, valid_window, min_train_months) {
  all_month_ids          <- sort(unique(train_data$month_id))
  max_month_id           <- max(all_month_ids)
  latest_validation_start <- max_month_id - valid_window + 1
  validation_starts      <- latest_validation_start - (n_folds - 1):0 * valid_window
  validation_starts      <- validation_starts[
    validation_starts >= (min(all_month_ids) + min_train_months)
  ]
  if (length(validation_starts) == 0)
    stop("No valid CV folds were created. Check training window and CV settings.")
  train_index <- list()
  valid_index <- list()
  for (i in seq_along(validation_starts)) {
    valid_start <- validation_starts[i]
    valid_end   <- valid_start + valid_window - 1
    fold_name   <- paste0("Fold", i)
    train_index[[fold_name]] <- which(train_data$month_id < valid_start)
    valid_index[[fold_name]] <- which(
      train_data$month_id >= valid_start & train_data$month_id <= valid_end
    )
  }
  list(index = train_index, indexOut = valid_index)
}

make_time_ctrl <- function(cv_object) {
  caret::trainControl(
    method          = "cv",
    index           = cv_object$index,
    indexOut        = cv_object$indexOut,
    savePredictions = "final",
    summaryFunction = caret::defaultSummary,
    allowParallel   = FALSE
  )
}

build_horizon_split <- function(panel_data, horizon_id, predictor_cols,
                                last_train, first_test, last_test) {
  outcome_name <- paste0("log_fatalities_ahead_", horizon_id, "m")
  train_rows <- panel_data %>%
    filter(month_id >= 1, month_id <= (last_train - horizon_id)) %>%
    mutate(horizon = horizon_id, origin_month_id = month_id,
           target_month_id = month_id + horizon_id)
  test_rows <- panel_data %>%
    filter(month_id >= (first_test - horizon_id),
           month_id <= (last_test  - horizon_id)) %>%
    mutate(horizon = horizon_id, origin_month_id = month_id,
           target_month_id = month_id + horizon_id)
  required_cols <- c(predictor_cols, "month_id", outcome_name)
  list(
    train        = train_rows %>% filter(if_all(all_of(required_cols), ~ !is.na(.x))),
    test         = test_rows  %>% filter(if_all(all_of(required_cols), ~ !is.na(.x))),
    outcome_name = outcome_name
  )
}

extract_best_row <- function(model_obj) {
  best_row <- model_obj$results
  for (nm in names(model_obj$bestTune))
    best_row <- best_row[best_row[[nm]] == model_obj$bestTune[[nm]][1], , drop = FALSE]
  best_row
}

extract_importance_table <- function(model_obj, horizon_id, model_code) {
  imp_raw     <- as.data.frame(caret::varImp(model_obj, scale = FALSE)$importance)
  imp_raw$feature <- rownames(imp_raw)
  rownames(imp_raw) <- NULL
  imp_col <- setdiff(names(imp_raw), "feature")[1]
  imp_raw %>%
    transmute(horizon = horizon_id, model = model_code, feature = feature,
              importance = as.numeric(.data[[imp_col]])) %>%
    arrange(horizon, desc(importance))
}

fit_theory_rf <- function(train_data, test_data, predictors, outcome_name,
                          cv_object, num_trees = 150) {
  model_formula <- as.formula(paste(outcome_name, "~", paste(predictors, collapse = " + ")))
  ctrl          <- make_time_ctrl(cv_object)
  mtry_value    <- min(max(1L, floor(sqrt(length(predictors)))), length(predictors))
  rf_model <- caret::train(
    form      = model_formula,
    data      = train_data,
    method    = "ranger",
    metric    = "RMSE",
    trControl = ctrl,
    tuneGrid  = expand.grid(mtry = mtry_value, splitrule = "variance",
                            min.node.size = c(5, 10)),
    num.trees   = num_trees,
    importance  = "permutation",
    num.threads = 1,
    verbose     = FALSE
  )
  best_row <- extract_best_row(rf_model)
  list(model       = rf_model,
       predictions = as.numeric(stats::predict(rf_model, newdata = test_data)),
       cv_rmse     = as.numeric(best_row$RMSE[1]))
}

fit_theory_gbm <- function(train_data, test_data, predictors, outcome_name,
                           cv_object, tree_values) {
  model_formula <- as.formula(paste(outcome_name, "~", paste(predictors, collapse = " + ")))
  ctrl          <- make_time_ctrl(cv_object)
  gbm_model <- caret::train(
    form      = model_formula,
    data      = train_data,
    method    = "gbm",
    metric    = "RMSE",
    trControl = ctrl,
    tuneGrid  = expand.grid(n.trees = tree_values, interaction.depth = 2,
                            shrinkage = 0.05, n.minobsinnode = 10),
    verbose   = FALSE
  )
  best_row <- extract_best_row(gbm_model)
  list(model       = gbm_model,
       predictions = as.numeric(stats::predict(gbm_model, newdata = test_data)),
       cv_rmse     = as.numeric(best_row$RMSE[1]))
}

compute_probabilistic_metrics <- function(prediction_table, tuning_table) {
  joined <- prediction_table %>%
    left_join(tuning_table %>% select(horizon, model, cv_rmse),
              by = c("horizon", "model")) %>%
    mutate(sd_pred = pmax(cv_rmse, 1e-6))
  joined %>%
    mutate(
      crps_prob = scoringRules::crps_norm(y = actual_log, mean = pred_log, sd = sd_pred),
      lower80   = qnorm(0.10, mean = pred_log, sd = sd_pred),
      upper80   = qnorm(0.90, mean = pred_log, sd = sd_pred),
      covered80 = as.integer(actual_log >= lower80 & actual_log <= upper80),
      width80   = upper80 - lower80
    ) %>%
    group_by(horizon, model) %>%
    summarise(n = n(), crps_prob = mean(crps_prob), coverage80 = mean(covered80),
              calibration_error80 = abs(coverage80 - 0.80),
              sharpness_width80 = mean(width80), .groups = "drop") %>%
    arrange(horizon, crps_prob)
}

save_model_outputs <- function(metric_table, prediction_table, tuning_table,
                               importance_table, model_code) {
  model_dir <- file.path(model_output_dir, model_code)
  dir.create(model_dir, recursive = TRUE, showWarnings = FALSE)
  readr::write_csv(metric_table,     file.path(model_dir, "metrics_log_dv.csv"))
  readr::write_csv(prediction_table, file.path(model_dir, "predictions_log_dv.csv"))
  readr::write_csv(tuning_table,     file.path(model_dir, "tuning_log_dv.csv"))
  readr::write_csv(importance_table, file.path(model_dir, "variable_importance_log_dv.csv"))
  det_crps_table <- prediction_table %>%
    mutate(crps_det = scoringRules::crps_sample(
      y = actual_log, dat = matrix(pred_log, ncol = 1))) %>%
    group_by(horizon, model) %>%
    summarise(n = n(), crps = mean(crps_det), .groups = "drop")
  prob_table <- compute_probabilistic_metrics(prediction_table, tuning_table)
  readr::write_csv(det_crps_table, file.path(model_dir, "metrics_crps_log_dv.csv"))
  readr::write_csv(prob_table,     file.path(model_dir, "metrics_probabilistic_log_dv.csv"))
}

write_results_guidance <- function(output_path, theory_name, theory_slug,
                                   student_root, quick_mode,
                                   best_grievance_model, benchmark_best_model,
                                   grievance_win_summary) {
  lines <- c(
    paste("Theory:", theory_name),
    paste("Theory slug:", theory_slug),
    paste("Project root:", student_root),
    paste("Quick mode:", ifelse(quick_mode, "ON", "OFF")),
    "", "Folder guide", "------------",
    "model_outputs/: per-model predictions, tuning tables, metrics, and feature importance.",
    "comparison/: benchmark comparison tables and summary scorecards.",
    "plots/: polished figures for the memo and presentation.",
    "", "Topline interpretation", "----------------------",
    paste("Best Grievance model by average RMSE:", best_grievance_model),
    paste("Best overall benchmark by average RMSE:", benchmark_best_model), ""
  )
  if (nrow(grievance_win_summary) > 0) {
    lines <- c(lines, "Grievance benchmark scorecard", "-----------------------------")
    for (i in seq_len(nrow(grievance_win_summary))) {
      row_i <- grievance_win_summary[i, ]
      lines <- c(lines, paste0(
        row_i$model, " on ", row_i$metric, ": ",
        row_i$horizons_beating_best_benchmark, " horizon wins, average gap ",
        sprintf("%.3f", row_i$mean_gap_to_best_benchmark)
      ))
    }
  }
  lines <- c(lines, "", "Recommended starting files", "--------------------------",
    "comparison/comparison_all_metrics_log_dv.csv",
    "comparison/grievance_vs_benchmark_summary.csv",
    "comparison/grievance_model_overview.csv",
    "plots/all_models_metrics_by_horizon.png",
    "plots/top_feature_importance.png")
  writeLines(lines, output_path)
}

project_plot_theme <- function() {
  ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(
      plot.title    = ggplot2::element_text(face = "bold", size = 14),
      plot.subtitle = ggplot2::element_text(color = "gray30"),
      panel.grid.minor = ggplot2::element_blank(),
      strip.text    = ggplot2::element_text(face = "bold"),
      legend.position = "bottom",
      legend.title  = ggplot2::element_text(face = "bold")
    )
}

# -----------------------------------------------------------------------------
# Read data
# -----------------------------------------------------------------------------
imputed_path          <- file.path(data_dir, "imputed_dataset.rds")
benchmark_lag_path    <- file.path(benchmark_csv_dir, "benchmark_lag123_predictions.csv")
benchmark_roll_path   <- file.path(benchmark_csv_dir, "benchmark_roll3_predictions.csv")
benchmark_lag_tuning_path  <- file.path(benchmark_csv_dir, "benchmark_lag123_tuning.csv")
benchmark_roll_tuning_path <- file.path(benchmark_csv_dir, "benchmark_roll3_tuning.csv")

required_inputs <- c(imputed_path, benchmark_lag_path, benchmark_roll_path,
                     benchmark_lag_tuning_path, benchmark_roll_tuning_path)
missing_inputs  <- required_inputs[!file.exists(required_inputs)]
if (length(missing_inputs) > 0)
  stop("Missing required input file(s):\n", paste(missing_inputs, collapse = "\n"))

imputed_dataset <- readRDS(imputed_path)

missing_predictors <- setdiff(selected_predictors, names(imputed_dataset))
if (length(missing_predictors) > 0)
  stop("These selected predictors are not in the imputed dataset:\n",
       paste(missing_predictors, collapse = "\n"))

panel_with_predictors <- imputed_dataset %>%
  mutate(country_name = as.character(country),
         country_id   = as.integer(country_id),
         month        = as.Date(month),
         month_id     = as.integer(month_id)) %>%
  transmute(
    country_name, country_id, month, month_id,
    log_fatalities_ahead_1m = ln_ged_best_sb_s1,
    log_fatalities_ahead_2m = ln_ged_best_sb_s2,
    log_fatalities_ahead_3m = ln_ged_best_sb_s3,
    log_fatalities_ahead_4m = ln_ged_best_sb_s4,
    log_fatalities_ahead_5m = ln_ged_best_sb_s5,
    log_fatalities_ahead_6m = ln_ged_best_sb_s6,
    log_fatalities_ahead_7m = ln_ged_best_sb_s7,
    across(all_of(selected_predictors))
  ) %>%
  arrange(country_id, month_id)

benchmark_lag_predictions <- readr::read_csv(benchmark_lag_path, show_col_types = FALSE) %>%
  mutate(model = recode_model_codes(model))
benchmark_roll_predictions <- readr::read_csv(benchmark_roll_path, show_col_types = FALSE) %>%
  mutate(model = recode_model_codes(model))
benchmark_lag_tuning <- readr::read_csv(benchmark_lag_tuning_path, show_col_types = FALSE) %>%
  mutate(model = recode_model_codes(model)) %>% select(horizon, model, cv_rmse)
benchmark_roll_tuning <- readr::read_csv(benchmark_roll_tuning_path, show_col_types = FALSE) %>%
  mutate(model = recode_model_codes(model)) %>% select(horizon, model, cv_rmse)

writeLines(c(
  paste("Theory:", theory_name),
  paste("Theory slug:", theory_slug),
  paste("Quick mode:", ifelse(quick_mode, "TRUE", "FALSE")),
  paste("CV folds:", cv_folds),
  paste("Validation window:", cv_valid_window),
  paste("Minimum train months:", cv_min_train_months),
  paste("RF trees:", rf_num_trees),
  paste("GBM trees:", paste(gbm_tree_values, collapse = ", ")),
  "", "Selected predictors:", selected_predictors
), spec_path)

# -----------------------------------------------------------------------------
# Fit the two Grievance models
# -----------------------------------------------------------------------------
rf_metrics_list     <- list(); rf_predictions_list  <- list()
rf_tuning_list      <- list(); rf_importance_list   <- list()
gbm_metrics_list    <- list(); gbm_predictions_list <- list()
gbm_tuning_list     <- list(); gbm_importance_list  <- list()

for (horizon_id in horizon_ids) {
  cat("Fitting horizon ", horizon_id, " of ", max(horizon_ids), "...\n", sep = "")

  split_obj <- build_horizon_split(
    panel_data    = panel_with_predictors,
    horizon_id    = horizon_id,
    predictor_cols = selected_predictors,
    last_train    = last_train_target,
    first_test    = first_test_target,
    last_test     = last_test_target
  )
  train_ready  <- split_obj$train
  test_ready   <- split_obj$test
  outcome_name <- split_obj$outcome_name

  cv_object <- make_time_cv_folds(
    train_data        = train_ready,
    n_folds           = cv_folds,
    valid_window      = cv_valid_window,
    min_train_months  = cv_min_train_months
  )

  # --- Random Forest ---
  rf_fit   <- fit_theory_rf(train_ready, test_ready, selected_predictors,
                            outcome_name, cv_object, rf_num_trees)
  rf_actual <- test_ready[[outcome_name]]
  rf_pred   <- rf_fit$predictions
  rf_best   <- rf_fit$model$bestTune

  rf_metrics_list[[as.character(horizon_id)]] <- tibble::tibble(
    horizon = horizon_id, model = "grievance_rf",
    n = length(rf_actual),
    mse  = mean((rf_actual - rf_pred)^2),
    rmse = sqrt(mean((rf_actual - rf_pred)^2))
  )
  rf_predictions_list[[as.character(horizon_id)]] <- test_ready %>%
    transmute(horizon = horizon_id, model = "grievance_rf",
              country_name, country_id, month, month_id,
              origin_month_id, target_month_id,
              actual_log = .data[[outcome_name]], pred_log = rf_pred)
  rf_tuning_list[[as.character(horizon_id)]] <- tibble::tibble(
    horizon = horizon_id, model = "grievance_rf",
    best_mtry    = rf_best$mtry[1],
    splitrule    = as.character(rf_best$splitrule[1]),
    min_node_size = rf_best$min.node.size[1],
    cv_mse  = rf_fit$cv_rmse^2,
    cv_rmse = rf_fit$cv_rmse
  )
  rf_importance_list[[as.character(horizon_id)]] <- extract_importance_table(
    rf_fit$model, horizon_id, "grievance_rf")

  # --- GBM ---
  gbm_fit   <- fit_theory_gbm(train_ready, test_ready, selected_predictors,
                              outcome_name, cv_object, gbm_tree_values)
  gbm_actual <- test_ready[[outcome_name]]
  gbm_pred   <- gbm_fit$predictions
  gbm_best   <- gbm_fit$model$bestTune

  gbm_metrics_list[[as.character(horizon_id)]] <- tibble::tibble(
    horizon = horizon_id, model = "grievance_gbm",
    n = length(gbm_actual),
    mse  = mean((gbm_actual - gbm_pred)^2),
    rmse = sqrt(mean((gbm_actual - gbm_pred)^2))
  )
  gbm_predictions_list[[as.character(horizon_id)]] <- test_ready %>%
    transmute(horizon = horizon_id, model = "grievance_gbm",
              country_name, country_id, month, month_id,
              origin_month_id, target_month_id,
              actual_log = .data[[outcome_name]], pred_log = gbm_pred)
  gbm_tuning_list[[as.character(horizon_id)]] <- tibble::tibble(
    horizon = horizon_id, model = "grievance_gbm",
    best_mtry    = NA_real_,
    splitrule    = paste0("gbm_depth_", gbm_best$interaction.depth[1],
                          "_trees_", gbm_best$n.trees[1]),
    min_node_size = gbm_best$n.minobsinnode[1],
    cv_mse  = gbm_fit$cv_rmse^2,
    cv_rmse = gbm_fit$cv_rmse
  )
  gbm_importance_list[[as.character(horizon_id)]] <- extract_importance_table(
    gbm_fit$model, horizon_id, "grievance_gbm")
}

rf_metric_table      <- bind_rows(rf_metrics_list)      %>% arrange(horizon)
rf_prediction_table  <- bind_rows(rf_predictions_list)  %>% arrange(horizon, country_id, month_id)
rf_tuning_table      <- bind_rows(rf_tuning_list)       %>% arrange(horizon)
rf_importance_table  <- bind_rows(rf_importance_list)   %>% arrange(horizon, desc(importance))

gbm_metric_table     <- bind_rows(gbm_metrics_list)     %>% arrange(horizon)
gbm_prediction_table <- bind_rows(gbm_predictions_list) %>% arrange(horizon, country_id, month_id)
gbm_tuning_table     <- bind_rows(gbm_tuning_list)      %>% arrange(horizon)
gbm_importance_table <- bind_rows(gbm_importance_list)  %>% arrange(horizon, desc(importance))

save_model_outputs(rf_metric_table,  rf_prediction_table,  rf_tuning_table,
                   rf_importance_table,  "grievance_rf")
save_model_outputs(gbm_metric_table, gbm_prediction_table, gbm_tuning_table,
                   gbm_importance_table, "grievance_gbm")

# -----------------------------------------------------------------------------
# Compare against the benchmark models
# -----------------------------------------------------------------------------
model_order_codes <- c("grievance_rf", "grievance_gbm", "benchmark_lag123", "benchmark_roll3")
key_columns       <- c("horizon", "country_id", "month_id", "origin_month_id", "target_month_id")

all_predictions <- bind_rows(
  rf_prediction_table, gbm_prediction_table,
  benchmark_lag_predictions, benchmark_roll_predictions
) %>% filter(model %in% model_order_codes)

matched_keys <- all_predictions %>%
  distinct(across(all_of(c(key_columns, "model")))) %>%
  count(across(all_of(key_columns)), name = "n_models") %>%
  filter(n_models == length(model_order_codes)) %>%
  select(all_of(key_columns))

matched_predictions <- all_predictions %>%
  inner_join(matched_keys, by = key_columns) %>%
  arrange(horizon, country_id, month_id, model)

comparison_point_metrics <- matched_predictions %>%
  group_by(horizon, model) %>%
  summarise(n = n(), mse = mean((actual_log - pred_log)^2),
            rmse = sqrt(mse), .groups = "drop") %>%
  arrange(horizon, rmse)

comparison_det_crps <- matched_predictions %>%
  mutate(crps_det = scoringRules::crps_sample(
    y = actual_log, dat = matrix(pred_log, ncol = 1))) %>%
  group_by(horizon, model) %>%
  summarise(n = n(), crps_det = mean(crps_det), .groups = "drop") %>%
  arrange(horizon, crps_det)

comparison_tuning_table <- bind_rows(
  rf_tuning_table  %>% select(horizon, model, cv_rmse),
  gbm_tuning_table %>% select(horizon, model, cv_rmse),
  benchmark_lag_tuning, benchmark_roll_tuning
)

comparison_prob_metrics <- compute_probabilistic_metrics(
  matched_predictions, comparison_tuning_table)

comparison_all_metrics <- comparison_point_metrics %>%
  left_join(comparison_det_crps %>% select(horizon, model, crps_det),
            by = c("horizon", "model")) %>%
  left_join(comparison_prob_metrics %>%
              select(horizon, model, crps_prob, coverage80,
                     calibration_error80, sharpness_width80),
            by = c("horizon", "model")) %>%
  arrange(horizon, rmse)

comparison_long_metrics <- comparison_all_metrics %>%
  select(horizon, model, mse, rmse, crps_det, crps_prob) %>%
  pivot_longer(cols = c(mse, rmse, crps_det, crps_prob),
               names_to = "metric", values_to = "value")

best_model_by_horizon_metric <- comparison_long_metrics %>%
  group_by(horizon, metric) %>%
  slice_min(order_by = value, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  rename(best_model = model, best_value = value) %>%
  arrange(metric, horizon)

best_benchmark_by_horizon_metric <- comparison_long_metrics %>%
  filter(model %in% c("benchmark_lag123", "benchmark_roll3")) %>%
  group_by(horizon, metric) %>%
  slice_min(order_by = value, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  rename(best_benchmark = model, best_benchmark_value = value)

grievance_vs_benchmark_by_horizon <- comparison_long_metrics %>%
  filter(model %in% c("grievance_rf", "grievance_gbm")) %>%
  left_join(best_benchmark_by_horizon_metric, by = c("horizon", "metric")) %>%
  mutate(gap_to_best_benchmark      = value - best_benchmark_value,
         improved_on_best_benchmark = gap_to_best_benchmark < 0) %>%
  arrange(metric, horizon, value)

grievance_vs_benchmark_summary <- grievance_vs_benchmark_by_horizon %>%
  group_by(model, metric) %>%
  summarise(horizons_beating_best_benchmark = sum(improved_on_best_benchmark),
            mean_gap_to_best_benchmark      = mean(gap_to_best_benchmark),
            best_single_horizon_gap         = min(gap_to_best_benchmark),
            .groups = "drop") %>%
  arrange(metric, mean_gap_to_best_benchmark)

grievance_model_overview <- comparison_all_metrics %>%
  group_by(model) %>%
  summarise(horizons = n(),
            mean_mse  = mean(mse),  mean_rmse = mean(rmse),
            mean_crps_det  = mean(crps_det),
            mean_crps_prob = mean(crps_prob),
            mean_coverage80          = mean(coverage80),
            mean_calibration_error80 = mean(calibration_error80),
            mean_sharpness_width80   = mean(sharpness_width80),
            .groups = "drop") %>%
  arrange(mean_rmse)

grievance_importance_all <- bind_rows(rf_importance_table, gbm_importance_table) %>%
  group_by(model, horizon) %>%
  arrange(desc(importance), .by_group = TRUE) %>%
  mutate(importance_share   = importance / sum(importance),
         rank_within_horizon = row_number()) %>%
  ungroup()

grievance_feature_importance_summary <- grievance_importance_all %>%
  group_by(model, feature) %>%
  summarise(mean_importance       = mean(importance),
            mean_importance_share = mean(importance_share),
            mean_rank             = mean(rank_within_horizon),
            .groups = "drop") %>%
  arrange(model, mean_rank)

readr::write_csv(matched_predictions,
  file.path(comparison_dir, "matched_predictions_all_models_log_dv.csv"))
readr::write_csv(comparison_point_metrics,
  file.path(comparison_dir, "comparison_metrics_log_dv.csv"))
readr::write_csv(comparison_det_crps,
  file.path(comparison_dir, "comparison_crps_det_log_dv.csv"))
readr::write_csv(comparison_prob_metrics,
  file.path(comparison_dir, "comparison_probabilistic_metrics_log_dv.csv"))
readr::write_csv(comparison_all_metrics,
  file.path(comparison_dir, "comparison_all_metrics_log_dv.csv"))
readr::write_csv(best_model_by_horizon_metric,
  file.path(comparison_dir, "best_model_by_horizon_metric_log_dv.csv"))
readr::write_csv(grievance_vs_benchmark_by_horizon,
  file.path(comparison_dir, "grievance_vs_best_benchmark_by_horizon.csv"))
readr::write_csv(grievance_vs_benchmark_summary,
  file.path(comparison_dir, "grievance_vs_benchmark_summary.csv"))
readr::write_csv(grievance_model_overview,
  file.path(comparison_dir, "grievance_model_overview.csv"))
readr::write_csv(grievance_feature_importance_summary,
  file.path(comparison_dir, "grievance_feature_importance_summary.csv"))

# -----------------------------------------------------------------------------
# Plots
# -----------------------------------------------------------------------------
model_label_map <- c(
  benchmark_lag123 = "Benchmark RF (Lag 1-3)",
  benchmark_roll3  = "Benchmark RF (Roll3)",
  grievance_rf     = "Grievance Random Forest",
  grievance_gbm    = "Grievance GBM"
)
model_color_map <- c(
  "Benchmark RF (Lag 1-3)"  = "#0B5CAB",
  "Benchmark RF (Roll3)"    = "#5C88C4",
  "Grievance Random Forest" = "#E87511",
  "Grievance GBM"           = "#A33B20"
)
metric_label_map <- c(
  mse = "MSE", rmse = "RMSE",
  crps_det = "CRPS (Deterministic)", crps_prob = "CRPS (Probabilistic)"
)

comparison_plot_table <- comparison_long_metrics %>%
  mutate(model_label  = recode(model,  !!!model_label_map),
         metric_label = recode(metric, !!!metric_label_map))

metrics_plot <- ggplot2::ggplot(
  comparison_plot_table,
  ggplot2::aes(x = horizon, y = value, color = model_label)
) +
  ggplot2::geom_point(size = 2.1) +
  ggplot2::scale_color_manual(values = model_color_map) +
  ggplot2::scale_x_continuous(breaks = sort(unique(horizon_ids))) +
  ggplot2::facet_wrap(~ metric_label, scales = "free_y", ncol = 2) +
  ggplot2::labs(title    = paste("Model Comparison:", theory_name),
                subtitle = "Grievance models compared against both benchmark models",
                x = "Forecast horizon", y = "Metric value", color = "Model") +
  project_plot_theme()
if (length(unique(comparison_plot_table$horizon)) > 1L)
  metrics_plot <- metrics_plot + ggplot2::geom_line(linewidth = 1.05)
ggplot2::ggsave(file.path(plots_dir, "all_models_metrics_by_horizon.png"),
                metrics_plot, width = 12, height = 8, dpi = 320)

calibration_plot <- comparison_prob_metrics %>%
  mutate(model_label = recode(model, !!!model_label_map)) %>%
  ggplot2::ggplot(ggplot2::aes(x = horizon, y = coverage80, color = model_label)) +
  ggplot2::geom_hline(yintercept = 0.80, linetype = "dashed", color = "gray35") +
  ggplot2::geom_point(size = 2.1) +
  ggplot2::scale_color_manual(values = model_color_map) +
  ggplot2::scale_x_continuous(breaks = sort(unique(horizon_ids))) +
  ggplot2::labs(title    = paste("80% Calibration by Horizon:", theory_name),
                subtitle = "Dashed line marks ideal 80% coverage",
                x = "Forecast horizon", y = "Empirical 80% coverage", color = "Model") +
  project_plot_theme()
if (length(unique(comparison_prob_metrics$horizon)) > 1L)
  calibration_plot <- calibration_plot + ggplot2::geom_line(linewidth = 1.05)
ggplot2::ggsave(file.path(plots_dir, "calibration80_by_horizon.png"),
                calibration_plot, width = 11, height = 6.5, dpi = 320)

sharpness_plot <- comparison_prob_metrics %>%
  mutate(model_label = recode(model, !!!model_label_map)) %>%
  ggplot2::ggplot(ggplot2::aes(x = horizon, y = sharpness_width80, color = model_label)) +
  ggplot2::geom_point(size = 2.1) +
  ggplot2::scale_color_manual(values = model_color_map) +
  ggplot2::scale_x_continuous(breaks = sort(unique(horizon_ids))) +
  ggplot2::labs(title    = paste("80% Interval Sharpness by Horizon:", theory_name),
                subtitle = "Lower values indicate narrower predictive intervals",
                x = "Forecast horizon", y = "Average 80% interval width", color = "Model") +
  project_plot_theme()
if (length(unique(comparison_prob_metrics$horizon)) > 1L)
  sharpness_plot <- sharpness_plot + ggplot2::geom_line(linewidth = 1.05)
ggplot2::ggsave(file.path(plots_dir, "sharpness80_by_horizon.png"),
                sharpness_plot, width = 11, height = 6.5, dpi = 320)

importance_plot_data <- grievance_feature_importance_summary %>%
  group_by(model) %>%
  arrange(mean_rank, .by_group = TRUE) %>%
  slice_head(n = 6) %>%
  ungroup() %>%
  mutate(model_label = recode(model, !!!model_label_map),
         feature     = factor(feature, levels = rev(unique(feature))))

importance_plot <- ggplot2::ggplot(
  importance_plot_data,
  ggplot2::aes(x = feature, y = mean_importance_share, fill = model_label)
) +
  ggplot2::geom_col(width = 0.7, show.legend = FALSE) +
  ggplot2::coord_flip() +
  ggplot2::facet_wrap(~ model_label, scales = "free_y") +
  ggplot2::scale_fill_manual(values = model_color_map) +
  ggplot2::labs(title    = paste("Top Grievance Indicators by Model:", theory_name),
                subtitle = "Average feature importance share across forecast horizons",
                x = NULL, y = "Average importance share") +
  project_plot_theme()
ggplot2::ggsave(file.path(plots_dir, "top_feature_importance.png"),
                importance_plot, width = 11, height = 7, dpi = 320)

best_grievance_model <- grievance_model_overview %>%
  filter(model %in% c("grievance_rf", "grievance_gbm")) %>%
  slice_min(order_by = mean_rmse, n = 1, with_ties = FALSE) %>%
  pull(model)
benchmark_best_model <- grievance_model_overview %>%
  filter(model %in% c("benchmark_lag123", "benchmark_roll3")) %>%
  slice_min(order_by = mean_rmse, n = 1, with_ties = FALSE) %>%
  pull(model)

write_results_guidance(
  output_path         = results_readme_path,
  theory_name         = theory_name,
  theory_slug         = theory_slug,
  student_root        = student_root,
  quick_mode          = quick_mode,
  best_grievance_model = best_grievance_model,
  benchmark_best_model = benchmark_best_model,
  grievance_win_summary = grievance_vs_benchmark_summary
)

writeLines(c(
  paste("Theory:", theory_name),
  paste("Best Grievance model by average RMSE:", best_grievance_model),
  paste("Best benchmark model by average RMSE:", benchmark_best_model),
  "", "Average performance by model", "----------------------------",
  apply(grievance_model_overview, 1, function(row_i) {
    paste(row_i[["model"]],
          "| mean RMSE =", sprintf("%.3f", as.numeric(row_i[["mean_rmse"]])),
          "| mean CRPS =", sprintf("%.3f", as.numeric(row_i[["mean_crps_prob"]])))
  })
), run_summary_path)

cat("Finished. Outputs saved to:\n", results_root, "\n", sep = "")