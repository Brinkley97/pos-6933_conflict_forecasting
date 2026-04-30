# =============================================================================
# Verify Predictor Availability
# =============================================================================
# Goal:
# Load the imputed dataset and check if all selected predictors exist.
# =============================================================================

# --- Path helper functions (to find the data file) ---
is_student_root <- function(path) {
  p <- normalizePath(path, winslash = "/", mustWork = FALSE)
  file.exists(file.path(p, "data", "imputed_dataset.rds")) &&
    file.exists(file.path(p, "benchmark_results", "csv", "benchmark_lag123_predictions.csv"))
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
    "Run this script from student_project/ or student_project/code/."
  )
}

# --- Main logic to load data and check variables ---
student_root <- resolve_student_root()
data_dir     <- file.path(student_root, "data")
imputed_path <- file.path(data_dir, "imputed_dataset.rds")

# Load the dataset
imputed_dataset <- readRDS(imputed_path)
all_variable_names <- names(imputed_dataset)

# Your chosen predictors for the Grievance Model
selected_predictors <- c(
  "vdem_v2x_egaldem", "vdem_v2x_egal", "vdem_v2xeg_eqprotec", 
  "vdem_v2xeg_eqaccess", "vdem_v2xeg_eqdr", "vdem_v2x_freexp_altinf", 
  "vdem_v2x_frassoc_thick", "vdem_v2x_suffr", "vdem_v2x_cspart", 
  "vdem_v2elfrfair", "vdem_v2elirreg", "vdem_v2elintim", 
  "vdem_v2elvotbuy", "fh_pr", "fh_cl"
)

# Check for any missing predictors
missing_predictors <- setdiff(selected_predictors, all_variable_names)

# Report the result
if (length(missing_predictors) == 0) {
  cat("Success: All selected predictors were found in the dataset.\n")
} else {
  cat("Error: The following predictors are missing from the dataset:\n")
  print(missing_predictors)
}
