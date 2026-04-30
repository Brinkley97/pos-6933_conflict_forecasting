# =============================================================================
# Print All Categorized Variable Names with Descriptions
# =============================================================================
# Goal:
# Load the imputed dataset and print all variable names, categorized by data
# source, including full names and descriptions based on the project documents.
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

# --- Main logic to load data and print categorized variables ---
student_root <- resolve_student_root()
data_dir     <- file.path(student_root, "data")
imputed_path <- file.path(data_dir, "imputed_dataset.rds")

# Load the dataset
imputed_dataset <- readRDS(imputed_path)
all_variable_names <- names(imputed_dataset)

# --- Define the descriptions for each data source based on project documents ---
source_info <- list(
  "vdem_" = list(
    full_name = "Varieties of Democracy (V-Dem)",
    description = "Captures multiple dimensions of democracy, such as electoral contestation, liberal protections, participation, and equality of political influence [3]."
  ),
  "wgi_" = list(
    full_name = "Worldwide Governance Indicators (WGI)",
    description = "Measures dimensions of governance including voice and accountability, government effectiveness, and rule of law [2, 3]."
  ),
  "polity_" = list(
    full_name = "Polity Dataset",
    description = "Provides a broad score for a country's regime type, capturing its overall democratic or autocratic structure [2, 3]."
  ),
  "fh_" = list(
    full_name = "Freedom House",
    description = "Measures political rights and civil liberties [2, 3]."
  ),
  "qog_" = list(
    full_name = "Quality of Government (QoG)",
    description = "Provides data on economic and governance factors, such as the value of oil and gas production and income inequality [4]."
  ),
  "wdi_" = list(
    full_name = "World Development Indicators (WDI)",
    description = "Contains data on economic structure, including GDP per capita and the share of agriculture and industry in the economy [4]."
  ),
  "ged_" = list(
    full_name = "Georeferenced Event Dataset (GED)",
    description = "Provides data on conflict events, including fatality counts which are used as the outcome variable in this project (e.g., ln_ged_best_sb_s1) [3]."
  )
)

source_prefixes <- names(source_info)

# Loop through each prefix, find matching variables, and print them with descriptions
for (prefix in source_prefixes) {
  # Get the full name and description from our list
  info <- source_info[[prefix]]
  
  # Find all variables that start with the current prefix
  matching_vars <- grep(paste0("^", prefix), all_variable_names, value = TRUE)
  
  # Only print the category if variables exist in the dataset
  if (length(matching_vars) > 0) {
    cat(paste("---", info$full_name, "---\n"))
    cat(paste("Description:", info$description, "\n\n"))
    cat("Variables in this category:\n")
    print(matching_vars)
    cat("\n\n") # Add extra space between major categories
  }
}

# Find and print any variables that did not match the main categories
matched_pattern <- paste(paste0("^", source_prefixes), collapse = "|")
unmatched_vars <- all_variable_names[!grepl(matched_pattern, all_variable_names)]

if (length(unmatched_vars) > 0) {
    cat("--- OTHER VARIABLES ---\n")
    cat("Description: These are variables that do not belong to the main data sources, such as country and time identifiers.\n\n")
    cat("Variables in this category:\n")
    print(unmatched_vars)
    cat("\n")
}
