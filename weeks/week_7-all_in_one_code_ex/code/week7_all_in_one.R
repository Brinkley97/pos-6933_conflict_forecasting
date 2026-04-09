# =============================================================================
# Week 7 - Full Imputation Pipeline using missRanger
#
# data_raw:( check week 6 code for details)
# - data_raw/ged_country_with_governance_iso3_year.rds
#
# Output:
# - output/week7_removed_variables.txt
# - data_processed/week7_imputed_dataset.rds
# - data_processed/week7_imputed_dataset.csv.gz
#
# =============================================================================


# package setup
required_pkgs <- c("readr", "dplyr", "digest", "missRanger")
missing_pkgs <- required_pkgs[!required_pkgs %in% rownames(installed.packages())]
if (length(missing_pkgs) > 0) {
  repos <- getOption("repos")
  if (is.null(repos[["CRAN"]]) || identical(repos[["CRAN"]], "@CRAN@")) {
    options(repos = c(CRAN = "https://cloud.r-project.org"))
  }
  install.packages(missing_pkgs)
}
invisible(lapply(required_pkgs, library, character.only = TRUE))

# helper functions
is_week7_root <- function(path) {
  p <- normalizePath(path, winslash = "/", mustWork = FALSE)
  dir.exists(file.path(p, "data_raw")) && dir.exists(file.path(p, "code"))
}

find_week7_root_upward <- function(start_dir, max_depth = 10L) {
  cur <- normalizePath(start_dir, winslash = "/", mustWork = FALSE)
  for (i in seq_len(max_depth)) {
    if (is_week7_root(cur)) {
      return(normalizePath(cur, winslash = "/", mustWork = TRUE))
    }
    parent <- dirname(cur)
    if (identical(parent, cur)) break
    cur <- parent
  }
  NA_character_
}

find_week7_child_upward <- function(start_dir, max_depth = 10L) {
  cur <- normalizePath(start_dir, winslash = "/", mustWork = FALSE)
  for (i in seq_len(max_depth)) {
    child <- file.path(cur, "Week_7")
    if (is_week7_root(child)) {
      return(normalizePath(child, winslash = "/", mustWork = TRUE))
    }
    parent <- dirname(cur)
    if (identical(parent, cur)) break
    cur <- parent
  }
  NA_character_
}

resolve_week7_root <- function() {
  # Candidate 0: explicit environment variable.
  env_root <- Sys.getenv("WEEK7_ROOT", unset = "")
  if (nzchar(env_root) && is_week7_root(env_root)) {
    return(normalizePath(env_root, winslash = "/", mustWork = TRUE))
  }

  # Candidate 1: current working directory and its parents.
  from_cwd <- find_week7_root_upward(getwd())
  if (!is.na(from_cwd)) return(from_cwd)
  from_cwd_child <- find_week7_child_upward(getwd())
  if (!is.na(from_cwd_child)) return(from_cwd_child)

  # Candidate 2: script location (works when run with Rscript --file=...).
  args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", args, value = TRUE)
  if (length(file_arg) > 0) {
    script_path <- sub("^--file=", "", file_arg[1])
    script_dir <- dirname(normalizePath(script_path, winslash = "/", mustWork = FALSE))
    from_script <- find_week7_root_upward(script_dir)
    if (!is.na(from_script)) return(from_script)
    from_script_child <- find_week7_child_upward(script_dir)
    if (!is.na(from_script_child)) return(from_script_child)
  }

  # Candidate 3: source()-based execution context (scan all frames).
  src_ofiles <- unique(unlist(lapply(sys.frames(), function(fr) {
    if (!is.null(fr$ofile)) as.character(fr$ofile) else NA_character_
  })))
  src_ofiles <- src_ofiles[!is.na(src_ofiles) & nzchar(src_ofiles)]
  if (length(src_ofiles) > 0) {
    for (sf in src_ofiles) {
      sf_dir <- dirname(normalizePath(sf, winslash = "/", mustWork = FALSE))
      from_source <- find_week7_root_upward(sf_dir)
      if (!is.na(from_source)) return(from_source)
      from_source_child <- find_week7_child_upward(sf_dir)
      if (!is.na(from_source_child)) return(from_source_child)
    }
  }

  # Candidate 4: active file in RStudio (optional).
  if (requireNamespace("rstudioapi", quietly = TRUE)) {
    if (isTRUE(tryCatch(rstudioapi::isAvailable(), error = function(e) FALSE))) {
      active_path <- tryCatch(rstudioapi::getSourceEditorContext()$path, error = function(e) "")
      if (nzchar(active_path)) {
        active_dir <- dirname(normalizePath(active_path, winslash = "/", mustWork = FALSE))
        from_active <- find_week7_root_upward(active_dir)
        if (!is.na(from_active)) return(from_active)
        from_active_child <- find_week7_child_upward(active_dir)
        if (!is.na(from_active_child)) return(from_active_child)
      }
    }
  }

  # Candidate 5: interactive prompt fallback.
  if (interactive()) {
    prompt_path <- readline("Week_7 root not auto-detected. Enter full path to Week_7 (or press Enter to cancel): ")
    if (nzchar(prompt_path) && is_week7_root(prompt_path)) {
      return(normalizePath(prompt_path, winslash = "/", mustWork = TRUE))
    }
  }

  stop(
    "Could not find Week_7 root.\n",
    "- Start R in Week_7/ or Week_7/code/\n",
    "- OR run with absolute script path\n",
    "- OR set Sys.setenv(WEEK7_ROOT = '/absolute/path/to/Week_7')"
  )
}

count_missing_cells <- function(df_part) {
  if (ncol(df_part) == 0) return(0L)
  as.integer(sum(vapply(df_part, function(x) sum(is.na(x)), numeric(1))))
}

split_chunks <- function(x, size) {
  if (length(x) == 0) return(list())
  split(x, ceiling(seq_along(x) / size))
}

to_mr_type <- function(x) {
  if (is.character(x) || is.logical(x)) return(as.factor(x))
  if (is.integer(x)) return(as.numeric(x))
  if (inherits(x, "Date")) return(as.numeric(x))
  x
}

restore_type <- function(x, template) {
  cls <- class(template)[1]
  if (cls == "character") return(as.character(x))
  if (cls == "logical") return(as.logical(x))
  if (cls == "integer") return(as.integer(round(x)))
  if (inherits(template, "Date")) return(as.Date(round(x), origin = "1970-01-01"))
  x
}

write_imputed_csv_gz <- function(df, out_path) {
  readr::write_csv(df, out_path, na = "")
}

profile_col <- function(x) {
  n_total <- length(x)
  n_missing <- sum(is.na(x))
  n_non_missing <- n_total - n_missing
  observed <- x[!is.na(x)]
  uniq <- if (n_non_missing == 0) 0L else dplyr::n_distinct(observed)
  mode_share <- if (n_non_missing == 0) NA_real_ else as.numeric(max(table(observed, useNA = "no")) / n_non_missing)
  c(
    n_total = n_total,
    n_missing = n_missing,
    n_non_missing = n_non_missing,
    missing_rate = if (n_total == 0) NA_real_ else n_missing / n_total,
    unique_non_missing = uniq,
    mode_share_non_missing = mode_share
  )
}

reason_label <- function(dup, id_like, all_missing, near_empty, suffix, time_meta) {
  tags <- character(0)
  if (isTRUE(dup)) tags <- c(tags, "duplicate_column")
  if (isTRUE(id_like)) tags <- c(tags, "id_like")
  if (isTRUE(all_missing)) tags <- c(tags, "all_missing")
  if (isTRUE(near_empty)) tags <- c(tags, "near_empty")
  if (isTRUE(suffix)) tags <- c(tags, "option4_suffix")
  if (isTRUE(time_meta)) tags <- c(tags, "time_metadata")
  if (length(tags) == 0) "keep" else paste(tags, collapse = ";")
}

fmt_int <- function(x) format(as.integer(x), big.mark = ",", scientific = FALSE, trim = TRUE)


# project paths
# Initialize to cwd first so external error handlers never see an undefined symbol.
week7_root <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)
week7_root <- resolve_week7_root()
raw_rds <- file.path(week7_root, "data_raw", "ged_country_with_governance_iso3_year.rds")
out_dir <- file.path(week7_root, "output")
processed_dir <- file.path(week7_root, "data_processed")
work_dir <- file.path(week7_root, "data_working")

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(processed_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(work_dir, recursive = TRUE, showWarnings = FALSE)

removed_txt <- file.path(out_dir, "week7_removed_variables.txt")
imputed_rds <- file.path(processed_dir, "week7_imputed_dataset.rds")
imputed_csv_gz <- file.path(processed_dir, "week7_imputed_dataset.csv.gz")
validation_txt <- file.path(work_dir, "week7_imputation_validation.txt")

# we impute only non-GED variables( because missings in UCDP GED are due to lagging)
non_ged_regex <- "^(vdem_|wgi_|wdi_|qog_|fh_|polity_)"
suffix_regex <- "(_codelow$|_codehigh$|_sd$|_mean$|_nr$|_osp$|_ord$)"
time_regex <- "(year|month|date|cyear|eyear|byear|bmonth|emonth|codingstart|codingend|gapstart|gapend)"
id_regex <- "(^id$|_id$|^iso[0-9]*$|_iso[0-9]*$|^gwno$|_gwno$|^ccode$|_ccode$|countrycode$)"
key_fields <- c("country_id", "month_id", "year", "month", "country")


# load raw data
if (!file.exists(raw_rds)) stop("Missing raw dataset: ", raw_rds)
raw_df <- readRDS(raw_rds)
vars <- names(raw_df)

# variable screening
profile <- dplyr::bind_rows(lapply(vars, function(v) {
  as.data.frame(as.list(profile_col(raw_df[[v]])), stringsAsFactors = FALSE) |>
    dplyr::mutate(variable = v, class = class(raw_df[[v]])[1], .before = 1)
}))

is_non_ged <- grepl(non_ged_regex, vars)
dup_of <- rep(NA_character_, length(vars))
non_ged_idx <- which(is_non_ged)

# duplicate detection for non-GED variables
if (length(non_ged_idx) > 0) {
  hashes <- vapply(non_ged_idx, function(i) {
    digest::digest(list(class = class(raw_df[[i]])[1], values = raw_df[[i]]), algo = "xxhash64", serialize = TRUE)
  }, character(1))

  seen <- new.env(parent = emptyenv())
  for (k in seq_along(non_ged_idx)) {
    i <- non_ged_idx[k]
    h <- hashes[k]
    if (!exists(h, envir = seen, inherits = FALSE)) {
      assign(h, vars[i], envir = seen)
    } else {
      dup_of[i] <- get(h, envir = seen, inherits = FALSE)
    }
  }
}

profile <- profile |>
  dplyr::mutate(
    is_non_ged = grepl(non_ged_regex, variable),
    duplicate_of = dup_of,
    is_key = variable %in% key_fields,
    rule_duplicate = is_non_ged & !is.na(duplicate_of),
    rule_id_like = is_non_ged & grepl(id_regex, variable, ignore.case = TRUE),
    rule_all_missing = is_non_ged & (n_non_missing == 0),
    rule_near_empty = is_non_ged & missing_rate > 0.70 &
      (unique_non_missing <= 3 | (!is.na(mode_share_non_missing) & mode_share_non_missing >= 0.95)),
    rule_option4_suffix = is_non_ged & grepl(suffix_regex, variable),
    rule_time_metadata = is_non_ged & grepl(time_regex, variable, ignore.case = TRUE),
    drop = (!is_key) & is_non_ged &
      (rule_duplicate | rule_id_like | rule_all_missing | rule_near_empty | rule_option4_suffix | rule_time_metadata)
  )

profile$removal_rationale <- mapply(
  reason_label,
  profile$rule_duplicate,
  profile$rule_id_like,
  profile$rule_all_missing,
  profile$rule_near_empty,
  profile$rule_option4_suffix,
  profile$rule_time_metadata
)

removed <- profile |>
  dplyr::filter(drop) |>
  dplyr::arrange(dplyr::desc(missing_rate), variable)

keep_vars <- profile$variable[!profile$drop]
pre_df <- raw_df[, keep_vars, drop = FALSE]
front <- key_fields[key_fields %in% names(pre_df)]
pre_df <- pre_df %>% dplyr::select(dplyr::all_of(front), dplyr::everything())


# random-forest imputation (non-UCDP GED only)
all_pre_vars <- names(pre_df)
non_ged_vars <- all_pre_vars[grepl(non_ged_regex, all_pre_vars)]
protected_vars <- setdiff(all_pre_vars, non_ged_vars)
protected_snapshot <- pre_df[protected_vars]

imp_df <- pre_df
seed <- 20260226L
set.seed(seed)

cfg <- list(
  num_trees = 30L,
  maxiter = 1L,
  sample_fraction = 0.20,
  max_depth = 10L,
  min_node_size = 10L,
  chunk_size = 120L
)

n_cores <- parallel::detectCores(logical = FALSE)
if (is.na(n_cores) || n_cores < 2) n_cores <- parallel::detectCores(logical = TRUE)
if (is.na(n_cores) || n_cores < 2) n_cores <- 2L
threads <- max(1L, as.integer(n_cores - 1L))

aux_predictors <- protected_vars[vapply(pre_df[protected_vars], function(x) all(!is.na(x)), logical(1))]
blocks <- c("vdem", "wgi", "wdi", "qog", "fh", "polity")

for (b in blocks) {
  block_vars <- non_ged_vars[grepl(paste0("^", b, "_"), non_ged_vars)]
  if (length(block_vars) == 0) next
  chunks <- split_chunks(block_vars, cfg$chunk_size)

  for (k in seq_along(chunks)) {
    target <- chunks[[k]]
    if (count_missing_cells(imp_df[target]) == 0) next

    work_vars <- unique(c(aux_predictors, target))
    work <- imp_df[work_vars]
    template <- work
    work[] <- lapply(work, to_mr_type)

    imp_work <- missRanger::missRanger(
      data = work,
      pmm.k = 0,
      num.trees = cfg$num_trees,
      maxiter = cfg$maxiter,
      sample.fraction = cfg$sample_fraction,
      max.depth = cfg$max_depth,
      min.node.size = cfg$min_node_size,
      num.threads = threads,
      seed = as.integer(seed + k),
      verbose = 0,
      data_only = TRUE
    )

    for (v in target) {
      restored <- restore_type(imp_work[[v]], template[[v]])
      na_idx <- is.na(imp_df[[v]])
      if (any(na_idx)) imp_df[[v]][na_idx] <- restored[na_idx]
    }
  }
}

saveRDS(imp_df, imputed_rds)
write_imputed_csv_gz(imp_df, imputed_csv_gz)


# validation checks
same_rows <- nrow(pre_df) == nrow(imp_df)
same_cols <- ncol(pre_df) == ncol(imp_df)
same_order <- identical(names(pre_df), names(imp_df))
protected_unchanged <- all(vapply(protected_vars, function(v) identical(pre_df[[v]], imp_df[[v]]), logical(1)))

observed_changed_cells <- 0L
for (v in non_ged_vars) {
  obs <- !is.na(pre_df[[v]])
  if (!any(obs)) next
  changed <- obs & (pre_df[[v]] != imp_df[[v]])
  changed[is.na(changed)] <- FALSE
  observed_changed_cells <- observed_changed_cells + as.integer(sum(changed))
}

non_ged_missing_before <- count_missing_cells(pre_df[non_ged_vars])
non_ged_missing_after <- count_missing_cells(imp_df[non_ged_vars])

validation_lines <- c(
  "Week 7 Imputation Validation",
  "============================",
  paste0("Generated: ", as.character(Sys.time())),
  paste0("same_rows: ", same_rows),
  paste0("same_cols: ", same_cols),
  paste0("same_column_order: ", same_order),
  paste0("protected_unchanged: ", protected_unchanged),
  paste0("observed_changed_cells_non_ged: ", observed_changed_cells),
  paste0("non_ged_missing_before: ", non_ged_missing_before),
  paste0("non_ged_missing_after: ", non_ged_missing_after)
)
writeLines(validation_lines, validation_txt)

if (!(same_rows && same_cols && same_order && protected_unchanged && observed_changed_cells == 0)) {
  stop("Validation failed. See: ", validation_txt)
}

# write and save removed variables text
removed_lines <- c(
  "Week 7 Removed Variables",
  "========================",
  "",
  paste0("Generated: ", as.character(Sys.time())),
  paste0("Source rows/cols: ", fmt_int(nrow(raw_df)), " / ", fmt_int(ncol(raw_df))),
  paste0("Pre-imputation rows/cols: ", fmt_int(nrow(pre_df)), " / ", fmt_int(ncol(pre_df))),
  paste0("Total removed variables: ", fmt_int(nrow(removed))),
  "",
  "Imputation summary:",
  paste0("- non_ged_missing: ", fmt_int(non_ged_missing_before), " -> ", fmt_int(non_ged_missing_after)),
  paste0("- protected_unchanged: ", protected_unchanged),
  paste0("- schema_checks: rows=", same_rows, ", cols=", same_cols, ", order=", same_order),
  "",
  "Format: variable | rationale | missing_rate",
  "",
  "Variables:",
  paste0("- ", removed$variable, " | ", removed$removal_rationale, " | missing_rate=", sprintf("%.4f", removed$missing_rate))
)
writeLines(removed_lines, removed_txt)

# summary output
cat("Done.\n")
cat("Raw source (unchanged): ", raw_rds, "\n", sep = "")
cat("Removed variables text: ", removed_txt, "\n", sep = "")
cat("Imputed dataset: ", imputed_rds, "\n", sep = "")
cat("Imputed dataset (csv.gz): ", imputed_csv_gz, "\n", sep = "")
cat("Validation: ", validation_txt, "\n", sep = "")
cat("Rows: ", nrow(raw_df), " -> ", nrow(imp_df), "\n", sep = "")
cat("Cols: ", ncol(raw_df), " -> ", ncol(imp_df), "\n", sep = "")
cat("Removed variables: ", nrow(removed), "\n", sep = "")
cat("Non-GED missing: ", non_ged_missing_before, " -> ", non_ged_missing_after, "\n", sep = "")

# end of script





