# ============================================================
# Week 6-1: Download datasets
# ============================================================


# packages
if (is.null(getOption("repos")[["CRAN"]]) || identical(getOption("repos")[["CRAN"]], "@CRAN@")) {
  options(repos = c(CRAN = "https://cloud.r-project.org"))
}
options(timeout = max(300, getOption("timeout")))

pkgs <- c("readr","dplyr","tidyr","httr","WDI","haven","readxl")
to_install <- pkgs[!pkgs %in% rownames(installed.packages())]
if(length(to_install)) install.packages(to_install)
invisible(lapply(pkgs, library, character.only = TRUE))

# common time horizon for all datasets
start_year <- 1990L
end_year <- 2024L

# Week 6 output location
week6_dir <- "Week_6"
raw_dir <- file.path(week6_dir, "data_raw")
dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)

# helpers

# download a file from a URL to a temporary location, returning the path to the temp file
download_to_temp <- function(url, ext = NULL) {
  if (is.null(ext)) ext <- tools::file_ext(url)
  f <- tempfile(fileext = paste0(".", ext))
  utils::download.file(url, f, mode = "wb", quiet = TRUE)
  f
}

# download a zip file from a URL, extract the first CSV inside, and read it into a data frame.
read_csv_from_zip <- function(zip_url, filename_pattern = NULL) {
  z <- download_to_temp(zip_url, "zip")
  td <- tempfile("unz_"); dir.create(td)
  utils::unzip(z, exdir = td)
  csvs <- list.files(td, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
  if (!length(csvs)) stop("No CSV found inside zip: ", zip_url)

  if (!is.null(filename_pattern)) {
    matches <- csvs[grepl(filename_pattern, basename(csvs), ignore.case = TRUE)]
    if (length(matches)) csvs <- matches
  }

  readr::read_csv(csvs[1], show_col_types = FALSE)
}

# Subset a data frame to rows where the specified year column is within the given range.
subset_year_range <- function(df, year_col, start_year, end_year) {
  if (!(year_col %in% names(df))) {
    stop("Missing year column '", year_col, "'.")
  }
  df[[year_col]] <- suppressWarnings(as.integer(df[[year_col]]))
  df[!is.na(df[[year_col]]) & df[[year_col]] >= start_year & df[[year_col]] <= end_year, , drop = FALSE]
}

# The WGI dataset has a wide format with one column per year;
# this function keeps only the year columns within the specified range, along with any non-year columns.
limit_wgi_year_columns <- function(df, start_year, end_year) {
  year_cols <- names(df)[grepl("^[0-9]{4}$", names(df))]
  keep_year_cols <- year_cols[as.integer(year_cols) >= start_year & as.integer(year_cols) <= end_year]
  non_year_cols <- setdiff(names(df), year_cols)
  df[, c(non_year_cols, keep_year_cols), drop = FALSE]
}

# extract the numeric years and return the min and max as a named vector.
range_or_na <- function(x) {
  x <- suppressWarnings(as.integer(x))
  x <- x[!is.na(x)]
  if (!length(x)) return(c(min = NA_integer_, max = NA_integer_))
  c(min = min(x), max = max(x))
}

# download a zip file from a URL, extract the first XLSX inside,
# and read it into a data frame.
read_first_xlsx_from_zip <- function(zip_url, sheet = 1) {
  z <- download_to_temp(zip_url, "zip")
  td <- tempfile("unz_"); dir.create(td)
  utils::unzip(z, exdir = td)
  xlsxs <- list.files(td, pattern = "\\.xlsx$", full.names = TRUE, recursive = TRUE)
  if (!length(xlsxs)) stop("No XLSX found inside zip: ", zip_url)
  readxl::read_xlsx(xlsxs[1], sheet = sheet)
}

# Try to read V-Dem from the vdemdata package; if not available,
# download the vdem.RData file directly from GitHub and load it.
read_vdem <- function() {
  if (requireNamespace("vdemdata", quietly = TRUE)) {
    return(vdemdata::vdem)
  }

  message("Package 'vdemdata' not available; downloading vdem.RData directly from GitHub.")
  vdem_rdata_url <- "https://raw.githubusercontent.com/vdeminstitute/vdemdata/master/data/vdem.RData"
  f <- download_to_temp(vdem_rdata_url, "RData")
  env <- new.env(parent = emptyenv())
  load(f, envir = env)

  if (!exists("vdem", envir = env, inherits = FALSE)) {
    stop("V-Dem fallback download did not contain object 'vdem'.")
  }

  get("vdem", envir = env, inherits = FALSE)
}

# Freedom House's historical ratings file has a complex layout with years and metrics in header rows; 
# this function identifies the relevant sheet, extracts the year and metric labels,
# and reshapes the data into a long format with one row per country-year-metric.
read_fh_long_historical <- function(fh_xlsx_url, start_year, end_year) {
  f <- download_to_temp(fh_xlsx_url, "xlsx")
  sheets <- readxl::excel_sheets(f)
  target <- sheets[grepl("^Country Ratings", sheets, ignore.case = TRUE)][1]
  if (is.na(target) || !nzchar(target)) {
    stop("Could not identify 'Country Ratings' sheet in Freedom House workbook.")
  }

  # read without column names because this workbook stores years and metrics in header rows.
  fh_raw <- readxl::read_xlsx(f, sheet = target, col_names = FALSE)
  if (nrow(fh_raw) < 4) stop("Unexpected Freedom House layout: not enough rows.")

  year_labels <- as.character(unlist(fh_raw[2, ], use.names = FALSE))
  year_labels <- trimws(year_labels)
  year_labels[year_labels == ""] <- NA_character_
  # labels appear once per PR/CL/Status triplet; carry forward within each triplet.
  for (i in seq_along(year_labels)) {
    if (is.na(year_labels[i]) && i > 1) year_labels[i] <- year_labels[i - 1]
  }
  metric_labels <- tolower(trimws(as.character(unlist(fh_raw[3, ], use.names = FALSE))))
  year_num <- suppressWarnings(as.integer(sub("^.*?(\\d{4}).*$", "\\1", year_labels)))

  keep_metric <- metric_labels %in% c("pr", "cl", "status")
  keep_year <- !is.na(year_num) & year_num >= start_year & year_num <= end_year
  keep_idx <- which(keep_metric & keep_year)

  if (!length(keep_idx)) {
    stop("No Freedom House year columns in requested range: ", start_year, "-", end_year)
  }

  data_rows <- fh_raw[-c(1, 2, 3), , drop = FALSE]
  names(data_rows)[1] <- "country_name"
  keep_cols <- names(data_rows)[keep_idx]

  key <- dplyr::tibble(
    col = keep_cols,
    year = year_num[keep_idx],
    metric = metric_labels[keep_idx]
  )

  fh_long <- data_rows |>
    dplyr::select(country_name, dplyr::all_of(keep_cols)) |>
    tidyr::pivot_longer(cols = -country_name, names_to = "col", values_to = "value") |>
    dplyr::left_join(key, by = "col") |>
    dplyr::mutate(
      country_name = trimws(as.character(country_name)),
      value = trimws(as.character(value)),
      value = dplyr::na_if(value, ""),
      value = dplyr::na_if(value, "-"),
      year = as.integer(year)
    ) |>
    dplyr::filter(!is.na(country_name), country_name != "", !is.na(year)) |>
    dplyr::filter(year >= start_year & year <= end_year) |>
    dplyr::select(country_name, year, metric, value)

  fh_out <- fh_long |>
    tidyr::pivot_wider(names_from = metric, values_from = value, values_fn = dplyr::first)

  if (!("pr" %in% names(fh_out))) fh_out$pr <- NA_character_
  if (!("cl" %in% names(fh_out))) fh_out$cl <- NA_character_
  if (!("status" %in% names(fh_out))) fh_out$status <- NA_character_

  fh_out <- fh_out |>
    dplyr::mutate(
      pr = suppressWarnings(as.integer(pr)),
      cl = suppressWarnings(as.integer(cl))
    ) |>
    dplyr::arrange(country_name, year)

  fh_out
}

# Freedom House's recent all-data file has a more straightforward layout,
# but includes both country-year rows and some non-country rows (e.g. aggregates, territories);
read_fh_recent_all_data <- function(fh_xlsx_url, start_year, end_year) {
  f <- download_to_temp(fh_xlsx_url, "xlsx")
  sheets <- readxl::excel_sheets(f)
  target <- sheets[grepl("^FIW", sheets, ignore.case = TRUE)][1]
  if (is.na(target) || !nzchar(target)) {
    stop("Could not identify FIW sheet in recent Freedom House workbook.")
  }

  fh_raw <- readxl::read_xlsx(f, sheet = target)
  if (nrow(fh_raw) < 2) stop("Unexpected recent Freedom House layout.")

  header <- as.character(unlist(fh_raw[1, ], use.names = FALSE))
  fh <- fh_raw[-1, , drop = FALSE]
  names(fh) <- header
  fh[] <- lapply(fh, function(col) type.convert(as.character(col), as.is = TRUE))

  needed <- c("Country/Territory", "C/T", "Edition", "Status", "PR rating", "CL rating")
  if (!all(needed %in% names(fh))) {
    stop("Recent Freedom House workbook missing one or more expected columns.")
  }

  fh |>
    dplyr::transmute(
      country_name = as.character(`Country/Territory`),
      year = suppressWarnings(as.integer(Edition)),
      status = as.character(Status),
      pr = suppressWarnings(as.integer(`PR rating`)),
      cl = suppressWarnings(as.integer(`CL rating`)),
      c_t = as.character(`C/T`)
    ) |>
    dplyr::filter(c_t == "c") |>
    dplyr::select(-c_t) |>
    dplyr::filter(!is.na(country_name), country_name != "", !is.na(year)) |>
    dplyr::filter(year >= start_year & year <= end_year)
}

read_fh <- function(historical_url, recent_url, start_year, end_year) {
  fh_hist <- read_fh_long_historical(historical_url, start_year, end_year) |>
    dplyr::mutate(source_priority = 1L)

  fh_recent <- read_fh_recent_all_data(recent_url, start_year, end_year) |>
    dplyr::mutate(source_priority = 2L)

  dplyr::bind_rows(fh_hist, fh_recent) |>
    dplyr::arrange(country_name, year, dplyr::desc(source_priority)) |>
    dplyr::distinct(country_name, year, .keep_all = TRUE) |>
    dplyr::select(country_name, year, status, pr, cl) |>
    dplyr::arrange(country_name, year)
}


# we now start downloading the datasets; for simplicity, we won't do any cleaning or harmonization yet,
# just get the raw data in place.


# ============================================================
# Dataset 1) V-Dem (Country-Year)  — via vdemdata package
# ============================================================


vdem <- read_vdem()
vdem <- subset_year_range(vdem, year_col = "year", start_year = start_year, end_year = end_year)

# ============================================================
# Dataset 2) WGI: Worldwide Governance Indicators — via direct CSV download from World Bank
# ============================================================
wgi_csv_zip   <- "https://databank.worldbank.org/data/download/WGI_CSV.zip"

wgi <- read_csv_from_zip(wgi_csv_zip, filename_pattern = "^WGICSV\\.csv$")
wgi <- limit_wgi_year_columns(wgi, start_year = start_year, end_year = end_year)


# ============================================================
# Dataset 3) WDI: World Development Indicators — API via WDI package
# ============================================================
wdi <- WDI::WDI(
  country = "all",
  indicator = c(
    gdp_pc_kd = "NY.GDP.PCAP.KD",
    pop = "SP.POP.TOTL",
    infl_cpi = "FP.CPI.TOTL.ZG"
  ),
  start = start_year,
  end = end_year
) |>
  dplyr::filter(!is.na(iso3c)) |>
  subset_year_range(year_col = "year", start_year = start_year, end_year = end_year)

# ============================================================
# Dataset 4) QoG: The Quality of Government Institute: direct CSV URL
# ============================================================
qog_ts_url <- "https://www.qogdata.pol.gu.se/data/qog_std_ts_jan26.csv"
qog <- readr::read_csv(download_to_temp(qog_ts_url, "csv"), show_col_types = FALSE)
qog <- subset_year_range(qog, year_col = "year", start_year = start_year, end_year = end_year)

# ============================================================
# Dataset 5) Freedom House FIW
# ============================================================
fh_historical_url <- "https://freedomhouse.org/sites/default/files/2025-10/Country_and_Territory_Ratings_and_Statuses_FIW_1973-2025.xlsx"
fh_recent_url <- "https://freedomhouse.org/sites/default/files/2025-10/All_data_FIW_2013-2025.xlsx"
fh <- read_fh(
  historical_url = fh_historical_url,
  recent_url = fh_recent_url,
  start_year = start_year,
  end_year = end_year
)

# ============================================================
# Dataset 6) Polity (Polity5) — direct SPSS .sav URL
# ============================================================
polity_url <- "https://www.systemicpeace.org/inscr/p5v2018.sav"
polity_raw <- haven::read_sav(download_to_temp(polity_url, "sav"))
polity_raw <- subset_year_range(polity_raw, year_col = "year", start_year = start_year, end_year = end_year)

# put everything in a list + save once
datasets <- list(
  vdem   = vdem,
  wgi    = wgi,
  wdi    = wdi,
  qog    = qog,
  fh     = fh,
  polity = polity_raw
)

# Save one file you can load instantly later
saveRDS(datasets, file = file.path(raw_dir, "governance_datasets_raw.rds"))

# quick sanity check:
lapply(datasets, function(x) c(nrow = nrow(x), ncol = ncol(x)))

# year-window check (1990-2024)
wgi_year_cols <- names(wgi)[grepl("^[0-9]{4}$", names(wgi))]
fh_years <- if ("year" %in% names(fh)) fh$year else integer()

year_windows <- list(
  vdem = range_or_na(vdem$year),
  wgi = range_or_na(wgi_year_cols),
  wdi = range_or_na(wdi$year),
  qog = range_or_na(qog$year),
  fh = range_or_na(fh_years),
  polity = range_or_na(polity_raw$year)
)
print(year_windows)
