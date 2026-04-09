# =============================================================================
# Week 6 -2: Merge Governance Datasets Into GED Country (ISO3 + Year)
# =============================================================================
# Base dataset (main table):
# - Week_6/data_processed/ged_country.csv
#
# Additional datasets (from Week_6_Adding_predictors.R):
# - Week_6/data_raw/governance_datasets_raw.rds
#
# Join keys for every merge:
# - iso3c
# - year
#
# Output:
# - Week_6/data_processed/ged_country_with_governance_iso3_year.rds
# - Week_6/data_processed/ged_country_with_governance_iso3_year.csv.gz
# - Week_6/data_processed/ged_country_governance_merge_coverage.csv
# =============================================================================

# -----------------------------------------------------------------------------
# 1) Packages
# -----------------------------------------------------------------------------
pkgs <- c("readr", "dplyr", "tidyr", "countrycode")
to_install <- pkgs[!pkgs %in% rownames(installed.packages())]
if (length(to_install)) {
  if (is.null(getOption("repos")[["CRAN"]]) || identical(getOption("repos")[["CRAN"]], "@CRAN@")) {
    options(repos = c(CRAN = "https://cloud.r-project.org"))
  }
  install.packages(to_install)
}
invisible(lapply(pkgs, library, character.only = TRUE))

options(timeout = max(300, getOption("timeout")))


# -----------------------------------------------------------------------------
# 2) Paths
# -----------------------------------------------------------------------------
week6_dir <- "Week_6"
proc_dir <- file.path(week6_dir, "data_processed")
raw_dir <- file.path(week6_dir, "data_raw")

dir.create(proc_dir, recursive = TRUE, showWarnings = FALSE)
dir.create(raw_dir, recursive = TRUE, showWarnings = FALSE)

ged_path <- file.path(proc_dir, "ged_country.csv")
governance_path <- file.path(raw_dir, "governance_datasets_raw.rds")

legacy_ged_path <- file.path("data_processed", "ged_country.csv")
legacy_governance_path <- "governance_datasets_raw.rds"

if (!file.exists(ged_path) && file.exists(legacy_ged_path)) {
  file.copy(legacy_ged_path, ged_path, overwrite = FALSE)
}
if (!file.exists(governance_path) && file.exists(legacy_governance_path)) {
  file.copy(legacy_governance_path, governance_path, overwrite = FALSE)
}

out_rds <- file.path(proc_dir, "ged_country_with_governance_iso3_year.rds")
out_csv <- file.path(proc_dir, "ged_country_with_governance_iso3_year.csv.gz")
coverage_path <- file.path(proc_dir, "ged_country_governance_merge_coverage.csv")


# -----------------------------------------------------------------------------
# 3) Helpers
# -----------------------------------------------------------------------------
clean_names_ascii <- function(x) {
  x <- gsub("[^A-Za-z0-9]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  x <- gsub("_+", "_", x)
  x[x == ""] <- "col"
  make.unique(x, sep = "_")
}

prefix_non_keys <- function(df, prefix, key_cols = c("iso3c", "year")) {
  cols_to_prefix <- setdiff(names(df), key_cols)
  names(df)[match(cols_to_prefix, names(df))] <- paste0(prefix, "_", cols_to_prefix)
  names(df) <- clean_names_ascii(names(df))
  df
}

dedupe_iso_year <- function(df, data_name) {
  dup <- df %>%
    filter(!is.na(iso3c), !is.na(year)) %>%
    count(iso3c, year, name = "n") %>%
    filter(n > 1)

  if (nrow(dup) > 0) {
    message(data_name, ": found ", nrow(dup), " duplicate iso3c-year keys; keeping first row per key.")
    df <- df %>%
      arrange(iso3c, year) %>%
      distinct(iso3c, year, .keep_all = TRUE)
  }

  df
}

key_coverage <- function(base_df, addon_df, data_name) {
  base_keys <- base_df %>% distinct(iso3c, year)
  addon_keys <- addon_df %>% distinct(iso3c, year) %>% mutate(has_match = 1L)
  merged_keys <- base_keys %>% left_join(addon_keys, by = c("iso3c", "year"))

  tibble(
    dataset = data_name,
    base_unique_keys = nrow(base_keys),
    addon_unique_keys = nrow(addon_keys),
    matched_keys = sum(!is.na(merged_keys$has_match)),
    match_rate = round(mean(!is.na(merged_keys$has_match)), 4)
  )
}


# -----------------------------------------------------------------------------
# 4) Load base GED data and create ISO3 + year keys
# -----------------------------------------------------------------------------
if (!file.exists(ged_path)) stop("Missing GED base file: ", ged_path)
if (!file.exists(governance_path)) stop("Missing governance file: ", governance_path)

ged <- readr::read_csv(ged_path, show_col_types = FALSE) %>%
  mutate(
    month = as.Date(month),
    year = as.integer(format(month, "%Y"))
  )

ged_country_map <- ged %>%
  distinct(country_id, country) %>%
  mutate(
    iso3c = countrycode::countrycode(
      sourcevar = country,
      origin = "country.name",
      destination = "iso3c",
      custom_match = c(
        "Yemen (North Yemen)" = "YEM"
      )
    )
  )

unmatched_ged <- ged_country_map %>% filter(is.na(iso3c))
if (nrow(unmatched_ged) > 0) {
  stop(
    "Could not map GED countries to ISO3 for: ",
    paste(unmatched_ged$country, collapse = ", ")
  )
}

ged_main <- ged %>%
  left_join(ged_country_map %>% select(country_id, iso3c), by = "country_id") %>%
  mutate(iso3c = toupper(iso3c))

base_nrow <- nrow(ged_main)


# -----------------------------------------------------------------------------
# 5) Load and prepare governance datasets for iso3c + year joins
# -----------------------------------------------------------------------------
gov <- readRDS(governance_path)
if (!all(c("vdem", "wgi", "wdi", "qog", "fh", "polity") %in% names(gov))) {
  stop("governance_datasets_raw.rds is missing expected datasets.")
}

# V-Dem
vdem <- gov$vdem %>%
  mutate(
    iso3c = toupper(as.character(country_text_id)),
    year = as.integer(year)
  ) %>%
  select(iso3c, year, everything(), -country_text_id) %>%
  filter(!is.na(iso3c), !is.na(year)) %>%
  dedupe_iso_year("vdem") %>%
  prefix_non_keys("vdem")

# WGI (reshape indicators to wide by iso3c-year)
wgi <- gov$wgi %>%
  select(`Country Code`, `Indicator Code`, matches("^[0-9]{4}$")) %>%
  tidyr::pivot_longer(
    cols = matches("^[0-9]{4}$"),
    names_to = "year",
    values_to = "wgi_value"
  ) %>%
  mutate(
    iso3c = toupper(as.character(`Country Code`)),
    year = as.integer(year),
    indicator_code = as.character(`Indicator Code`)
  ) %>%
  filter(!is.na(iso3c), !is.na(year), !is.na(indicator_code), !is.na(wgi_value)) %>%
  group_by(iso3c, year, indicator_code) %>%
  summarise(wgi_value = dplyr::first(wgi_value), .groups = "drop") %>%
  tidyr::pivot_wider(
    names_from = indicator_code,
    values_from = wgi_value
  ) %>%
  dedupe_iso_year("wgi") %>%
  prefix_non_keys("wgi")

# WDI
wdi <- gov$wdi %>%
  mutate(
    iso3c = toupper(as.character(iso3c)),
    year = as.integer(year)
  ) %>%
  select(iso3c, year, everything()) %>%
  filter(!is.na(iso3c), !is.na(year)) %>%
  dedupe_iso_year("wdi") %>%
  prefix_non_keys("wdi")

# QoG
qog <- gov$qog %>%
  mutate(
    iso3c = toupper(as.character(ccodealp)),
    year = as.integer(year)
  ) %>%
  select(iso3c, year, everything(), -ccodealp) %>%
  filter(!is.na(iso3c), !is.na(year)) %>%
  dedupe_iso_year("qog") %>%
  prefix_non_keys("qog")

# Freedom House (support both old wide-raw sheet and new tidy country-year format)
fh_raw <- gov$fh

if (all(c("country_name", "year") %in% names(fh_raw))) {
  fh <- fh_raw %>%
    mutate(
      country_name = as.character(country_name),
      year = as.integer(year)
    )
} else {
  fh_header <- as.character(fh_raw[1, ])
  fh <- fh_raw[-1, , drop = FALSE]
  names(fh) <- fh_header
  fh[] <- lapply(fh, function(col) type.convert(as.character(col), as.is = TRUE))

  fh <- fh %>%
    rename(
      country_name = `Country/Territory`,
      country_type = `C/T`,
      year = Edition
    ) %>%
    filter(country_type == "c") %>%
    select(-country_type)
}

fh <- fh %>%
  mutate(
    year = as.integer(year),
    iso3c = countrycode::countrycode(
      sourcevar = country_name,
      origin = "country.name",
      destination = "iso3c",
      custom_match = c(
        "Congo (Brazzaville)" = "COG",
        "Congo (Kinshasa)" = "COD",
        "Kosovo" = "XKX",
        "Micronesia" = "FSM",
        "North Macedonia" = "MKD",
        "Russia" = "RUS",
        "Syria" = "SYR",
        "Venezuela" = "VEN",
        "Vietnam" = "VNM",
        "Yemen" = "YEM",
        "Czechoslovakia"="CSK",
        "Serbia and Montenegro"="SRB",
        "Yugoslavia"="YUG"
      )
    )
  ) %>%
  filter(!is.na(iso3c), !is.na(year)) %>%
  dedupe_iso_year("fh") %>%
  prefix_non_keys("fh")

# Polity
polity <- gov$polity %>%
  mutate(
    iso3c = toupper(as.character(scode)),
    year = as.integer(year)
  ) %>%
  select(iso3c, year, everything(), -scode) %>%
  filter(!is.na(iso3c), !is.na(year)) %>%
  dedupe_iso_year("polity") %>%
  prefix_non_keys("polity")


# -----------------------------------------------------------------------------
# 6) Merge all add-on datasets into GED main table (LEFT JOINS ONLY)
# -----------------------------------------------------------------------------
addons <- list(
  vdem = vdem,
  wgi = wgi,
  wdi = wdi,
  qog = qog,
  fh = fh,
  polity = polity
)

coverage_tbl <- bind_rows(lapply(names(addons), function(nm) key_coverage(ged_main, addons[[nm]], nm)))

merged <- ged_main
for (nm in names(addons)) {
  before_cols <- ncol(merged)
  merged <- merged %>% left_join(addons[[nm]], by = c("iso3c", "year"))

  if (nrow(merged) != base_nrow) {
    stop("Row count changed after joining ", nm, ". This should not happen with left joins on unique keys.")
  }

  message("Joined ", nm, ": +", ncol(merged) - before_cols, " columns")
}

# Keep GED identifiers only (country/year from GED side), remove addon ID fields.
ged_base_cols <- names(ged)
keep_cols <- union(ged_base_cols, "year")
addon_cols <- setdiff(names(merged), keep_cols)

id_patterns <- c(
  "^iso3c$",
  "^fh_country_name$",
  "^vdem_(country_name|country_id|histname|cowcode)$",
  "^wdi_(country|iso2c)$",
  "^qog_(ccode|ccode_qog|ccodealp_year|ccodecow|cname|cname_qog|cname_year)$",
  "^polity_(country|ccode|cyear|eyear|byear|emonth|bmonth)$"
)

id_drop_cols <- addon_cols[vapply(
  addon_cols,
  function(col) any(vapply(id_patterns, function(p) grepl(p, col, ignore.case = TRUE), logical(1))),
  logical(1)
)]

if (length(id_drop_cols)) {
  merged <- merged %>% select(-all_of(id_drop_cols))
  message("Removed identifier columns from add-on datasets: ", length(id_drop_cols))
}


# -----------------------------------------------------------------------------
# 7) Save outputs
# -----------------------------------------------------------------------------
saveRDS(merged, out_rds)
readr::write_csv(merged, out_csv, na = "")
readr::write_csv(coverage_tbl, coverage_path)

message("Merge complete.")
message("Rows: ", nrow(merged), " | Cols: ", ncol(merged))
message("Saved: ", out_rds)
message("Saved: ", out_csv)
message("Saved: ", coverage_path)
