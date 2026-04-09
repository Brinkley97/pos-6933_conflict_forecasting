

# Week 3 - Unit of Analysis and Target Variables
# Downloads UCDP GED data, aggregates to country-month panel, creates forecasting targets


# packages
library(tidyverse)
library(lubridate)
library(readr)




# Create project directories

base_dir <- "POS6933_Data" # root project folder
raw_dir  <- file.path(base_dir, "Data", "data_raw") # raw data directory
proc_dir <- file.path(base_dir, "Data", "data_processed") # processed data directory

dir.create(raw_dir,  recursive = TRUE, showWarnings = FALSE)
dir.create(proc_dir, recursive = TRUE, showWarnings = FALSE)


# Download + unzip UCDP GED

# Source: https://ucdp.uu.se/downloads/ged/ged251-csv.zip
ucdp_url <- "https://ucdp.uu.se/downloads/ged/ged251-csv.zip"
temp_zip <- tempfile(fileext = ".zip")

utils::download.file(ucdp_url, temp_zip, mode = "wb")
utils::unzip(temp_zip, exdir = raw_dir)

# Download skeleton panel from GitHub (RAW URL)
skeleton_url <- "https://raw.githubusercontent.com/surenmohammed/conflict-forecasting-project/main/skeleton_data.csv"
skeleton_path <- file.path(raw_dir, "skeleton_panel.csv")

utils::download.file(skeleton_url, skeleton_path, mode = "wb")
skeleton_panel <- read.csv(skeleton_path)


# Load UCDP GED
ged_path <- file.path(raw_dir, "GEDEvent_v25_1.csv")
ucdp_ged <- readr::read_csv(ged_path, show_col_types = FALSE)


# Aggregate to Country-Month(cm)

# Violence types: 1=state-based, 2=non-state, 3=one-sided

fatalities_cm <- ucdp_ged %>%
  filter(year >= 1980) %>%
  mutate(month_start = floor_date(as.Date(date_start), "month")) %>%
  group_by(country, month_start) %>%
  summarise(
    fatalities_total = sum(best, na.rm = TRUE),
    fatalities_state_based = sum(if_else(type_of_violence == 1, best, 0), na.rm = TRUE), # outcome of interest
    fatalities_non_state = sum(if_else(type_of_violence == 2, best, 0), na.rm = TRUE),
    fatalities_one_sided = sum(if_else(type_of_violence == 3, best, 0), na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(country, month_start) %>%
  mutate(country_id = as.integer(factor(country)))


# Create Month Index
earliest_month <- min(fatalities_cm$month_start, na.rm = TRUE)

fatalities_cm <- fatalities_cm %>%
  mutate(
    month_id = (year(month_start) - year(earliest_month)) * 12 +
               (month(month_start) - month(earliest_month)) + 1
  ) %>%
  rename(country_name = country)


# Prepare Skeleton Panel for Merging
skeleton_panel <- skeleton_panel %>%
  mutate(month = as.Date(month))

# Merge with Skeleton
merged_panel <- full_join(
  skeleton_panel,
  fatalities_cm,
  by = c("country_name", "month" = "month_start")
)

# Finalize Country-Month Panel
country_month_panel <- merged_panel %>%
  select(
    country_name,
    country_id = country_id.x,
    month,
    month_id = month_id.x,
    fatalities_total,
    fatalities_state_based,
    fatalities_non_state,
    fatalities_one_sided
  ) %>%
  mutate(
    month = as.Date(month),
    year = year(month),
    across(starts_with("fatalities"), ~ coalesce(.x, 0))
  )

# drop year
country_month_panel <- country_month_panel %>%
  select(-year)

# Create step-ahead targets
country_month_panel <- country_month_panel %>%
  arrange(country_id, month_id) %>%
  group_by(country_id) %>%
  mutate(
    fatalities_ahead_1m = lead(fatalities_total, 1), # 1 month ahead
    fatalities_ahead_2m = lead(fatalities_total, 2), # 2 months ahead
    fatalities_ahead_3m = lead(fatalities_total, 3), # 3 months ahead
    fatalities_ahead_4m = lead(fatalities_total, 4), # 4 months ahead
    fatalities_ahead_5m = lead(fatalities_total, 5), # 5 months ahead
    fatalities_ahead_6m = lead(fatalities_total, 6), # 6 months ahead
    fatalities_ahead_7m = lead(fatalities_total, 7)  # 7 months ahead
  ) %>%
  ungroup()

# log transform targets (+1 to avoid log(0))
country_month_panel <- country_month_panel %>%
  mutate(
    log_fatalities_ahead_1m = log1p(fatalities_ahead_1m),
    log_fatalities_ahead_2m = log1p(fatalities_ahead_2m),
    log_fatalities_ahead_3m = log1p(fatalities_ahead_3m),
    log_fatalities_ahead_4m = log1p(fatalities_ahead_4m),
    log_fatalities_ahead_5m = log1p(fatalities_ahead_5m),
    log_fatalities_ahead_6m = log1p(fatalities_ahead_6m),
    log_fatalities_ahead_7m = log1p(fatalities_ahead_7m)
  )

# save it for future use
out_path <- file.path(proc_dir, "ucdp_country_month_panel.csv")
readr::write_csv(country_month_panel, out_path)

