
## week 4: data partitioning - create train/test splits for 7 forecast horizons

# packages
library(readr)
library(dplyr)

# data(please make sure you have ever-thing downloaded in a folder you created last week )
ucdp_country_month_panel <- read_csv("POS6933_Data/Data/data_processed/ucdp_country_month_panel.csv", show_col_types = FALSE)

# Remove rows with missing month_id
ucdp_country_month_panel <- ucdp_country_month_panel %>% filter(!is.na(month_id))

# Partition parameters
last_train_target <- 372   # Dec 2020
first_test_target <- 373   # Jan 2021
last_test_target <- 420    # Dec 2024

# Forecast horizons
horizons <- c(1, 2, 3, 4, 5, 6, 7)  # Seven forecast horizons


# Create train/test splits for each horizon
train_sets <- list()
test_sets <- list()

for (s in horizons) {
  
  # train set
  train_sets[[paste0("s", s)]] <- ucdp_country_month_panel %>%
    filter(month_id >= 1 & month_id <= (last_train_target - s)) %>%
    mutate(
      horizon = s,
      origin_month_id = month_id,
      target_month_id = month_id + s
    )
  
  # test set
  test_sets[[paste0("s", s)]] <- ucdp_country_month_panel %>%
    filter(month_id >= (first_test_target - s) & month_id <= (last_test_target - s)) %>%
    mutate(
      horizon = s,
      origin_month_id = month_id,
      target_month_id = month_id + s
    )
}


# Summary

for (s in horizons) {
  key <- paste0("s", s)
  train_range <- range(train_sets[[key]]$month_id)
  test_range <- range(test_sets[[key]]$month_id)
  
  cat(sprintf("Horizon s=%d:\n", s))
  cat(sprintf("  Train origins: %d--%d | Train targets: %d--%d\n",
              train_range[1], train_range[2],
              train_range[1] + s, train_range[2] + s))
  cat(sprintf("  Test origins:  %d--%d | Test targets:  %d--%d\n",
              test_range[1], test_range[2],
              test_range[1] + s, test_range[2] + s))
  cat(sprintf("  Train: %d obs | Test: %d obs\n\n",
              nrow(train_sets[[key]]), nrow(test_sets[[key]])))
}


# save to CSV files

# folder we want to save into
out_dir <- file.path("POS6933_Data", "Data", "data_processed")
 
# Make sure the folder exists
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
 
 for (s in horizons) {
   key <- paste0("s", s)
   
   write.csv(
     train_sets[[key]],
     file = file.path(out_dir, paste0("train_horizon_", s, ".csv")),
     row.names = FALSE
   )
   
   write.csv(
     test_sets[[key]],
     file = file.path(out_dir, paste0("test_horizon_", s, ".csv")),
     row.names = FALSE
   )
 }
 





