# =============================================================================
# Week 6 -3: Download Dataset Codebooks to a PDF Folder
# =============================================================================
# Datasets covered:
# - GED (UCDP)
# - V-Dem
# - WGI (World Bank)
# - WDI (World Bank) -> generated from API metadata for selected indicators
# - QoG
# - Freedom House (FIW)
# - Polity5
#
# Output folder:
# - Week_6/pdfs/week_6_codebooks/
# =============================================================================





# packages

pkgs <- c("httr", "readr", "dplyr", "WDI")
to_install <- pkgs[!pkgs %in% rownames(installed.packages())]
if (length(to_install)) {
  if (is.null(getOption("repos")[["CRAN"]]) || identical(getOption("repos")[["CRAN"]], "@CRAN@")) {
    options(repos = c(CRAN = "https://cloud.r-project.org"))
  }
  install.packages(to_install)
}
invisible(lapply(pkgs, library, character.only = TRUE))

options(timeout = max(300, getOption("timeout")))



# paths

base_dir <- file.path("Week_6", "pdfs", "week_6_codebooks")
dataset_dirs <- c("GED", "VDem", "WGI", "WDI", "QoG", "FreedomHouse", "Polity")

dir.create(base_dir, recursive = TRUE, showWarnings = FALSE)
invisible(lapply(file.path(base_dir, dataset_dirs), dir.create, recursive = TRUE, showWarnings = FALSE))

log_path <- file.path(base_dir, "download_log.csv")
readme_path <- file.path(base_dir, "README.txt")



# helpers
is_pdf_file <- function(path) {
  if (!file.exists(path)) return(FALSE)
  sig <- readBin(path, what = "raw", n = 4)
  if (length(sig) < 4) return(FALSE)
  paste(sprintf("%02X", as.integer(sig)), collapse = "") == "25504446"
}

download_pdf <- function(dataset, label, url, out_file) {
  out_dir <- dirname(out_file)
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

  status_code <- NA_integer_
  content_type <- NA_character_
  ok <- FALSE
  err <- NA_character_

  tryCatch({
    resp <- httr::GET(
      url,
      httr::user_agent("POS6933-week6-codebook-downloader"),
      httr::write_disk(out_file, overwrite = TRUE),
      httr::timeout(240)
    )

    status_code <- httr::status_code(resp)
    content_type <- httr::headers(resp)[["content-type"]]
    ok <- status_code >= 200 && status_code < 300 && is_pdf_file(out_file)

    if (!ok && file.exists(out_file)) {
      unlink(out_file, force = TRUE)
    }
  }, error = function(e) {
    err <<- conditionMessage(e)
    if (file.exists(out_file)) unlink(out_file, force = TRUE)
  })

  tibble::tibble(
    dataset = dataset,
    item = label,
    url = url,
    output_file = out_file,
    status_code = status_code,
    content_type = content_type,
    success = ok,
    error = err,
    downloaded_at = as.character(Sys.time())
  )
}

write_text_pdf <- function(lines, out_file, title = NULL) {
  lines <- as.character(lines)
  if (!is.null(title) && nzchar(title)) {
    lines <- c(title, strrep("=", nchar(title)), "", lines)
  }

  grDevices::pdf(out_file, width = 8.5, height = 11, onefile = TRUE)
  on.exit(grDevices::dev.off(), add = TRUE)

  graphics::plot.new()
  y <- 0.97
  line_h <- 0.024
  width_chars <- 108

  for (ln in lines) {
    wrapped <- strwrap(ifelse(is.na(ln), "", ln), width = width_chars)
    if (!length(wrapped)) wrapped <- ""

    for (w in wrapped) {
      if (y < 0.05) {
        graphics::plot.new()
        y <- 0.97
      }
      graphics::text(0.03, y, labels = w, adj = c(0, 1), cex = 0.72, family = "mono")
      y <- y - line_h
    }

    y <- y - (line_h * 0.2)
  }
}

generate_wdi_metadata_pdf <- function(out_file) {
  indicators_used <- c(
    "NY.GDP.PCAP.KD",
    "SP.POP.TOTL",
    "FP.CPI.TOTL.ZG"
  )

  cache <- WDI::WDIcache()
  series <- cache$series %>%
    dplyr::filter(indicator %in% indicators_used) %>%
    dplyr::mutate(order_key = match(indicator, indicators_used)) %>%
    dplyr::arrange(order_key)

  lines <- c(
    paste0("Generated: ", Sys.time()),
    "Source: WDI API metadata via WDI::WDIcache()",
    "",
    "Indicators used in Week_6_Adding_predictors.R:",
    paste(indicators_used, collapse = ", "),
    ""
  )

  if (nrow(series) == 0) {
    lines <- c(lines, "No indicator metadata was returned by WDIcache().")
  } else {
    for (i in seq_len(nrow(series))) {
      row <- series[i, ]
      lines <- c(
        lines,
        paste0("Indicator: ", row$indicator),
        paste0("Name: ", row$name),
        paste0("Source DB: ", row$sourceDatabase),
        "Description:",
        as.character(row$description),
        "Source Organization:",
        as.character(row$sourceOrganization),
        strrep("-", 90)
      )
    }
  }

  write_text_pdf(
    lines = lines,
    out_file = out_file,
    title = "World Development Indicators (WDI) - Selected Indicator Metadata"
  )
}


# All PDFs to download
targets <- tibble::tribble(
  ~dataset,        ~item,                       ~url,                                                                                                                      ~subdir,         ~filename,
  "GED",           "GED Codebook v25.1",        "https://ucdp.uu.se/downloads/ged/ged251.pdf",                                                                            "GED",           "ged_codebook_v25_1.pdf",
  "VDem",          "V-Dem Codebook (v15)",      "https://www.v-dem.net/documents/55/codebook.pdf",                                                                        "VDem",          "vdem_codebook_v15.pdf",
  "WGI",           "WGI Methodology (2025)",    "https://www.worldbank.org/content/dam/sites/govindicators/doc/The%20Worldwide%20Governance%20Indicators%202025%20Methodology%20Revision.pdf", "WGI",           "wgi_methodology_2025.pdf",
  "WGI",           "WGI CC (2025)",             "https://www.worldbank.org/content/dam/sites/govindicators/doc/CC-2025.pdf",                                             "WGI",           "wgi_control_of_corruption_2025.pdf",
  "WGI",           "WGI GE (2025)",             "https://www.worldbank.org/content/dam/sites/govindicators/doc/GE-2025.pdf",                                             "WGI",           "wgi_government_effectiveness_2025.pdf",
  "WGI",           "WGI PV (2025)",             "https://www.worldbank.org/content/dam/sites/govindicators/doc/PV-2025.pdf",                                             "WGI",           "wgi_political_stability_2025.pdf",
  "WGI",           "WGI RL (2025)",             "https://www.worldbank.org/content/dam/sites/govindicators/doc/RL-2025.pdf",                                             "WGI",           "wgi_rule_of_law_2025.pdf",
  "WGI",           "WGI RQ (2025)",             "https://www.worldbank.org/content/dam/sites/govindicators/doc/RQ-2025.pdf",                                             "WGI",           "wgi_regulatory_quality_2025.pdf",
  "WGI",           "WGI VA (2025)",             "https://www.worldbank.org/content/dam/sites/govindicators/doc/VA-2025.pdf",                                             "WGI",           "wgi_voice_and_accountability_2025.pdf",
  "QoG",           "QoG Std TS Codebook Jan26", "https://www.qogdata.pol.gu.se/data/codebook_std_jan26.pdf",                                                             "QoG",           "qog_std_ts_codebook_jan26.pdf",
  "FreedomHouse",  "FIW Methodology (2025)",    "https://freedomhouse.org/sites/default/files/2025-06/FIW25%20Methodology.pdf",                                          "FreedomHouse",  "freedom_house_fiw_methodology_2025.pdf",
  "Polity",        "Polity5 Manual (2018)",     "https://www.systemicpeace.org/inscr/p5manualv2018.pdf",                                                                  "Polity",        "polity5_manual_v2018.pdf"
)



# run downloads
download_log <- vector("list", nrow(targets))

for (i in seq_len(nrow(targets))) {
  row <- targets[i, ]
  out_file <- file.path(base_dir, row$subdir, row$filename)
  message("Downloading: ", row$dataset, " - ", row$item)
  download_log[[i]] <- download_pdf(
    dataset = row$dataset,
    label = row$item,
    url = row$url,
    out_file = out_file
  )
}

download_log <- dplyr::bind_rows(download_log)



# WDI metadata PDF (no single official WDI codebook PDF endpoint)

wdi_pdf <- file.path(base_dir, "WDI", "wdi_selected_indicators_metadata_codebook.pdf")
wdi_ok <- TRUE
wdi_err <- NA_character_

tryCatch({
  message("Generating: WDI selected indicators metadata PDF")
  generate_wdi_metadata_pdf(wdi_pdf)
}, error = function(e) {
  wdi_ok <<- FALSE
  wdi_err <<- conditionMessage(e)
  if (file.exists(wdi_pdf)) unlink(wdi_pdf, force = TRUE)
})

download_log <- dplyr::bind_rows(
  download_log,
  tibble::tibble(
    dataset = "WDI",
    item = "WDI selected indicators metadata (generated PDF)",
    url = "WDI::WDIcache() metadata",
    output_file = wdi_pdf,
    status_code = NA_integer_,
    content_type = "application/pdf (generated)",
    success = wdi_ok && file.exists(wdi_pdf) && is_pdf_file(wdi_pdf),
    error = wdi_err,
    downloaded_at = as.character(Sys.time())
  )
)


# save logs + README
# -----------------------------------------------------------------------------
readr::write_csv(download_log, log_path, na = "")

success_n <- sum(download_log$success, na.rm = TRUE)
total_n <- nrow(download_log)

readme_lines <- c(
  "Week 6 Codebooks",
  "================",
  "",
  paste0("Generated on: ", Sys.time()),
  paste0("Location: ", normalizePath(base_dir, winslash = "/", mustWork = FALSE)),
  "",
  "Contents",
  "--------",
  "- GED: UCDP GED codebook PDF",
  "- VDem: V-Dem codebook PDF",
  "- WGI: WGI methodology + six governance dimension PDFs",
  "- WDI: generated PDF from WDI API metadata for indicators used in Week_6_Adding_predictors.R",
  "- QoG: Standard Time-series codebook PDF",
  "- FreedomHouse: FIW methodology PDF",
  "- Polity: Polity5 manual PDF",
  "",
  paste0("Success: ", success_n, "/", total_n),
  paste0("Log file: ", log_path)
)

writeLines(readme_lines, con = readme_path)

message("Done. Successful files: ", success_n, "/", total_n)
message("Codebook folder: ", base_dir)
message("Log: ", log_path)
