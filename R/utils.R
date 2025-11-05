#' Copy hydb.sqlite and keep only the latest backup.
#' @param path Directory where hydb.sqlite and backups are stored.
#' @return TRUE if copy succeeded and cleanup was performed.
#' @export

hydb_copy_db <- function(path = "C:/Users/joshualerickson/USDA/Northern Region Hydrology - Documents/data-madness/hydb") {

    # Ensure path is normalized
    path <- normalizePath(path, winslash = .Platform$file.sep)

    # Timestamped backup name
    timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    backup_name <- paste0("hydb_", timestamp, ".sqlite")

    # Source and destination paths
    source_db <- file.path(path, "hydb.sqlite")
    destination_db <- file.path(path, backup_name)

    # Copy the DB
    success <- file.copy(from = source_db, to = destination_db, overwrite = TRUE)

    # If copy succeeded, clean up old backups
    if (success) {
      # List all backup files matching pattern
      backup_files <- list.files(path, pattern = "^hydb_\\d{8}_\\d{6}\\.sqlite$", full.names = TRUE)

      # Keep the latest one only
      if (length(backup_files) > 1) {
        # Sort by filename descending (latest first)
        backup_files_sorted <- sort(backup_files, decreasing = TRUE)

        # Remove all except the first (latest)
        file.remove(backup_files_sorted[-1])
      }
    }

    return(success)
}



#' @param table_type A character.
#'
#' @return A character of param code.
#' @noRd
param_cd <- function(table_type) {
  switch(
    gsub('_.*', '', table_type),
    'flow' = '00060',
    'tss' = '70288',
    'precip' = '00045',
    'stage' = '00065',
    'airtemp' = '00021',
    'wtemp' = '00011',
    'swidth' = '00004',
    'svel' = '72255',
    'sarea' = '82632'

  )
}

#' @param stat_type A character.
#'
#' @return A character of stat code.
#' @noRd
stat_cd <- function(stat_type) {
  dplyr::case_when(
    stringr::str_detect(stat_type,'mean') ~ '00003',
    stringr::str_detect(stat_type, 'max') ~ '00001',
    stringr::str_detect(stat_type,'sum') ~ '00006',
    stringr::str_detect(stat_type, 'min') ~ '00002',
    stringr::str_detect(stat_type, 'median') ~ '00008',
    stringr::str_detect(stat_type, 'stdev') ~ '00009',
    stringr::str_detect(stat_type, 'coef_var') ~ '00004'

  )
}

#' @param stat_type A character.
#'
#' @return A character of stat code.
#' @noRd
stat_cd_to_name <- function(stat_type) {
  dplyr::case_when(
    stringr::str_detect(stat_type,'00003') ~ 'mean',
    stringr::str_detect(stat_type, '00001') ~ 'max',
    stringr::str_detect(stat_type,'00006') ~ 'sum',
    stringr::str_detect(stat_type, '00002') ~ 'min',
    stringr::str_detect(stat_type, '00008') ~ 'median',
    stringr::str_detect(stat_type, '00009') ~ 'stdev',
    stringr::str_detect(stat_type, '00004') ~ 'coef_var'

  )
}

#' 'Clean' a character/factor vector like `janitor::clean_names()` does for data frame columns
#'
#' Most of the internals are from `janitor::clean_names()`
#'
#' @param x a vector of strings or factors
#' @param refactor if `x` is a factor, return a ref-factored factor? Default: `FALSE` == return character vector.
#' @param dupe_count logical.
#' @return A vector with clean names
#' @importFrom dplyr "%>%"
#'
clean_vec <- function (x, refactor=FALSE, dupe_count = FALSE) {

  require(magrittr, quietly=TRUE)

  if (!(is.character(x) || is.factor(x))) return(x)

  x_is_factor <- is.factor(x)

  old_names <- as.character(x)

  new_names <- old_names %>%
    gsub("'", "", .) %>%
    gsub("\"", "", .) %>%
    gsub("%", "percent", .) %>%
    gsub("^[ ]+", "", .) %>%
    make.names(.) %>%
    gsub("[.]+", "_", .) %>%
    gsub("[_]+", "_", .) %>%
    tolower(.) %>%
    gsub("_$", "", .)

  if(dupe_count){
  dupe_count <- sapply(1:length(new_names), function(i) {
    sum(new_names[i] == new_names[1:i])
  })

  new_names[dupe_count > 1] <- paste(
    new_names[dupe_count > 1], dupe_count[dupe_count > 1], sep = "_"
  )
  }

  if (x_is_factor && refactor) factor(new_names) else new_names

}




#' comids
#'
#' @description
#' Get NHDPLus comid by point location.
#'
#' @param point An sf POINT object
#'
#' @return A character vector with comid
#' @export
#'
comids <- function(point) {
  clat <- point$geometry[[1]][[2]]
  clng <- point$geometry[[1]][[1]]

  ids <- paste0("https://api.water.usgs.gov/geoserver/wmadata", "/ows", "?service=WFS&request=GetFeature&version=1.0.0",
         "&typeName=catchmentsp&outputFormat=application/json",
         "&CQL_FILTER=INTERSECTS(the_geom, POINT (", clng,
         " ", clat, "))", "&propertyName=featureid")

  d <- jsonlite::fromJSON(rawToChar(httr::RETRY("GET", utils::URLencode(ids))$content))

  as.integer(d$features$properties$featureid)

}

#'Water Year These functions are hi-jacked from smwrBase package.
#'
#'Create an ordered factor or numeric values from a vector of dates based on
#'the water year.
#' @noRd
#' @param x an object of class "Date" or "POSIXt." Missing values are permitted and
#'result in corresponding missing values in the output.
#' @param wy_month A numeric indicating the month the water year begins.
#' @param numeric a logical value that indicates whether the returned values
#'should be numeric \code{TRUE} or an ordered factor \code{FALSE}. The default
#'value is \code{FALSE}.
#' @return An ordered factor or numeric vector corresponding to the water year.
#' @note The water year is defined as the period from October 1 to September 30.
#'The water year is designated by the calendar year in which it ends. Thus, the
#'year ending September 30, 1999, is the "1999 water year."
#' @seealso
#Flip for production/manual
#'\code{\link[lubridate]{year}}
#\code{year} (in lubridate package)

waterYear <- function(x, wy_month = 10, numeric=FALSE) {
  ## Coding history:
  ##    2005Jul14 DLLorenz Initial dated verion
  ##    2010Feb17 DLLorenz Added option to return numerics
  ##    2011Jun07 DLLorenz Conversion to R
  ##    2012Aug11 DLLorenz Integer fixes
  ##    2013Feb15 DLLorenz Prep for gitHub
  ##
  x <- as.POSIXlt(x)
  yr <- x$year + 1900L
  mn <- x$mon + 1L
  ## adjust for water year
  yr <- yr + ifelse(mn < as.integer(wy_month), 0L, 1L)
  if(numeric)
    return(yr)
  ordered(yr)
}


#' Add Counts
#' @description Adds counts of observation per water and month
#'
#' @param data A daily value df
#'
#' @return counts within df
#' @importFrom dplyr "%>%"
#' @noRd
add_date_counts <- function(data) {


  data <- dplyr::group_by(data, sid, wy) %>%
    dplyr::add_count(name = 'obs_per_wy') %>%
    dplyr::ungroup() %>%
    dplyr::group_by(sid, wy, month) %>%
    dplyr::add_count(name = 'obs_per_month') %>%
    dplyr::ungroup()

  if(sub('_.*', '', colnames(data)[2]) == 'iv') {

    data <- data %>%
      dplyr::group_by(sid, wy, month, day) %>%
      dplyr::add_count(name = 'obs_per_day') %>%
      dplyr::ungroup()
  }

  data

}




#' water year to months
#' @description Change wy_month to doy.
#' @param wy_month A numeric
#' @param leap Logical
#' @return A numeric value
#' @noRd
month_to_doy <- function(wy_month, leap = FALSE) {


  ifelse(isTRUE(leap),
         dplyr::case_when(wy_month == 1 ~ 1,
                          wy_month == 2 ~ 32,
                          wy_month == 3 ~ 61,
                          wy_month == 4 ~ 92,
                          wy_month == 5 ~ 122,
                          wy_month == 6 ~ 153,
                          wy_month == 7 ~ 183,
                          wy_month == 8 ~ 214,
                          wy_month == 9 ~ 245,
                          wy_month == 10 ~ 275,
                          wy_month == 11 ~ 306,
                          wy_month == 12 ~ 336,
                          TRUE ~ NA_real_)
         ,
         dplyr::case_when(wy_month == 1 ~ 1,
                          wy_month == 2 ~ 32,
                          wy_month == 3 ~ 60,
                          wy_month == 4 ~ 91,
                          wy_month == 5 ~ 122,
                          wy_month == 6 ~ 152,
                          wy_month == 7 ~ 182,
                          wy_month == 8 ~ 213,
                          wy_month == 9 ~ 244,
                          wy_month == 10 ~ 274,
                          wy_month == 11 ~ 305,
                          wy_month == 12 ~ 335,
                          TRUE ~ NA_real_)
  )

}


yesno <- function(msg, .envir = parent.frame()) {
  yeses <- c("Yes", "Definitely", "For sure", "Yup", "Yeah", "Of course", "Absolutely")
  nos <- c("No way", "Not yet", "No", "Nope")

  cli::cli_inform(msg, .envir = .envir)
  qs <- c(sample(yeses, 1), sample(nos, 2))
  rand <- sample(length(qs))

  utils::menu(qs[rand]) != which(rand == 1)
}

