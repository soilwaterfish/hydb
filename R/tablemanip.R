#' Prep 'hydb' Table
#' @describeIn This function preps the data for more analysis.
#' @param data A `hydb` table.
#' @param wy_month Numeric. Month starting water year.
#'
#' @importFrom dplyr "%>%"
#'
#' @return `data` but with added attributes
#' @export
#'
hydb_prep_data <- function(data, wy_month = 10) {

  leap_years <- c(seq(1832,by = 4, length.out = 2000))


  data <- data %>%
    dt_to_tibble()

  colname1 <- colnames(data)[!is.na(match(colnames(data), c('dt', 'date')))]

  data %>%
    dplyr::mutate(year = lubridate::year(.data[[colname1]]),
           month = lubridate::month(.data[[colname1]]),
           day = lubridate::day(.data[[colname1]]),
           doy=lubridate::yday(.data[[colname1]]),
           wy_doy = ifelse(!(year %in% leap_years),ifelse(doy >= month_to_doy(wy_month, leap = F),
                                                          doy-month_to_doy(wy_month, leap = F)+1,
                                                          (365-month_to_doy(wy_month, leap = F)+1+doy)),
                           ifelse(doy >= month_to_doy(wy_month, leap = T),
                                  doy-month_to_doy(wy_month, leap = T)+1,
                                  (366-month_to_doy(wy_month, leap = T)+1+doy))),
           month_day = stringr::str_c(month, day, sep = "-"),
           wy = waterYear(.data[[colname1]], wy_month, TRUE),
           month_abb = factor(month.abb[month], levels = month.abb),
           month_day = stringr::str_c(month, day, sep = "-")) %>%
    add_date_counts()

}

#' Setup Metadata
#' @describeIn A function to setup user submitted stations metadata.
#' @param point An sf POINT cleaned.
#'
#' @return A `tibble()` ready for appending to `hydb`.
#' @export
#'
hydb_setup_metadata <- function(point) {

  if(any(!names(point) %in% c('station_nm', 'purpose', 'comments', 'geometry'))) stop(message('need correct column names'))

  # now give each station its sid
  # read in the admin districts
  # will need to change from local drive to data at some point

  meta_data <- point %>%
    sf::st_transform(4326) %>%
    sf::st_make_valid()

  meta_data_copy <- meta_data %>%
    sf::st_intersection(admin_districts)

  if(any(!meta_data$station_nm %in% meta_data_copy$station_nm)){
    site_off_fs_land <- meta_data[!meta_data$station_nm %in% meta_data_copy$station_nm,] %>%
      dplyr::mutate(
        sid = admin_districts[sf::st_nearest_feature(., admin_districts),]$sid,
        forest = admin_districts[sf::st_nearest_feature(., admin_districts),]$forest,
        district = admin_districts[sf::st_nearest_feature(., admin_districts),]$district,
        comments = paste0('Off NFS Land; ',comments)
      )

    meta_data_copy <- meta_data_copy %>% dplyr::bind_rows(site_off_fs_land) %>% sf::st_as_sf()

  }


  meta_data_coordinates <- meta_data_copy %>% dplyr::bind_cols(dplyr::tibble(Long = sf::st_coordinates(.)[,1],Lat = sf::st_coordinates(.)[,2])) %>% sf::st_drop_geometry()
  meta_data_copy <- meta_data_copy %>% dplyr::bind_cols(meta_data_coordinates %>% dplyr::select(Long, Lat))

  #check to see if it already exists and also get a unique suffix

  md_stations <- hydb_fetch('station_metadata', collect = T)

  district_filter <- unique(meta_data_copy$district)

  md <- md_stations %>% dplyr::filter(district %in% district_filter) %>% dplyr::collect()

  md_start <- max(as.numeric(substr(md$sid, 7, nchar(md$sid))))

  #now add unique 5-digit code

  meta_data_copy <- meta_data_copy %>%
    dplyr::arrange(dplyr::desc(Lat)) %>%
    dplyr::mutate(sid = paste0(sid, sprintf("%05d", md_start + dplyr::row_number())))

  if(any(meta_data$station_nm %in% md_stations$station_nm)){
    if (yesno(paste0('There is ',sum(meta_data$station_nm %in% md_stations$station_nm > 0) ,' `station_nm` named the same.\n Do you wnat to continue?'))) {
      return(invisible())
    }
  }

  #then run to get COMID
  comids_nwis <- meta_data_copy %>%
    split(.$sid) %>%
    purrr::map(~comids(.))


  comids_nwis <- dplyr::tibble(COMID = as.character(comids_nwis),
                        sid = names(comids_nwis))

  meta_data_final <- meta_data_copy  %>%
                    dplyr::left_join(comids_nwis) %>%
                    sf::st_drop_geometry() %>%
                    dplyr::mutate(region = 'Northern Region')


}


#' Transform IV to DV or DV to IV
#'
#' @param data A data.frame
#'
#' @return A `tibble()`.
#' @export
#' @importFrom dplyr "%>%"
#' @note Need to have columns `dt`, `sid` and `iv_*` and `iv_*` needs to be column position 2.

hydb_daily_transform <- function(data) {


  data <- data %>%
    dt_to_tibble()

  if(any(startsWith(colnames(data), 'iv'))) {

  colname <- colnames(data)[startsWith(colnames(data), 'iv')]

  data_add_daily_stats <-  data %>%
    dplyr::mutate( date = lubridate::as_date(dt)) %>%
    dplyr::group_by(sid, date) %>%
    dplyr::mutate(dplyr::across(dplyr::any_of(dplyr::starts_with('iv_')),
                     list(
                       sum = ~sum(.x, na.rm = TRUE),
                       max = ~max(.x, na.rm = TRUE),
                       min = ~min(.x, na.rm = TRUE),
                       mean = ~mean(.x, na.rm = TRUE),
                       median = ~median(.x, na.rm = TRUE),
                       stdev = ~sd(.x, na.rm = TRUE),
                       coef_var = ~sd(.x, na.rm = TRUE)/mean(.x, na.rm = TRUE))))  %>%
    dplyr::slice(1) %>%
    dplyr::ungroup() %>%
    dplyr::select(-dplyr::all_of(c(colname, 'dt'))) %>%
    dplyr::relocate(date, dplyr::starts_with('iv_')) %>%
    tidyr::pivot_longer(dplyr::starts_with('iv_')) %>%
    dplyr::mutate(statistic_type_code = stat_cd(name))

  param_type <- paste0('dv',unique(substr(data_add_daily_stats$name, 3, 8)))

  variable_to_rename <- 'value'

  data_add_daily_stats %>%
  dplyr::rename(!!param_type := !!rlang::sym('value')) %>%
  dplyr::mutate(param = colname) %>%
  dplyr::select(-name)  %>%
  dplyr::mutate(statistic_type_code = stat_cd_to_name(statistic_type_code)) %>%
  tidyr::pivot_wider(values_from = dplyr::starts_with('dv_'), names_from = statistic_type_code)%>%
  dplyr::relocate(date, sid, param, sum:coef_var)

  } else if (any(startsWith(colnames(data), 'dv'))) {

  data %>%
  dplyr::mutate(statistic_type_code = ifelse(is.na(statistic_type_code), 'mean', stat_cd_to_name(statistic_type_code))) %>%
  tidyr::pivot_wider(values_from = dplyr::starts_with('dv_'), names_from = statistic_type_code)

  } else if (any(!is.na(match(colnames(data), c('sum', 'max', 'min', 'mean', 'median', 'stdev', 'coef_var'))))) {

    data %>%
      tidyr::pivot_longer(sum:coef_var) %>%
      dplyr::mutate(statistic_type_code = stat_cd(name))

  } else {

    message('Not used for tables other than iv_ or dv_')
  }

}


#' Transform IV to DV
#'
#' @param data A data.frame
#'
#' @return A `tibble()`.
#' @export
#' @importFrom dplyr "%>%"
#' @note Need to have columns `dt`, `sid` and `iv_*` and `iv_*` needs to be column position 2.

hydb_qaqc <- function(data) {

  if(any(startsWith(colnames(data), 'iv'))) {

    data %>%
      dplyr::mutate(date = lubridate::as_date(dt)) %>%
      dplyr::group_by(sid, date) %>%
      dplyr::mutate(dplyr::across(dplyr::any_of(dplyr::starts_with('iv_')),
                                  list(
                                    iqr = ~quantile(.x, prob = 0.75) - quantile(.x, prob = 0.25),
                                    lb = ~quantile(.x, prob = 0.25) - 1.5*(quantile(.x, prob = 0.75) - quantile(.x, prob = 0.25)),
                                    ub = ~quantile(.x, prob = 0.75) + 1.5*(quantile(.x, prob = 0.75) - quantile(.x, prob = 0.25)),
                                    daily_z_score_outlier = ~ifelse(abs((.x - mean(.x, na.rm = TRUE))/sd(.x, na.rm = TRUE)) > 3, 'outlier', NA_character_),
                                    daily_iqr_outlier = ~ifelse(.x < quantile(.x, prob = 0.25) - 1.5*(quantile(.x, prob = 0.75) - quantile(.x, prob = 0.25)) |
                                                            .x > quantile(.x, prob = 0.75) + 1.5*(quantile(.x, prob = 0.75) - quantile(.x, prob = 0.25)),
                                                          'outlier', NA_character_)), .names = "{fn}"))  %>%
      #dplyr::slice(1) %>%
      dplyr::ungroup() %>%
      dplyr::select(-c('date', 'iqr', 'lb', 'ub'))

  } else if (any(startsWith(colnames(data), 'dv'))) {

    data %>%
      dplyr::mutate(statistic_type_code = stat_cd_to_name(statistic_type_code)) %>%
      tidyr::pivot_wider(values_from = dplyr::starts_with('dv_'), names_from = statistic_type_code)

  } else if (any(!is.na(match(colnames(data), c('sum', 'max', 'min', 'mean', 'median', 'stdev', 'coef_var'))))) {

    data %>%
      tidyr::pivot_longer(sum:coef_var) %>%
      dplyr::mutate(statistic_type_code = stat_cd(name))

  } else {

    message('Not used for tables other than iv_ or dv_')
  }

}
#' Convert dt to tibble
#'
#' @param data a data.frame and data.table
#' @noRd
#' @importFrom dplyr "%>%"
#' @return a tibble df
dt_to_tibble <- function(data) {

  if(any(class(data) %in% c("tbl_SQLiteConnection", "tbl_dbi", "tbl_sql", "tbl_lazy"))) stop(message('Cannot perform on class "tbl_SQLiteConnection", "tbl_dbi", "tbl_sql", "tbl_lazy"'))

  class(data) <- 'data.frame'

  data <- dplyr::tibble(data)

  prefixes <- c('iv_', 'dv_', 'obs_', 'date', 'dt')

  matched_prefixes <- sapply(colnames(data), function(col) {
    match <- prefixes[grepl(paste(prefixes, collapse = '|'), col)]
    if(length(match) > 0) match else NA
  })


  colname1 <- names(matched_prefixes[!is.na(matched_prefixes)])[1]

  switch(sub('_.*', '', colname1),
        'dv' = data %>%
          dplyr::mutate(date = lubridate::as_date(date)),
        'date' = data %>%
          dplyr::mutate(date = lubridate::as_date(date)),
        'iv' = data %>%
          dplyr::mutate(dt = lubridate::as_datetime(dt)),
        'dt' = data %>%
          dplyr::mutate(dt = lubridate::as_datetime(dt)),
        'obs' = data %>%
          dplyr::mutate(date = lubridate::as_date(date)))

}

#' Conversions
#'
#' Convert Metrics
#' @param x data.frame
#' @param type character.
#'
#' @note When using 'c' for Celsius, this converts Fahrenheit to Celsius and vice versa for 'f'.
#'
#'
#' @return Side effect transformation on data.frame
#' @export
#'
hydb_conversions <- function(x, type) {

  switch(type,
         'c' = (x - 32) * (5/9),
         'f' = x * (9/5) + 32)

}

#' Stations in Tables
#'
#' @param data data.frame with station sid's or character vector of sid's.
#' @param hydbtables character. tables to select from, default 'all'.
#' @importFrom dplyr "%>%"
#'
#' @return Nothing. Side effect to `hydb`.
#' @export
#'

hydb_station_info <- function(data, hydbtables = 'all') {

  if(any(class(data) %in% c('data.frame', 'tbl', 'tbl_df'))){

    stored_sid <- unique(data[['sid']])

  } else {

    stored_sid <- data

  }

  tables <- DBI::dbListTables(hydb_connect())

  if(any(tables != 'all')){

    tables <- tables[tables %in% hydbtables]

  } else {

    tables <- tables[!tables %in% c('station_metadata', 'stat_codes', 'param_codes')]

  }

  station_info_func <- function(table, stored_sid) {

    final_data <- dplyr::tibble()

    for(i in table) {

    rows <- hydb_fetch(table = i, sid %in% stored_sid, collect = FALSE) %>%
                   dplyr::mutate(n = n()) %>%
                   utils::head(1) %>%
                   dplyr::pull(n)

    if(length(rows) > 0){

    info <- hydb_fetch(table = i, sid %in% stored_sid, collect = FALSE) %>% utils::head(1) %>%  dplyr::collect()

    colnames1 <- colnames(info)[!is.na(match(colnames(info), c('dt', 'date')))]


    years <- hydb_fetch(table = i, sid %in% stored_sid, collect = FALSE) %>%
        dplyr::group_by(year = lubridate::year(.data[[colnames1]])) %>%
        slice_hydb(1) %>%
        dplyr::ungroup() %>%
        dplyr::collect() %>%
        dt_to_tibble() %>%
        dplyr::mutate(year = lubridate::year(.data[[colnames1]])) %>%
        dplyr::pull(year)

    #years <- paste(years, collapse = ', ')

    } else {

    years <- NA_real_
    rows <- NA_real_

    }

    info_list <- dplyr::tibble(
                'table' = i,
                'count' = rows,
                'years' = list(years),
                'sid' = stored_sid
                )

    final_data <- dplyr::bind_rows(final_data, info_list)

    }


    final_data

  }

  final_list <- purrr::map(stored_sid,~station_info_func(tables,.x))

  if(any(class(data) %in% c('data.frame', 'tbl', 'tbl_df'))){

    dplyr::bind_rows(final_list) %>%
      dplyr::filter(!is.na(years)) %>%
      dplyr::rowwise() %>%
      dplyr::mutate(
      min_year = min(years),
      max_year = max(years)) %>%
      dplyr::mutate(
        longest_streak = longest_streak(years)[[1]]-1,
        missing_years = ifelse(length(as.character(longest_streak(years)[[2]])) == 0,
                               NA_character_,
                               paste(as.character(longest_streak(years)[[2]]), collapse = ', ')),
        total_missing_years = length(as.character(longest_streak(years)[[2]]))
      ) %>%
      dplyr::ungroup() %>%
    dplyr::left_join(data, by = 'sid') %>%
      dplyr::rowwise() %>%
      dplyr::mutate(years = as.character(paste0(years, collapse = ', '))) %>%
      dplyr::ungroup()

  } else {

    dplyr::bind_rows(final_list) %>%
      dplyr::filter(!is.na(years)) %>%
      dplyr::rowwise() %>%
      dplyr::mutate(
      min_year = min(years),
      max_year = max(years)) %>%
      dplyr::mutate(
        longest_streak = longest_streak(years)[[1]]-1,
        missing_years = ifelse(length(as.character(longest_streak(years)[[2]])) == 0,
                               NA_character_,
                               paste(as.character(longest_streak(years)[[2]]), collapse = ', ')),
        total_missing_years = length(as.character(longest_streak(years)[[2]]))
      ) %>%
      dplyr::ungroup() %>%
      dplyr::rowwise() %>%
      dplyr::mutate(years = as.character(paste0(years, collapse = ', '))) %>%
      dplyr::ungroup()
  }
}



#' Find Closest Match
#'
#' @param text A character string
#' @param keys A character string
#'
#' @return A character string

find_closest_match <- function(text, keys) {

  cleaned_text <- gsub("[^a-zA-Z]", " ", gsub("_", " ", text))

  distances <- stringdist::stringdist(cleaned_text, keys, method = 'jw', p = 0.1)

  closest_match <- keys[which.min(distances)]

  as.character(closest_match)

}

#' Longest Streak
#'
#' @param num_list A list with numeric input.
#'
#' @return A list

longest_streak <- function(num_list) {
  if (length(num_list) == 0) return(list(longest_streak = 0, missing_numbers = integer(0)))

  num_list <- sort(unique(num_list))  # Sort and remove duplicates
  diff_seq <- c(1, diff(num_list))    # Compute differences

  # Identify streaks (consecutive numbers have diff of 1)
  streaks <- rle(diff_seq == 1)

  # Find the longest streak
  longest_streak <- ifelse(any(streaks$values),
                           max(streaks$lengths[streaks$values]) + 1, 1)

  # Identify missing numbers
  full_range <- seq(min(num_list), max(num_list))
  missing_numbers <- setdiff(full_range, num_list)

  return(list(longest_streak = longest_streak, missing_numbers = missing_numbers))
}

