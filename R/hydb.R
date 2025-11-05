
#' Connect to hydb
#'
#' @param path A character vector file path to `.sqlite` database.
#' @return Nothing. Side effect connecting to hydb.
#' @export
#'
#' @note Must be a member of the `USDA Northern Region Hydrology` sharepoint group if `path = NULL`.

hydb_connect <- function(path = NULL) {

  if(is.null(path)){

  windows_path <- normalizePath(file.path(Sys.getenv("HOMEDRIVE"), Sys.getenv("HOMEPATH")), winslash = .Platform$file.sep)

  path <- file.path('/USDA/Northern Region Hydrology - Documents/data-madness/hydb')

  mydb <- DBI::dbConnect(RSQLite::SQLite(), paste0(windows_path,path,"/hydb.sqlite"))

  } else {

  mydb <- DBI::dbConnect(RSQLite::SQLite(), path)

  }

  assign('mydb', mydb)

  mydb

}

#' Disconnect to hydb
#'
#' @return Nothing. Side effect connecting to hydb.
#' @export
#'
#' @note Must be a member of the `USDA Northern Region Hydrology` sharepoint group.

hydb_disconnect <- function(db = NULL) {

  if(is.null(db)) {

    DBI::dbDisconnect(mydb)

  } else {

    DBI::dbDisconnect(db)

  }

}
