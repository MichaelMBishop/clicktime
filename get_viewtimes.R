####~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
### Observe Time Taken to Observe IFPs Before Forecasting
###
### Purpose
###  * LoadIfpClicksYear() does initial processing of raw click data
###  * GetIfpViewTimes() loads processed click data and calculates 
###       question viewing time variables transformed a couple ways
###       - producing output for aggregation algorithms
### 
### Notes: Each row in the primary data.table represents a user's click ("action") 
###        Variables indicate user_id, ifp_id, type of action ("OPEN" or "FCAST"),
###           and time stamp)  
###    
###        
###  
### Primary Creator(s): Michael Bishop
####~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

require(data.table)
require(zoo)

#load global variable and data loading functions
setwd("~/ACE/data")
source("../global_vars.R")
source(file.path(kRootPath, "util", "load_data.R"), chdir = TRUE)
source(file.path(kRootPath, "util", "util_functions.R"),chdir = TRUE)

#all ifp details
all.ifps <- LoadIfps(return.type="data.table")

# Returns processed click data (optionally saves it as .RData)
LoadIfpClicksYear <- function(year = c("year3","year2"),
                              load.previous = TRUE,
                              drop.disabled.users = TRUE,
                              keep.only.active.ifps = FALSE,
                              keep.only.action.open = FALSE,
                              overwrite.rdata = TRUE,
                              progress = TRUE){
  
  #define paths to be used in function
  data.path <- file.path(kDataPath,"behavior",switch(year,
                                                     year3 = "user_actions.yr3.csv",
                                                     year2 = "user_actions.yr2.csv"))
  if(!file.exists(data.path)) stop("Cannot load viewtimes data because ", data.path, " does not exist.")
  
  args          <- c(as.list(environment()), list())
  cached.result <- CheckCache("LoadIfpClicksYear", args)
  if(!is.null(cached.result)) return(cached.result)
  
  if(progress) {
    message("Preparing Click Data...")
    n.steps <- 10
    prog <- txtProgressBar(min=0, max=n.steps, style=3)
  }
  
  year <- match.arg(year)
  opens.path <- file.path( kDataPath, switch(year,
                                             year2 = "ifp_opens.yr2.RData",
                                             year3 = "ifp_opens.yr3.Rdata"))
  clicks.path <- file.path( kDataPath, switch(year,
                                              year2 = "ifp_clicks.yr2.RData",
                                              year3 = "ifp_clicks.yr3.Rdata"))
  
  ifp.clicks.all <- fread(data.path)
  
  # rename "date" to "date_action"
  if("date" %in% names(ifp.clicks.all)) 
    setnames(ifp.clicks.all, "date", "date_action")
  
  setkey(ifp.clicks.all, date_action)
  
  if(progress) setTxtProgressBar(prog, 1)
  # load previously processed data if available and applicable
  if(load.previous == TRUE) {
    if(keep.only.action.open == TRUE &
         file.exists(opens.path)) {
      load(opens.path); prev.ifp.clicks <- ifp.clicks
    } else if(file.exists(clicks.path)) {
      load(clicks.path); prev.ifp.clicks <- ifp.clicks
    } 
  }
  
  # drop duplicate clicks
  ifp.clicks <- unique(ifp.clicks.all, by = NULL)
  
  # rename "userid" & tgjuserid to "user_id"
  if("userid" %in% names(ifp.clicks)) {
    setnames(ifp.clicks, "userid", "user_id")
  } else if ("tgjuserid" %in% names(ifp.clicks)) {
    setnames(ifp.clicks, "tgjuserid", "user_id")
  }
  
  # drop researchers
  ifp.clicks <- switch(year,
                       year3 = ifp.clicks[ as.numeric(user_id) > 0 & as.numeric(user_id) < 17000 , ],
                       year2 = ifp.clicks[ as.numeric(user_id) > 0 & as.numeric(user_id) < 7000 , ])
  
  ifp.clicks$index <- seq(1, nrow(ifp.clicks), by = 1)
  
  # get action type (i.e. "OPEN", "FCAST", etc.) but not necessarily on an IFP
  ifp.clicks[ , act := unlist(strsplit(action, split = " "))[1] , by = "index"]
  
  if(progress) setTxtProgressBar(prog, 2)
  
  # confirm there is new raw data and return old .RData if there isn't
  if(load.previous == TRUE & exists("prev.ifp.clicks")) {
    if(max(as.Date(ifp.clicks[act == "OPEN"]$date_action)) <= 
         max(as.Date(prev.ifp.clicks[act == "OPEN"]$date_action))) {
      message("No new raw click data since ", max(prev.ifp.clicks[act == "OPEN"]$date_action),
              "\nreturning previously processed data stored in\n",
              switch(paste(keep.only.action.open),
                     "TRUE" = opens.path,
                     "FALSE" = clicks.path))
      setTxtProgressBar(prog, n.steps)
      close(prog)
      SaveCache(prev.ifp.clicks, fn.name="LoadIfpClicksYear", args = args )
      return(prev.ifp.clicks)    
    } else {
      #otherwise we want to eliminate all the previously processed rows to save time
      ifp.clicks <- ifp.clicks[ date_action > max(prev.ifp.clicks[act=="OPEN"]$date_action) ]
    }
  }
  #   message("Processing clicks for ",nrow(ifp.clicks)," actions.")
  if(progress) setTxtProgressBar(prog, 3)
  
  ###########################################
  ### BEGIN PROCESSING NEW RAW CLICK DATA ###
  ###########################################
  
  # these actions imply potential interaction with an IFP
  ifp.clicks <- ifp.clicks[ act %in% c("OPEN", "FCAST", "CANCEL", "LOGOUT", "RETURN"),  ]
  
  # get ifp_id (and some others)
  ifp.clicks[ , ifp_id := strsplit(action, split = " ")[[1]][3] , by = "index" ]
  if(progress) setTxtProgressBar(prog, 4)
  
  # get second string for distinguishing opening IFPs from opening other things
  ifp.clicks[ , sec_str_from_action := strsplit(action, split = " ")[[1]][2] , by = "index" ]
  if(progress) setTxtProgressBar(prog, 5)

  # pull missing ifp_ids from the ends of the action strings
  ifp.clicks[ , ifp_tail := strsplit( strsplit(action, split = "=")[[1]][2], split="&")[[1]][1], by="index"]
  ifp.clicks[ ifp_id == "" | is.na(ifp_id) & !is.na(ifp_tail) ]$ifp_id <-
    ifp.clicks[ ifp_id == "" | is.na(ifp_id) & !is.na(ifp_tail) ]$ifp_tail
  if(progress) setTxtProgressBar(prog, 6)

  
  if (year == "year2") {
    # problem: some FCAST are missing ifp_id, I fill them in with the ifp_id
    # from the previous action (which should be opening that IFP)
    setkey(ifp.clicks, user_id, date_action)
    ifp.clicks[act %in% c("OPEN", "FCAST") & sec_str_from_action == "IFP"]$ifp_id <- 
      na.locf(ifp.clicks[act %in% c("OPEN", "FCAST") & sec_str_from_action == "IFP"]$ifp_id, fromLast=FALSE) 
    
    # I'm not sure what clicks on ifps with ids > 9000 are, I think #1214 should be highest in season 2
    ifp.clicks <- ifp.clicks[ !(ifp_id %in% c("9001", "9002")) , ] # drops 12 cases
    ifp.clicks <- unique(ifp.clicks, by = NULL) 
    
    ##########################################################
    # Fix Missing Data Problem
    #   Year2 click data is missing all forecasts after 7/16/12 10:12:59 
    #     and before 8/26/12 10:01:28, BUT luckily it is redundant, we just
    #     have to load the regular forecast data and get the timestamps for
    #     the place where its missing
    ##########################################################
    
    source("~/ACE/util/load_data.R", chdir = TRUE)
    fcasts <- LoadFcasts(start.date = "2012-07-16", 
                         end.date = "2012-08-26", 
                         date.type = "date", 
                         fcast.type=c(0:2), 
                         q.status=c("active","closed","voided"))
    
    fcasts <- fcasts[answer_option == "a", ]
    fcasts$ifp_id <- substr(fcasts$ifp_id, 1, 4)
  
    # subset forecast data from period for which forecasts are missing in click data
    missing.fcasts <- fcasts[ created_at > "2012-07-16 10:12:59", ]
    missing.fcasts <- missing.fcasts[ created_at < "2012-08-26 10:01:28", ]
    missing.fcasts[ , action := paste("FCAST IFP", ifp_id, sep = " "), ]
    missing.fcasts$act <- "FCAST"
    missing.fcasts$sec_str_from_action <- "IFP"
    missing.fcasts$ifp_tail <- NA
  
    missing.fcasts <- rename(missing.fcasts, c("created_at" = "date_action"))
  
    # create index which doesn't overlap with index in ifp.clicks
    missing.fcasts$index <- seq(1000001, 1000000 + nrow(missing.fcasts), by = 1)
    missing.fcasts <- missing.fcasts[ , list(user_id, date_action, action, index, act, 
                                             ifp_id, sec_str_from_action, ifp_tail),  ]
  
    # Hooray you can fill in the missing fcasts now!
    ifp.clicks <- rbind(ifp.clicks, missing.fcasts)
    setkey(ifp.clicks, date_action, ifp_id, user_id)
    ifp.clicks$index_old <- ifp.clicks$index
    ifp.clicks$index <- seq(1, nrow(ifp.clicks), by = 1)
  
    ifp.clicks$ifp_id <- substr(ifp.clicks$ifp_id, 1, 4)
  }
  
  #get rid of these weirdos
  ifp.clicks <- ifp.clicks[ !(ifp_id == "1255-") ]
  ifp.clicks <- ifp.clicks[ !(ifp_id == "-1794591734") ]
  ifp.clicks <- ifp.clicks[ !(ifp_id == c("undefined")) ]
  
  # handy reference for all ifps currently referenced
  ifp.ids.in.data <- unique( ifp.clicks[ sec_str_from_action == "IFP" ]$ifp_id )
  ifp.ids.in.data <- ifp.ids.in.data[ !is.na(ifp.ids.in.data) ]
  
  # CALCULATE TIME FROM EVERY ACTION UNTIL EVERY NEXT ACTION  
  setkey(ifp.clicks, user_id, date_action)
  
  ifp.clicks[ , date_numeric := as.numeric(as.POSIXct(date_action, origin="1970-01-01: 00:00:00")) ]
  setkey(ifp.clicks, user_id, date_numeric)
  ifp.clicks[ , time_to_next := diff(date_numeric), by = "user_id"]
  if(progress) setTxtProgressBar(prog, 7)
  
  # set time_to_next missing for every user's last action
  ifp.clicks[ ,last_action := date_numeric == max(date_numeric), by="user_id"]
  ifp.clicks[ last_action == TRUE ]$time_to_next <- NA
  
  #deal with timeouts
  ifp.clicks[ ,time_to_next_ceiling     := min(time_to_next, 600), by="index" ]
  ifp.clicks[ ,log_time_to_next_ceiling     := log(time_to_next_ceiling     + 1) ]
  
  setkey( ifp.clicks, user_id, ifp_id, date_action)
  if(progress) setTxtProgressBar(prog, 8)
  
  ####################################################
  # drop a whole variety of actions that aren't "OPEN" or "FCAST" 
  ifp.clicks <- ifp.clicks[ ifp_id %in% ifp.ids.in.data, ]
  ifp.clicks[ , ifp_id_short := substr(ifp_id, 1, 4) ]
  ifp.clicks[ , ifp_id_length := nchar(ifp_id), by="ifp_id" ]
  
  all.ifps[, ifp_id_short := substr(ifp_id, 1, 4) ]
  
  clicks.4 <- ifp.clicks[ ifp_id_length == 4 ]
  ifps.4 <- all.ifps[ substr(ifp_id,6,6) %in% c("0","6"),list(ifp_id_long = ifp_id,ifp_id_short) ]
  clicks.6 <- ifp.clicks[ ifp_id_length == 6 ]
  clicks.4 <- merge(clicks.4,
                    ifps.4,
                    by="ifp_id_short")
  clicks.4$ifp_id <- NULL; setnames(clicks.4, "ifp_id_long", "ifp_id")
  setcolorder(clicks.4, names(clicks.6))
  ifp.clicks <- rbindlist(list(clicks.4,clicks.6))
  ifp.clicks$ifp_id_short <- ifp.clicks$ifp_id_length <- NULL
  if(progress) setTxtProgressBar(prog, 9)
  
  ifp.clicks$user_id <- as.character(ifp.clicks$user_id)
  
  # merge previously processed and newly processed data
  if(load.previous == TRUE & exists("prev.ifp.clicks")) {
    prev.ifp.clicks <- prev.ifp.clicks[,names(ifp.clicks),with=FALSE]
    setcolorder(prev.ifp.clicks, names(ifp.clicks))
    ifp.clicks <- unique(rbind(prev.ifp.clicks, ifp.clicks))
    setkey(ifp.clicks, user_id, date_action)
  }
  
  start.date <- switch(year,
                       "year3" = kYear3Start,
                       "year2" = kYear2Start)
  end.date <- switch(year,
                     "year3" = kYear3End,
                     "year2" = kYear2End)
  
  if(drop.disabled.users) {
    active.users <- LoadUsers(years=switch(year,
                                           "year3" = 3,
                                           "year2" = 2), user.statuses=1)
    ifp.clicks <- ifp.clicks[ user_id %in% active.users$user_id ]
  }
  
  if(keep.only.active.ifps) {
    ifp.clicks <- ifp.clicks[ ifp_id %in% all.ifps[ q_status == "active" ]$ifp_id, ]
  }
  
  if(keep.only.action.open) {
    # IFP openings are the only actions necessary to calculate
    ifp.clicks <- ifp.clicks[ act == "OPEN" ]
  }
  
  # Calculate cum_time_ifp, a running total of time spent viewing ifps for each combination of ifp_id/user_id
  #   note: this sums across all branches of continuous ifps
  setkey(ifp.clicks, user_id, ifp_id, date_action)
  
  #this should really just exclude FCAST actions, rather than only opens
  #if the user is only keeping OPENs, they will be the only ones left already
  ifp.clicks[ (act != "FCAST" & !is.na(log_time_to_next_ceiling)), 
              cum_log_time_ifp := cumsum(log_time_to_next_ceiling), by="user_id,ifp_id"]
  if(progress) setTxtProgressBar(prog, 10)
  
  # Saving output to file requires overwrite.rdata = TRUE
  # seems like it should be the default..
  if(overwrite.rdata) {
    if(keep.only.action.open) {
      save(ifp.clicks, file = opens.path)
    } else {
      save(ifp.clicks, file = clicks.path)
    }
  }
  
  SaveCache(ifp.clicks, fn.name="LoadIfpClicksYear", args = args )
  return(ifp.clicks)
}



# GetIfpViewTimes() runs on the output of LoadIfpClicksYear()
#   if data is not provided it checks default s
GetIfpViewTimes <- function(input.data = NULL,
                            action.category = c("opens","clicks"),
                            users = NULL,
                            date = NULL) {
  if(is.null(date)) stop("You must send a 'date' parameter to GetIfpViewTimes()")
  action.category <- match.arg(action.category)
  year <- GetTournamentYear( date )
  data.path <- file.path( kDataPath, switch(year,
                                            year2 = switch(action.category,
                                                           opens = "ifp_opens.yr2.RData",
                                                           clicks = "ifp_clicks.yr2.RData"),
                                            year3 = switch(action.category,
                                                           opens = "ifp_opens.yr3.Rdata",
                                                           clicks = "ifp_clicks.yr3.RData")))
  
  # if input data is not provided, load it
  if(!is.null(input.data)) {
    ifp.opens <- input.data
  } else if(file.exists(data.path)) {
    ifp.opens <- LoadIfpClicksYear(year=year,
                                   load.previous=TRUE,
                                   drop.disabled.users=TRUE,
                                   keep.only.active.ifps=FALSE,
                                   keep.only.action.open=switch(action.category,
                                                                opens = TRUE,
                                                                clicks = FALSE),
                                   overwrite.rdata=TRUE,
                                   progress = TRUE)
  } else if(!file.exists(data.path)) {
    ifp.opens <- LoadIfpClicksYear(year=year,
                                   load.previous=FALSE,
                                   drop.disabled.users=TRUE,
                                   keep.only.active.ifps=FALSE,
                                   keep.only.action.open=switch(action.category,
                                                                opens = TRUE,
                                                                clicks = FALSE),
                                   overwrite.rdata=TRUE,
                                   progress = TRUE)
  }
  
  # drop unneeded users
  if(!is.null(users)) {
    ifp.opens <- ifp.opens[ user_id %in% users ]
  }
  
  # drop data after relevant date
  ifp.opens <- ifp.opens[ date_action <= date ]
  
  # identify most recent action by each forecaster on each ifp 
  ifp.opens[ ,most_recent_action := date_action == max(date_action), by="user_id,ifp_id" ]
  ifp.opens <- ifp.opens[ most_recent_action == TRUE ]
  
  # calculate percentile standing of question viewing time
  ifp.opens[ ,per_cum_log_time_ifp := 
              rank(cum_log_time_ifp, na.last = "keep", ties.method = "average")/
              sum(!is.na(cum_log_time_ifp)), "ifp_id"]
  
  # adding 1 second for good measure
  ifp.opens[ ,log_cum_log_time_ifp := log(cum_log_time_ifp + 1) ]
  return(ifp.opens[ ,list( user_id, ifp_id, log_time = log_cum_log_time_ifp, q_time = per_cum_log_time_ifp) ] )
} 


if (FALSE) {
  library(ggplot2)
  # initial click processing
  system.time(clicks <- LoadIfpClicksYear(year = "year2",
                                          load.previous = FALSE, 
                                          keep.only.action.open = TRUE, 
                                          overwrite.rdata = TRUE))
  #run the above again to see how caching speeds it up
  system.time(clicks.yr3 <- LoadIfpClicksYear(year = "year3",
                                          load.previous = FALSE, 
                                          keep.only.action.open = TRUE, 
                                          overwrite.rdata = TRUE))
  
  #now using load.previous will speed things up slightly if the 
  #program is run for the first time in a new session
  system.time(clicks.yr2 <- LoadIfpClicksYear(year = "year2",
                                          load.previous = TRUE, 
                                          keep.only.action.open = TRUE, 
                                          overwrite.rdata = TRUE))
  
  #again, with caching running the same call twice is now extremely quick
  system.time(clicks <- LoadIfpClicksYear(year = "year3",
                                          load.previous = TRUE, 
                                          keep.only.action.open = TRUE, 
                                          overwrite.rdata = TRUE))
  
  # calculate ifp view times
  system.time(vtime <- GetIfpViewTimes(users = NULL,
                                       input.data=NULL,
                                       action.category="opens", 
                                       date = "2014-01-01"))
  
  #now it's actually slightly faster to leave input.data as NULL
  system.time(vtime <- GetIfpViewTimes(users = NULL,
                                       input.data=clicks,
                                       action.category="opens", 
                                       date = "2014-01-01"))
  
  #plot some examples
  qplot(vtime$log_cum_log_time_ifp)
  qplot(vtime[ifp_id == "1128-0"]$log_cum_log_time_ifp)
  qplot(vtime[ifp_id == "1128-0"]$per_cum_log_time_ifp)
  
}