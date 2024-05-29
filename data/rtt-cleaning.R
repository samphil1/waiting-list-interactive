##### Clean RTT data for waiting list interactive calculator #####
# Sam's version
# Define custom colors
mycolours <- c("#DC2937", "#005c5d", "#744284", "#744284", "#2ca365", "#f39214", "#ffd412", "#092a40")

# load packages
library(readxl)
library(openxlsx)
library(janitor)
library(lubridate)
library(dplyr)
library(zoo)
library(tidyverse)
library(data.table)

 
##### Load raw data (download from https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/)

# Functions to fetch and process data

GetLinks <- function(url_name, string) {
  files <- c()
  for (i in seq_along(url_name)) {
    pg <- rvest::read_html(url_name[i])
    pg <- rvest::html_attr(rvest::html_nodes(pg, "a"), "href")
    files <- c(files, pg[grepl(string, pg, ignore.case = TRUE)])
    files <- unique(files)
  }
  return(files)
}

UnzipCSV <- function(file) {
  temp <- tempfile()
  download.file(file, temp)
  file_names <- unzip(temp, list = TRUE)$Name
  csv_files <- file_names[grepl('.csv', file_names)]
  data <- lapply(csv_files, function(x) {
    dirty_data <- unzip(temp, x, exdir = tempdir())
    CleanFiles(dirty_data, dirty_data)
    cleaned_data <- fread(dirty_data, encoding = "UTF-8")
    names(cleaned_data) <- make_clean_names(names(cleaned_data))
    return(cleaned_data)
  })
  names(data) <- csv_files
  unlink(temp)
  return(data)
}

CleanFiles <- function(file, newfile) {
  writeLines(iconv(readLines(file, skipNul = TRUE)), newfile)
}

# Scrape data, Urls for datasets 

rtt_url <- "https://england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2023-24/"
rtt_links <- GetLinks(rtt_url, "statistical-work-areas/rtt-waiting-times/rtt-data-")
rtt_link_final <- GetLinks(rtt_links, 'Full-CSV-data')

# Take last 10 datasets

rtt_results <- rtt_link_final[1:10]
# Read data
rtt_data <- sapply(rtt_results,
                   function(x){
                     UnzipCSV(x)
                   })

#Apply function to all .zip links

FINAL_rtt <- lapply(rtt_data,
                    function(x){
                      x %>%
                        dplyr::mutate(all_rtt = total_all,
                                      date = zoo::as.yearmon(substr(period, 5, 99), '%B-%Y')) %>%
                        #Remove all the columns that are 'waiting 18 to 20 weeks' or whatever,
                        #We aren't using them and there is mismatch after 2019/20
                        dplyr::select(-starts_with('gt')) %>%
                        #Select ALL specialties
                        #dplyr::filter(treatment_function_name == 'Total') %>%
                        dplyr::rename( 'trust_code' = provider_org_code)
                    }) %>%
  #collapse the list
  data.table::rbindlist(fill=T) %>%
  #remove treatment_function_name if needed?
  dplyr::group_by(date,treatment_function_code,treatment_function_name,rtt_part_description,trust_code) %>%
  dplyr::summarise(all_rtt = sum(all_rtt,na.rm=T)) %>%
  #Useful for left_joins
  tidyr::pivot_wider(.,names_from=rtt_part_description,values_from=all_rtt)
FINAL_rtt

##### initial formatting of raw data #####

rtt_data_raw <- read.xlsx("data/RTT-Overview-Timeseries-Including-Estimates-for-Missing-Trusts-Mar24-XLS-103K-16220.xlsx", na.strings = "-", startRow = 11, fillMergedCells = TRUE, skipEmptyCols = TRUE)

rtt_data_raw[204, 2] <- 45323
rtt_data_raw$Month <- convert_to_date(rtt_data_raw$Month)

# add the merged column headers back in as part of colnames
names(rtt_data_raw)[3:20] <- paste0("incomplete_", names(rtt_data_raw)[3:20])
names(rtt_data_raw)[21:26] <- paste0("admitted_unadj_", names(rtt_data_raw)[21:26])
names(rtt_data_raw)[27:32] <- paste0("non_admitted_", names(rtt_data_raw)[27:32])
names(rtt_data_raw)[33:34] <- paste0("new_referrals_", names(rtt_data_raw)[33:34])
names(rtt_data_raw)[35:39] <- paste0("admitted_adj_", names(rtt_data_raw)[35:39])

# final cleaning of names
rtt_data_raw <- rtt_data_raw %>% 
  clean_names() %>% 
  rename(fiscal_year = year, month_year = month)

##### Clean data #####

rtt_data <- rtt_data_raw %>% 
  
  # select only columns of interest
  select(month_year
         , incomplete_total_waiting_mil_with_estimates_for_missing_data
         , new_referrals_no_of_new_rtt_periods_with_estimates_for_missing_data
         , non_admitted_no_of_pathways_all_with_estimates_for_missing_data
         , admitted_unadj_no_of_pathways_all_with_estimates_for_missing_data
  ) %>%
  
  # make names nicer
  rename(
    waiting_list = incomplete_total_waiting_mil_with_estimates_for_missing_data
    , new_referrals = new_referrals_no_of_new_rtt_periods_with_estimates_for_missing_data
    , completed_non_admitted = non_admitted_no_of_pathways_all_with_estimates_for_missing_data
    , completed_admitted = admitted_unadj_no_of_pathways_all_with_estimates_for_missing_data
  ) %>% 
  
  # convert month year get total completed
  mutate(
    month_year = ymd(month_year)
    , completed_total = completed_admitted + completed_non_admitted
  ) %>% 

  # get total activity/outflow
  mutate(
    waiting_list_last_val = lag(waiting_list) # find the last value-- doing explicitly for QA purposes
  , total_activity = waiting_list_last_val - waiting_list + new_referrals
  , other_reasons = total_activity - completed_total
  ) %>% 
  
  # only include data from FY 2016 to June 2023
  filter(month_year >= ymd("2016-04-01") & month_year <= latest_data) 
  
  
###### Calculate daily seasonality #####

# read in CSV with number of working days each month 
workdays_table <- read.csv("data/working-days-table.csv") %>% 
  mutate(month_year = ymd(month_year))

rtt_data <- rtt_data %>% 
  left_join(workdays_table, by = "month_year") %>% 
  # get day rates for each month
  mutate(new_referrals_day_rate = new_referrals / workdays
         , total_activity_day_rate = total_activity / workdays) %>%
  # get financial year 
  mutate(fin_year = floor(quarter(month_year, with_year = TRUE, fiscal_start = 4))) 

# Create a table of seasonality multiplicative factors
seasonality <- rtt_data %>% 
  filter(month_year < ymd("2019-04-01")) %>% 
  # group by year to get a yearly average
  group_by(fin_year) %>% 
  mutate(avg_yearly_referral_day_rate = sum(new_referrals)/sum(workdays)
         , avg_yearly_activity_day_rate = sum(total_activity)/sum(workdays)) %>% 
  ungroup() %>% 
  # get proportional difference between month and year
  mutate(referrals_diff = new_referrals_day_rate/avg_yearly_referral_day_rate
         , activity_diff = total_activity_day_rate/avg_yearly_activity_day_rate) %>% 
  # group by month and get average monthly seasonality
  group_by(month = month(month_year)) %>% 
  summarise(referrals_seasonality = mean(referrals_diff)
            , activity_seasonality = mean(activity_diff))

##### Get trendlines for pre and post-pandemic #####

# Add a month number column 
rtt_data <- rtt_data %>% 
  mutate(month_no = interval(ymd("2016-03-01"), month_year) %/% months(1)
         , month = month(month_year)) %>% 
  left_join(seasonality, by = "month")

# fit linear trend to day rates of referrals and completed pathways
pre_pandemic_referrals_day <- lm(new_referrals_day_rate/referrals_seasonality ~ month_no, data = rtt_data[rtt_data$month_year < ymd("2020-03-01"),])
post_pandemic_referrals_day <- lm(new_referrals_day_rate/referrals_seasonality ~ month_no, data = rtt_data[rtt_data$month_year > ymd("2021-04-01"),])
pre_pandemic_activity_day <- lm(total_activity_day_rate/activity_seasonality ~ month_no, data = rtt_data[rtt_data$month_year < ymd("2020-03-01"),])
post_pandemic_activity_day <- lm(total_activity_day_rate/activity_seasonality ~ month_no, data = rtt_data[rtt_data$month_year > ymd("2021-04-01") & rtt_data$month_year <= ymd("2023-02-01"),])


# fit a line to pre and post pandemic referrals and completed
# for post-pandemic, only use up to march 23 for completeds (IA starts after this)
pre_pandemic_referrals_day_line <- predict(pre_pandemic_referrals_day)
post_pandemic_referrals_day_line <- predict(post_pandemic_referrals_day)
pre_pandemic_activity_day_line <- predict(pre_pandemic_activity_day)
post_pandemic_activity_day_line <- predict(post_pandemic_activity_day)

# get vector to plot lines and assign it to a new var in rtt_data
# rep NA 14 times for no line during COVID months
# predict "counterfactual" of no IA after March 23

# get countefactual dates
no_ia_counterfactual <- data.frame(month_year = seq(ymd("2023-03-01"), latest_data, by = "months")) %>% 
  mutate(month_no = interval(ymd("2016-03-01"), month_year) %/% months(1))

# put projections together to get day trendlines for graphing later
rtt_data$referrals_day_trend <- c(pre_pandemic_referrals_day_line
                              , rep(NA_real_, 14)
                              , post_pandemic_referrals_day_line)
rtt_data$activity_day_trend <- c(pre_pandemic_activity_day_line
                             , rep(NA_real_, 14)
                             , post_pandemic_activity_day_line
                             , predict(post_pandemic_activity_day, newdata = no_ia_counterfactual))

# multiply the day trendline to get month data
rtt_data <- rtt_data %>% 
  mutate(referrals_day_to_month = referrals_day_trend * workdays * referrals_seasonality
         , activity_day_to_month = activity_day_trend * workdays * activity_seasonality)

# get a monthly trendline based on projected monthly rates
pre_pandemic_referrals <- lm(referrals_day_to_month ~ month_no, data = rtt_data[rtt_data$month_year < ymd("2020-03-01"),])
post_pandemic_referrals <- lm(referrals_day_to_month ~ month_no, data = rtt_data[rtt_data$month_year > ymd("2021-04-01"),])
pre_pandemic_activity <- lm(activity_day_to_month ~ month_no, data = rtt_data[rtt_data$month_year < ymd("2020-03-01"),])
post_pandemic_activity <- lm(activity_day_to_month ~ month_no, data = rtt_data[rtt_data$month_year > ymd("2021-04-01"),])


# fit a line to pre and post pandemic referrals and completed
pre_pandemic_referrals_line <- predict(pre_pandemic_referrals)
post_pandemic_referrals_line <- predict(post_pandemic_referrals)
pre_pandemic_activity_line <- predict(pre_pandemic_activity)
post_pandemic_activity_line <- predict(post_pandemic_activity)


# put projections together to get month trendlines
rtt_data$referrals_trend <- c(pre_pandemic_referrals_line
                                  , rep(NA_real_, 14)
                                  , post_pandemic_referrals_line)

rtt_data$activity_trend <- c(pre_pandemic_activity_line
                                 , rep(NA_real_, 14)
                                 , post_pandemic_activity_line)

rtt_data %>% select(-month_no)


##### Save data to use in app #####
saveRDS(rtt_data, "data/rtt_data.RDS", compress = FALSE)

saveRDS(seasonality, "data/seasonality.RDS", compress = FALSE)

saveRDS(workdays_table, "data/workdays_table.RDS", compress = FALSE)

