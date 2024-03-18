rm(list = ls())
library(prism)
library(future.apply)
gc()



# Set your working directory and prism.path option

loc <-  "C:/Users/Sangha/Documents/PRISM"
setwd(loc)


# Date range
start_date <- as.Date("2016-01-01")
end_date <- as.Date("2017-12-31")

# Variable
var_type <- "ppt"

# Generate a sequence of dates for downloading
dates <- seq(from = start_date, to = end_date, by = "day")

# num_cores <- parallel::detectCores() - 10
# Set up parallel processing
# plan(multisession, workers = num_cores) 

plan(multisession) 
# Function to download PRISM data for a single date
download_prism_day <- function(date) {
  options(prism.path = "./data/")                                                             # IMPORTANT Create a folder called data
  prism::get_prism_dailys(type = var_type, minDate = date, maxDate = date, keepZip = FALSE)
}

start_time <- Sys.time()
future.apply::future_lapply(dates, download_prism_day,future.seed = TRUE)
end_time <- Sys.time()
lapply.time <- end_time - start_time
lapply.time # Done in 3 minutes 
############################################################################################################
#
library(tidyverse)

dates_ls <- seq(from = start_date, to = end_date, by = "day")
dates_prism_txt <- str_remove_all(dates_ls, "-")

library(sf)
library(terra)

#********* IMPORTANT ####################################
#*create a vector file for sounties for specific state.
#*Save and load in the process_day function.
#*
# load(paste0(loc,"/shape.rda"))
# sc_counties <- shape %>%
#   dplyr::filter(Name == "SOUTH CAROLINA")
# sc_counties_vect <- vect(sc_counties)
# rm(shape)
# saveRDS(sc_counties_vect, file = paste0(loc, "/sc_counties_vect.rds"))


# Create a function to process each day
process_day <- function(date_prism_txt, var_type, sc_counties_vect) {
  sc_counties_vect <- readRDS(paste0(loc, "/sc_counties_vect.rds"))
  folder_name <- paste0("PRISM_", var_type, "_stable_4kmD2_", date_prism_txt, "_bil")
  file_name <- paste0(folder_name, ".bil")
  file_path <- paste0("./data/", folder_name, "/", file_name)
  temp_stars1 <- terra::rast(file_path)
  values_by_county <- terra::extract(temp_stars1, sc_counties_vect, fun = mean, na.rm = TRUE)
  
  # Convert to a data.frame
  extracted_df <- as.data.frame(t(values_by_county))
  extracted_df <- extracted_df[-1, ] # Adjust based on your actual requirement
  
  county_names <- terra::values(sc_counties_vect)$NAME
  colnames(extracted_df) <- county_names
  
  date <- as.Date(gsub(paste0("PRISM_", var_type, "_stable_4kmD2_([0-9]{8})_bil"), "\\1", date_prism_txt), format="%Y%m%d")
  extracted_df$Date <- date
  
  extracted_df <- extracted_df[, c("Date", setdiff(names(extracted_df), "Date"))]
  
  rownames(extracted_df) <- NULL
  
  extracted_df <- extracted_df %>%
    mutate(across(-Date, ~round(.x, digits = 2)))
  
  return(extracted_df)
}

start_time1 <- Sys.time()
results_list <- future_lapply(dates_prism_txt, function(x) process_day(x, var_type, sc_counties_vect))
results_df <- bind_rows(results_list)
results_df <- results_df %>% arrange(Date)


file.name<-paste0(var_type,"PRISM_county",start_date,"_",end_date)

write.csv(results_df, file = paste0(loc, "/", file.name, ".csv"), row.names = FALSE)
openxlsx::write.xlsx(results_df, file = paste0(loc, "/", file.name, ".xlsx"), rowNames = FALSE)

save(results_df, file = paste0(loc, "/", file.name, ".RData"))

end_time1 <- Sys.time()
lapply.time1 <- end_time1 - start_time1
lapply.time1   ### Done in 5 seconds
