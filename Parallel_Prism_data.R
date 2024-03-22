rm(list = ls())
library(prism)
library(future.apply)
gc()
# Set your working directory and prism.path option

loc <- getwd()

data_dir <- file.path(loc, "data")
if (!dir.exists(data_dir)) {
  dir.create(data_dir)
}

# Date range
start_date <- as.Date("2001-01-01")
end_date <- as.Date("2022-12-31")

# Variable
var_type <- "vpdmin"

# Generate a sequence of dates for downloading
dates <- seq(from = start_date, to = end_date, by = "day")

num_cores <- parallel::detectCores()
# Set up parallel processing
# plan(multisession, workers = num_cores) 

plan(multisession) 
# Function to download PRISM data for a single date
download_prism_day <- function(date) {
  options(prism.path = "./data/")
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

dates_prism_txt <- str_remove_all(dates, "-")

# Initialize the missing_folders vector
missing_folders <- NULL

max_attempts <- 2  # PRISM allows for 2 downloads in a single day, set as needed
attempt <- 0

repeat {
  # Generate the list of expected folder names based on the dates
  dates_prism_txt <- format(dates, "%Y%m%d")  # Remove dashes from dates for folder name generation
  expected_folders <- paste0("PRISM_", var_type, "_stable_4kmD2_", dates_prism_txt, "_bil")
  
  # List the actual folders/files in your data directory
  actual_folders <- list.files(path = "./data/")
  
  # Identify missing folders by comparing expected and actual folder names
  missing_folders <- expected_folders[!expected_folders %in% actual_folders]
  
  if (length(missing_folders) == 0) {
    message("All expected folders are present.")
    break  # Exit if no missing folders
  }
  
  # Extract dates from missing folder names for redownload
  date_strings <- sub(paste0("PRISM_", var_type, "_stable_4kmD2_(\\d{8})_bil"), "\\1", missing_folders)
  dates_to_redownload <- as.Date(date_strings, format = "%Y%m%d")
  
  # Redownload data for missing dates, if any
  if (!all(is.na(dates_to_redownload))) {
    future.apply::future_lapply(dates_to_redownload, download_prism_day, future.seed = TRUE)
  }
  
  # Increment and check the attempt counter
  attempt <- attempt + 1
  if (attempt >= max_attempts) {
    message("Maximum attempt limit reached. Check for any remaining missing folders.")
    break  # Prevent infinite loop
  }
}


library(sf)
library(terra)


# load(paste0(loc,"/shape.rda"))
# sc_counties <- shape %>%
#   dplyr::filter(Name == "SOUTH CAROLINA")
# sc_counties_vect <- vect(sc_counties)
# rm(shape)



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
