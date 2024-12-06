# Nødvendige libraries
library(httr)
library(rvest)
library(DBI)
library(RMySQL)

db_password <- Sys.getenv("DB_PASSWORD")

# Opret forbindelse til MySQL-databasen
db_connection <- dbConnect(
  RMySQL::MySQL(),
  dbname = "airbase",
  host = "localhost",   # Skift til din serveradresse, hvis den ikke er lokal
  user = "root",        # Din MySQL-bruger
  password = db_password
)

# Funktion til at hente og indsætte data fra websitet i SQL
fetch_data <- function(location_name, location_id, token_url, post_url) {
  print(paste("Fetching data for location:", location_name))
  
  # Hent CSRF-token
  res <- GET(token_url)
  html <- content(res, as = "text")
  csrf_token <- read_html(html) %>%
    html_node("input[name='__RequestVerificationToken']") %>%
    html_attr("value")
  
  if (is.null(csrf_token)) {
    print(paste("Failed to fetch CSRF token for:", location_name))
    return(NULL)
  }
  
  # Send POST-anmodning med CSRF-token
  res_post <- POST(
    url = post_url,
    body = list("__RequestVerificationToken" = csrf_token),
    encode = "form"
  )
  
  if (status_code(res_post) != 200) {
    print(paste("Failed to fetch data for:", location_name, "- Status code:", status_code(res_post)))
    return(NULL)
  }
  
  # Parse tabellen
  raw_data <- read_html(content(res_post, as = "text")) %>%
    html_table(fill = TRUE) %>%
    .[[1]]
  
  # Debug: Print kolonner i tabellen
  print(paste("Available columns for", location_name, ":"))
  print(colnames(raw_data))
  
  # Normaliser kolonnenavne
  colnames(raw_data) <- gsub("\\.", "_", colnames(raw_data))  # Erstat "." med "_"
  colnames(raw_data)[colnames(raw_data) == "Målt (starttid)"] <- "measured_at"  # Konverter "Målt (starttid)" til "measured_at"
  
  # Ønskede kolonner (matcher SQL-databasen)
  desired_columns <- c("measured_at", "CO", "NO2", "NOX", "SO2", "O3", "PM10", "PM2_5")
  
  # Håndter manglende kolonner
  for (col in desired_columns) {
    if (!col %in% colnames(raw_data)) {
      print(paste("Column", col, "is missing for location:", location_name, "- Filling with NA"))
      raw_data[[col]] <- NA
    }
  }
  
  # Filtrér kun de ønskede kolonner
  filtered_data <- raw_data[, desired_columns, drop = FALSE]
  filtered_data$location_id <- location_id
  
  # Konverter kolonner til korrekt datatyper
  filtered_data$measured_at <- as.POSIXct(filtered_data$measured_at, format = "%d-%m-%Y %H:%M", tz = "UTC")
  numeric_columns <- setdiff(desired_columns, "measured_at")
  for (col in numeric_columns) {
    if (!is.null(filtered_data[[col]])) {
      filtered_data[[col]] <- suppressWarnings(as.numeric(gsub(",", ".", filtered_data[[col]])))
    } else {
      filtered_data[[col]] <- NA
    }
  }
  
  # Debug: Tjek filtrerede data
  print("Filtered data preview:")
  print(head(filtered_data))
  
  # Indsæt data i Measurement-tabellen, hvis de ikke allerede findes
  for (i in 1:nrow(filtered_data)) {
    check_query <- sprintf(
      "SELECT COUNT(*) FROM Measurement WHERE location_id = %d AND measured_at = '%s'",
      filtered_data$location_id[i],
      filtered_data$measured_at[i]
    )
    
    exists <- dbGetQuery(db_connection, check_query)[1, 1]
    
    if (exists == 0) {
      query <- sprintf(
        "INSERT INTO Measurement (location_id, measured_at, CO, NO2, NOX, SO2, O3, PM10, PM2_5)
        VALUES (%d, '%s', %s, %s, %s, %s, %s, %s, %s)",
        filtered_data$location_id[i],
        filtered_data$measured_at[i],
        ifelse(is.na(filtered_data$CO[i]), "NULL", filtered_data$CO[i]),
        ifelse(is.na(filtered_data$NO2[i]), "NULL", filtered_data$NO2[i]),
        ifelse(is.na(filtered_data$NOX[i]), "NULL", filtered_data$NOX[i]),
        ifelse(is.na(filtered_data$SO2[i]), "NULL", filtered_data$SO2[i]),
        ifelse(is.na(filtered_data$O3[i]), "NULL", filtered_data$O3[i]),
        ifelse(is.na(filtered_data$PM10[i]), "NULL", filtered_data$PM10[i]),
        ifelse(is.na(filtered_data$PM2_5[i]), "NULL", filtered_data$PM2_5[i])
      )
      dbExecute(db_connection, query)
      print(paste("Inserted data for", location_name, "- Time:", filtered_data$measured_at[i]))
    } else {
      print(paste("Data already exists for", location_name, "- Time:", filtered_data$measured_at[i]))
    }
  }
}

# Lokationer og deres URLs
locations <- list(
  HCAB = list(
    name = "HC Andersens Boulevard",
    location_id = 1,
    token_url = "https://envs2.au.dk/Luftdata/Presentation/table/Copenhagen/HCAB",
    post_url = "https://envs2.au.dk/Luftdata/Presentation/table/MainTable/Copenhagen/HCAB"
  ),
  ANHO = list(
    name = "Anholt",
    location_id = 2,
    token_url = "https://envs2.au.dk/Luftdata/Presentation/table/Rural/ANHO",
    post_url = "https://envs2.au.dk/Luftdata/Presentation/table/MainTable/Rural/ANHO"
  ),
  AARH3 = list(
    name = "Banegårdsgade Aarhus",
    location_id = 3,
    token_url = "https://envs2.au.dk/Luftdata/Presentation/table/Aarhus/AARH3",
    post_url = "https://envs2.au.dk/Luftdata/Presentation/table/MainTable/Aarhus/AARH3"
  ),
  RISOE = list(
    name = "Risø",
    location_id = 4,
    token_url = "https://envs2.au.dk/Luftdata/Presentation/table/Rural/RISOE",
    post_url = "https://envs2.au.dk/Luftdata/Presentation/table/MainTable/Rural/RISOE"
  )
)

# Loop gennem lokationerne og hent/gem data
for (location in names(locations)) {
  loc <- locations[[location]]
  fetch_data(
    location_name = loc$name,
    location_id = loc$location_id,
    token_url = loc$token_url,
    post_url = loc$post_url
  )
}

# Luk databaseforbindelsen
dbDisconnect(db_connection)
