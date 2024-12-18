library(dplyr)
library(httr)
library(furrr)
library(jsonlite)
library(future)


summarize_data_parallel <- function(
    input_file, 
    #output_file, 
    url = "http://localhost:11434/api/generate", 
    workers = parallel::detectCores() - 1
) {
  # Set up parallel processing
  plan(multisession, workers = workers)
  
  # Read input data
  dat <- read.csv(input_file)
  
  # Function to summarize with Ollama API
  ollama_summarize <- function(text, type = "quote") {
    # Wrap the API call in tryCatch for robustness
    tryCatch({
      prompt <- if (type == "quote") {
        paste("Summarize this in at most three sentences using a first-person point of view as a question:", text)
      } else {
        paste("Using this as a question:", type, 
              "Summarize this in at most three sentences using a second-person point of view as an answer:", text)
      }
      
      body <- list(
        model = "llama3.2",
        stream = FALSE,
        prompt = prompt
      )
      
      response <- POST(url, body = body, encode = "json")
      content <- fromJSON(content(response, "text", encoding = "UTF-8"))
      return(content$response)
    }, error = function(e) {
      warning(paste("Error processing text:", text, "Error:", e$message))
      return(NA_character_)
    })
  }
  
  # Parallel processing of quotes
  summarized_quotes <- future_map_chr(
    dat$Quoted, 
    ~ ollama_summarize(.x, "quote"),
    .progress = TRUE
  )
  
  # Parallel processing of answers
  summarized_answers <- future_map2_chr(
    summarized_quotes, 
    dat$Replied, 
    ~ ollama_summarize(.y, .x),
    .progress = TRUE
  )
  
  # Create results dataframe
  df_deviative <- data.frame(
    Original_Quote = dat$Quoted,
    Summarized_Quote = summarized_quotes,
    Summarized_Answer = summarized_answers,
    stringsAsFactors = FALSE
  )
  
  # Stop parallel processing
  plan(sequential)
  
  # Write results to CSV
  #write.csv(df_deviative, output_file, row.names = FALSE)
  
  return(df_deviative)
}



# Example usage
result <- summarize_data_parallel(
  input_file = "niceQuestions_shuffled.csv", 
  #output_file = "nice_df_deviative_v2.csv"
)

write.csv(result, "QandA_derviatives.csv")
