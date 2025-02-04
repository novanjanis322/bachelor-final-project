# Social Media Tourism Sentiment Analysis

This project implements an automated ETL pipeline to analyze sentiment from social media comments (YouTube and TikTok) about tourist destinations in Indonesia. The system collects user comments, performs sentiment analysis using a trained Naive Bayes model, and stores the processed data in a data warehouse for visualization and analysis.

## Features

- Automated data collection from multiple sources:
  - TikTok comments using TikTok API
  - YouTube comments
  - Tourism destination data from Google Maps
- Sentiment analysis using trained Naive Bayes model
- ETL pipeline with data cleaning and transformation
- Snowflake schema data warehouse implementation
- Interactive visualization dashboard in Tableau

## Tech Stack

- **Python Libraries**:
  - pandas: Data manipulation and analysis
  - mysql-connector: Database connection
  - NLTK: Natural Language Processing
  - scikit-learn: Machine Learning (Naive Bayes)
  - TikTokApi: TikTok data extraction (https://github.com/davidteather/TikTok-Api)
  - Selenium: Web scraping
- **Database**: MySQL
- **Visualization**: Tableau



3. **Visualization**:
   - Static dashboard preview:
   ![tableau visualization](<Yogyakarta Tourism Sentiment Dashboard-final.png>)
   

## Data Warehouse Schema

The data warehouse follows a snowflake schema with the following structure:

- Fact Table: `fact_table`
  - Contains comment data, sentiment scores, and number of likes
- Dimension Tables:
  - `dim_social_media`: Social media platform details
  - `dim_user`: User information
  - `dim_date`: Date dimension
  - `dim_tourism`: Tourism destination details
  - `dim_tourism_category`: Tourism categories
  - `dim_tourism_regency`: Regional information

## Sentiment Analysis

The sentiment analysis model:
- Uses Naive Bayes classification
- Trained on Indonesian language comments
- Classifies comments as positive, negative, or neutral
- Implements custom stopword removal for Indonesian language
- Saved model is loaded during ETL process

## Contact

Project Creator: Novan Janis