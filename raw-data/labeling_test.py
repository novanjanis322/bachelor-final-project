import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

# Download VADER lexicon if not already downloaded
# nltk.download('vader_lexicon')

# Create a sentiment analyzer object
sid = SentimentIntensityAnalyzer()

# Example text
text = "the sprinkle is so bad"

# Analyze sentiment
sentiment_scores = sid.polarity_scores(text)

# Print sentiment scores
print(sentiment_scores)