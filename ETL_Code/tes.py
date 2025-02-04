from transformers import pipeline

pretrained_name = "w11wo/indonesian-roberta-base-sentiment-classifier"

nlp = pipeline(
    "sentiment-analysis",
    model=pretrained_name,
    tokenizer=pretrained_name
)

result = nlp("tempatnya kotor")
print(f"Sentiment: {result[0]['label']}")
print(f"Confidence Score: {result[0]['score']:.4f}")  # :.4f limits decimals to 4 places