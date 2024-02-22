from transformers import BertTokenizer, BertModel
import torch
from scipy.spatial.distance import cosine
import re

# Carregar Tokenizer e Modelo
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertModel.from_pretrained('bert-base-uncased')


def normalize_text(text: str) -> str:
    """
    Normalize the text by removing special characters and converting to lowercase.
    """
    text = re.sub(r'[^A-Za-z0-9 ]+', '', text)  # Remove caracteres especiais
    return text.lower()  # Converte para minúsculas


def get_embedding(text: str) -> torch.Tensor:
    """
    Get the BERT embedding for a given text.
    """
    text = normalize_text(text)  # Normalize o texto antes de obter o embedding
    inputs = tokenizer(text, return_tensors='pt', padding=True, truncation=True, max_length=512)
    outputs = model(**inputs)
    # Use a pooling strategy (mean pooling here) for sentence-level representations
    return outputs.last_hidden_state.mean(dim=1)


def calculate_similarity(text1: str, text2: str) -> float:
    """
    Calculate cosine similarity between the embeddings of two texts.
    """
    embedding1 = get_embedding(text1)
    embedding2 = get_embedding(text2)
    embedding1 = embedding1.squeeze()
    embedding2 = embedding2.squeeze()
    return 1 - cosine(embedding1.detach().numpy(), embedding2.detach().numpy())


def lexical_similarity(text1: str, text2: str) -> float:
    """
    Calculate the lexical similarity between two texts.
    """
    text1, text2 = normalize_text(text1), normalize_text(text2)  # Normalize os textos
    words1 = set(text1.split())
    words2 = set(text2.split())
    common_elements = words1.intersection(words2)
    total_elements = words1.union(words2)
    return len(common_elements) / len(total_elements) if total_elements else 0


def word_overlap_similarity(text1: str, text2: str) -> float:
    """
    Calculate the proportion of words in text1 that are also in text2.
    """
    text1, text2 = normalize_text(text1), normalize_text(text2)  # Normalize os textos
    words1 = set(text1.split())
    words2 = set(text2.split())
    overlap = words1.intersection(words2)
    return len(overlap) / len(words1) if words1 else 0


def combined_similarity(text1: str, text2: str, semantic_weight: float = 0.33, lexical_weight: float = 0.33) -> float:
    """
    Calculate a combined similarity score based on semantic, lexical, and word overlap analysis.
    :return: Percentage from 0.0 to 1.0
    """
    sem_similarity = calculate_similarity(text1, text2)
    lex_similarity = lexical_similarity(text1, text2)
    overlap_similarity = word_overlap_similarity(text1, text2)
    return (semantic_weight * sem_similarity) + (lexical_weight * lex_similarity) + ((1 - semantic_weight - lexical_weight) * overlap_similarity)

