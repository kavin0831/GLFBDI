# Hugging Face Spaces — Docker SDK
FROM python:3.11-slim

# System deps for pdfplumber / cryptography / sentence-transformers
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# HF Spaces runs as UID 1000; create a non-root user so writes work
RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PORT=7860

WORKDIR $HOME/app

# Install Python deps first (better Docker layer caching)
COPY --chown=user requirements.txt ./
RUN pip install --user --no-cache-dir -r requirements.txt

# Pre-download the sentence-transformers model into the image so cold starts
# don't time out on Hugging Face's request-bound startup window
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('all-MiniLM-L6-v2')"

# App code
COPY --chown=user . .

# Make the directories the app writes into (read-only FS otherwise)
RUN mkdir -p storage/logs config

EXPOSE 7860
CMD ["python", "app.py"]
