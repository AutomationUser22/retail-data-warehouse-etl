FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Create output directories
RUN mkdir -p data/raw data/processed data/quarantine logs

CMD ["python", "-m", "src.pipeline"]
