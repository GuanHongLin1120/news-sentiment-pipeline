# Use a clean, minimal Python 3.12 base image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Force Python to flush output immediately (so logs appear in kubectl logs)
ENV PYTHONUNBUFFERED=1

# Install dependencies first (this layer is cached unless requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY scraper/ ./scraper/
COPY spark_processor/ ./spark_processor/

# Default command: run the scoring service
CMD ["python", "spark_processor/scoring_service.py"]