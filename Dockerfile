# Dockerfile for Concurrent Data Processing Pipeline
# Example usage:
# 1. docker build -t my-fastapi-app . 
# 2. docker run -p 8000:8000 my-fastapi-app
# 3. Visit http://localhost:8000/docs and see FastAPI Swagger UI and test the endpoints

FROM python:3.10-slim

WORKDIR /

ENV PYTHONPATH=/app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# copy the application code
COPY ./app /app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
