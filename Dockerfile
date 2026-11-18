FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ ./common/
COPY decision_service/ ./decision_service/
COPY consumers/ ./consumers/

CMD ["uvicorn", "decision_service.main:app", "--host", "0.0.0.0", "--port", "9000", "--workers", "2"]
