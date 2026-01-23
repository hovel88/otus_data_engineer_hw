https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html

Generate Fernet key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())" >> .env
```

Set permissions:

```bash
echo -e "AIRFLOW_UID=$(id -u)" >> .env
```


Build and start:

```bash
docker-compose build
docker-compose up airflow-init
docker-compose up -d
```

Access Airflow:

Web UI: http://localhost:8080

Username: admin

Password: admin

Flower (Celery monitoring): http://localhost:5555