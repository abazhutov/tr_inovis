# Terms of Reference from the company "Inovis"

## Deploy the project:

1.  Add a `.env` file with the following variables:

```
    POSTGRES_PASSWORD
    AIRFLOW_ADMIN_PASS
    AIRFLOW__WEBSERVER__SECRET_KEY
    AIRFLOW__CORE__FERNET_KEY
```

2.  Run the commands:

```
    docker compose up airflow-init
    docker compose up -d
```