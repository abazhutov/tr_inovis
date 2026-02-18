# Technical Specification for Inovis

## Deploy the project:

1.  Add a `.env` file with the following variables:

```
    POSTGRES_PASSWORD
    AIRFLOW_ADMIN_PASS
```

2.  Run the commands:

```
    docker compose up airflow-init
    docker compose up -d
```