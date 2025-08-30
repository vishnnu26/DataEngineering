# DataEngineering

AirFlow Documentation - https://airflow.apache.org/docs/
AirFlow Macros        - https://airflow.apache.org/docs/apache-airflow/1.10.9/macros.html


# MINIO DOCKER INSTALL AND DOCKER CONTAINER UP:
docker run \
   -p 9000:9000 \
   -p 9001:9001 \
   -e "MINIO_ROOT_USER=AKIAIOSFODNN7EXAMPLE" \
   -e "MINIO_ROOT_PASSWORD=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
   quay.io/minio/minio server /data --console-address ":9001"

# DOCKER COMMANDS:

* docker compose up airflow-init (This command is to initialize the airflow init scripts... create db / webservers / cheduler)
* docker compose up       - Runs on the terminal
       * docker compose up -d (DETACHED MODE RUN --background)
* docker compose down
       * docke compose down -v (REMOVE THE VOLUMES INCLUDED).
             The volumes is the location where the data is persisted in the database.
             Inorder to remove the persisted data -v is given and container is restarted.
* docker compose down && docker compose up
* docker compose build --no-cache (BUILDS A FRESH Image --> ignoring any cached layers).
* docker ps 	# Docker command that is used to list running containers.
* docker ps -a  # Docker command to list all the containers (Running / Stopped)

BACKFILL COMMAND:
=================
* docker exec -it <container_id> bash	# to Bash command a particular container instance.
* airflow dags backfill -s <start_date> -e <end_date> <dag_id>
* Version 2.0+ airflow backfill -s <start_date> -e <end_date> <dag_id>
      Now ==> airflow backfill create \
                         --dag-id <dag_id> \
                         --from-date <start_date> \
                         --to-date <to_date>
       ((start_date and end_date format ==> 2025-08-23))
* docker exec -it <postgres_container_name / id> -U <user_name> -d <database_name>
      -> docker exec -it <postgres_container_name / id> -U <user_name>
      -> \l {Lists all the databases}.
      -> \c postgres / ur_database_name ==> goes inside postgres / new database.
      -> \dt {Lists all the tables}.
      -> \dt <table_name> ==> Returns present or not.
     
* docker exec -it <container_id> python
       import pkgutil
       from airflow.providers.postgres as pkg
     
       print([m.name for m in pkgutil.iter_modules(pkg.__path__)])
             # Brings out all the submodules inside the providers.postgres package.
