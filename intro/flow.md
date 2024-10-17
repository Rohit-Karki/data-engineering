Let's build the image:

```ssh
docker build -t test:pandas .
```

- The image name will be `test` and its tag will be `pandas`. If the tag isn't specified it will default to `latest`.

We can now run the container and pass an argument to it, so that our pipeline will receive it:

```ssh
docker run -it test:pandas some_number
```

You can run a containerized version of Postgres that doesn't require any installation steps. You only need to provide a few _environment variables_ to it as well as a _volume_ for storing data.

Create a folder anywhere you'd like for Postgres to store data in. We will use the example folder ny_taxi_postgres_data. Here's how to run the container:

```dockerfile
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:13
```

The container needs 3 environment variables:
POSTGRES_USER is the username for logging into the database. We chose root.
POSTGRES_PASSWORD is the password for the database. We chose root
IMPORTANT: These values are only meant for testing. Please change them for any serious project.
POSTGRES_DB is the name that we will give the database. We chose ny_taxi.
-v points to the volume directory. The colon : separates the first part (path to the folder in the host computer) from the second part (path to the folder inside the container).
Path names must be absolute. If you're in a UNIX-like system, you can use pwd to print you local folder as a shortcut; this example should work with both bash and zsh shells, but fish will require you to remove the $.
This command will only work if you run it from a directory which contains the ny_taxi_postgres_data subdirectory you created above.
The -p is for port mapping. We map the default Postgres port to the same port in the host.

- The container needs 3 environment variables:
  - `POSTGRES_USER` is the username for logging into the database. We chose `root`.
  - `POSTGRES_PASSWORD` is the password for the database. We chose `root`
    - **_IMPORTANT: These values are only meant for testing. Please change them for any serious project._**
  - `POSTGRES_DB` is the name that we will give the database. We chose `ny_taxi`.
- `-v` points to the volume directory. The colon `:` separates the first part (path to the folder in the host computer) from the second part (path to the folder inside the container).
  - Path names must be absolute. If you're in a UNIX-like system, you can use `pwd` to print you local folder as a shortcut; this example should work with both `bash` and `zsh` shells, but `fish` will require you to remove the `$`.
  - This command will only work if you run it from a directory which contains the `ny_taxi_postgres_data` subdirectory you created above.
- The `-p` is for port mapping. We map the default Postgres port to the same port in the host.
- The last argument is the image name and tag. We run the official `postgres` image on its version `13`.

Once the container is running, we can log into our database with [pgcli](https://www.pgcli.com/) with the following command:

```bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

- `-h` is the host. Since we're running locally we can use `localhost`.
- `-p` is the port.
- `-u` is the username.
- `-d` is the database name.
- The password is not provided; it will be requested after running the command.

## Ingesting data to Postgres with Python

We will now create a Jupyter Notebook `upload-data.ipynb` file which we will use to read a CSV file and export it to Postgres.

We will use data from the [NYC TLC Trip Record Data website](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Specifically, we will use the [Yellow taxi trip records CSV file for January 2021](https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv). A dictionary to understand each field is available [here](https://www1.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf).

> Note: knowledge of Jupyter Notebook, Python environment management and Pandas is asumed in these notes. Please check [this link](https://gist.github.com/ziritrion/9b80e47956adc0f20ecce209d494cd0a#pandas) for a Pandas cheatsheet and [this link](https://gist.github.com/ziritrion/8024025672ea92b8bdeb320d6015aa0d) for a Conda cheatsheet for Python environment management.

Check the completed `upload-data.ipynb` [in this link](../1_intro/upload-data.ipynb) for a detailed guide. Feel free to copy the file to your work directory; in the same directory you will need to have the CSV file linked above and the `ny_taxi_postgres_data` subdirectory.

## Connecting pgAdmin and Postgres with Docker networking

`pgcli` is a handy tool but it's cumbersome to use. [`pgAdmin` is a web-based tool](https://www.pgadmin.org/) that makes it more convenient to access and manage our databases. It's possible to run pgAdmin as as container along with the Postgres container, but both containers will have to be in the same _virtual network_ so that they can find each other.

Let's create a virtual Docker network called `pg-network`:

```bash
docker network create pg-network
```

> You can remove the network later with the command `docker network rm pg-network` . You can look at the existing networks with `docker network ls` .

We will now re-run our Postgres container with the added network name and the container network name, so that the pgAdmin container can find it (we'll use `pg-database` for the container name):

```bash
docker run -it \
    -e POSTGRES_USER="root" \
    -e POSTGRES_PASSWORD="root" \
    -e POSTGRES_DB="ny_taxi" \
    -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
    -p 5432:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13
```

We will now run the pgAdmin container on another terminal:

```bash
docker run -it \
    -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
    -e PGADMIN_DEFAULT_PASSWORD="root" \
    -p 8080:80 \
    --network=pg-network \
    --name pgadmin \
    dpage/pgadmin4
```

- The container needs 2 environment variables: a login email and a password. We use `admin@admin.com` and `root` in this example.
- **_IMPORTANT: these are example values for testing and should never be used on production. Change them accordingly when needed._**
- pgAdmin is a web app and its default port is 80; we map it to 8080 in our localhost to avoid any possible conflicts.
- Just like with the Postgres container, we specify a network and a name. However, the name in this example isn't really necessary because there won't be any containers trying to access this particular container.
- The actual image name is `dpage/pgadmin4` .

> The pg-admin container and the main postgres container are not the same. They are only there to connect to each other.

You should now be able to load pgAdmin on a web browser by browsing to `localhost:8080`. Use the same email and password you used for running the container to log in.

Right-click on _Servers_ on the left sidebar and select _Create_ > _Server.._

Under _General_ give the Server a name and under _Connection_ add the same host name, user and password you used when running the container.

Click on _Save_. You should now be connected to the database.

We will explore using pgAdmin in later lessons.

## Using the ingestion script with Docker

We will now export the Jupyter notebook file to a regular Python script and use Docker to run it.

### Exporting and testing the script

You can export the `ipynb` file to `py` with this command:

```bash
jupyter nbconvert --to=script upload-data.ipynb
```

Clean up the script by removing everything we don't need. We will also rename it to `ingest_data.py` and add a few modifications:

- We will use [argparse](https://docs.python.org/3/library/argparse.html) to handle the following command line arguments:
  - Username
  - Password
  - Host
  - Port
  - Database name
  - Table name
  - URL for the CSV file
- The _engine_ we created for connecting to Postgres will be tweaked so that we pass the parameters and build the URL from them, like this:
  ```python
  engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
  ```
- We will also download the CSV using the provided URL argument.

In order to test the script we will have to drop the table we previously created. In pgAdmin, in the sidebar navigate to _Servers > Docker localhost > Databases > ny_taxi > Schemas > public > Tables > yellow_taxi_data_, right click on _yellow_taxi_data_ and select _Query tool_. Introduce the following command:

```sql
DROP TABLE yellow_taxi_data;
```

```bash
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"
```

### Dockerizing the script

Let's modify the [Dockerfile we created before](#creating-a-custom-pipeline-with-docker) to include our `ingest_data.py` script and create a new image:

```dockerfile
FROM python:3.9.1

# We need to install wget to download the csv file
RUN apt-get install wget
# psycopg2 is a postgres db adapter for python: sqlalchemy needs it
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]
```

Build the image:

```bash
docker build -t taxi_ingest:v001 .
```

And run it:

```bash
docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url="https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2021-01.csv"
```

- We need to provide the network for Docker to find the Postgres container. It goes before the name of the image.
- Since Postgres is running on a separate container, the host argument will have to point to the container name of Postgres.
- You can drop the table in pgAdmin beforehand if you want, but the script will automatically replace the pre-existing table.

## Running Postgres and pgAdmin with Docker-compose
