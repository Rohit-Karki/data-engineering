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

## Terraform basics

There are 2 important components to Terraform: the code files and Terraform commands.

The set of files used to describe infrastructure in Terraform is known as a Terraform **_configuration_**. Terraform configuration files end up in `.tf` for files wtritten in Terraform language or `tf.json` for JSON files. A Terraform configuration must be in its own working directory; you cannot have 2 or more separate configurations in the same folder.

Here's a basic `main.tf` file written in Terraform language with all of the necesary info to describe basic infrastructure:

```java
terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "3.5.0"
    }
  }
}

provider "google" {
  credentials = file("<NAME>.json")

  project = "<PROJECT_ID>"
  region  = "us-central1"
  zone    = "us-central1-c"
}

resource "google_compute_network" "vpc_network" {
  name = "terraform-network"
}
```

- Terraform divides information into **_blocks_**, which are defined within braces (`{}`), similar to Java or C++. However, unlike these languages, statements are not required to end with a semicolon `;` but use linebreaks instead.
- By convention, arguments with single-line values in the same nesting level have their equal signs (`=`) aligned for easier reading.
- There are 3 main blocks: `terraform`, `provider` and `resource`. There must only be a single `terraform` block but there may be multiple `provider` and `resource` blocks.
- The `terraform` block contains settings:
  - The `required_providers` sub-block specifies the providers required by the configuration. In this example there's only a single provider which we've called `google`.
    - A _provider_ is a plugin that Terraform uses to create and manage resources.
    - Each provider needs a `source` in order to install the right plugin. By default the Hashicorp repository is used, in a similar way to Docker images.
      - `hashicorp/google` is short for `registry.terraform.io/hashicorp/google` .
    - Optionally, a provider can have an enforced `version`. If this is not specified the latest version will be used by default, which could introduce breaking changes in some rare cases.
  - We'll see other settings to use in this block later.
- The `provider` block configures a specific provider. Since we only have a single provider, there's only a single `provider` block for the `google` provider.
  - The contents of a provider block are provider-specific. The contents in this example are meant for GCP but may be different for AWS or Azure.
  - Some of the variables seen in this example, such as `credentials` or `zone`, can be provided by other means which we'll cover later.
- The `resource` blocks define the actual components of our infrastructure. In this example we have a single resource.
  - `resource` blocks have 2 strings before the block: the resource **_type_** and the resource **_name_**. Together the create the _resource ID_ in the shape of `type.name`.
  - About resource types:
    - The first prefix of the resource type maps to the name of the provider. For example, the resource type `google_compute_network` has the prefix `google` and thus maps to the provider `google`.
    - The resource types are defined in the Terraform documentation and refer to resources that cloud providers offer. In our example [`google_compute_network` (Terraform documentation link)](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/compute_network) refers to GCP's [Virtual Private Cloud service](https://cloud.google.com/vpc).
  - Resource names are the internal names that we use in our Terraform configurations to refer to each resource and have no impact on the actual infrastructure.
  - The contents of a resource block are specific to the resource type. [Check the Terraform docs](https://registry.terraform.io/browse/providers) to see a list of resource types by provider.
    - In this example, the `google_compute_network` resource type has a single mandatory argument called `name`, which is the name that the resource will have within GCP's infrastructure.
      - Do not confuse the _resource name_ with the _`name`_ argument!

Besides these 3 blocks, there are additional available blocks:

- **_Input variables_** block types are useful for customizing aspects of other blocks without altering the other blocks' source code. They are often referred to as simply _variables_. They are passed at runtime.
  ```java
  variable "region" {
      description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
      default = "europe-west6"
      type = string
  }
  ```
  - Description:
    - An input variable block starts with the type `variable` followed by a name of our choosing.
    - The block may contain a number of fields. In this example we use the fields `description`, `type` and `default`.
    - `description` contains a simple description for documentation purposes.
    - `type` specifies the accepted value types for the variable
    - If the `default` field is defined, the variable becomes optional because a default value is already provided by this field. Otherwise, a value must be provided when running the Terraform configuration.
    - For additional fields, check the [Terraform docs](https://www.terraform.io/language/values/variables).
  - Variables must be accessed with the keyword `var.` and then the name of the variable.
  - In our `main.tf` file above, we could access this variable inside the `google` provider block with this line:
    ```java
    region = var.region
    ```
- **_Local values_** block types behave more like constants.
  ```java
  locals{
      region  = "us-central1"
      zone    = "us-central1-c"
  }
  ```
  - Description:
    - Local values may be grouped in one or more blocks of type `locals`. Local values are often grouped according to usage.
    - Local values are simpler to declare than input variables because they are only a key-value pair.
  - Local values must be accessed with the word `local` (_mind the lack of `s` at the end!_).
    ```java
    region = local.region
    zone = local.zone
    ```

With a configuration ready, you are now ready to create your infrastructure. There are a number of commands that must be followed:

- `terraform init` : initialize your work directory by downloading the necessary providers/plugins.
- `terraform fmt` (optional): formats your configuration files so that the format is consistent.
- `terraform validate` (optional): returns a success message if the configuration is valid and no errors are apparent.
- `terraform plan` : creates a preview of the changes to be applied against a remote state, allowing you to review the changes before applying them.
- `terraform apply` : applies the changes to the infrastructure.
- `terraform destroy` : removes your stack from the infrastructure.

## Creating GCP infrastructure with Terraform

_([Video source](https://www.youtube.com/watch?v=dNkEgO-CExg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=10))_

We will now create a new `main.tf` file as well as an auxiliary `variables.tf` file with all the blocks we will need for our project.

The infrastructure we will need consists of a Cloud Storage Bucket (`google_storage-bucket`) for our _Data Lake_ and a BigQuery Dataset (`google_bigquery_dataset`).

In `main.tf` we will configure the `terraform` block as follows:

```java
terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}
```

- The `required_version` field states the minimum Terraform version to be used.
- The `backend` field states where we'd like to store the _state_ of the infrastructure. `local` means that we'll store it locally in our computers. Alternatively, you could store the state online.
