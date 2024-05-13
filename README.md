# Cars Normalization
This project provides functionality for normalizing and denormalizing the data collected from the xlsx file. All the data is uploaded to SQL Server database.
## Entity Relationship Diagram (ERD)
https://dbdiagram.io/d/Normalization-66381fc05b24a634d090a57e
## DB Size
### Total size of normalized data:
Reserved Space: 648KB

Used Space: 168KB
### Total size of denormalized data:
Reserved Space: 144KB

Used Space: 32KB
## System Setup (Linux)
1. Clone the repository:
   ```bash
    git clone https://github.com/DimaKuriptya/Cars-Normalization.git
   ```
2. Create a virtual environment:
   ```bash
    python3 -m venv venv
   ```
3. Activate the virtual environment:
   ```bash
    source venv/bin/activate
   ```
4. Install the dependencies:
   ```bash
    pip install -r requirements.txt
   ```
5. Create a file `config.conf` inside `config` folder. Fill the file by the folowing template:
```bash
[db_connection]
server = host.docker.internal
database = normalization
driver = ODBC Driver 18 for SQL Server
username = sa
password = your_password
```
6. Install the driver if necessary and create an SQL Server database with the same name as in config.
7. Start the containers:

   Run airflow-init:
   ```bash
     docker-compose up airflow-init
   ```

   Wait for airflow-init container to finish its job and then run the following command:
   ```
   docker-compose up
   ```
8. Launch the Airflow web UI.
   ```bash
    open http://localhost:8080
   ```
