# Cars Normalization
This project provides functionality for normalizing and denormalizing the data collected from the xlsx file. All the data is uploaded to SQL Server database.
## Entity Relationship Diagram (ERD)
https://dbdiagram.io/d/Normalization-66381fc05b24a634d090a57e
## System Setup (Windows)
1. Clone the repository:
   ```bash
    git clone https://github.com/DimaKuriptya/Cars-Normalization.git
   ```
2. Create a virtual environment:
   ```bash
    python -m venv venv
   ```
3. Activate the virtual environment:
   ```bash
    venv\Scripts\activate
   ```
4. Install the dependencies:
   ```bash
    pip install -r requirements.txt
   ```
5. Create a file `config.conf` inside `config` folder. Fill the file by the folowing template:
```bash
[db_connection]
server = USER\SERVERNAME
database = db_name
driver = ODBC Driver 17 for SQL Server
```
6. Install the chosen driver if necessary and create a database with the same name as in config.
7. Run normalization script:
   ```bash
    python pipelines/normalize_pipeline.py
   ```
8. Run denormalization script:
   ```bash
    python pipelines/denormalize_pipeline.py
   ```
