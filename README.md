# Bravisa Project

## Overview
The Bravisa Project is an automated tool developed to streamline the creation of various reports, such as Bravisa Temple Tree lists (BTT List), EPS, SMR, PRS, ERS, IRS, etc. The software is designed to handle daily FB data, which can be inserted into a PostgreSQL database through the application. This data is then processed through a series of transformations that involve combining columns across different tables to generate the reports.

### Key Features
- **Data Insertion and Management:** The tool allows users to insert daily FB data into the database and manage the data by deleting records for a specific day or a span of days.
- **Report Generation:** Users can generate various reports either together or separately. The reports can also be downloaded for further use.
- **Data Transformation:** The application automatically processes the inserted data through complex transformations to create the required reports by combining information from different tables.
- **Industry Details Management:** The tool includes a GUI page for adding new industry details into the database.

The project is built using Flask as the web framework, PostgreSQL as the database, and Python 3.10.9 for the backend logic. The `requirements.txt` file lists the dependencies required for the project, which include various libraries and tools used for tasks such as data handling, report generation, and web interface management.

### Key Packages
- **Flask:** For building the web application and handling HTTP requests.
- **PostgreSQL:** For managing the database.
- **Pandas:** For data manipulation and transformation.
- **Numpy:** For mathematical and statistical calculations.

## Installation and Setup
This section will guide you through the process of installing and setting up the Bravisa Project. Follow the steps below to get started.

### Prerequisites
Before you begin, ensure you have the following installed on your system:
- **Operating System:** Windows.
- **Python:** Version 3.10.9 or later. [Download Python](https://www.python.org/downloads/).
- **Package Manager:** `pip` or `conda`. Ensure `pip` is installed with Python.
- **Database:** [Download PgAdmin](https://www.pgadmin.org/download/). [Download PostgreSQL](https://www.postgresql.org/download/).

### Step 1: Clone the Repository
First, clone the project repository to your local machine using Git.
```bash
git clone https://github.com/AchuAshwath/BravisaProject.git
cd BravisaProject
```

### Step 2: Setting up the database
First install Postgres and PgAdmin, [Download Postgres](https://www.postgresql.org/download/).
```bash
psql -h your_host -U your_username -d your_database -f create_tables.sql
# DBsetup.ipynb includes the script to create schemas and tables
# Run this cell to create all the tables and schemas 
```
