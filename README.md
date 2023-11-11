# ML_data_streaming_challenge

Welcome to the Machine learning Data Streaming Code Challenge! This project leverages Scikit-learn and Apache Kafka to predict happiness scores in different countries with a regression machine learning model, using a dataset of 5 CSV files containing information about each country. The entire process includes Exploratory Data Analysis (EDA) and Extract, Transform, Load (ETL) operations on the 5 CSV files such as transformations, feature selection, a 70-30 data split for training, streaming transformed data, performig predictions, and storing predictions in a database. 

## Prerequisites

Before running this project, you need to have the following prerequisites in place:

- Python
- MySQL Database

## Datasets Used

- 2015.csv
- 2016.csv
- 2017.csv
- 2018.csv
- 2019.csv

All of these datasets contain information about the happiness in different countries, with multiple variables that contribute to the evaluation of the happinnes of a country such as GDP per capita, Life expectancy, among others. Offering a comprehensive overview of happiness metrics and contributing factors across different nations.

## Repository Structure

The repository has the following structure:
- `requirements.txt`: This file contains the necessary dependencies for the project.
- ['utils'](./utils) folder:
   - `db_operations.py`: This file contains all the methods used for the database such as connection, creating table, and loading data.
   - `transformations.py`: This file contains all the methods used for the transformation of the data.
- `producer.py`: This file contains the kafka producer that streams the data.
- `consumer.py`: This file contains the kafka consumer that receives the data, makes the prediction and loads into the database.
- `docker-compose.yml`: This file contains the kafka configuration.
- ['notebooks'](./notebooks) folder:
   - `model_train.ipynb`: This file contains the exploratory data analysis of the data and the model training.
   - `rf_regressor.pkl`: This file corresponds to the trained model.
   - `metrics.py`: This file contains the evaluation of the model's performance.


## How to Run This Project

Follow these steps to run the project:

1. Clone the repository: 
    ```bash
    git clone https://github.com/YOANGELICA/ML_data_streaming_challenge
    ```

2. Navigate to the project directory: 
   ```bash
    cd ml_data_streaming_challenge
   ```

3. Install project dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Create database credentials file 'db_config.json' in a folder called config with the following content:
      ```
      {
        "user": "your_username",
        "password": "your_password",
        "host": "your_host"
        "server": "your_server",
        "db": "your_database"
      }
      ```

    > **Note:** This file is necessary, by not having it you won't be able to access the database unless you state the credentials directly (not recommended). If you choose to give it a different name or location, you must change the the access route in the code.
    >

5. Launch the containers:

   ```bash
   docker compose up
   ```

Now you're all set to start exploring the project!

## Author
Maria Angelica Portocarrero Quintero
