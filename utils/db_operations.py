import json
import mysql.connector
import pandas as pd
import os


# current file directory route
current_dir = os.path.dirname(os.path.abspath(__file__))
# config flie route
config_path = os.path.join(current_dir, '../config/db_config.json')

def connection():

    try:

        with open(config_path) as config_json:
            config = json.load(config_json)

        conx = mysql.connector.connect(**config)
        print("Succesfully connected to the database")
        return conx
    
    except mysql.connector.Error as err:
        print(f"Error while connecting to the database : {err}")
        return None
    

def create_table():

    try:
        conx = connection()
        cursor = conx.cursor()

        cursor.execute("""CREATE TABLE if not exists HappinessData (
                        ID INT AUTO_INCREMENT PRIMARY KEY,
                        GDP_per_capita FLOAT,
                        Health_Life_Expectancy FLOAT,
                        Freedom FLOAT,
                        Generosity FLOAT,
                        Government_Corruption FLOAT,
                        Continent_Africa BOOLEAN,
                        Continent_America BOOLEAN,
                        Continent_Asia BOOLEAN,
                        Continent_Europe BOOLEAN,
                        Continent_Oceania BOOLEAN,
                        Happiness_Score FLOAT,
                        Predicted_Happiness_Score FLOAT)""")
  
        conx.commit()
        cursor.close()
        conx.close()
        print("Table created successfully")

    except mysql.connector.Error as err:
        print(f"Error while creating table: {err}")


def load(data):
    try: 
        conx = connection()
        cursor = conx.cursor()

        insert = """INSERT INTO HappinessData(GDP_per_capita,Health_Life_Expectancy,Freedom,Generosity,Government_Corruption,Continent_Africa,
                            Continent_America,Continent_Asia,Continent_Europe,Continent_Oceania,Happiness_Score, Predicted_Happiness_Score)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        
        data = (
            float(data['GDP per capita'].iloc[0]), 
            float(data['Health (Life Expectancy)'].iloc[0]), 
            float(data['Freedom'].iloc[0]), 
            float(data['Generosity'].iloc[0]),
            float(data['Government Corruption'].iloc[0]), 
            bool(data['Continent_Africa'].iloc[0]),
            bool(data['Continent_America'].iloc[0]), 
            bool(data['Continent_Asia'].iloc[0]), 
            bool(data['Continent_Europe'].iloc[0]), 
            bool(data['Continent_Oceania'].iloc[0]), 
            float(data['Happiness Score'].iloc[0]), 
            float(data['Predicted Happiness Score'].iloc[0])
        )
        
        cursor.execute(insert, data)

        conx.commit()
        cursor.close()
        conx.close()

        print("row inserted successfully")

    except mysql.connector.Error as err:
        print(f"Error while inserting data: {err}")

def get_data():
        
    try: 
        conx = connection()
        cursor = conx.cursor()

        get_data = "SELECT * FROM happinessdata"
        
        cursor.execute(get_data)

        data = cursor.fetchall()
        columns = ['ID', 'GDP_per_capita', 'Health_Life_Expectancy', 'Freedom', 'Generosity', 'Government_Corruption',
                   'Continent_Africa', 'Continent_America', 'Continent_Asia', 'Continent_Europe', 'Continent_Oceania',
                   'Happiness_Score', 'Predicted_Happiness_Score']
        
        df = pd.DataFrame(data, columns=columns)

        conx.commit()
        cursor.close()
        conx.close()

        # print(df.head())
        print("Data fetched successfully")
        return df

    except mysql.connector.Error as err:
        print(f"Error while getting data: {err}")

# if __name__ == "__main__":
#    get_data()
