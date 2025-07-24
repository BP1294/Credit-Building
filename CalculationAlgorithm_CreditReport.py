# CREDIT BUILDING FOR UNDERBANKED POPULATIONS
# TD x Experian
# KJ AND BILLY
# This script takes the average between the TD Boosted score from the TDBOOST table, and the Experian Boosted Score from the EXPERIANBOOST table, 
# and updates the final boosted score in the CREDITREPORT table of the MariaDB "DM_TD_EXP_PROJECT"
# This script should be ran after the TD Boost and Experian Boost table scores have been updated 
# using their corresponding scripts (CalculationAlgorithm_TDBoost.py and CalculationAlgorithm_ExperianBoost.py)
#   Note when running, the records in the database will only be updated if there is a change in the existing score 
#   The script will print how many records were updated in your terminal


import pymysql
import pandas as pd

# Database connection details
DB_HOST = 'localhost'
DB_USER = 'root'  # Replace with your MariaDB username
DB_PASSWORD = ''  # Replace with your MariaDB password
DB_NAME = 'dm_td_exp_project'

try:
    # Connect to the database
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    with connection.cursor() as cursor:
        # Fetch data from EXPERIANBOOST and TD Boost tables
        query = """
        SELECT 
            eb.experianid, 
            eb.score as experianboost_score,  
            td.tdboost_score as tdboost_score
        FROM 
            EXPERIANBOOST eb
        LEFT JOIN 
            CREDITREPORT cr ON eb.experianid = cr.experianid AND eb.ficoscore = cr.ficoscore
        LEFT JOIN 
            TDBOOST td ON cr.tdboostid = td.id  
        GROUP BY 
            eb.experianid, eb.ficoscore;
        """
        cursor.execute(query)
        records = cursor.fetchall()

        # Process the data into a DataFrame
        columns = ['experianid', 'experianboost_score', 'tdboost_score']
        data = pd.DataFrame(records, columns=columns)

        # Calculate the boosted score as the average of the new score and the tdboost score
        data['boostedscore'] = data.apply(
            lambda row: (row['experianboost_score'] + row['tdboost_score']) / 2, axis=1
        )

        # Ensure no boosted score exceeds 900
        data['boostedscore'] = data['boostedscore'].apply(lambda x: min(x, 900))

        # Update the creditreport table with the boosted scores
        rows_updated = 0
        for _, row in data.iterrows():
            update_query = """
            UPDATE creditreport 
            SET boostedscore = %s 
            WHERE experianid = %s;
            """
            cursor.execute(update_query, (row['boostedscore'], row['experianid']))
            rows_updated += cursor.rowcount

        # Commit the changes to the database
        connection.commit()
        print(f"Updated {rows_updated} rows in creditreport table.")

except Exception as e:
    print("Error:", e)

finally:
    if 'connection' in locals():
        connection.close()
