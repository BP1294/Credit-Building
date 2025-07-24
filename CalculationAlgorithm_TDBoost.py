# CREDIT BUILDING FOR UNDERBANKED POPULATIONS
# TD x Experian
# KJ AND BILLY
# This script calculates and updates the TD Boosted Score in the TDBOOST table 
# using an algorithm for determining the boost inside the calculate_tdboost_score() and calculate_creditboost() functions
#   Note when running, the records in the database will only be updated if there is a change in the existing score 
#   The script will print how many records were updated in your terminal

import pymysql
import pandas as pd

# Database connection details
DB_HOST = 'localhost'
DB_USER = 'root'  
DB_PASSWORD = ''  
DB_NAME = 'dm_td_exp_project'

def calculate_creditboost(affiliate_count):
    """
    Calculate credit boost based on affiliate count.
    """
    if affiliate_count >= 9:
        return 20
    elif affiliate_count >= 6:
        return 15
    elif affiliate_count >= 2:
        return 10
    else:
        return 0

def calculate_tdboost_score(credit_score, investment_multiplier, creditboost):
    """
    Calculate the TD Boost score by applying the investment multiplier and credit boost.
    """
    return round(credit_score * investment_multiplier + creditboost)

try:
    # Connect to the database
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )

    with connection.cursor() as cursor:
        # Fetch transaction data grouped by client and affiliate counts
        affiliate_query = """
        SELECT clientid, COUNT(DISTINCT affiliateid) AS affiliate_count
        FROM transaction
        GROUP BY clientid;
        """
        cursor.execute(affiliate_query)
        affiliate_data = cursor.fetchall()
        
        affiliate_df = pd.DataFrame(affiliate_data, columns=['clientid', 'affiliate_count'])

        # Fetch investment multiplier data
        investment_query = """
        SELECT clientid, MIN(multiplier) AS investment_multiplier
        FROM investmentmultiplier
        GROUP BY clientid;
        """
        cursor.execute(investment_query)
        investment_data = cursor.fetchall()
        
        investment_df = pd.DataFrame(investment_data, columns=['clientid', 'investment_multiplier'])

        # Fetch TD Boost data
        tdboost_query = "SELECT id, clientid, credit_score FROM tdboost;"
        cursor.execute(tdboost_query)
        tdboost_data = cursor.fetchall()
        
        tdboost_df = pd.DataFrame(tdboost_data, columns=['id', 'clientid', 'credit_score'])

        # Merge dataframes to enrich TD Boost data
        enriched_data = (tdboost_df
                         .merge(affiliate_df, on='clientid', how='left')
                         .merge(investment_df, on='clientid', how='left'))

        # Fill missing values for clients without transactions or investments
        enriched_data['affiliate_count'] = enriched_data['affiliate_count'].fillna(0).astype(int)
        enriched_data['investment_multiplier'] = enriched_data['investment_multiplier'].fillna(1.01)

        # Calculate creditboost and tdboost_score
        enriched_data['creditboost'] = enriched_data['affiliate_count'].apply(calculate_creditboost)
        enriched_data['tdboost_score'] = enriched_data.apply(
            lambda row: calculate_tdboost_score(
                row['credit_score'], row['investment_multiplier'], row['creditboost']
            ), axis=1
        )

        # Update the TD Boost table
        rows_updated = 0
        for _, row in enriched_data.iterrows():
            update_query = """
            UPDATE tdboost
            SET creditboost = %s, investment_multiplier = %s, tdboost_score = %s
            WHERE id = %s;
            """
            cursor.execute(update_query, (row['creditboost'], row['investment_multiplier'], row['tdboost_score'], row['id']))
            rows_updated += cursor.rowcount

        # Commit the changes
        connection.commit()
        print(f"Updated {rows_updated} rows in the tdboost table.")

except Exception as e:
    print("Error:", e)

finally:
    if 'connection' in locals():
        connection.close()
