# CREDIT BUILDING FOR UNDERBANKED POPULATIONS
# TD x Experian
# KJ AND BILLY
# This script calculates and updates the Experian Boosted Score in the EXPERIANBOOST table 
# using an algorithm for determining the boost inside the calculate_new_score() function
#   Note when running, the records in the database will only be updated if there is a change in the existing score 
#   The script will print how many records were updated in your terminal

import pymysql
import pandas as pd

# Database connection details
DB_HOST = 'localhost'
DB_USER = 'root'   
DB_PASSWORD = ''   
DB_NAME = 'dm_td_exp_project'

def calculate_new_score(ficoscore, late_payments, nsf, autopay, bill_frequency, boost_impact, billing_status):
    """
    Calculate a new score based on the provided attributes, including late payments, NSF, 
    autopay, bill frequency, boost impact, and billing status.
    """
    # Initialize the base score (FICO score)
    score = ficoscore

    # Handle None (NULL) values by converting them to default values
    late_payments = int(late_payments) if late_payments is not None else 0
    bill_frequency = int(bill_frequency.split(',')[0]) if bill_frequency else 0  # Use the first value if it's a list
    boost_impact = int(boost_impact) if boost_impact is not None else 0

    # Penalize for late payments
    score -= late_payments  # Example: Subtract per late payment

    # Penalize for NSF (Non-Sufficient Funds)
    if nsf == 'Yes':  # nsf is a string, so check for 'Yes'
        score -= 5  # Example: Subtract 5 points for each NSF

    # Reward if autopay is enabled
    if autopay == 'Yes':  # autopay is a string, so check for 'Yes'
        score += 15  # Example: Add 15 points if autopay is enabled

    # Penalize based on bill frequency
    score -= bill_frequency  # Example: Subtract for each bill frequency (higher frequency means higher penalty)

    # Apply boost impact
    score += boost_impact * 10  # Example: Add 10 points for each boost impact (higher boost impact means more points)

    # Penalize if billing status is "Closed"
    if billing_status == 'Closed':
        score -= 5  # Example: Subtract 5 points if the account is closed

    # Ensure score does not drop below 300 or exceed 900
    return max(300, min(900, round(score)))


try:
    # Connect to the database
    connection = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME
    )
    
    with connection.cursor() as cursor:
        # Fetch data from EXPERIANBOOST and BILLHISTORY tables
        query = """
        SELECT 
            eb.experianid, 
            eb.ficoscore, 
            IFNULL(SUM(bh.latepayments), 0) AS total_late_payments,
            IFNULL(SUM(bh.boostimpact), 0) AS total_boost_impact,
            IFNULL(SUM(af.cost), 0) AS total_affiliate_cost,
            GROUP_CONCAT(DISTINCT bh.nsf) AS nsf,
            GROUP_CONCAT(DISTINCT bh.autopay) AS autopay,
            GROUP_CONCAT(DISTINCT bh.billfrequency) AS bill_frequency,
            GROUP_CONCAT(DISTINCT bh.billingstatus) AS billing_status
        FROM 
            EXPERIANBOOST eb
        LEFT JOIN 
            BILLHISTORY bh ON eb.experianid = bh.experianid AND eb.ficoscore = bh.ficoscore
        LEFT JOIN 
            AFFILIATE af ON bh.affiliateid = af.id
        GROUP BY 
            eb.experianid, eb.ficoscore;
        """
        cursor.execute(query)
        records = cursor.fetchall()

        # Process the data into a DataFrame
        columns = ['experianid', 'ficoscore', 'total_late_payments', 'total_boost_impact', 'total_affiliate_cost', 
                   'nsf', 'autopay', 'bill_frequency', 'billing_status']
        data = pd.DataFrame(records, columns=columns)

        # Calculate new scores for each record
        data['new_score'] = data.apply(
            lambda row: calculate_new_score(
                row['ficoscore'], row['total_late_payments'], row['nsf'], row['autopay'], 
                row['bill_frequency'], row['total_boost_impact'], row['billing_status']
            ), axis=1
        )

        # Ensure no score exceeds 900
        data['new_score'] = data['new_score'].apply(lambda x: min(x, 900))

        # Update the EXPERIANBOOST table with the new scores
        rows_updated = 0
        for _, row in data.iterrows():
            update_query = """
            UPDATE EXPERIANBOOST 
            SET score = %s 
            WHERE experianid = %s AND ficoscore = %s;
            """
            cursor.execute(update_query, (row['new_score'], row['experianid'], row['ficoscore']))
            rows_updated += cursor.rowcount

        # Commit the changes to the database
        connection.commit()
        print(f"Updated {rows_updated} rows in EXPERIANBOOST table.")

except Exception as e:
    print("Error:", e)

finally:
    if 'connection' in locals():
        connection.close()
