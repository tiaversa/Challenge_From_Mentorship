import pandas as pd
import sqlalchemy
import _mysql_connector as mysql

def pandas_to_mysqlTable(df):
    #connecting to the database
    db = mysql.connect(
        host='127.0.0.1',
        user='root',
        password='*Saltlake5',
        database = 'mentorshipchallenge'
    )
    #created a cursor instance for the database
    cursor = db.cursor()
    #definig query
    query = 'SHOW TABLES;'
    #execute query
    cursor.execute(query)
    #fetching all the results from the cursor object
    tables = cursor.fetchall()

    #check if table already exists
    for table in tables:
        if table == "('challenge',)":
            #if does exists compare the input with the df
            #Defining the query
            query = 'SELECT *FROM challenge;'
            #getting records from the table
            cursor.execute(query)
            #fetching all records from the cursor object
            records = cursor.fetchall()

            #turn into a dataframe
            df_from_existing_table = df # this is unfinished, needs to be edited
            #compare both dataframes for any differences
            df_diff = pd.concat([df_from_existing_table,df]).drop_duplicates(keep=False)
            #add the differences
            return 'Concluded'

    #if it doesn't exist
    database_username = 'root'
    database_password = '*Saltlake5'
    database_ip       = '127.0.0.1'
    database_name     = 'mentorshipchallenge'
    database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                                format(database_username, database_password, 
                                                        database_ip, database_name), pool_recycle=1, pool_timeout=57600).connect()

    df.to_sql(con=database_connection, name='challenge', if_exists='append',chunksize=100)
    database_connection.close()
    return 'Concluded'