import _mysql_connector as mysql

def db_check_creation():
    db = mysql.connect(
    host = 'localhost',
    user = 'root',
    password = '*Saltlake5'
    )
    cursor = db.cursor()
    
    #test if db exists
    def test_db():
        cursor.execute('SHOW DATABASES;')
        databases = cursor.fetchall()
        for database in databases:
            if database == "('mentorshipchallenge',)":
                return database
    test_db()
    
    #if not then we create
    cursor.execute('CREATE DATABASE mentorshipchallenge;')
    test_db()