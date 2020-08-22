import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class Database():
    # replace the user, password, hostname and database according to your configuration according to your information
    engine = db.create_engine('postgresql://vcastro:P4ssw0rd$@sandbox1.cpgtaxfz391x.us-east-2.rds.amazonaws.com/db1',
                              pool_recycle=3600)
    Session = sessionmaker(bind=engine)
    session = Session()

    def __init__(self):
        print("DB Instance created")

    def fetchByQyery(self, query):
        self.connection = self.engine.connect()
        fetchQuery = self.connection.execute(f"SELECT * FROM {query}")

        for data in fetchQuery.fetchall():
            print(data)

        self.connection.close()
