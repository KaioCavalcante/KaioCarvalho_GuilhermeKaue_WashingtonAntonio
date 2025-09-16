import psycopg

class DB:
    def __init__(self, host, port, dbname, user, password):
        self.conninfo = f"host={host} port={port} dbname={dbname} user={user} password={password}"

    def connect(self):
        return psycopg.connect(self.conninfo, autocommit=False, connect_timeout=10)
