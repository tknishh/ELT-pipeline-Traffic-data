import os
from mysql.connector import Error
import mysql.connector as connector
import pandas as pd
import psycopg2


class Loader():

    def __init__(self):
        pass

    def connect_to_server(self,host:str = "localhost", port:int = 5432, user:str = "warehouse", password:str = "warehouse", dbName:str= "warehouse"):
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=dbName
            )
            cur = conn.cursor()
            print("Connected to server")
            return conn, cur
        except Exception as e:
            print(f"Error: {e}")

    def connect_to_mysql_server(self,host:str, port:int, user:str, password:str, dbName:str=None):
        try:
            connection = connector.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=dbName
                ssl_disabled = True,
                buffered = True
            )
            cursor = connection.cursor()

            print("Connected to server")

            return connection, cursor
        except Exception as e:
            print(f"Error: {e}")


    def load_from_source(self, conn, table, limit, path):
        try:
            query = f"SELECT * FROM {table} LIMIT {limit}"
            results = pd.read_sql_query(query, conn)

            results.to_csv(path)
            return results

        except Exception
        