import psycopg2
import pandas as pd
import sys

def connect_to_db(database, user, host, password, port):
    try:
        conn = psycopg2.connect(
            database=database,
            user=user,
            host=host,
            password=password,
            port=port
        )
        return conn
    except psycopg2.Error as e:
        print(f"Erreur lors de la connexion à la base de données : {e}")
        sys.exit(1)

def disconnect_from_db(conn):
    if conn:
        conn.close()

def sql_to_dataframe(conn, query, column_names):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=column_names)
        return df
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erreur lors de l'exécution de la requête : {error}")
        return None
    finally:
        cursor.close()

def pattern_historique(route_id):
    return 0
