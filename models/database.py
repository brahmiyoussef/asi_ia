import psycopg2
import pandas as pd
import sys

def connect_to_db(database):
    try:
        conn = psycopg2.connect(
            database=database,
            user="postgres",
            host="localhost",
            password="admin",
            port=5432
        )
        return conn
    except psycopg2.Error as e:
        print(f"Erreur lors de la connexion à la base de données : {e}")
        sys.exit(1)

def disconnect_from_db(conn):
    if conn:
        conn.close()

def sql_to_dataframe(conn, query):
    column_names=['id', 'received_at', 'sent_at', 'status', 'payload', 'creation_date', 'route_id', 'operation_type', 'type', 'fk_matching_id', 'reject_rsn', 'storage_key_file_in']

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


QUERY='''SELECT *
FROM messages
WHERE received_at >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month');'''