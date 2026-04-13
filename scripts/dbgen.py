import duckdb
import os

# creation du dossier de sortie pour les données générées TCPH-DBGEN
ROOT_DIR = os.path.dirname(os.path.abspath(__file__)).replace("scripts", "")

# different tables of the TCPH-DBGEN dataset
tables = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]

def is_table_exported(table):
    # vérification de l'existence des fichiers de données générés
    return os.path.exists(os.path.join(ROOT_DIR, f"data/{table}.parquet"))

def create_persistent_database(dbname = "elt_database"):
    print("Création de la base de données à partir des fichiers Parquet existants...")

    # Connexion (crée le fichier .db automatiquement)
    con = duckdb.connect(f"{ROOT_DIR}/data/{dbname}.db")

    # Récupération de tous les fichiers Parquet
    for table in tables:
        if os.path.exists(f"{ROOT_DIR}/data/{table}.parquet"):
            print(f"Ingestion de {table}...")

            file = f"{ROOT_DIR}/data/{table}.parquet"
            
            # Création de la table et importation des données depuis le fichier Parquet
            query = f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet('{file}');"
            con.execute(query)

    tables_created = con.execute("SHOW TABLES;").fetchall()
    print(f"\n✅ Terminé ! Base de données \"{dbname}.db\" créée avec les tables : {', '.join([table[0] for table in tables_created])}\n")

    return con

def create_memory_database():
    print("--- Création d'une base de données en mémoire à partir des fichiers Parquet existants ---\n")

    # Connexion à une base de données en mémoire
    con = duckdb.connect(database=":memory:")

    # Récupération de tous les fichiers Parquet
    for table in tables:
        if os.path.exists(f"{ROOT_DIR}/data/{table}.parquet"):
            print(f"Ingestion de {table}...")

            file = f"{ROOT_DIR}/data/{table}.parquet"
            
            # Création de la table et importation des données depuis le fichier Parquet
            query = f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM read_parquet('{file}');"
            con.execute(query)

    tables_created = con.execute("SHOW TABLES;").fetchall()
    print(f"\n✅ Terminé ! Base de données en mémoire créée avec les tables : {', '.join([table[0] for table in tables_created])}\n")

    return con

def count_parquet_tables():
    # Compte le nombre de tables déjà exportées au format Parquet
    count = 0
    for table in tables:
        if is_table_exported(table):
            count += 1
    return count

def generate_data(scale_factor, overwrite=False):

    # création du dossier de sortie si inexistant
    os.makedirs(os.path.join(ROOT_DIR, "data"), exist_ok=True)
 
    # connexion à une base de données DuckDB en mémoire
    conn = duckdb.connect(database=":memory:")

    # installation et chargement de l'extension TPCH pour la génération des données
    conn.execute("INSTALL tpch;")
    conn.execute("LOAD tpch;")

    # generation des données TCPH-DBGEN avec une échelle de 10 (10GB)
    conn.execute(f"CALL dbgen(sf={scale_factor});")

    # exportation des données générées au format CSV
    for table in tables:
        output_path = f"{ROOT_DIR}/data/{table}.parquet"

        if not overwrite and is_table_exported(table):
            print(f"⚠️ La table {table} existe déjà. Pour generer la table a nouveau, utilisez le mode 'overwrite' ou supprimez manuellement le fichier existant.")
            continue

        if overwrite and is_table_exported(table):
            os.remove(output_path)
            print(f"🗑️ Fichier existant pour la table {table} supprimé.")
        
        print(f"Exportation de la table {table} au format PARQUET...")

        query = f"COPY {table} TO '{output_path}' (FORMAT PARQUET);"

        conn.execute(query=query)

    print("✅ Génération et exportation terminées !")
    conn.close()

    