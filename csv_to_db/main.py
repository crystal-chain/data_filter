# main.py

import os
import time
import pandas as pd
from sftp_operations import connect_to_sftp, list_today_files, move_file_on_sftp
from db_operations import (
    insert_dataframe_with_conflict,
    insert_into_table,
    merge_staging_to_final,
    clear_staging_table
)
from get_engine import get_engine
from io import BytesIO
from csv_processing import process_csv_file

files_received = {"fournisseur": False, "produit": False}

def get_file_type(file_name):
    """
    Détermine le type de fichier à partir de son nom.
    """
    if "FOURNISSEUR" in file_name.upper():
        return "fournisseur"
    elif "PRODUIT" in file_name.upper():
        return "produit"
    else:
        raise ValueError(f"Type de fichier inconnu pour {file_name}")

def process_and_update(sftp, remote_path, file_name):
    """
    Traite un fichier SFTP en :
      - Lisant son contenu
      - L'insérant dans la base de données
      - Le déplaçant dans un dossier de sauvegarde
      - Fusionnant les tables si les deux fichiers sont traités
    """
    try:
        file_type = get_file_type(file_name)
        print(f"📂 Traitement du fichier {file_name} de type {file_type}")
        
        # Récupération du fichier depuis SFTP
        file_stream = BytesIO()
        sftp.getfo(os.path.join(remote_path, file_name), file_stream)
        file_stream.seek(0)
                
        # Traitement du CSV
        data = process_csv_file(file_stream, file_type, file_name)
        df = pd.DataFrame(data)
        
        engine = get_engine()
        
        # Insertion dans la table principale
        insert_dataframe_with_conflict(engine, file_type, df)
        
        # Insertion dans la table de staging
        staging_table = f"staging_{file_type}"
        insert_into_table(engine, staging_table, data)

        time.sleep(5)
        
        # Marquer le fichier comme reçu
        files_received[file_type] = True

        # # Déplacer le fichier dans le dossier backup
        # destination_path = os.path.join("backup", file_name)
        # move_file_on_sftp(sftp, os.path.join(remote_path, file_name), destination_path)
        
        # Si les deux fichiers sont reçus, on fusionne les données
        if all(files_received.values()):
            print("✅ Fusion des données fournisseur/produit")
            merge_staging_to_final(engine)
            clear_staging_table(engine, "staging_fournisseur")
            clear_staging_table(engine, "staging_produit")
            files_received["fournisseur"] = False
            files_received["produit"] = False
            
    except Exception as e:
        print(f"⚠️ Erreur lors du traitement du fichier {file_name} : {e}")

def main():
    """
    Exécute le traitement des fichiers du jour.
    """
    print("🚀 Lancement du traitement des fichiers SFTP...")
    
    sftp, remote_path = connect_to_sftp()
    
    today_files = list_today_files(sftp, remote_path)
    
    if not today_files:
        print("Aucun fichier reçu aujourd'hui.")
    else:
        for file_name in today_files:
            process_and_update(sftp, remote_path, file_name)
    
    sftp.close()
    print("Fin du traitement.")

if __name__ == "__main__":
    main()
