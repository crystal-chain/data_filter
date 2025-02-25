# csv_processing.py

import polars as pl #type: ignore
import re

def process_csv_file(file_stream, table_name, file_name):
    """Traite un fichier CSV depuis un flux BytesIO et ajoute une colonne 'source_file'
       qui contient le nom du fichier.
    """
    try:
        if table_name.lower() == "fournisseur":
            # Lecture du fichier fournisseur (exemple : encodage UTF-8 et séparateur virgule)
            df = pl.read_csv(
                file_stream, 
                encoding="cp1252", 
                separator=";",
                columns=list(range(19)),
                dtypes={"code_traitement": pl.Utf8 ,"ref_produit":pl.Utf8}


            )
        elif table_name.lower() == "produit":
            # Lecture du fichier produit (exemple : encodage cp1252 et séparateur point-virgule)
            df = pl.read_csv(
                file_stream, 
                encoding="cp1252", 
                separator=";", 
                dtypes={"code_traitement": pl.Utf8 ,"EAN_UVC":pl.Utf8 ,"ref_produit":pl.Utf8},
                columns=list(range(33))
            )
        else:
            raise ValueError(f"Type de table inconnu : {table_name}")
        # Ajout de la colonne 'source_file' contenant le nom du fichier
        df.columns = [re.sub(r'[^\w]', '', re.sub(r'\s+', '_', col.strip())) for col in df.columns]

        df = df.with_columns(pl.lit(file_name).alias("source_file"))
        # Nettoyage des noms de colonnes
        trimmed_columns = [col.strip() for col in df.columns]
        df.columns = trimmed_columns
        print(f"Colonnes traitées ({table_name}) : {trimmed_columns}")


        # df = df.filter(~pl.all_horizontal(pl.all().is_null()))
        
        

        return df.to_dicts()
    except Exception as e:
        error_message = f"Erreur lors du traitement du fichier pour la table {table_name} : {e}"
        print(error_message)
        raise
