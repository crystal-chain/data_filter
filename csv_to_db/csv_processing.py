# csv_processing.py

import polars as pl  # type: ignore
import re

def is_malformed_in_series(series: pl.Series) -> bool:
    bad_chars = ['Ã', '�', '�']
    return any(any(bad_char in str(cell) for bad_char in bad_chars) for cell in series)

def has_malformed_characters(df: pl.DataFrame) -> bool:
    for column in df.columns:
        if df[column].dtype == pl.Utf8:
            if is_malformed_in_series(df[column]):
                return True
    return False

def read_produit_file_with_encoding_detection(file_stream):
    encodings = ['cp1252', 'utf8']
    for encoding in encodings:
        try:
            file_stream.seek(0)  # repositionner à chaque tentative
            print(f"Tentative de lecture avec encodage : {encoding}")
            df = pl.read_csv(
                file_stream,
                encoding=encoding,
                separator=";",
                dtypes={
                    "code_traitement": pl.Utf8,
                    "ean_uvc": pl.Utf8,
                    "ref_produit": pl.Utf8,
                    "Informations supplémentaires (fournisseur)": pl.Utf8
                },
                columns=list(range(33))
            )
            if not has_malformed_characters(df):
                print(f"Lecture réussie avec encodage : {encoding}")
                return df
            else:
                print(f"Caractères mal encodés détectés avec encodage : {encoding}")
        except Exception as e:
            print(f"Erreur lors de la lecture avec encodage {encoding} : {e}")
    raise ValueError("Aucune lecture correcte du fichier 'produit' n'a pu être effectuée.")


def process_csv_file(file_stream, table_name, file_name):
    """Traite un fichier CSV depuis un flux BytesIO et ajoute une colonne 'source_file'
       qui contient le nom du fichier.
    """
    try:
        if table_name.lower() == "fournisseur":
            df = pl.read_csv(
                file_stream,
                encoding="cp1252",
                separator=";",
                columns=list(range(19)),
                dtypes={
                    "code_traitement": pl.Utf8,
                    "ref_produit": pl.Utf8
                }
            )

        elif table_name.lower() == "produit":
            df = read_produit_file_with_encoding_detection(file_stream)

        else:
            raise ValueError(f"Type de table inconnu : {table_name}")

        # Nettoyage des noms de colonnes
        df.columns = [re.sub(r'[^\w]', '', re.sub(r'\s+', '_', col.strip())) for col in df.columns]
        df = df.with_columns(pl.lit(file_name).alias("source_file"))
        trimmed_columns = [col.strip() for col in df.columns]
        df.columns = trimmed_columns

        print(f"Colonnes traitées ({table_name}) : {trimmed_columns}")

        return df.to_dicts()

    except Exception as e:
        error_message = f"Erreur lors du traitement du fichier pour la table {table_name} : {e}"
        print(error_message)
        raise
