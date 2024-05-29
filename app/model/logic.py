import re 
import pandas as pd 
import numpy as np
import os
from io import BytesIO



ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    """
    Vérifie si le nom de fichier est autorisé en se basant sur l'extension.
    """
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS



def remove_duplicates(df):
    """
    Supprime les doublons du DataFrame en se basant sur certaines colonnes.
    """
    # Exclure les colonnes TraceNumber, TraceId, _ErrorCode et _ErrorMessage pour la vérification des doublons parfaits
    subset_cols = df.columns.difference(['TraceNumber', 'TraceId', '_ErrorCode', '_ErrorMessage'])
    
    # Retourner le DataFrame filtré avec les doublons parfaits supprimés
    return df[~df.duplicated(subset=subset_cols, keep=False)]


def keep_first_occurrence_for_missing_relationship(df,col_name):
    """
    Conserve uniquement la première occurrence du traceId et des champs de compositions
    """
    # Filtrer les lignes où 'ErrorType' commence par "Missing relationship"
    df_missing_relationship = df[df['ErrorType'].str.startswith('Missing relationship')]

    # Garder uniquement les colonnes qui commencent par 'C.'
    columns_to_keep = df.columns[df.columns.str.startswith('C.')].tolist()

    # Ajouter des colonnes spécifiques à la liste
    columns_to_keep.append('ParentType')
    columns_to_keep.append('TraceNumber')
    columns_to_keep.append('TraceType')

    # Ajouter les colonnes dont le nom contient '->'
    columns_with_arrow = df.columns[df.columns.str.contains('->', regex=False)].tolist()
    columns_to_keep.extend(columns_with_arrow)

    # Créer un masque pour garder seulement les colonnes qui sont dans 'columns_to_keep' ou qui sont 'ErrorType'
    mask = df.columns.isin(columns_to_keep) | (df.columns == 'ErrorType')

    # Garder la première occurrence de chaque TraceId
    first_occurrence_idx = df_missing_relationship[col_name].drop_duplicates(keep='first').index

   # Copier le DataFrame pour le modifier
    df_missing_relationship_masked = df_missing_relationship.copy()

    # Convertir les colonnes non incluses dans le masque en type 'object' pour permettre l'affectation de chaînes vides
    cols_to_convert = df_missing_relationship_masked.columns[~mask].tolist()
    df_missing_relationship_masked[cols_to_convert] = df_missing_relationship_masked[cols_to_convert].astype(object)

    # Garder les valeurs pour la première occurrence de chaque TraceId et les autres valeurs à une chaîne vide
    df_missing_relationship_masked.loc[~df_missing_relationship_masked.index.isin(first_occurrence_idx), ~mask] = ''

    return df_missing_relationship_masked

def add_columns_and_remove(df):
    """
    Ajoute de nouvelles colonnes au DataFrame et supprime des colonnes inutiles.
    """
    # Faire une copie du DataFrame pour éviter les avertissements "SettingWithCopyWarning"
    df = df.copy()

    # Ajouter une colonne TraceType avec "DATA-QUALITY-" + contenu de ParentType
    df.insert(0, 'TraceType', 'DATA-QUALITY-' + df['ParentType'])

    # Ajouter une colonne TraceNumber avec le contenu de ParentNumber
    df.insert(1, 'TraceNumber', df['ParentNumber'])

    df['TimeSinceError'] = pd.to_datetime(df['createdAt']).dt.strftime('%Y / %m / %d')


    # Supprimer les colonnes hours_since_error, businessName et createdAt

    df.drop(['hours_since_error', 'businessName', 'createdAt','_ErrorCode','formatted_supplier',
            'C.preceding_event_number_rnm01_first_part','C.preceding_event_number_rnm01_second_part',
            'C.formatted_preceding_article_supplier'], axis=1, inplace=True, errors='ignore')

    return df



def modify_error_type(df):
    """
    Modifie les messages d'erreur pour les rendre plus explicites pour les missing relationship 
    """
    # Faire une copie du DataFrame pour éviter les avertissements "SettingWithCopyWarning"
    df = df.copy()

    # Fonction pour extraire le nom du lot du message d'erreur
    def extract_lot_name(text):
        match = re.search(r'\b([a-zA-Z]*Number[a-zA-Z]*)\b', text)
        if not match:
            match = re.search(r'\b([a-zA-Z]*Code[a-zA-Z]*)\b', text)
        return match.group(1) if match else ''
    
    def extract_word(text):
        match = re.search(r'\b([A-Z]+(?:-[A-Z]+)*)\b', text)
        if match:
            return match.group(1)
        else:
            return ''
        
    # Fonction pour extraire la valeur du lot, qui suit immédiatement le nom du lot et se termine avant un espace ou une parenthèse
    def extract_lot_value(value):
        match = re.search(r'\b\w*Number\w*\s*:\s*([^\s\)]+)', value)
        if not match : 
            match = re.search(r'\b\w*Code\w*\s*:\s*([^\s\)]+)', value)
        return match.group(1) if match else ''
    
    # Identifier toutes les colonnes qui contiennent '->'
    arrow_columns = [col for col in df.columns if '->' in col]
   
    # Fonction pour modifier le message d'erreur
    def modify_message(row):
        for col in arrow_columns:
            if isinstance(row[col], str) and row[col].strip():  # Vérifier si la cellule a une valeur non vide
                parts = row[col].split(',')
                modified_parts = []
                for part in parts:
                    word = extract_word(col)
                    lot_name = extract_lot_name(part)
                    lot_value = extract_lot_value(part)
                    if lot_name:
                        new_message = f"{lot_name} {lot_value} not found in {word}"
                        modified_parts.append(new_message)
                # Recombiner les parties modifiées en une seule chaîne, séparées par des virgules
                row[col] = ',\n'.join(modified_parts)
        return row

    df = df.apply(modify_message, axis=1)
    
    # Supprimer la colonne d'erreur d'origine
    df.drop(columns=['ErrorType'], inplace=True)

    return df



def extract_error_types(error_type):
    # Expression régulière pour capturer les types d'erreur avec leur contenu entre parenthèses
    pattern = r'(\b\w+(?:-\w+)?->\w+(?:-\w+)?) \(([^)]+)\)'
    error_types = re.findall(pattern, error_type)
    return error_types


def sort_missing_relationships(df):
    """
    Trie et organise les erreurs de relations manquantes dans des colonnes dédiées
    """
    # Dictionnaire pour accumuler les colonnes d'erreurs uniques
    error_columns = {}

    # Parcourir chaque ligne du DataFrame
    for index, row in df.iterrows():
        # Extraire les types d'erreur de l'erreur actuelle
        current_error_types = extract_error_types(row['ErrorType'])
        for error_type, content in current_error_types:
            # Créer la colonne si elle n'existe pas déjà
            if error_type not in error_columns:
                error_columns[error_type] = [''] * len(df)
            # Ajouter le contenu spécifique à cette occurrence d'erreur
            if error_columns[error_type][index]:
                error_columns[error_type][index] += ',\n' + f"{error_type} ({content})"
            else:
                error_columns[error_type][index] = f"{error_type} ({content})"

    # Ajouter les colonnes d'erreur au DataFrame
    for error_type, contents in error_columns.items():
        df[error_type] = contents

    return df

 # Modification du champ _ErrorMessage en ErrorType
def changer_errormessage(df):
    """
    Modifie les messages d'erreur pour les rendre plus explicites   
    """

    df['ErrorType'] = df['_ErrorMessage'].str.replace('Duplicate with', 'Perfect Duplicate with')
    df['ErrorType'] = df['ErrorType'].str.replace(r'Duplicate value of field .*?(TR_)', 'Logical Duplicate with \\1',regex=True)
    # Suppression de la colonne _ErrorMessage
    df.drop('_ErrorMessage', axis=1, inplace=True)
    return df

def nettoyer_ligne_colonne(df):
    """
    Suppression des lignes vides et colonnes vides 
    """
    df = df.dropna(axis=0, how='all')
    # Suppression des colonnes vides
    df = df.dropna(axis=1, how='all')
    return df 


# Rennomer les colonnes selon marghertira
def rename_medor(df):
    df.rename(columns={'TraceType': 'ParentType', 'TraceNumber': 'ParentNumber', 'traceId': 'ParentId'}, inplace=True)
    return df

def ajouter_data_quality_type(df):
    """
    Ajouter la colonne DataQualityType
    """
    df['DataQualityType'] = 'not defined'
    df.loc[df['ErrorType'].str.contains('Duplicate'), 'DataQualityType'] = 'Duplicates'
    df.loc[df['ErrorType'].str.contains('PRODUCTION->trace'), 'DataQualityType'] = 'rec_no_prod'
    df.loc[df['ErrorType'].str.contains('trace ->MATERIAL-RECEPTION'), 'DataQualityType'] = 'mat-rec_not-found'
    df.loc[df['ErrorType'].str.contains('trace ->PRODUCTION'), 'DataQualityType'] = 'no_prod_in_prod'
    df.loc[df['ErrorType'].str.contains('PAIRING->trace'), 'DataQualityType'] = 'no_prod_in_pairing'
    return df


def save_dfs_to_excel(df1,df2,df3, excel_buffer):
    """
    Sauvegarde des DataFrames dans un fichier Excel, chacun dans un onglet distinct.
    """
    with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
        # Écrire le DataFrame 1 et figer la première ligne
        df1.to_excel(writer, sheet_name='Logical_Duplicate sheet', index=False)
        worksheet1 = writer.sheets['Logical_Duplicate sheet']
        worksheet1.freeze_panes(1, 0)  # Figer la première ligne de l'onglet 'Logical_Duplicate sheet'

        # Écrire le DataFrame 2 et figer la première ligne
        df2.to_excel(writer, sheet_name='Perfect_Duplicate sheet', index=False)
        worksheet2 = writer.sheets['Perfect_Duplicate sheet']
        worksheet2.freeze_panes(1, 0)  # Figer la première ligne de l'onglet 'Perfect_Duplicate sheet'

        # Écrire le DataFrame 3 et figer la première ligne
        df3.to_excel(writer, sheet_name='Missing_relationship sheet', index=False)
        worksheet3 = writer.sheets['Missing_relationship sheet']
        worksheet3.freeze_panes(1, 0)  # Figer la première ligne de l'onglet 'Missing_relationship sheet'

def count_errors_by_type_and_manufacturer(df):
    """
    Transforme un DataFrame pour compter les occurrences de types d'erreurs par manufacture et par date,
    et ajoute un résumé des totaux par manufacture et par type d'erreur sans considération des dates.
    """
    new_df = df.copy()
    new_df['ParentType'] = df['ParentType']
    new_df['Date'] = pd.to_datetime(df['createdAt']).dt.date

    # Détermination du type d'erreur
    new_df['KPIType'] = df['ErrorType'].apply(lambda x: 'Missing Relationship' if 'Missing' in x else 'Duplicate')

    # Groupement initial par TraceType, Manufacturer, Date et ErrorType
    new_df = new_df.groupby(['ParentType', 'Manufacturer', 'Date', 'KPIType']).size().reset_index(name='Count')
    new_df.insert(loc=0, column='TraceType', value='KPI-MONITORING')
    new_df.insert(loc=1 , column ='TraceNumber',value=np.arange(1, len(new_df) + 1))
    new_df['Deactivated'] = "False"

    return new_df

def modify_error_type_carl(df):
    """
    Modifie les messages d'erreur pour les rendre plus explicites pour les missing relationship 
    """
    # Faire une copie du DataFrame pour éviter les avertissements "SettingWithCopyWarning"
    df = df.copy()
    def extract_word(text):
        match = re.search(r'\b([A-Z]+(?:-[A-Z]+)*)\b', text)
        if match:
            return match.group(1)
        else:
            return ''
        
    # Fonction pour extraire la valeur du lot, qui suit immédiatement le nom du lot et se termine avant un espace ou une parenthèse
    def extract_lot_value(value):
        match = re.search(r'(\(.*?\))', value)
        unique_tracetype = df['TraceType'].unique()[0]
        if match: 
            cleaned_value = match.group(1)
            if unique_tracetype=="BRASSERIE-COND":
                cleaned_value = cleaned_value.replace("NumeroLotProductionSource", "NumeroLotProduction")   
            elif unique_tracetype=="BRASSERIE-OF":
                if "trace->BRASSERIE-REC " in value : 
                    cleaned_value = cleaned_value.replace("C.NumeroLotSource", "NumeroLotReception ")  
                if "trace -> BRASSERIE-OF" in value : 
                    cleaned_value=cleaned_value.replace("C.NumeroLotSource","NumeroLotProduction")
                if "BRASSERIE-COND->trace" in value : 
                    cleaned_value=cleaned_value.replace("NumeroLotProduction","NumeroLotProductionSource ")
                if "BRASSERIE-OF->trace" in value : 
                    cleaned_value=cleaned_value.replace("NumeroLotProduction","C.NumeroLotSource")
            elif unique_tracetype =="BRASSERIE-REC":
                if "trace->MALTERIE-EXP" in value : 
                    cleaned_value=cleaned_value.replace("BonLivraison","NumeroBL")
                if "BRASSERIE-OF->trace" in value : 
                    cleaned_value=cleaned_value.replace("NumeroLotReception","C.NumeroLotSource")
            elif unique_tracetype=="MALTERIE-EXP" :
                if "trace->MALTERIE-OF" in value:
                    cleaned_value=cleaned_value.replace("C.PredecesseurCelluleOrigine","CelluleDestination")
                    cleaned_value=cleaned_value.replace("C.Predecesseur","")
                if "BRASSERIE-REC->trace" in value :
                    cleaned_value=cleaned_value.replace("NumeroBL","BonLivraison")
            elif unique_tracetype=="MALTERIE-OF":
                if "trace->" in value :
                    cleaned_value=cleaned_value.replace("C.PredecesseurCelluleOrigine","CelluleDestination")
                    cleaned_value=cleaned_value.replace("C.Predecesseur","")
                if "->trace" in value : 
                    cleaned_value=cleaned_value.replace("TypeFlux","C.PredecesseurTypeFlux")
                    cleaned_value=cleaned_value.replace("NumeroFlux","C.PredecesseurNumeroFlux")
                    cleaned_value=cleaned_value.replace("CelluleDestination","C.PredecesseurCelluleOrigine")
            elif unique_tracetype=="MALTERIE-REC":
                if "MALTERIE-OF->trace" in value : 
                    cleaned_value=cleaned_value.replace("TypeFlux","C.PredecesseurTypeFlux")
                    cleaned_value=cleaned_value.replace("NumeroFlux","C.PredecesseurNumeroFlux")
                    cleaned_value=cleaned_value.replace("CelluleDestination","C.PredecesseurCelluleOrigine")
            return cleaned_value
        else:
            return None


    # Identifier toutes les colonnes qui contiennent '->'
    arrow_columns = [col for col in df.columns if '->' in col]
    col_names=["BRASSERIE-OF->trace","MALTERIE-OF->trace","MALTERIE-EXP->trace"]
    # Fonction pour modifier le message d'erreur
    def modify_message(row):
        for col in arrow_columns:
            if isinstance(row[col], str) and row[col].strip():  # Vérifier si la cellule a une valeur non vide
                parts = re.split(r'\),\s*',row[col])
                parts = [part + ')' if not part.endswith(')') and part else part for part in parts]

                modified_parts = []
                for part in parts:
                    word = extract_word(col)
                    lot_value = extract_lot_value(part)
                    if col in col_names : 
                        new_message = f"Absence {word} aval dont un {lot_value}"
                    elif 'trace->' in col : 
                        new_message = f"Absence {word} amont dont {lot_value}"
                    else:
                        new_message = f"Absence {word} aval dont {lot_value}"
                    modified_parts.append(new_message)
                # Recombiner les parties modifiées en une seule chaîne, séparées par des virgules
                row[col] = ',\n'.join(modified_parts)
        return row

    df = df.apply(modify_message, axis=1)
    
    # Supprimer la colonne d'erreur d'origine

    return df