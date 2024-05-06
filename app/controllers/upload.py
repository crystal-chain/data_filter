from flask import Blueprint, render_template, request, redirect, url_for
from ..model.logic import *
import pandas as pd 
import os

upload_blueprint = Blueprint('upload', __name__)
UPLOAD_FOLDER = 'filtred_files'
upload_blueprint.config = {'UPLOAD_FOLDER': UPLOAD_FOLDER}

@upload_blueprint.route('/', methods=['GET'])
def select_page():
    return render_template('select_page.html')

@upload_blueprint.route('/medor', methods=['GET','POST'])
def upload_file_medor():
    if request.method == 'POST':
        if 'file' not in request.files :
            return render_template('upload.html', error_message='Aucun fichier téléchargé.')

        file = request.files['file']
        if file.filename == '':
            return render_template('upload.html', error_message='Aucun fichier sélectionné.')
    
        if file and allowed_file(file.filename):
            try:
                df = pd.read_csv(file, sep=";", low_memory=False)
                # Renommer les colonnes selon marghertira
                df = rename_medor(df)
                # Modifier le champ _ErrorMessage en Error Type
                df = changer_errormessage(df)

                # Supprimer les lignes vides et colonnes vides 
                df = nettoyer_ligne_colonne(df)

                # Ajouter la colonne DataQualityType
                df = ajouter_data_quality_type(df)

                # Ajouter la colonne ErrorStatus
                df['ErrorStatus'] = 'on going'

                # Filtrer les données pour chaque type d'erreur
                df_logic_duplicate = df[df['Error Type'].str.startswith('Logical Duplicate with')]
                df_perfect_duplicate = df[df['Error Type'].str.startswith('Perfect Duplicate with')]
                df_missing_relationship = df[df['Error Type'].str.startswith('Missing relationship')]

                # Construction des noms de fichier pour chaque type d'erreur
                logic_duplicate_filename = f"Logic_duplicate_{file.filename}"
                perfect_duplicate_filename = f"Perfect_duplicate_{file.filename}"
                missing_relationship_filename = f"Missing_relationship_{file.filename}"

                # Chemins d'accès aux fichiers CSV pour chaque type d'erreur
                logic_duplicate_filepath = os.path.join(upload_blueprint.config['UPLOAD_FOLDER'], logic_duplicate_filename)
                perfect_duplicate_filepath = os.path.join(upload_blueprint.config['UPLOAD_FOLDER'], perfect_duplicate_filename)
                missing_relationship_filepath = os.path.join(upload_blueprint.config['UPLOAD_FOLDER'], missing_relationship_filename)

                # Ajouter les colonnes et supprimer les colonnes inutiles
                df_logic_duplicate = add_columns_and_remove(df_logic_duplicate)
                df_perfect_duplicate = add_columns_and_remove(df_perfect_duplicate)
                df_missing_relationship = add_columns_and_remove(df_missing_relationship)

                # Classifier les types d'erreur dans une colonne spécifiée 
                df_missing_relationship = sort_missing_relationships(df_missing_relationship)   
                # Garder la première occurrence pour les Missing relationship
                df_missing_relationship = keep_first_occurrence_for_missing_relationship(df_missing_relationship,"ParentId")
                # Traduction du message de log par quelque chose de plus intelligible par le client 
                df_missing_relationship=modify_error_type(df_missing_relationship)

                # Enregistrement des fichiers CSV
                df_logic_duplicate.to_csv(logic_duplicate_filepath, index=False, encoding="utf-8", sep=";")
                df_perfect_duplicate.to_csv(perfect_duplicate_filepath, index=False, encoding="utf-8", sep=";")
                df_missing_relationship.to_csv(missing_relationship_filepath, index=False, encoding="utf-8", sep=";")

                # Mettre tous dans un ficher excel ou chaque type d'erreur dans un onglet spécifique.
                final_output=os.path.join(upload_blueprint.config['UPLOAD_FOLDER'],'errors_report.xlsx')

                save_dfs_to_excel(df_logic_duplicate,df_perfect_duplicate,df_missing_relationship,final_output)
            except Exception as e:
              return render_template('upload.html', error_message=f'Erreur lors du traitement du fichier : {str(e)}')
        else:
            return render_template('upload.html', error_message='Extension de fichier non autorisée.')
    return render_template('upload.html')

@upload_blueprint.route('/carlsberg', methods=['GET','POST'])
def upload_file_carl():
    if request.method == 'POST':
        if 'file' not in request.files :
            return render_template('upload.html', error_message='Aucun fichier téléchargé.')

        file = request.files['file']
        if file.filename == '':
            return render_template('upload.html', error_message='Aucun fichier sélectionné.')
    
        if file and allowed_file(file.filename):
            try:
                df = pd.read_csv(file, sep=";", low_memory=False)

                # Modifier le champ _ErrorMessage en Error Type
                df = changer_errormessage(df)

                # Supprimer les lignes vides et colonnes vides 
                df = nettoyer_ligne_colonne(df)

                # Filtrer les données pour chaque type d'erreur
                df_logic_duplicate = df[df['Error Type'].str.startswith('Logical Duplicate with')]
                df_perfect_duplicate = df[df['Error Type'].str.startswith('Perfect Duplicate with')]
                df_missing_relationship = df[df['Error Type'].str.startswith('Missing relationship')]

                # Construction des noms de fichier pour chaque type d'erreur
                logic_duplicate_filename = f"Logic_duplicate_{file.filename}"
                perfect_duplicate_filename = f"Perfect_duplicate_{file.filename}"
                missing_relationship_filename = f"Missing_relationship_{file.filename}"

                # Chemins d'accès aux fichiers CSV pour chaque type d'erreur
                logic_duplicate_filepath = os.path.join(upload_blueprint.config['UPLOAD_FOLDER'], logic_duplicate_filename)
                perfect_duplicate_filepath = os.path.join(upload_blueprint.config['UPLOAD_FOLDER'], perfect_duplicate_filename)
                missing_relationship_filepath = os.path.join(upload_blueprint.config['UPLOAD_FOLDER'], missing_relationship_filename)

                # Classifier les types d'erreur dans une colonne spécifiée 
                df_missing_relationship = sort_missing_relationships(df_missing_relationship)   
                # Garder la première occurrence pour les Missing relationship
                df_missing_relationship = keep_first_occurrence_for_missing_relationship(df_missing_relationship,"traceId")

                #df_missing_relationship=modify_error_type(df_missing_relationship)

                # Enregistrement des fichiers CSV
                df_logic_duplicate.to_csv(logic_duplicate_filepath, index=False, encoding="utf-8", sep=";")
                df_perfect_duplicate.to_csv(perfect_duplicate_filepath, index=False, encoding="utf-8", sep=";")
                df_missing_relationship.to_csv(missing_relationship_filepath, index=False, encoding="utf-8", sep=";")

                # Mettre tous dans un ficher excel ou chaque type d'erreur dans un onglet spécifique.

                final_output=os.path.join(upload_blueprint.config['UPLOAD_FOLDER'],'errors_report.xls')

                save_dfs_to_excel(df_logic_duplicate,df_perfect_duplicate,df_missing_relationship,final_output)

            except Exception as e:
              return render_template('upload.html', error_message=f'Erreur lors du traitement du fichier : {str(e)}')
        else:
            return render_template('upload.html', error_message='Extension de fichier non autorisée.')
    return render_template('upload.html')