from flask import Blueprint, render_template, request, send_file
from ..model.logic import *
import pandas as pd 
from io import BytesIO
import os
import zipfile
from datetime import datetime

upload_blueprint = Blueprint('upload', __name__)

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
                # Modifier le champ _ErrorMessage en ErrorType
                df = changer_errormessage(df)
                # Cree le CSV de KPI
                df_kpi=count_errors_by_type_and_manufacturer(df)

                # Supprimer les lignes vides et colonnes vides 
                df = nettoyer_ligne_colonne(df)

                # Ajouter la colonne DataQualityType
                df = ajouter_data_quality_type(df)

                # Ajouter la colonne ErrorStatus
                df['ErrorStatus'] = 'on going'

                # Filtrer les données pour chaque type d'erreur
                df_logic_duplicate = df[df['ErrorType'].str.startswith('Logical Duplicate with')]
                df_perfect_duplicate = df[df['ErrorType'].str.startswith('Perfect Duplicate with')]
                df_missing_relationship = df[df['ErrorType'].str.startswith('Missing relationship')]

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

                # Ajout de la date d'aujourd'hui
                today = datetime.today().strftime('%Y-%m-%d')

                file.filename="_".join(file.filename.split("_")[:3])
            
                excel_buffer= BytesIO()
                save_dfs_to_excel(df_logic_duplicate,df_perfect_duplicate,df_missing_relationship,excel_buffer)


                zip_buffer=BytesIO()
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED, False) as zip_file:
                    # Ajouter les fichiers CSV et excel dans le fichier ZIP en utilisant les noms de fichiers construits
                    zip_file.writestr(f"Logic_duplicate_{file.filename}_{today}.csv", df_logic_duplicate.to_csv(index=False, encoding="utf-8", sep=";"))
                    zip_file.writestr(f"Perfect_duplicate_{file.filename}_{today}.csv", df_perfect_duplicate.to_csv(index=False, encoding="utf-8", sep=";"))
                    zip_file.writestr(f"Missing_relationship_{file.filename}_{today}.csv", df_missing_relationship.to_csv(index=False, encoding="utf-8", sep=";"))
                    zip_file.writestr(f'ALL_Errors_report_{file.filename}_{today}.xlsx', excel_buffer.getvalue())
                    zip_file.writestr(f"KPI_{file.filename}_{today}.csv", df_kpi.to_csv(index=False, encoding="utf-8", sep=";"))

                zip_buffer.seek(0)
                # Retourner le fichier ZIP en tant que réponse à la requête POST
                return send_file(
                    zip_buffer,
                    as_attachment=True,
                    mimetype='application/zip',
                    download_name=f'filtered_data_{file.filename}.zip'
                    )
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

                # Modifier le champ _ErrorMessage en ErrorType
                df = changer_errormessage(df)

                # Supprimer les lignes vides et colonnes vides 
                df = nettoyer_ligne_colonne(df)

                # Filtrer les données pour chaque type d'erreur
                df_logic_duplicate = df[df['ErrorType'].str.startswith('Logical Duplicate with')]
                df_perfect_duplicate = df[df['ErrorType'].str.startswith('Perfect Duplicate with')]
                df_missing_relationship = df[df['ErrorType'].str.startswith('Missing relationship')]


                #  Classifier les types d'erreur dans une colonne spécifiée 
                df_missing_relationship = sort_missing_relationships(df_missing_relationship)   
                # Garder la première occurrence pour les Missing relationship
                df_missing_relationship = keep_first_occurrence_for_missing_relationship(df_missing_relationship,"traceId")
                # Traduction du message de log par quelque chose de plus intelligible par le client 
                df_missing_relationship=modify_error_type_carl(df_missing_relationship)

                file.filename="_".join(file.filename.split("_")[:3])

                excel_buffer= BytesIO()
                save_dfs_to_excel(df_logic_duplicate,df_perfect_duplicate,df_missing_relationship,excel_buffer)

                zip_buffer=BytesIO()
                
                with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED, False) as zip_file:
                    # Ajouter les fichiers CSV dans le fichier ZIP en utilisant les noms de fichiers construits
                    zip_file.writestr(f"Logic_duplicate_{file.filename}.csv", df_logic_duplicate.to_csv(index=False, encoding="utf-8", sep=";"))
                    zip_file.writestr(f"Perfect_duplicate_{file.filename}.csv", df_perfect_duplicate.to_csv(index=False, encoding="utf-8", sep=";"))
                    zip_file.writestr(f"Missing_relationship_{file.filename}.csv", df_missing_relationship.to_csv(index=False, encoding="utf-8", sep=";"))
                    zip_file.writestr(f'ALL_Errors_report_{file.filename}.xlsx', excel_buffer.getvalue())
                zip_buffer.seek(0)
                # Retourner le fichier ZIP en tant que réponse à la requête POST
                return send_file(
                    zip_buffer,
                    as_attachment=True,
                    mimetype='application/zip',
                    download_name=f'filtered_data_{file.filename}.zip'
                    )

            except Exception as e:
              return render_template('upload.html', error_message=f'Erreur lors du traitement du fichier : {str(e)}')
        else:
            return render_template('upload.html', error_message='Extension de fichier non autorisée.')
    return render_template('upload.html')