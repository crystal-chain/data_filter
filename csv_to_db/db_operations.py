import os
from sqlalchemy import  text
from dotenv import load_dotenv
load_dotenv()


def insert_into_table(engine, table_name, data):
    """
    Insère les données (liste de dictionnaires) dans la table spécifiée.
    Ajoute une gestion de conflit sur la colonne "ref_produit" pour garder la dernière donnée en cas de doublon.
    Cette fonction est utilisée pour insérer dans les tables de base (fournisseur, produit)
    ainsi que dans les tables de staging.
    """
    if not data:
        print(f"Aucune donnée à insérer dans {table_name}.")
        return

    columns = data[0].keys()
    col_list = ", ".join([f'"{col}"' for col in columns])  # Ajout des guillemets autour des colonnes
    placeholders = ", ".join([f":{col}" for col in columns])
    update_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])
    
    query = text(f'''
        INSERT INTO {table_name} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ("ref_produit")
        DO UPDATE SET {update_clause};
    ''')
    
    with engine.begin() as conn:
        for row in data:
            conn.execute(query, row)
    
    print(f"{len(data)} enregistrements insérés dans {table_name}.")



def insert_dataframe_with_conflict(engine, table_name, df):
    """
    Insère chaque ligne du DataFrame dans la table spécifiée avec gestion de conflit sur "ref_produit".
    Si une ligne avec le même "ref_produit" existe déjà, elle est mise à jour avec les nouvelles valeurs.
    """
    with engine.begin() as conn:
        for index, row in df.iterrows():
            # Construction dynamique de la requête d'insertion avec ON CONFLICT
            insert_query = f"""
            INSERT INTO "{table_name}" ({", ".join(f'"{col}"' for col in df.columns)})
            VALUES ({", ".join(f":{col}" for col in df.columns)})
            ON CONFLICT ("ref_produit")
            DO UPDATE SET {", ".join(f'"{col}" = EXCLUDED."{col}"' for col in df.columns)};
            """
            conn.execute(text(insert_query), row.to_dict())

def merge_staging_to_final(engine):
    """
    Fusionne les nouvelles données des tables de staging (staging_fournisseur et staging_produit)
    via un FULL OUTER JOIN et met à jour la table finale fournisseur_produit.
    En cas de conflit sur ref_produits, toutes les colonnes sont mises à jour.
    """
    merge_query = text("""
    WITH new_data AS (
      SELECT 
        COALESCE(f.ref_produit, p.ref_produit) AS ref_produits,
        f."Nom_et_role_contact_1",
        f."localisationcontact1",
        f."Email_Contact_1",
        f."Téléphone_Contact_1",
        f."Langues_parlées_contact1",
        f."Nom_et_role_contact_2",
        f."localisationcontact2",
        f."Email_Contact_2",
        f."Téléphone_Contact_2",
        f."Langues_parlées_contact2",
        f."Nom_et_role_contact_3",
        f."localisationcontact3",
        f."Email_Contact_3",
        f."Téléphone_Contact_3",
        f."Langues_parlées_contact3",
        f."nom_fournisseur",
        f."localisation",
        f."type_fournisseur",
        f."source_file" AS "source_file_fournisseur",
        p."column",
        p."row_number",
        p."entreprise",
        p."node_name",
        p."nom_du_produit",
        p."ref_produit",
        p."Type_de_produit",
        p."URL_photo",
        p."recyclabilite",
        p."incorporation_matiere_recyclee",
        p."Pays_de_tissagetricotage_ou_piquage",
        p."Pays_de_teintureimpression_ou_montage",
        p."Pays_de_confection_ou_finition",
        p."Présence_Substances_Dangereuses",
        p."Nom_des_Substances_Dangereuses",
        p."Présence_de_microfibres_plastiques",
        p."Primes_et_pénalités",
        p."Emballage__recyclabilité",
        p."Emballage__informations_supplémentaires",
        p."Emballage__Incorporation_matière_recyclée",
        p."Emballage__présence_substances_dangereuses",
        p."Emballage__noms_des_substances_dangereuses",
        p."Collection",
        p."Description",
        p."Id_Certif_1",
        p."Id_Certif_2",
        p."Id_Certif_3",
        p."Informations_supplémentaires_fournisseur",
        p."Commentaires",
        p."Réference_modele_pour_fournisseur",
        p."EAN_UVC",
        p."Inactif",
        p."code_traitement",
        p."source_file"
      FROM staging_fournisseur f
      FULL OUTER JOIN staging_produit p
          ON f.ref_produit = p.ref_produit
    )
    INSERT INTO fournisseur_produit (
      ref_produits,
      "Nom_et_role_contact_1",
      "localisationcontact1",
      "Email_Contact_1",
      "Téléphone_Contact_1",
      "Langues_parlées_contact1",
      "Nom_et_role_contact_2",
      "localisationcontact2",
      "Email_Contact_2",
      "Téléphone_Contact_2",
      "Langues_parlées_contact2",
      "Nom_et_role_contact_3",
      "localisationcontact3",
      "Email_Contact_3",
      "Téléphone_Contact_3",
      "Langues_parlées_contact3",
      "nom_fournisseur",
      "localisation",
      "type_fournisseur",
      "source_file_fournisseur",
      "column",
      "row_number",
      "entreprise",
      "node_name",
      "nom_du_produit",
      "ref_produit",
      "Type_de_produit",
      "URL_photo",
      "recyclabilite",
      "incorporation_matiere_recyclee",
      "Pays_de_tissagetricotage_ou_piquage",
      "Pays_de_teintureimpression_ou_montage",
      "Pays_de_confection_ou_finition",
      "Présence_Substances_Dangereuses",
      "Nom_des_Substances_Dangereuses",
      "Présence_de_microfibres_plastiques",
      "Primes_et_pénalités",
      "Emballage__recyclabilité",
      "Emballage__informations_supplémentaires",
      "Emballage__Incorporation_matière_recyclée",
      "Emballage__présence_substances_dangereuses",
      "Emballage__noms_des_substances_dangereuses",
      "Collection",
      "Description",
      "Id_Certif_1",
      "Id_Certif_2",
      "Id_Certif_3",
      "Informations_supplémentaires_fournisseur",
      "Commentaires",
      "Réference_modele_pour_fournisseur",
      "EAN_UVC",
      "Inactif",
      "code_traitement",
      "source_file"
    )
    SELECT 
      ref_produits,
      "Nom_et_role_contact_1",
      "localisationcontact1",
      "Email_Contact_1",
      "Téléphone_Contact_1",
      "Langues_parlées_contact1",
      "Nom_et_role_contact_2",
      "localisationcontact2",
      "Email_Contact_2",
      "Téléphone_Contact_2",
      "Langues_parlées_contact2",
      "Nom_et_role_contact_3",
      "localisationcontact3",
      "Email_Contact_3",
      "Téléphone_Contact_3",
      "Langues_parlées_contact3",
      "nom_fournisseur",
      "localisation",
      "type_fournisseur",
      "source_file_fournisseur",
      "column",
      "row_number",
      "entreprise",
      "node_name",
      "nom_du_produit",
      "ref_produit",
      "Type_de_produit",
      "URL_photo",
      "recyclabilite",
      "incorporation_matiere_recyclee",
      "Pays_de_tissagetricotage_ou_piquage",
      "Pays_de_teintureimpression_ou_montage",
      "Pays_de_confection_ou_finition",
      "Présence_Substances_Dangereuses",
      "Nom_des_Substances_Dangereuses",
      "Présence_de_microfibres_plastiques",
      "Primes_et_pénalités",
      "Emballage__recyclabilité",
      "Emballage__informations_supplémentaires",
      "Emballage__Incorporation_matière_recyclée",
      "Emballage__présence_substances_dangereuses",
      "Emballage__noms_des_substances_dangereuses",
      "Collection",
      "Description",
      "Id_Certif_1",
      "Id_Certif_2",
      "Id_Certif_3",
      "Informations_supplémentaires_fournisseur",
      "Commentaires",
      "Réference_modele_pour_fournisseur",
      "EAN_UVC",
      "Inactif",
      "code_traitement",
      "source_file"
    FROM new_data
    ON CONFLICT (ref_produits) DO UPDATE SET
      "Nom_et_role_contact_1" = EXCLUDED."Nom_et_role_contact_1",
      "localisationcontact1" = EXCLUDED."localisationcontact1",
      "Email_Contact_1" = EXCLUDED."Email_Contact_1",
      "Téléphone_Contact_1" = EXCLUDED."Téléphone_Contact_1",
      "Langues_parlées_contact1" = EXCLUDED."Langues_parlées_contact1",
      "Nom_et_role_contact_2" = EXCLUDED."Nom_et_role_contact_2",
      "localisationcontact2" = EXCLUDED."localisationcontact2",
      "Email_Contact_2" = EXCLUDED."Email_Contact_2",
      "Téléphone_Contact_2" = EXCLUDED."Téléphone_Contact_2",
      "Langues_parlées_contact2" = EXCLUDED."Langues_parlées_contact2",
      "Nom_et_role_contact_3" = EXCLUDED."Nom_et_role_contact_3",
      "localisationcontact3" = EXCLUDED."localisationcontact3",
      "Email_Contact_3" = EXCLUDED."Email_Contact_3",
      "Téléphone_Contact_3" = EXCLUDED."Téléphone_Contact_3",
      "Langues_parlées_contact3" = EXCLUDED."Langues_parlées_contact3",
      "nom_fournisseur" = EXCLUDED."nom_fournisseur",
      "localisation" = EXCLUDED."localisation",
      "type_fournisseur" = EXCLUDED."type_fournisseur",
      "source_file_fournisseur" = EXCLUDED."source_file_fournisseur",
      "column" = EXCLUDED."column",
      "row_number" = EXCLUDED."row_number",
      "entreprise" = EXCLUDED."entreprise",
      "node_name" = EXCLUDED."node_name",
      "nom_du_produit" = EXCLUDED."nom_du_produit",
      "ref_produit" = EXCLUDED."ref_produit",
      "Type_de_produit" = EXCLUDED."Type_de_produit",
      "URL_photo" = EXCLUDED."URL_photo",
      "recyclabilite" = EXCLUDED."recyclabilite",
      "incorporation_matiere_recyclee" = EXCLUDED."incorporation_matiere_recyclee",
      "Pays_de_tissagetricotage_ou_piquage" = EXCLUDED."Pays_de_tissagetricotage_ou_piquage",
      "Pays_de_teintureimpression_ou_montage" = EXCLUDED."Pays_de_teintureimpression_ou_montage",
      "Pays_de_confection_ou_finition" = EXCLUDED."Pays_de_confection_ou_finition",
      "Présence_Substances_Dangereuses" = EXCLUDED."Présence_Substances_Dangereuses",
      "Nom_des_Substances_Dangereuses" = EXCLUDED."Nom_des_Substances_Dangereuses",
      "Présence_de_microfibres_plastiques" = EXCLUDED."Présence_de_microfibres_plastiques",
      "Primes_et_pénalités" = EXCLUDED."Primes_et_pénalités",
      "Emballage__recyclabilité" = EXCLUDED."Emballage__recyclabilité",
      "Emballage__informations_supplémentaires" = EXCLUDED."Emballage__informations_supplémentaires",
      "Emballage__Incorporation_matière_recyclée" = EXCLUDED."Emballage__Incorporation_matière_recyclée",
      "Emballage__présence_substances_dangereuses" = EXCLUDED."Emballage__présence_substances_dangereuses",
      "Emballage__noms_des_substances_dangereuses" = EXCLUDED."Emballage__noms_des_substances_dangereuses",
      "Collection" = EXCLUDED."Collection",
      "Description" = EXCLUDED."Description",
      "Id_Certif_1" = EXCLUDED."Id_Certif_1",
      "Id_Certif_2" = EXCLUDED."Id_Certif_2",
      "Id_Certif_3" = EXCLUDED."Id_Certif_3",
      "Informations_supplémentaires_fournisseur" = EXCLUDED."Informations_supplémentaires_fournisseur",
      "Commentaires" = EXCLUDED."Commentaires",
      "Réference_modele_pour_fournisseur" = EXCLUDED."Réference_modele_pour_fournisseur",
      "EAN_UVC" = EXCLUDED."EAN_UVC",
      "Inactif" = EXCLUDED."Inactif",
      "code_traitement" = EXCLUDED."code_traitement",
      "source_file" = EXCLUDED."source_file"
    """)
    with engine.begin() as conn:
        conn.execute(merge_query)
    print("Mise à jour de la table fournisseur_produit effectuée avec succès.")

def clear_staging_table(engine, staging_table):
    """
    Vide la table de staging spécifiée.
    """
    query = text(f"TRUNCATE TABLE {staging_table};")
    with engine.begin() as conn:
        conn.execute(query)
    print(f"Table de staging {staging_table} vidée.")
