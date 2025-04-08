import os

from dotenv import load_dotenv
from sqlalchemy import text

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
    col_list = ", ".join(
        [f'"{col}"' for col in columns]
    )  # Ajout des guillemets autour des colonnes
    placeholders = ", ".join([f":{col}" for col in columns])
    update_clause = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns])

    query = text(
        f"""
        INSERT INTO {table_name} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ("ref_produit")
        DO UPDATE SET {update_clause};
    """
    )

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
    En cas de conflit sur ref_produits, pour chaque colonne, si la nouvelle valeur est NULL,
    on conserve la valeur déjà présente dans fournisseur_produit.
    """
    merge_query = text(
        """
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
        p."rep_saisie",
        p."ean_uvc",
        p."Inactif",
        p."code_traitement",
        p."source_file",
        f."usine",
        p."column"
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
      "ean_uvc",
      "Inactif",
      "code_traitement",
      "source_file",
      "usine",
      "rep_saisie"
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
      "ean_uvc",
      "Inactif",
      "code_traitement",
      "source_file",
      "usine",
      "rep_saisie"
    FROM new_data
    ON CONFLICT (ref_produits) DO UPDATE SET
      "Nom_et_role_contact_1" = COALESCE(EXCLUDED."Nom_et_role_contact_1", fournisseur_produit."Nom_et_role_contact_1"),
      "localisationcontact1" = COALESCE(EXCLUDED."localisationcontact1", fournisseur_produit."localisationcontact1"),
      "Email_Contact_1" = COALESCE(EXCLUDED."Email_Contact_1", fournisseur_produit."Email_Contact_1"),
      "Téléphone_Contact_1" = COALESCE(EXCLUDED."Téléphone_Contact_1", fournisseur_produit."Téléphone_Contact_1"),
      "Langues_parlées_contact1" = COALESCE(EXCLUDED."Langues_parlées_contact1", fournisseur_produit."Langues_parlées_contact1"),
      "Nom_et_role_contact_2" = COALESCE(EXCLUDED."Nom_et_role_contact_2", fournisseur_produit."Nom_et_role_contact_2"),
      "localisationcontact2" = COALESCE(EXCLUDED."localisationcontact2", fournisseur_produit."localisationcontact2"),
      "Email_Contact_2" = COALESCE(EXCLUDED."Email_Contact_2", fournisseur_produit."Email_Contact_2"),
      "Téléphone_Contact_2" = COALESCE(EXCLUDED."Téléphone_Contact_2", fournisseur_produit."Téléphone_Contact_2"),
      "Langues_parlées_contact2" = COALESCE(EXCLUDED."Langues_parlées_contact2", fournisseur_produit."Langues_parlées_contact2"),
      "Nom_et_role_contact_3" = COALESCE(EXCLUDED."Nom_et_role_contact_3", fournisseur_produit."Nom_et_role_contact_3"),
      "localisationcontact3" = COALESCE(EXCLUDED."localisationcontact3", fournisseur_produit."localisationcontact3"),
      "Email_Contact_3" = COALESCE(EXCLUDED."Email_Contact_3", fournisseur_produit."Email_Contact_3"),
      "Téléphone_Contact_3" = COALESCE(EXCLUDED."Téléphone_Contact_3", fournisseur_produit."Téléphone_Contact_3"),
      "Langues_parlées_contact3" = COALESCE(EXCLUDED."Langues_parlées_contact3", fournisseur_produit."Langues_parlées_contact3"),
      "nom_fournisseur" = COALESCE(EXCLUDED."nom_fournisseur", fournisseur_produit."nom_fournisseur"),
      "localisation" = COALESCE(EXCLUDED."localisation", fournisseur_produit."localisation"),
      "type_fournisseur" = COALESCE(EXCLUDED."type_fournisseur", fournisseur_produit."type_fournisseur"),
      "source_file_fournisseur" = COALESCE(EXCLUDED."source_file_fournisseur", fournisseur_produit."source_file_fournisseur"),
      "column" = COALESCE(EXCLUDED."column", fournisseur_produit."column"),
      "row_number" = COALESCE(EXCLUDED."row_number", fournisseur_produit."row_number"),
      "entreprise" = COALESCE(EXCLUDED."entreprise", fournisseur_produit."entreprise"),
      "node_name" = COALESCE(EXCLUDED."node_name", fournisseur_produit."node_name"),
      "nom_du_produit" = COALESCE(EXCLUDED."nom_du_produit", fournisseur_produit."nom_du_produit"),
      "ref_produit" = COALESCE(EXCLUDED."ref_produit", fournisseur_produit."ref_produit"),
      "usine" = COALESCE(EXCLUDED."usine", fournisseur_produit."usine"),
      "rep_saisie" = COALESCE(EXCLUDED."rep_saisie", fournisseur_produit."rep_saisie"),
      "URL_photo" = COALESCE(EXCLUDED."URL_photo", fournisseur_produit."URL_photo"),
      "recyclabilite" = COALESCE(EXCLUDED."recyclabilite", fournisseur_produit."recyclabilite"),
      "incorporation_matiere_recyclee" = COALESCE(EXCLUDED."incorporation_matiere_recyclee", fournisseur_produit."incorporation_matiere_recyclee"),
      "Pays_de_tissagetricotage_ou_piquage" = COALESCE(EXCLUDED."Pays_de_tissagetricotage_ou_piquage", fournisseur_produit."Pays_de_tissagetricotage_ou_piquage"),
      "Pays_de_teintureimpression_ou_montage" = COALESCE(EXCLUDED."Pays_de_teintureimpression_ou_montage", fournisseur_produit."Pays_de_teintureimpression_ou_montage"),
      "Pays_de_confection_ou_finition" = COALESCE(EXCLUDED."Pays_de_confection_ou_finition", fournisseur_produit."Pays_de_confection_ou_finition"),
      "Présence_Substances_Dangereuses" = COALESCE(EXCLUDED."Présence_Substances_Dangereuses", fournisseur_produit."Présence_Substances_Dangereuses"),
      "Nom_des_Substances_Dangereuses" = COALESCE(EXCLUDED."Nom_des_Substances_Dangereuses", fournisseur_produit."Nom_des_Substances_Dangereuses"),
      "Présence_de_microfibres_plastiques" = COALESCE(EXCLUDED."Présence_de_microfibres_plastiques", fournisseur_produit."Présence_de_microfibres_plastiques"),
      "Primes_et_pénalités" = COALESCE(EXCLUDED."Primes_et_pénalités", fournisseur_produit."Primes_et_pénalités"),
      "Emballage__recyclabilité" = COALESCE(EXCLUDED."Emballage__recyclabilité", fournisseur_produit."Emballage__recyclabilité"),
      "Emballage__informations_supplémentaires" = COALESCE(EXCLUDED."Emballage__informations_supplémentaires", fournisseur_produit."Emballage__informations_supplémentaires"),
      "Emballage__Incorporation_matière_recyclée" = COALESCE(EXCLUDED."Emballage__Incorporation_matière_recyclée", fournisseur_produit."Emballage__Incorporation_matière_recyclée"),
      "Emballage__présence_substances_dangereuses" = COALESCE(EXCLUDED."Emballage__présence_substances_dangereuses", fournisseur_produit."Emballage__présence_substances_dangereuses"),
      "Emballage__noms_des_substances_dangereuses" = COALESCE(EXCLUDED."Emballage__noms_des_substances_dangereuses", fournisseur_produit."Emballage__noms_des_substances_dangereuses"),
      "Collection" = COALESCE(EXCLUDED."Collection", fournisseur_produit."Collection"),
      "Description" = COALESCE(EXCLUDED."Description", fournisseur_produit."Description"),
      "Id_Certif_1" = COALESCE(EXCLUDED."Id_Certif_1", fournisseur_produit."Id_Certif_1"),
      "Id_Certif_2" = COALESCE(EXCLUDED."Id_Certif_2", fournisseur_produit."Id_Certif_2"),
      "Id_Certif_3" = COALESCE(EXCLUDED."Id_Certif_3", fournisseur_produit."Id_Certif_3"),
      "Informations_supplémentaires_fournisseur" = COALESCE(EXCLUDED."Informations_supplémentaires_fournisseur", fournisseur_produit."Informations_supplémentaires_fournisseur"),
      "Commentaires" = COALESCE(EXCLUDED."Commentaires", fournisseur_produit."Commentaires"),
      "Réference_modele_pour_fournisseur" = COALESCE(EXCLUDED."Réference_modele_pour_fournisseur", fournisseur_produit."Réference_modele_pour_fournisseur"),
      "ean_uvc" = COALESCE(EXCLUDED."ean_uvc", fournisseur_produit."ean_uvc"),
      "Inactif" = COALESCE(EXCLUDED."Inactif", fournisseur_produit."Inactif"),
      "code_traitement" = COALESCE(EXCLUDED."code_traitement", fournisseur_produit."code_traitement"),
      "source_file" = COALESCE(EXCLUDED."source_file", fournisseur_produit."source_file")
    """
    )
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
