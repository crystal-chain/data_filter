import requests
import logging
import os
from dotenv import load_dotenv  # type: ignore
from sqlalchemy import  text  # type: ignore
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from get_engine import get_engine 
import time


def setup_logger():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler("project.log", mode='a'), 
            logging.StreamHandler()
        ]
    )

def login(api_url, email, password):
    headers = {
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    }
    
    data = {"email": email, "password": password}
    
    try:
        response = requests.post(api_url, json=data, headers=headers)
        
        if response.status_code == 200:
            access_token = response.headers.get("access-token")
            if not access_token:
                logging.error("Échec de récupération du token d'accès !")
                return None

            result = {
                "access-token": access_token,
                "client": response.headers.get("client"),
                "uid": response.headers.get("uid"),
                "token-type": response.headers.get("token-type"),
                "expiry": response.headers.get("expiry"),
                "authorization": f"Bearer {access_token}"
            }
            logging.info(f"Connexion réussie ! Token reçu: {access_token}")
            return result
        else:
            logging.error(f"Échec de connexion : {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur de connexion : {str(e)}")
        return None

def get_number_of_elements(api_url, auth_headers):
    try:
        response = requests.post(api_url, headers=auth_headers)
        
        if response.status_code == 200:
            data = response.json()
            number_of_elements = data.get("numberOfElements", None)
            logging.info(f"Nombre d'éléments récupéré: {number_of_elements}")
            return number_of_elements
        else:
            logging.error(f"Échec de récupération des données : {response.status_code}")
            logging.error(f"Réponse complète : {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur lors de l'accès à l'API : {str(e)}")
        return None

def get_all_elements(api_url, auth_headers):
    number_of_elements = get_number_of_elements(api_url, auth_headers)
    if not number_of_elements:
        return None
    
    full_url = api_url.replace("lines_per_page=10", f"lines_per_page={number_of_elements}")
    
    try:
        response = requests.post(full_url, headers=auth_headers)
        
        if response.status_code == 200:
            data = response.json()
            logging.info(f"Données complètes récupérées avec {number_of_elements} éléments.")
            return data
        else:
            logging.error(f"Échec de récupération des données complètes : {response.status_code}")
            logging.error(f"Réponse complète : {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur lors de l'accès à l'API : {str(e)}")
        return None

def update_database_status(reference_produits):
    engine = get_engine()
    with engine.connect() as connection:
        try:
            connection.execute(
                text("""
                    UPDATE fournisseur_produit
                    SET status = CASE
                        WHEN ref_produits IN :ref_list THEN 'PRESENT'
                        ELSE 'ABSENT'
                    END
                """),
                {"ref_list": tuple(reference_produits)}
            )
            connection.commit()
            logging.info("Mise à jour de la base de données effectuée avec succès.")
        except Exception as e:
            logging.error(f"Erreur lors de la mise à jour de la base de données : {str(e)}")
def update_mpx_stats():
    """
    Met à jour la table mpx_stats à partir des données agrégées de la table fournisseur_produit.
    """
    engine = get_engine()

    truncate_query = text("TRUNCATE TABLE mpx_stats;")
    
    insert_query = text("""
        INSERT INTO mpx_stats
        SELECT
            COUNT(*) AS total_products,
            COUNT(DISTINCT nom_fournisseur) AS total_unique_fournisseurs,
            SUM(CASE WHEN status = 'PRESENT' THEN 1 ELSE 0 END) AS total_present,
            SUM(CASE WHEN status = 'ABSENT' THEN 1 ELSE 0 END) AS total_absent,
            ROUND(100.0 * SUM(CASE WHEN status = 'PRESENT' THEN 1 ELSE 0 END) / COUNT(*), 2)::text || '%' AS pourcentage_prime,
            ROUND(100.0 * SUM(CASE WHEN status = 'ABSENT' THEN 1 ELSE 0 END) / COUNT(*), 2)::text || '%' AS pourcentage_absent
        FROM fournisseur_produit;
    """)
    
    try:
        with engine.begin() as conn:
            conn.execute(truncate_query)
            conn.execute(insert_query)
        print("La table mpx_stats a été mise à jour avec succès.")
    except Exception as e:
        print("Erreur lors de la mise à jour de la table mpx_stats :", e)


def update_mpx_report():
    engine = get_engine()
    upsert_query = text("""
    WITH new_data AS (
      SELECT
        nom_fournisseur,
        COUNT(ref_produit) AS nombre_total_produits,
        SUM(CASE WHEN status = 'ABSENT' THEN 1 ELSE 0 END) AS nombre_produits_absent,
        SUM(CASE WHEN status = 'PRESENT' THEN 1 ELSE 0 END) AS nombre_produits_prime
      FROM fournisseur_produit
      GROUP BY nom_fournisseur
    )
    INSERT INTO mpx_report (nom_fournisseur, nombre_total_produits, nombre_produits_absent, nombre_produits_prime)
    SELECT nd.nom_fournisseur,
           nd.nombre_total_produits,
           nd.nombre_produits_absent,
           nd.nombre_produits_prime
    FROM new_data nd
    ON CONFLICT (nom_fournisseur)
    DO UPDATE SET
      nombre_total_produits = EXCLUDED.nombre_total_produits,
      nombre_produits_absent = EXCLUDED.nombre_produits_absent,
      nombre_produits_prime = EXCLUDED.nombre_produits_prime;
    """)

    try:
        with engine.begin() as conn:
            conn.execute(upsert_query)
        print("La table mpx_report a été mise à jour avec succès (sans écraser id et nombre_produit_sollicite).")
    except Exception as e:
        print("Erreur lors de la mise à jour de la table mpx_report :", e)





def run_workflow():
    load_dotenv()
    setup_logger()  
    
    API_URL = os.getenv("API_URL")
    EMAIL = os.getenv("EMAIL")
    PASSWORD = os.getenv("PASSWORD")
    DATA_API_URL = "https://traceability.crystalchain.io/api/v2/commondashboard/agec/Repscontenttable?query=&sorting=%5B%5D&lines_per_page=10&filters=%7B%7D&page_number=1"
    
    login_result = login(API_URL, EMAIL, PASSWORD)
    if not login_result:
        print("Impossible de s'authentifier. Vérifiez les logs.")
        return

    auth_headers = {
        "access-token": login_result["access-token"],
        "client": login_result["client"],
        "content-type": "application/json",
        "expiry": login_result["expiry"],
        "uid": login_result["uid"],
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36"
    }
    
    full_data = get_all_elements(DATA_API_URL, auth_headers)
    reference_produits = [item["ReferenceProduit"] for item in full_data.get("data", [])]
    
    if full_data is not None:
        print(f"Données récupérées avec {len(full_data['data'])} éléments.")
        update_database_status(reference_produits)
        print("Fin de la mise à jour.")
    else:
        print("Impossible de récupérer les données complètes. Vérifiez les logs.")

    print("***********************************")
    print("Mise à jour de la table mpx_stats.")
    time.sleep(10)
    update_mpx_stats()
    print("***********************************")
    print("Mise à jour de la table mpx_report.")
    time.sleep(10)
    update_mpx_report()


    print("Fin du traitement.")

if __name__ == "__main__":
    run_workflow()
