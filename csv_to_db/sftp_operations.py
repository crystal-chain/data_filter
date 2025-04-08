# sftp_operations.py

import os
import paramiko
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def connect_to_sftp():
    """
    Se connecte au serveur SFTP en utilisant les variables d'environnement.
    """
    server = os.getenv("SFTP_SERVER")
    username = os.getenv("SFTP_USERNAME")
    private_key_path = os.getenv("SFTP_PRIVATE_KEY_PATH")
    remote_path = os.getenv("SFTP_REMOTE_PATH")

    private_key = paramiko.Ed25519Key.from_private_key_file(private_key_path)
    transport = paramiko.Transport((server, 22))
    transport.connect(username=username, pkey=private_key)
    sftp = paramiko.SFTPClient.from_transport(transport)
    return sftp, remote_path

def list_today_files(sftp, remote_path):
    """
    Liste uniquement les fichiers reçus aujourd'hui sur le SFTP.
    """
    today = datetime.today().strftime("%Y-%m-%d")
    files = sftp.listdir_attr(remote_path)

    today_files = [
        f.filename for f in files
        if datetime.fromtimestamp(f.st_mtime).strftime("%Y-%m-%d") == today
    ]
    return today_files

def move_file_on_sftp(sftp, source_path, destination_path):
    """
    Déplace un fichier du chemin source vers le chemin destination sur le serveur SFTP.
    """
    try:
        sftp.rename(source_path, destination_path)
        print(f"Fichier déplacé de {source_path} à {destination_path}.")
    except Exception as e:
        print(f"Erreur lors du déplacement du fichier : {e}")
        raise
