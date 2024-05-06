from flask import Flask
from .controllers.upload import upload_blueprint

def create_app():
    app = Flask(__name__, template_folder='templates')

    # Enregistrement du blueprint pour les routes liées au téléchargement
    app.register_blueprint(upload_blueprint)

    return app
