# app/__init__.py
from flask import Flask
from .controllers.upload import upload_blueprint
from app.integration import integration_blueprint

def create_app():
    app = Flask(__name__, template_folder='templates', static_folder='static')
    # Enregistrement de vos endpoints existants
    app.register_blueprint(upload_blueprint)
    # Enregistrement du blueprint d’intégration (nouvel endpoint /monoprix)
    app.register_blueprint(integration_blueprint)
    return app
