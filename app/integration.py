# app/integration.py
from flask import Blueprint, render_template, jsonify, request, send_file, url_for
from tasks import generate_templates_task
from celery.result import AsyncResult
import os 

integration_blueprint = Blueprint('integration', __name__)

@integration_blueprint.route('/monoprix', methods=['GET'])
def monoprix():
    """
    Affiche la page Monoprix avec le bouton Generate.
    """
    return render_template('monoprix_result.html')

@integration_blueprint.route('/start_generate', methods=['POST'])
def start_generate():
    """
    Lance la tâche de génération des templates en arrière-plan.
    """
    task = generate_templates_task.delay()
    print("task : " , task)
    return jsonify({'task_id': task.id}), 202

@integration_blueprint.route('/task_status/<task_id>', methods=['GET'])
def task_status(task_id):
    from flask import jsonify
    from celery.result import AsyncResult
    task_result = AsyncResult(task_id)
    # On récupère info et on s'assure qu'il s'agit d'un dict
    info = task_result.info
    meta = info if isinstance(info, dict) else {}
    return jsonify({
        "state": task_result.state,
        "progress": meta.get("progress", 0),
        "message": meta.get("message", "")
    })


@integration_blueprint.route('/download/<task_id>', methods=['GET'])
def download_file(task_id):
    from flask import jsonify, send_file
    task_result = AsyncResult(task_id)
    if task_result.state == 'SUCCESS':
        result = task_result.get()
        print("DEBUG: task_result.get() =", result, type(result))
        # Forçons la conversion en chaîne si ce n'est pas déjà une chaîne
        zip_file_path = str(result) if not isinstance(result, str) else result
        print("DEBUG: zip_file_path =", zip_file_path, type(zip_file_path))
        if not os.path.exists(zip_file_path):
            return jsonify({"error": "File not found", "path": zip_file_path}), 404
        return send_file(zip_file_path, as_attachment=True, download_name="generated_templates.zip")
    else:
        return jsonify({"error": "File not ready", "state": task_result.state}), 404
