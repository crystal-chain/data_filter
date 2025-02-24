document.addEventListener('DOMContentLoaded', function() {
    const generateBtn = document.getElementById('generate-btn');
    const statusDiv = document.getElementById('status');
    if (generateBtn) {
        generateBtn.addEventListener('click', function() {
            if (statusDiv) {
                statusDiv.innerText = "Démarrage de la génération, veuillez patienter...";
            }
            fetch('/start_generate', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                }
            })
            .then(response => response.json())
            .then(data => {
                console.log("Response from /start_generate:", data);
                const taskId = data.task_id;
                if (taskId) {
                    pollTaskStatus(taskId);
                } else {
                    console.error("Task ID is undefined");
                    if (statusDiv) {
                        statusDiv.innerText = "Erreur lors du démarrage de la génération.";
                    }
                }
            })
            .catch(error => {
                console.error("Error starting generation:", error);
                if (statusDiv) {
                    statusDiv.innerText = "Erreur lors du démarrage de la génération.";
                }
            });
        });
    }
});

function pollTaskStatus(taskId) {
    const statusDiv = document.getElementById('status');
    fetch('/task_status/' + taskId)
        .then(response => response.json())
        .then(data => {
            console.log("Polling status:", data);
            if (data.state === 'PENDING' || data.state === 'STARTED' || data.state === 'PROGRESS') {
                if (statusDiv) {
                    statusDiv.innerText = `Running... ${data.progress || 0}% - ${data.message || ""}`;
                }
                setTimeout(() => pollTaskStatus(taskId), 2000);
            } else if (data.state === 'SUCCESS') {
                if (statusDiv) {
                    statusDiv.innerText = "Génération terminée. Téléchargement en cours...";
                }
                window.location.href = '/download/' + taskId;
            } else {
                // En cas d'erreur (FAILURE ou autre)
                console.error("Task error:", data);
                if (statusDiv) {
                    statusDiv.innerText = "Erreur lors de la génération des templates.";
                }
            }
        })
        .catch(error => {
            console.error("Error polling task status:", error);
            if (statusDiv) {
                statusDiv.innerText = "Erreur lors de la récupération du statut.";
            }
        });
}
