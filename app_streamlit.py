import streamlit as st #type: ignore 
import requests
import time

st.title("Génération des Templates Monoprix")
if st.button("Generate"):
    response = requests.post("http://localhost:5000/start_generate", json={})
    if response.status_code == 202:
        task_id = response.json()["task_id"]
        st.write("Tâche lancée, ID:", task_id)
        status_placeholder = st.empty()
        while True:
            status_response = requests.get(f"http://localhost:5000/task_status/{task_id}")
            status_data = status_response.json()
            state = status_data.get("state")
            progress = status_data.get("progress", 0)
            message = status_data.get("message", "")
            status_placeholder.write(f"Running... État: {state} - {progress}% - {message}")
            if state == "SUCCESS":
                download_url = status_data.get("download_url")
                st.success("Génération terminée!")
                st.markdown(f"[Télécharger le ZIP]({download_url})")
                break
            elif state in ("FAILURE", "REVOKED"):
                st.error("Erreur lors de la génération: " + message)
                break
            time.sleep(2)
