# cli/main.py
# Importer la fonction de création de la base de données
import json
from src.databases import create_db

# Importer la fonction d'extraction des données
from src.etl import extractionfile
from src.utils import NOBEL_API_URL, get_logger


def main():
    # Création de la base de données à partir du script schema.sql
    logger = get_logger(__name__)
    logger.info("Creation de la base de données...")
    create_db()
    logger.info("Base de données créée avec succès !")
  
    # Construction des endpoints à partir de NOBEL_API_URL défini dans la configuration
    # (Adaptez ces chemins en fonction de la documentation réelle de l'API Nobel)
    endpoints = {
        "nobelPrizes": f"{NOBEL_API_URL}/nobelPrizes",
        "laureates": f"{NOBEL_API_URL}/laureates"
    }
    
    for name, url in endpoints.items():
        logger.info(f"Récupération des données pour l'endpoint '{name}'...")
        data = extractionfile(url)
        if data:
            # Affichage du JSON de manière lisible
            print(f"\nDonnées récupérées pour '{name}':")
            print(json.dumps(data, indent=4, ensure_ascii=False))
        else:
            logger.error(f"Impossible de récupérer les données pour l'endpoint '{name}'.")


if __name__ == '__main__':
    main()
