# cli/main.py
from src.utils import get_logger

# Importer la fonction de création de la base de données
from src.databases import create_db

def main():
    # Création de la base de données à partir du script schema.sql
    logger = get_logger(__name__)
    logger.info("Creation de la base de données...")
    create_db()
    logger.info("Base de données créée avec succès !")
if __name__ == '__main__':
    main()
