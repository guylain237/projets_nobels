import json
import requests

from src.utils import get_logger
def extractionfile(url):
    """
    Envoie une requête GET à l'URL spécifiée et retourne le contenu JSON.

    Args:
        url (str): URL de l'API à interroger.

    Returns:
        dict or None: Le résultat en JSON si la requête réussit, sinon None.
    """
    logger = get_logger(__name__)
    headers = {
    'Accept': 'application/json'
              }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status() # Lever une exception en cas d'erreur HTTP
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.info(f"Une erreur s'est produite: {url} : {e}")
        return None