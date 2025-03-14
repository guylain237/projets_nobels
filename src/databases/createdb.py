import sqlite3
import os

def create_db():
    #chemin absolu
    base_dir = os.path.dirname(__file__)

    #chemin relatif vers la base de données et le schema
    db_path = os.path.join(base_dir, 'Nobels.db')
    schema_path = os.path.join(base_dir, 'schema.sql')

    #connexion à la base de données
    connexion = sqlite3.connect(db_path)
    cursor = connexion.cursor()


    # Vérifier que le fichier schema.sql existe
    if not os.path.isfile(schema_path):
        raise FileNotFoundError(f"Le fichier {schema_path} n'existe pas")
        return

    # Lire et exécuter le script SQL
    with open(schema_path, 'r', encoding='utf-8') as f:
        schema_script = f.read()
    cursor.executescript(schema_script)
    
    # Sauvegarder les modifications et fermer la connexion
    connexion.commit()
    connexion.close()
    print(f"Base de données '{db_path}' créée avec succès !")
    
