# Documentation d'Analyse des Données France Travail

## Introduction

Cette documentation décrit le processus d'analyse des données d'offres d'emploi France Travail qui ont été extraites, transformées et chargées dans la base de données PostgreSQL via notre pipeline ETL. L'analyse vise à identifier les tendances du marché de l'emploi, les types de contrats les plus courants, les zones géographiques avec le plus d'offres, et les technologies les plus demandées.

## Architecture de l'Analyse

Le processus d'analyse est implémenté dans le script `scripts/analysis/analyze_job_data.py` qui :

1. Se connecte à la base de données PostgreSQL en utilisant les variables d'environnement
2. Extrait les données de la table `france_travail_jobs`
3. Effectue plusieurs analyses thématiques
4. Génère des visualisations sous forme de graphiques
5. Crée un tableau de bord HTML interactif

## Analyses Réalisées

## Analyses de Base

### 1. Distribution des Types de Contrat

Cette analyse examine la répartition des différents types de contrats (CDI, CDD, stage, etc.) dans les offres d'emploi.

**Méthodologie :**
- Utilisation de la colonne `contract_type_std` qui contient les types de contrat normalisés
- Calcul des fréquences de chaque type de contrat
- Visualisation sous forme de graphique à barres

**Résultats attendus :**
- Identification des types de contrats les plus proposés
- Comparaison entre contrats stables (CDI) et temporaires (CDD, intérim)
- Proportion des stages et contrats d'alternance

### 2. Distribution Géographique des Offres

Cette analyse examine la répartition géographique des offres d'emploi par ville.

**Méthodologie :**
- Extraction des noms de villes à partir de la colonne `lieu_travail`
- Extraction du nom de la ville à partir du libellé (format "XX - NOM_VILLE")
- Calcul des fréquences pour chaque ville
- Visualisation des 15 villes avec le plus d'offres

**Résultats attendus :**
- Identification des zones géographiques avec la plus forte demande
- Concentration des offres dans les grandes métropoles vs. zones rurales
- Détection de pôles d'emploi régionaux

### 3. Technologies les Plus Demandées

Cette analyse identifie les technologies et compétences techniques les plus recherchées dans les offres d'emploi.

**Méthodologie :**
- Utilisation des colonnes `has_python`, `has_java`, etc. pour les technologies déjà identifiées
- Recherche textuelle dans les colonnes `intitule` et `description_clean` pour d'autres technologies
- Comptage des occurrences de chaque technologie
- Visualisation sous forme de graphique à barres

**Résultats attendus :**
- Identification des langages de programmation les plus demandés
- Tendances dans les technologies cloud et DevOps
- Demande pour les compétences en intelligence artificielle et data science

### 4. Durée des Contrats CDD

Cette analyse examine la durée des contrats à durée déterminée.

**Méthodologie :**
- Filtrage des offres avec `contract_type_std` = "CDD"
- Extraction de la durée du contrat si disponible
- Calcul de statistiques descriptives (moyenne, médiane, min, max)
- Visualisation sous forme d'histogramme

**Résultats attendus :**
- Distribution des durées de CDD
- Identification des durées les plus courantes
- Détection de tendances saisonnières ou sectorielles

## Tableau de Bord

Toutes les visualisations sont regroupées dans un tableau de bord HTML interactif accessible à `data/analysis/visualizations/dashboard.html`. Ce tableau de bord permet de :

- Visualiser l'ensemble des analyses en un coup d'œil
- Comparer les différentes métriques
- Partager facilement les résultats avec les parties prenantes

## Interprétation des Résultats

## Analyses de Base

### Types de Contrat
Les résultats permettent d'évaluer la stabilité du marché de l'emploi. Une forte proportion de CDI indique un marché stable, tandis qu'une prédominance de contrats temporaires peut suggérer une précarité ou une flexibilité accrue.

### Distribution Géographique
La concentration des offres dans certaines villes peut révéler des disparités territoriales et orienter les politiques d'emploi. Elle peut également guider les chercheurs d'emploi vers les zones à fort potentiel.

### Technologies
L'analyse des technologies permet d'identifier les compétences les plus valorisées sur le marché et d'orienter les formations professionnelles. Elle révèle également les tendances technologiques émergentes.

### Durée des CDD
La durée des CDD peut indiquer la nature des besoins des entreprises (remplacement, projet spécifique, etc.) et la précarité potentielle de certains secteurs.

## Analyses Avancées

### Salaires
L'analyse des salaires révèle les niveaux de rémunération du marché et les facteurs qui les influencent. Elle permet d'identifier les technologies associées à une meilleure valorisation financière et d'orienter les choix de formation ou de spécialisation.

### Corrélation Technologies/Contrats
Cette analyse permet de comprendre comment les entreprises structurent leurs recrutements selon les compétences recherchées. Certaines technologies peuvent être plus souvent associées à des contrats stables, d'autres à des missions temporaires ou des stages.

### Analyse Géographique Avancée
La comparaison entre régions et départements met en lumière les spécificités des marchés de l'emploi locaux. Elle peut révéler des pôles de spécialisation technologique et des disparités dans les types de contrats proposés selon les territoires.

### Évolution Temporelle
L'analyse temporelle permet de détecter les tendances émergentes et les cycles saisonniers du marché de l'emploi. Elle offre une vision dynamique de l'évolution des besoins en compétences et peut aider à anticiper les futures demandes.

## Utilisation du Script d'Analyse

Pour exécuter l'analyse complète et générer le tableau de bord :

```bash
python scripts/analysis/analyze_job_data.py
```

Le script nécessite :
- Un accès à la base de données PostgreSQL configuré via le fichier `.env`
- Les bibliothèques Python : pandas, matplotlib, seaborn, sqlalchemy

## Analyses Avancées

### 5. Analyse des Salaires

Cette analyse examine la distribution des salaires et leur relation avec les technologies demandées.

**Méthodologie :**
- Utilisation des colonnes `min_salary`, `max_salary`, `salary_periodicity` et `currency`
- Calcul des statistiques descriptives (min, max, moyenne, médiane)
- Analyse des écarts salariaux entre différentes technologies
- Visualisation sous forme de boîtes à moustaches et graphiques à barres

**Résultats attendus :**
- Distribution des fourchettes salariales
- Identification des technologies associées à des salaires plus élevés
- Analyse des écarts de rémunération entre différents profils

### 6. Corrélation entre Technologies et Types de Contrat

Cette analyse examine les relations entre les technologies demandées et les types de contrat proposés.

**Méthodologie :**
- Croisement des données de technologies (`has_python`, `has_java`, etc.) avec les types de contrat
- Calcul des proportions de chaque technologie par type de contrat
- Visualisation sous forme de heatmap et graphiques à barres empilées

**Résultats attendus :**
- Identification des technologies plus fréquemment demandées pour certains types de contrat
- Détection des tendances de recrutement par type de contrat
- Analyse des spécificités technologiques selon la durée d'engagement

### 7. Analyse Géographique Avancée

Cette analyse approfondit la distribution géographique en examinant les données au niveau des régions et départements.

**Méthodologie :**
- Extraction des codes départements à partir des codes INSEE des villes
- Mapping des départements aux régions administratives
- Analyse croisée des technologies et types de contrat par région
- Visualisation sous forme de graphiques à barres et heatmaps

**Résultats attendus :**
- Cartographie des offres par région et département
- Identification des spécificités régionales en termes de technologies et types de contrat
- Repérage des pôles d'excellence technologique par région

### 8. Évolution Temporelle des Offres

Cette analyse examine l'évolution des offres d'emploi dans le temps.

**Méthodologie :**
- Utilisation des colonnes `date_creation` et `date_actualisation`
- Agrégation des offres par jour, mois et trimestre
- Analyse de l'évolution des types de contrat et technologies au fil du temps
- Visualisation sous forme de séries temporelles

**Résultats attendus :**
- Détection des tendances saisonnières dans les offres d'emploi
- Observation de l'évolution de la demande pour certaines technologies
- Anticipation des évolutions du marché de l'emploi

## Extensions Possibles

L'analyse pourrait être encore enrichie par :

1. **Analyse sectorielle** : Répartition par secteur d'activité
2. **Analyse des compétences non techniques** : Soft skills les plus demandées
3. **Modèles prédictifs** : Prévision des tendances futures du marché de l'emploi
4. **Analyse des exigences de formation** : Niveaux d'études et certifications demandés
5. **Analyse de sentiment** : Évaluation du ton et du langage utilisé dans les descriptions d'offres

## Conclusion

Cette analyse fournit une vision globale du marché de l'emploi à partir des données France Travail. Elle permet d'identifier les tendances actuelles et d'orienter les décisions stratégiques en matière de recherche d'emploi, de formation professionnelle et de politiques publiques.
