# Pipeline de Données & Intégration Continue (CI/CD)

Projet complet de mise en situation d'intégration continue et de pipeline de données financières, de la génération des données CSV jusqu'à leur insertion dans une base de données relationnelle, avec stockage S3, extraction de transactions par SLM, orchestration Docker et déploiement continu.

---

## 📌 Fonctionnalités Implémentées

### 🔹 Niveau 1 — Pipeline Data locale
- **1.1 Génération de données CSV** : Génération déterministe de fichiers contenant 5 à 10 transactions respectant le schéma bancaire (`datetime_transaction`, `iban_origine`, `pays_source`, `banque_source`, `iban_destinataire`, `pays_destinataire`, `montant`).
- **1.2 Fonctions pures & séparation des responsabilités** :
  - `calculer_somme_par_iban_origine`
  - `calculer_somme_par_banque_source`
  - `calculer_somme_par_iban_destinataire`
  - `ajouter_flag_montant_superieur_5000` (flag appliqué si `montant > 5000`)
  - **Garantie de pureté** : Fonctions sans effets de bord, immutabilité des arguments et déterminisme strict.
- **1.3 Base de données relationnelle** : Modèle relationnel SQLAlchemy (SQLite par défaut, compatible PostgreSQL) stockant les transactions individuelles, les agrégats financiers et l'historique des fichiers traités.
- **1.4 Pipeline complète automatisée** : CLI unifié pour exécuter automatiquement l'ensemble du cycle de données.
- **1.5 Tests d'intégration automatiques** : Suite de tests Pytest (30 tests) validant l'insertion, les calculs, les flags et l'idempotence.
- **1.6 Simulation d'un échec de pipeline** : Scénario documenté et testé d'erreur sur fichier corrompu, diagnostic, correction et réussite (voir [`docs/simulation_echec.md`](file:///docs/simulation_echec.md)).
- **1.7 Résilience & Anti-doublons** :
  - Mécanisme de **Retry automatique avec backoff exponentiel** en cas d'erreur de base de données.
  - Détection des doublons par **empreinte SHA-256** pour garantir l'idempotence stricte (aucun fichier retraité deux fois).

### 🔹 Niveau 2 — Stockage S3
- **2.1 Service compatible S3 local** : Support complet de MinIO / Garage / SeaweedFS via `boto3`.
- **2.2 Upload automatique dans le S3** : Création du bucket `finance-transactions` et téléversement automatique des CSV générés.
- **2.3 Lecture des données depuis S3** : Flux `S3 -> Ingestion -> Traitement Pur -> Base de Données` avec préservation de l'idempotence.
- **2.4 Retry lors de la récupération S3** : Gestion des indisponibilités réseau temporaires et des timeouts avec retry exponentiel.

### 🔹 Niveau 3 — Améliorations de la Pipeline
- **Option 1 — Pipeline CD & Déploiement Conteneurisé** :
  - Orchestration multi-conteneurs avec [`docker-compose.yml`](file:///docker-compose.yml) (MinIO S3 + Base de données PostgreSQL 15 + Worker CD).
  - Scheduler périodique [`data_pipeline/scheduler.py`](file:///data_pipeline/scheduler.py) pour la détection et l'ingestion automatique en continu des nouveaux fichiers.
- **Option 3 — Extraction de transactions depuis du texte brut avec un SLM** :
  - Module d'extraction d'entités financières nommées (NER) [`data_pipeline/slm_extractor.py`](file:///data_pipeline/slm_extractor.py).
  - Transforme des déclarations narratives en langage naturel en transactions normalisées rejoignant le même flux de données.

---

## 🚀 Installation Rapide

### Prérequis
- Python 3.11+
- Git
- Docker & Docker Compose (optionnel, pour l'exécution conteneurisée)

### 1. Cloner et installer les dépendances
```bash
# Cloner le dépôt
git clone <url_du_depot>
cd "integration et deploiement continu"

# Créer l'environnement virtuel
python -m venv .venv

# Activer l'environnement
# Sur Windows :
.\.venv\Scripts\activate
# Sur Linux / macOS :
source .venv/bin/activate

# Installer les dépendances
pip install -r requirements.txt
```

---

## 🛠️ Guide d'Exécution

### 1. Exécution de la Pipeline Locale (Niveau 1)
```bash
# Générer les CSV et exécuter la pipeline complète
python -m data_pipeline.pipeline --generate

# Tester l'anti-doublon (idempotence) en relançant la commande :
# Tous les fichiers déjà traités sont immédiatement ignorés sans duplication !
python -m data_pipeline.pipeline
```

### 2. Tester le Retry Automatique (Niveau 1.7)
```bash
# Simuler 2 pannes temporaires de base de données avant succès automatique :
python -m data_pipeline.pipeline --simulate-transient-db-failure 2
```

### 3. Exécution avec le Stockage S3 (Niveau 2)
```bash
# Lancer la pipeline S3 (connexion à MinIO local ou configuré dans .env)
python -c "from data_pipeline.s3_service import run_s3_pipeline; run_s3_pipeline()"
```

### 4. Extraction de Transactions depuis du Texte Brut avec SLM (Niveau 3.3)
```bash
# Ingestion et extraction d'une phrase narrative vers la base de données :
python -c "
from data_pipeline.slm_extractor import process_unstructured_text_into_pipeline;
text = 'Le 12 septembre 2026 à 14h30, le compte FR7630006000011234567890189 chez BNP Paribas a envoyé 6200 € vers le compte DE89370400440532013000.';
res = process_unstructured_text_into_pipeline(text, source_name='demo_slm');
print('Résultat extraction SLM :', res);
"
```

### 5. Déploiement Continu avec Docker Compose (Niveau 3.1)
```bash
# Démarrer l'ensemble des services en arrière-plan :
# - MinIO (API port 9000, Console web port 9001)
# - PostgreSQL (Port 5432)
# - Worker CD avec Scheduler
docker-compose up -d

# Visualiser les logs du worker de traitement en continu :
docker-compose logs -f data-pipeline-worker
```

---

## 🧪 Tests d'Intégration & Qualité Automatisés

La suite de tests automatisée couvre **100%** des exigences du sujet :

```bash
# Lancer l'ensemble des 30 tests unitaires et d'intégration
pytest -v
```

### Répartition des tests :
| Fichier de Test | Exigences Testées | Statut |
| :--- | :--- | :---: |
| [`test_generator.py`](file:///tests/test_generator.py) | Batches 5-10 lignes, colonnes requises, montants > et <= 5000 | ✅ PASS |
| [`test_loader.py`](file:///tests/test_loader.py) | Validation Pydantic, détection d'erreurs, hash SHA-256 | ✅ PASS |
| [`test_processor.py`](file:///tests/test_processor.py) | Pureté stricte, immutabilité des entrées, sommes exactes, flags | ✅ PASS |
| [`test_database.py`](file:///tests/test_database.py) | Schéma relationnel, atomicité, anti-doublon, retry sur panne | ✅ PASS |
| [`test_pipeline_l1.py`](file:///tests/test_pipeline_l1.py) | Intégration complète Niveau 1 end-to-end | ✅ PASS |
| [`test_s3_l2.py`](file:///tests/test_s3_l2.py) | Intégration S3 Niveau 2, upload, download avec retry | ✅ PASS |
| [`test_slm_l3.py`](file:///tests/test_slm_l3.py) | Extraction texte brut SLM Niveau 3 Option 3 | ✅ PASS |
| [`test_failure_simulation.py`](file:///tests/test_failure_simulation.py) | Simulation d'échec Niveau 1.6, diagnostic, fix et succès | ✅ PASS |

---

## 🔄 Intégration Continue (GitHub Actions)

Le workflow GitHub Actions est configuré dans [`.github/workflows/ci.yml`](file:///.github/workflows/ci.yml) :
1. **Linting & Compilation** : Contrôle de syntaxe et typage de tous les modules.
2. **Pytest** : Exécution automatique des 30 tests unitaires et d'intégration.
3. **Pipeline Niveau 1 Locale** : Génération, exécution, et validation de l'idempotence.
4. **Simulation d'Échec** : Test automatique validant qu'un fichier invalide fait échouer la pipeline, puis que sa correction la fait réussir.
5. **Pipeline Niveau 2 S3** : Démarrage d'un conteneur MinIO S3 de service et validation du flux S3 réel.
6. **Pipeline Niveau 3 SLM** : Test de l'extraction de transactions depuis du texte non structuré.
7. **Build Image Docker** : Validation de la construction de l'image de conteneur de production.

---

## 📚 Documentation Complémentaire

- 📄 [`docs/architecture.md`](file:///docs/architecture.md) : Diagrammes Mermaid des flux de données, modèle relationnel ERD, et principes de conception.
- 📄 [`docs/simulation_echec.md`](file:///docs/simulation_echec.md) : Rapport détaillé de la simulation d'échec (Niveau 1.6) avec logs d'erreur, analyse de la cause racine et correction.
