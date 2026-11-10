# Rapport de Simulation d'Échec et de Récupération de Pipeline (Niveau 1.6)

Ce document présente en détail le scénario d'échec de la pipeline CI/CD, le diagnostic de l'erreur, la correction appliquée, et la validation du succès après correction.

---

## 1. Contexte et Cause du Problème

Dans une pipeline de données de production, des erreurs surviennent fréquemment lors de la réception de fichiers externes corrompus (fichiers tronqués, types non respectés, montants négatifs ou champs manquants).

### Cause identifiée
Un fichier `transactions_corrupted.csv` a été introduit dans le répertoire d'ingestion `data/raw/`. 

Ce fichier contient des violations strictes du contrat d'interface défini par le schéma `RawTransaction` :
1. **Ligne 2** : Montant négatif (`-999.00 €`) alors qu'une transaction financière valide doit avoir un montant strictement positif (`montant > 0`).
2. **Ligne 3** : Montant textuel non convertible (`"NOT_A_NUMBER"`).

```csv
datetime_transaction,iban_origine,pays_source,banque_source,iban_destinataire,pays_destinataire,montant
2026-11-04T10:00:00,FR7630006000011234567890189,FR,BNP_PARIBAS,DE89370400440532013000,DE,-999.00
2026-11-04T11:00:00,FR7630006000011234567890189,FR,BNP_PARIBAS,ES9121000418450200051332,ES,NOT_A_NUMBER
```

---

## 2. Déclenchement et Échec de la Pipeline

### Commande exécutée
```bash
python -m data_pipeline.pipeline --data-dir data/raw_corrupted --db-url sqlite:///data/finance.db
```

### Trace d'Exécution et Message d'Erreur Obtenu
La validation stricte du module `data_pipeline/loader.py` intercepte l'anomalie dès la lecture du flux, empêchant toute corruption de la base de données :

```text
============================================================
 DÉMARRAGE DE LA PIPELINE DATA LOCALE (Niveau 1)
============================================================
[ÉTAPE 1/4] 1 fichier(s) CSV existant(s) trouvé(s).
[ÉTAPE 2/4] Initialisation de la base de données : sqlite:///data/finance.db
Base de données initialisée avec succès (sqlite:///data/finance.db)
[ÉTAPE 3 & 4/4] Chargement, traitement et insertion des transactions...
--- Traitement du fichier : transactions_corrupted.csv ---
11:53:41 [ERROR] data_pipeline.pipeline - [ERREUR SUR transactions_corrupted.csv] TransactionValidationError : 
Erreur de validation à la ligne 2 de 'transactions_corrupted.csv': 1 validation error for RawTransaction
montant
  Input should be greater than 0 [type=greater_than, input_value=-999.0, input_type=float]
    For further information visit https://errors.pydantic.dev/2.12/v/greater_than

============================================================
 RÉSUMÉ DE L'EXÉCUTION DE LA PIPELINE
 Fichiers traités avec succès : 0
 Fichiers ignorés (doublons)   : 0
 Fichiers en échec             : 1
 Transactions insérées en DB   : 0
============================================================

[PIPELINE ÉCHOUÉE] 1 fichier(s) ont échoué lors de l'exécution de la pipeline.
Process exited with code 1
```

### Impact en Intégration Continue (CI)
Le processus se termine avec un **code de sortie `1`** (`sys.exit(1)`).
Dans la pipeline GitHub Actions, cette étape passe immédiatement au statut **`FAILED` (rouge)**, bloquant tout déploiement ou mise en production d'un code ou de données erronées.

---

## 3. Correction Apportée

La correction consiste à :
1. Identifier la source de la corruption (script de génération amont ou fichier entrant).
2. Valider les données financières avant écriture ou appliquer un filtre de nettoyage.
3. Remplacer les données corrompues par un batch conforme aux contraintes de schéma (`montant > 0`, types stricts, IBANs valides).

### Fichier corrigé
```bash
python -m data_pipeline.generator --output-dir data/raw_corrupted
```

Le fichier `transactions_corrupted.csv` est remplacé par un ensemble de transactions valides conformes aux spécifications bancaires.

---

## 4. Réussite de la Pipeline Après Correction

### Commande ré-exécutée
```bash
python -m data_pipeline.pipeline --data-dir data/raw_corrupted --db-url sqlite:///data/finance.db
```

### Résultat obtenu
```text
============================================================
 DÉMARRAGE DE LA PIPELINE DATA LOCALE (Niveau 1)
============================================================
[ÉTAPE 1/4] 3 fichier(s) CSV existant(s) trouvé(s).
[ÉTAPE 2/4] Initialisation de la base de données : sqlite:///data/finance.db
Base de données initialisée avec succès (sqlite:///data/finance.db)
[ÉTAPE 3 & 4/4] Chargement, traitement et insertion des transactions...
--- Traitement du fichier : transactions_batch_1.csv ---
  Chargé: 7 transactions (SHA256: 0669ca539e...)
  Traitement pur terminé : 3 transactions avec montant > 5000.0 €
[DB INSERT OK] Fichier 'transactions_batch_1.csv' : 7 transactions insérées.
--- Traitement du fichier : transactions_batch_2.csv ---
  Chargé: 6 transactions (SHA256: bad8d9cf62...)
  Traitement pur terminé : 2 transactions avec montant > 5000.0 €
[DB INSERT OK] Fichier 'transactions_batch_2.csv' : 6 transactions insérées.
--- Traitement du fichier : transactions_batch_3.csv ---
  Chargé: 8 transactions (SHA256: 5cdf27d23c...)
  Traitement pur terminé : 3 transactions avec montant > 5000.0 €
[DB INSERT OK] Fichier 'transactions_batch_3.csv' : 8 transactions insérées.
============================================================
 RÉSUMÉ DE L'EXÉCUTION DE LA PIPELINE
 Fichiers traités avec succès : 3
 Fichiers ignorés (doublons)   : 0
 Fichiers en échec             : 0
 Transactions insérées en DB   : 21
============================================================
Process exited with code 0
```

### Validation CI
Le job CI repasse au statut **`PASSED` (vert)**. Ce cycle complet d'échec / correction / succès est testé automatiquement dans le fichier [`tests/test_failure_simulation.py`](file:///tests/test_failure_simulation.py) et dans le workflow [`.github/workflows/ci.yml`](file:///.github/workflows/ci.yml).
