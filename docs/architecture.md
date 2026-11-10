# Architecture Technique & Conception Détaillée

Ce document présente l'architecture globale, les choix de conception, le modèle relationnel et les mécanismes de résilience mis en œuvre dans le projet.

---

## 1. Flux Général des Données

```mermaid
flowchart TD
    subgraph Sources ["1. Sources de Données"]
        CSV_Local["CSV Locaux (Niveau 1)"]
        S3_Bucket["Stockage S3 MinIO (Niveau 2)"]
        Text_SLM["Texte Brut / Narrative (Niveau 3.3)"]
    end

    subgraph Ingestion ["2. Ingestion & Validation"]
        Loader["data_pipeline.loader\n- Parsing CSV\n- Validation Pydantic\n- Hash SHA-256"]
        SLM_Extractor["data_pipeline.slm_extractor\n- NER Financier\n- Normalisation entités"]
    end

    subgraph Processing ["3. Traitement Pur (data_pipeline.processor)"]
        Flag[Ajout Flag > 5000 €]
        AggIban[Somme par IBAN Origine]
        AggBanque[Somme par Banque Source]
        AggDest[Somme par IBAN Destinataire]
    end

    subgraph Resilience ["4. Résilience & Idempotence (data_pipeline.retry)"]
        CheckIdempotence{"Fichier déjà traité ?\n(Hash SHA-256 dans processed_files)"}
        Skip["Ignorer (Anti-doublon)"]
        RetryLoop["Mécanisme de Retry\n(Backoff exponentiel max 3 essais)"]
    end

    subgraph Storage ["5. Base de Données Relationnelle"]
        DB_Files[Table: processed_files]
        DB_Txs[Table: transactions]
        DB_Aggs[Tables d'Agrégats]
    end

    CSV_Local --> Loader
    S3_Bucket -->|Récupération avec retry| Loader
    Text_SLM --> SLM_Extractor --> Loader

    Loader --> CheckIdempotence
    CheckIdempotence -->|Oui| Skip
    CheckIdempotence -->|Non| Processing

    Processing --> Flag & AggIban & AggBanque & AggDest
    Flag & AggIban & AggBanque & AggDest --> RetryLoop
    RetryLoop --> DB_Files & DB_Txs & DB_Aggs
```

---

## 2. Modèle de Données Relationnel

La base de données relationnelle est conçue pour stocker les transactions individuelles, historiser les fichiers ingérés pour garantir l'idempotence, et maintenir à jour les tables d'agrégation.

```mermaid
erDiagram
    PROCESSED_FILES ||--o{ TRANSACTIONS : "source de"
    
    PROCESSED_FILES {
        int id PK
        string filename
        string file_hash UK "SHA-256 Unique"
        int record_count
        string status "SUCCESS / FAILED"
        datetime processed_at
    }

    TRANSACTIONS {
        int id PK
        datetime datetime_transaction
        string iban_origine FK
        string pays_source
        string banque_source FK
        string iban_destinataire FK
        string pays_destinataire
        float montant
        boolean flag_montant_superieur_5000
        string source_file_hash FK
        datetime created_at
    }

    AGGREGATS_IBAN_ORIGINE {
        string iban_origine PK
        float total_envoye
        int nb_transactions
        datetime last_updated
    }

    AGGREGATS_BANQUE_SOURCE {
        string banque_source PK
        float total_envoye
        int nb_transactions
        datetime last_updated
    }

    AGGREGATS_IBAN_DESTINATAIRE {
        string iban_destinataire PK
        float total_recu
        int nb_transactions
        datetime last_updated
    }
```

---

## 3. Contraintes de Pureté des Fonctions (Niveau 1.2)

Toutes les fonctions du module [`data_pipeline.processor`](file:///data_pipeline/processor.py) répondent rigoureusement aux 3 critères de pureté logicielle :
1. **Immutabilité des entrées** : les structures reçues en argument (`transactions: list`) ne sont jamais mutées. De nouvelles instances de dictionnaires ou de modèles Pydantic `ProcessedTransaction` sont instanciées.
2. **Absence d'état externe** : aucune variable globale, lecture de base de données, appel réseau ou interaction avec le système de fichiers n'a lieu au sein des calculs.
3. **Déterminisme strict** : pour un ensemble de transactions d'entrée identique, la fonction renvoie invariablement le même résultat au centime près.

---

## 4. Garantie d'Anti-Doublons et Idempotence

Le traitement d'un fichier peut échouer à mi-parcours ou être relancé plusieurs fois par un scheduler. Pour éviter l'insertion de doublons :
- Un **hash SHA-256** unique du contenu du fichier est calculé dès le chargement (`compute_content_hash`).
- Avant toute insertion, une requête vérifie l'existence de ce hash dans la table `processed_files`.
- Si le hash est déjà présent avec le statut `SUCCESS`, l'insertion est **immédiatement ignorée** avec le statut `SKIPPED`, et aucune transaction n'est dupliquée.
- L'insertion des transactions, la mise à jour des agrégats et l'enregistrement du hash sont encapsulés dans une **transaction SQL unique** (`BEGIN ... COMMIT`) assurant l'atomicité (`ACID`).

---

## 5. Résilience et Retry avec Exponential Backoff

Le décorateur [`@with_retry`](file:///data_pipeline/retry.py) encapsule :
- Les insertions en base de données relationnelle (`insert_processed_data_with_retry`).
- Les téléchargements depuis le service compatible S3 (`get_s3_file_content_with_retry`).

### Formule de temporisation
$$\text{Délai} = \text{initial\_delay} \times (\text{backoff\_factor})^{\text{tentative} - 1}$$

Par exemple :
- Tentative 1 : Échec -> Attente de `0.5s`
- Tentative 2 : Échec -> Attente de `1.0s`
- Tentative 3 : Échec -> Arrêt propre et levée de l'exception `MaxRetriesExceededError`.
