# Modèle de Réseau Fret Rwanda — AFD

Modélisation du transport de fret routier au Rwanda : construction du réseau routier, calcul des coûts de transport, modèle gravitaire doublement contraint, analyse de vulnérabilité et modèle économique.

---

## Lancer le code pour la première fois

### 1. Cloner le dépôt

```bash
git clone https://github.com/Yanis2001/Fret_Rwanda_AFD.git
cd Fret_Rwanda_AFD
```

### 2. Ouvrir le projet dans RStudio

Double-cliquez sur le fichier `.Rproj` à la racine du dépôt, ou faites **File > Open Project**.

### 3. Installer les dépendances R

Les packages nécessaires sont installés automatiquement au premier lancement. Rien à faire manuellement.

### 4. Configurer votre token GitHub *(optionnel — uniquement si vous souhaitez pousser des modifications)*

> Si vous voulez seulement **faire tourner le code et voir les résultats**, passez directement à l'étape 5.

Le token est nécessaire uniquement pour synchroniser vos modifications avec GitHub. Il se configure **une seule fois** et persiste ensuite dans toutes vos sessions R futures :

```r
source("setup.R")
```

Le script vous guidera : il vous demande votre token, l'enregistre dans `~/.Renviron` et le vérifie immédiatement. Pour créer un token : https://github.com/settings/tokens/new (cochez uniquement `repo`).

**Sur Onyxia (SSP Cloud)** : vous pouvez aussi stocker `GITHUB_PAT` dans le gestionnaire de secrets d'Onyxia (*Mon compte > Secrets*) et le lier à votre service RStudio — il sera alors injecté automatiquement sans avoir à relancer `setup.R`.

### 5. Lancer le modèle

Ouvrez `run_all.R` et sourcez-le, ou configurez les modules à exécuter en tête de fichier :

```r
RUN_PARAMETRES   <- TRUE   # 00 — configuration (toujours TRUE)
RUN_RESEAU       <- TRUE   # 01 — construction du réseau (~30 min)
RUN_COUTS        <- TRUE   # 02 — calcul des coûts (~5 min)
RUN_TRANSPORT    <- TRUE   # 03 — matrice OD + gravitaire (~1h)
RUN_VULNERAB     <- FALSE  # 04 — analyse de criticité (~2h)
RUN_ARIO         <- FALSE  # 05 — modèle ARIO (~1 min)
```

Puis :

```r
source("run_all.R")
```

Les résultats (cartes, CSV, Parquet, GeoPackage) sont écrits dans le dossier `outputs/`.

---

## Structure du projet

| Fichier | Rôle |
|---|---|
| `run_all.R` | Point d'entrée — orchestre tous les modules |
| `setup.R` | Configuration unique du token GitHub |
| `00_parametres.R` | Paramètres globaux du modèle (packages, DuckDB, palettes) |
| `01_reseau.R` | Construction du réseau routier depuis OSM + pentes SRTM |
| `02_couts.R` | Calcul des coûts de transport par véhicule |
| `03_transport.R` | Matrice OD multi-modale + modèle gravitaire |
| `04_vulnerabilite.R` | Analyse de criticité et scénarios de perturbation |
| `05_ario.R` | Modèle économique ARIO (pertes indirectes) |
| `viz_*.R` | Scripts de visualisation (cartes, Sankey, graphiques) |
| `outputs/` | Résultats générés (non versionnés) |

---

## Données requises

Le code télécharge automatiquement :
- Le réseau routier Rwanda depuis [Geofabrik](https://download.geofabrik.de/africa/rwanda.html) (fichier `.pbf`)
- Le modèle d'élévation SRTM via `elevatr`
- Les données de population WorldPop
- Les frontières administratives GADM

Fichiers à placer manuellement dans `data/raw/` (non distribués pour raisons de licence) :
- `rwa_admpop_adm2_2023.csv` — population par district (NISR)
- `rwa_emploi_district_secteur_2022.csv` — emploi sectoriel RPHC5 2022

---

## Dépendances logicielles

- R ≥ 4.3
- Les packages R sont installés automatiquement au premier `source("run_all.R")`
