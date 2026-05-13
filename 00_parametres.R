################################################################################
# 00_parametres.R
# RÔLE : Point d'entrée unique de la configuration.
#        Chargé en première ligne de TOUS les autres scripts via source().
#        Ne produit aucun fichier — configure uniquement l'environnement.
# USAGE : source("00_parametres.R")
################################################################################

################################################################################
# PROJET : Réseau Routier pour Modélisation du Commerce de Fret - Rwanda
# OBJECTIF : Construire un graphe routier pondéré par les coûts de transport
#            généralisés, puis modéliser les flux de fret entre zones via un
#            modèle gravitaire calibré sur une table Input-Output fictive.
# AUTEUR  : Yanis
#
# ── RÔLE DE DUCKDB ────────────────────────────────────────────────────────────
#  DuckDB est une base analytique embarquée (pas de serveur).
#  Dans ce projet, il remplace les boucles et mutate() R pour :
#    • le nettoyage attributaire des routes (CASE WHEN SQL)
#    • le calcul des coûts généralisés (CTEs chaînées)
#    • le stockage de la matrice OD en format long
#    • le modèle gravitaire (CROSS JOIN sur offres × demandes × frictions)
#    • les exports Parquet/CSV (COPY TO, plus rapide que write.csv)
#  Les opérations spatiales (géométries, Dijkstra) restent dans sf/igraph
#  car DuckDB spatial n'est pas encore intégré avec sfnetworks.
#
# ── POUR RETROUVER LE DÉPÔT GITHUB ────────────────────────────────────────────
#  system("git clone https://github.com/Yanis2001/Fret_Rwanda_AFD.git")
################################################################################

# ==============================================================================
# CONNEXION GIT
# ==============================================================================

# Authentification Git via le Personal Access Token stocké en variable d'env.
# Sys.getenv() lit la variable d'environnement GITHUB_PAT sans l'exposer
# dans le code source (bonne pratique de sécurité).
token <- Sys.getenv("GITHUB_PAT")
# Configurer le helper de credentials : plus besoin de mettre le mot de passe et nom d'utilisateur avant de pusher sur Git
system("git config --global credential.helper '!f() { echo \"username=token\"; echo \"password=$GITHUB_PAT\"; }; f'")

# S'assurer que le remote 'origin' pointe vers mon dépôt perso
system("git remote set-url origin https://github.com/Yanis2001/Fret_Rwanda_AFD.git")
# Pusher le script sur deux Git
system("git remote set-url --add --push origin https://github.com/Yanis2001/Fret_Rwanda_AFD.git")
system("git remote set-url --add --push origin https://github.com/GEMMES-AFD/Transport.git")
# Vérifier la configuration
system("git remote -v")

################################################################################
# PARTIE I — INITIALISATION ET CONFIGURATION
# Met en place l'environnement complet avant tout traitement :
# packages, base DuckDB, palettes graphiques et paramètres de la flotte.
# Modifier cette partie impacte potentiellement l'ensemble du script.
################################################################################

# ==============================================================================
# I.1 : Packages et options
# Installe et charge les packages nécessaires. Augmente le timeout pour
# les téléchargements de gros fichiers (DEM, PBF).
# ==============================================================================

packages_requis <- c(
  "sf",            # Manipulation de données géospatiales vectorielles (points, lignes, polygones)
  "osmdata",       # Extraction de données OpenStreetMap via l'API Overpass
  "elevatr",       # Téléchargement de données d'élévation SRTM depuis AWS
  "terra",         # Manipulation de rasters (données d'élévation pixel par pixel)
  "sfnetworks",    # Création et analyse de réseaux spatiaux (graphes géoréférencés)
  "tidyverse",     # Suite de packages data science (dplyr, ggplot2, tidyr, stringr…)
  "igraph",        # Analyse de graphes et algorithmes de plus court chemin (Dijkstra)
  "tmap",          # Cartographie thématique (équivalent ggplot2 pour les cartes)
  "units",         # Gestion rigoureuse des unités de mesure (mètres, km, etc.)
  "lwgeom",        # Opérations géométriques avancées (compléments de sf)
  "tidygraph",     # Interface tidyverse pour manipuler des graphes igraph
  "geodata",       # Téléchargement de frontières administratives GADM
  "rnaturalearth", # Données géographiques Natural Earth (pays, côtes…)
  "aws.s3",        # Accès aux objets stockés sur SSP Cloud (MinIO compatible S3)
  "duckdb",        # Base analytique embarquée — moteur SQL sans serveur
  "DBI",           # Interface R standard pour les bases de données (pilote DuckDB)
  "scales",        # Mise à l'échelle et formatage pour ggplot2 (rescale, percent…)
  "progress",      # Barre de progression
  "exactextractr"  # Agrégation précise de rasters sur des polygones
)

# Cette fonction vérifie quels packages de la liste ne sont pas encore installés
# sur la machine, puis les installe automatiquement.
# Sans cette vérification, R tenterait de réinstaller tous les packages à chaque
# exécution, ce qui prendrait plusieurs minutes inutilement.
# "dependencies = TRUE" signifie : installe aussi les packages dont ces packages
# ont eux-mêmes besoin pour fonctionner.
installer_si_necessaire <- function(packages) {
  # installed.packages()[,"Package"] retourne le vecteur des packages installés
  nouveaux <- packages[!(packages %in% installed.packages()[, "Package"])]
  if (length(nouveaux)) install.packages(nouveaux, dependencies = TRUE)
}

installer_si_necessaire(packages_requis)

# invisible() évite d'afficher un message de confirmation dans la console pour
# chaque package chargé. lapply() est une boucle compacte qui applique la
# fonction library() à chaque élément de la liste packages_requis.
invisible(lapply(packages_requis, library, character.only = TRUE))

# options(timeout = 600) : donne 600 secondes (10 minutes) au lieu des 60 secondes
# par défaut avant d'abandonner un téléchargement. Utile pour les gros fichiers
# géographiques comme le DEM (modèle d'élévation) ou le PBF (données OSM Rwanda).
options(timeout = 600)

# set.seed(123) : fixe le "germe" du générateur de nombres aléatoires.
# R utilise des nombres pseudo-aléatoires : en fixant le germe, on garantit
# que les mêmes "nombres aléatoires" seront générés à chaque exécution,
# ce qui rend les résultats reproductibles.
set.seed(123)

cat("✓ Tous les packages sont chargés\n\n")

################################################################################
# PARAMÈTRES GLOBAUX DU MODÈLE
# Tous les paramètres hard-codés du script sont centralisés ici.
# Modifier ce bloc unique suffit à reconfigurer l'ensemble du modèle.
################################################################################

# ==============================================================================
# Chemins et fichiers
# ==============================================================================

DB_PATH        <- "reseau_rwanda.duckdb"   # Fichier DuckDB persistant
DIR_OUTPUT     <- "outputs"                # Dossier de sortie de tous les fichiers
chemin_pbf     <- "rwanda-260315.osm.pbf"  # Nom local du fichier PBF après téléchargement

# Chemins MinIO (SSP Cloud)
MINIO_BUCKET   <- "yanisdumas"
MINIO_PBF_PATH <- "data/raw/rwanda-260315.osm.pbf"
MINIO_BASE_URL <- "minio.lab.sspcloud.fr"

# Chemin local du raster WorldPop si déjà téléchargé 
WORLDPOP_LOCAL_PATH <- file.path(DIR_OUTPUT, "worldpop_rwanda_100m.tif")

# Chemin du fichier NISR
NISR_CSV_PATH <- "data/raw/rwa_admpop_adm2_2023.csv"


# URL fichier RWI
RWI_ZIP_URL <- paste0(
  "https://data.humdata.org/dataset/",
  "76f2a2ea-ba50-40f5-b79c-db95d668b843/resource/",
  "de2f953e-940c-43bb-b1f8-4d02d28124b5/download/",
  "relative-wealth-index-april-2021.zip"
)

# Nom du fichier Rwanda dans le ZIP (convention ISO3 en majuscules)
RWI_FICHIER_RWANDA <- "RWA_relative_wealth_index.csv"

# Chemin local pour le cache du ZIP et du CSV extrait
RWI_ZIP_LOCAL   <- file.path(DIR_OUTPUT, "rwi_all_countries.zip")
RWI_CSV_LOCAL   <- file.path(DIR_OUTPUT, "RWA_relative_wealth_index.csv")

# Chemin vers le fichier raster de risque (GeoTIFF ou format terra-compatible).
# Exemples de sources de données :
#   - JRC Global Surface Water  : https://global-surface-water.appspot.com/
#   - HAND (Height Above Nearest Drainage) : https://www.earthenv.org/
#   - NASA LSAF (glissements)   : https://pmm.nasa.gov/landslides
#   - Modèles hydrologiques locaux (HEC-RAS, LISFLOOD-FP, etc.)
CHEMIN_RASTER_RISQUE        <- "data/raw/zones_inondables_rwanda.tif"  # À modifier

# ==============================================================================
# Paramètres DEM (Modèle Numérique de Terrain)
# ==============================================================================

DEM_ZOOM          <- 9      # Niveau de zoom elevatr (~300 m/pixel) 
DEM_ESPACEMENT_M  <- 100    # Pas d'échantillonnage le long des arêtes (mètres)
DEM_ALTITUDE_MIN  <- 800    # Altitude minimale réaliste au Rwanda (m)
DEM_ALTITUDE_MAX  <- 4600   # Altitude maximale réaliste au Rwanda (m)

# Paramètres du DEM fictif (utilisé si le téléchargement SRTM échoue)
DEM_FICTIF_ALT_EST      <- 1500   # Altitude de base côté Est (m)
DEM_FICTIF_ALT_OUEST    <- 2300   # Altitude de base côté Ouest — dorsale Congo-Nil (m)
DEM_FICTIF_BRUIT_SD     <- 150    # Écart-type du bruit gaussien simulant les collines (m)
DEM_FICTIF_RESOLUTION_M <- 90     # Résolution du raster fictif (~comparable SRTM niveau 3)

# ==============================================================================
# Zones urbaines et entrepôts
# ==============================================================================

# Types de landuse OSM considérés comme zones urbaines
LANDUSE_URBAIN      <- c("residential", "commercial", "retail")

# Seuil d'aire minimale pour les zones industrielles (km²)
AIRE_MIN_INDUSTRIEL_KM2 <- 0.01

# Seuil d'aire minimale pour les zones retail (km²)
AIRE_MIN_RETAIL_KM2 <- 0.005

# Seuil d'aire minimale pour les zones industrielles retenues comme entrepôts (km²)
AIRE_MIN_ENTREPOT_INDUSTRIEL_KM2 <- 0.05

# Seuil d'aire minimale pour les zones retail retenues comme entrepôts (km²)
AIRE_MIN_ENTREPOT_RETAIL_KM2 <- 0.01

# Rayon du buffer autour de chaque entrepôt pour le calcul de la composition landuse (m)
BUFFER_ENTREPOT_M <- 2000

# Distance maximale pour considérer deux entrepôts comme doublons (m)
DISTANCE_DEDUP_VILLES_M     <- 3000
DISTANCE_DEDUP_INDUSTRIEL_M <- 2000
DISTANCE_DEDUP_RETAIL_M     <- 1000

# Buffer autour de la frontière nationale pour inclure les villes frontalières (m)
BUFFER_FRONTIERE_VILLES_M <- 5000

# ==============================================================================
# Paramètres du graphe et de Dijkstra
# ==============================================================================

# Seuil de longueur minimale d'une arête pour ne pas être considérée dégénérée (m)
# Les arêtes en dessous de ce seuil sont supprimées après to_spatial_subdivision()
SEUIL_LONGUEUR_ARETE_M <- 0.5

# ==============================================================================
# Véhicule de référence et paramètres multi-modal
# ==============================================================================

# Véhicule utilisé par défaut pour la matrice OD et le modèle gravitaire
VEHICULE_REFERENCE <- "camion_moyen"

# ==============================================================================
# Paramètres démographiques 
# ==============================================================================

# ── Rayon du buffer pour l'agrégation de population autour d'un entrepôt ──────
# On calcule la population dans un cercle de ce rayon autour de chaque nœud.
# 5 km est un compromis raisonnable au Rwanda (densité ~400 hab/km²) :
# trop petit → manque les zones périurbaines ; trop grand → chevauche les zones.
BUFFER_DEMO_M <- 5000

# ── Zoom du raster WorldPop pour l'approche B ─────────────────────────────────
# Résolution disponible sur le portail WorldPop :
#   z=10 → ~100m/pixel (précis, fichier lourd ~200 Mo)
#   z=8  → ~400m/pixel (moins précis, fichier léger ~15 Mo)
# Pour le Rwanda entier, z=9 (~200m) est le meilleur compromis.
WORLDPOP_ZOOM <- 9

# Noms attendus des colonnes dans le CSV NISR (à adapter selon le fichier réel)
# Ces noms correspondent au format typique des exports NISR data.gov.rw.
NISR_COL_DISTRICT  <- "District"       # Colonne du nom du district
NISR_COL_PROVINCE  <- "Province"       # Colonne de la province
NISR_COL_POP_TOTAL <- "Total"          # Colonne de population totale

# ==============================================================================
# Paramètres RWI
# ==============================================================================

# ── Rayon du buffer pour l'agrégation IDW ─────────────────────────────────────
# On réutilise BUFFER_ENTREPOT_M = 2000m (défini en Partie IV.3) pour rester
# cohérent avec le calcul des parts d'usage des sols.
# Si on veut un rayon différent pour le RWI, le décommenter :
# BUFFER_RWI_M <- 5000

# Pour l'instant on garde la même valeur que le landuse
BUFFER_RWI_M <- BUFFER_ENTREPOT_M   # 2000m par défaut

# ── Paramètre de l'interpolation IDW (inverse distance weighting) ─────────────
# La pondération de chaque cellule RWI vaut 1 / distance^RWI_IDW_PUISSANCE.
# Puissance = 1 : décroissance linéaire (lisse mais peu discriminante)
# Puissance = 2 : décroissance quadratique (standard en géostatistique)
# Puissance = 3 : décroissance cubique (très locale, amplifier les pôles proches)
# On recommande 2 pour être cohérent avec la littérature géostatistique.
RWI_IDW_PUISSANCE <- 2

# Distance minimale utilisée dans l'IDW pour éviter la division par zéro.
# Si un point RWI est exactement sur le centroïde de l'entrepôt (rare mais
# possible avec les données grillées), on le plafonne à 50m.
RWI_DISTANCE_MIN_M <- 50

# ==============================================================================
# Paramètres RPHC5 — Emploi sectoriel (profils d'offre empiriques)
# ==============================================================================

# Chemin vers le fichier d'emploi sectoriel par district issu du RPHC5 2022.
# PROCÉDURE :
#   1. Aller sur https://www.statistics.gov.rw/datasource/census-2022
#   2. Télécharger le tableau "Employment by district and sector"
#   3. Exporter en CSV avec une ligne par district, une colonne par secteur
#   4. Sauvegarder sous le chemin ci-dessous
RPHC5_EMPLOI_CSV_PATH   <- "data/raw/rwa_emploi_district_secteur_2022.csv"

# Nom de la colonne "district" dans le fichier d'emploi (à adapter si besoin)
RPHC5_COL_DISTRICT_EMPLOI <- "District"

# Correspondance entre colonnes du CSV RPHC5 et secteurs du modèle.
# Format : "Nom_colonne_CSV" = list(Secteur_modele = part, ...)
# Les parts de chaque groupe doivent sommer à 1.
# Justification des parts Manufacturing → Agro_industrie/Industrie :
#   dans le RPHC5, "Manufacturing" regroupe l'agroalimentaire (~45%)
#   et l'industrie manufacturière au sens strict (~55%).
RPHC5_CORRESPONDANCE_SECTEURS <- list(
  Emploi_Agriculture  = list(Agriculture    = 1.0),
  Emploi_Mines        = list(Mines          = 1.0),
  Emploi_Industrie    = list(Agro_industrie = 0.45, Industrie = 0.55),
  Emploi_Construction = list(Construction   = 1.0),
  Emploi_Commerce     = list(Commerce       = 1.0),
  Emploi_Transport    = list(Transport      = 1.0),
  Emploi_Services     = list(Services       = 1.0)
)

# Poids des données RPHC5 dans le profil d'offre final (interpolation convexe).
# 1.0 → profil entièrement déterminé par l'emploi sectoriel observé
# 0.0 → profil entièrement déterminé par PROFILS_OFFRE (qualitatif, comme avant)
# 0.7 → recommandé : fort ancrage empirique, correction résiduelle qualitative
#   (utile pour les zones frontalières dont le district peut être peu documenté)
POIDS_PROFIL_EMPLOI_RPHC5 <- 0.7

# Exposant log pour la taille composite OFFRE (analogue à ALPHA_LOG_POP côté demande).
# Même raisonnement que ALPHA_LOG_POP : l'emploi varie sur plusieurs ordres de
# grandeur entre districts, l'exposant évite que Kigali écrase tout.
ALPHA_LOG_EMPLOI <- 1.5

# Importance du RWI dans la taille composite OFFRE.
# Plus faible que K_RWI_TAILLE (côté demande) : la richesse amplifie davantage
# la consommation des ménages que la capacité productive des entreprises.
K_RWI_OFFRE <- 0.5

# Plafond d'emploi pour les zones de type "industrie" 
CAP_EMPLOI_INDUSTRIE <- 30000


# ==============================================================================
# Paramètres du modèle économique
# ==============================================================================

# Secteurs économiques modélisés (ordre fixe — ne pas modifier sans recalculer A)
SECTEURS <- c("Agriculture", "Mines", "Agro_industrie", "Industrie",
              "Construction", "Commerce", "Transport", "Services")

N_SECTEURS <- length(SECTEURS)

# Part du PIB considérée comme échangeable entre zones
# (le reste est consommé localement et ne génère pas de fret interzonal)
PART_ECHANGEABLE <- 0.35

# Part de la valeur ajoutée qui constitue la demande finale
PART_DEMANDE_FINALE <- 0.85

# Paramètres de friction par secteur (beta du modèle gravitaire)
# Beta élevé = très sensible au coût de transport (produits lourds/périssables)
# Beta faible = peu sensible (haute valeur ajoutée, services quasi-immatériels)
BETA_SECTEUR <- c(
  Agriculture    = 2.2,
  Mines          = 1.2,
  Agro_industrie = 1.8,
  Industrie      = 1.6,
  Construction   = 2.5,
  Commerce       = 1.7,
  Transport      = 1.3,
  Services       = 0.9
)

# Matrice des coefficients techniques A 
# a[i,j] = proportion de la production du secteur j (colonne)
# consommée comme intrant par le secteur i (ligne)
#
# Exemple de lecture :
#   A["Agriculture","Agro_industrie"] = 0.45 → 45% des intrants de l'Agro-industrie
#   proviennent de l'Agriculture (grains, fruits, légumes pour transformation)
# matrix() crée une matrice à partir d'un vecteur de valeurs.
# nrow/ncol : dimensions. byrow = TRUE : remplit ligne par ligne (pas colonne par colonne).
# dimnames : noms des lignes et colonnes.
A <- matrix(c(
  # ← Secteur fournisseur (lignes) / Secteur consommateur (colonnes) →
  # Agri  Mines AgroI Indus Const Comm  Trans Serv
  0.08, 0.00, 0.45, 0.05, 0.01, 0.05, 0.02, 0.03,  # Agriculture
  0.00, 0.05, 0.01, 0.08, 0.05, 0.00, 0.01, 0.00,  # Mines
  0.05, 0.00, 0.08, 0.02, 0.00, 0.06, 0.03, 0.04,  # Agro-industrie
  0.02, 0.03, 0.03, 0.06, 0.15, 0.03, 0.04, 0.02,  # Industrie
  0.01, 0.02, 0.01, 0.02, 0.08, 0.02, 0.03, 0.05,  # Construction
  0.04, 0.05, 0.06, 0.08, 0.05, 0.06, 0.05, 0.06,  # Commerce
  0.03, 0.06, 0.04, 0.05, 0.06, 0.07, 0.06, 0.04,  # Transport
  0.02, 0.02, 0.02, 0.03, 0.04, 0.06, 0.05, 0.08   # Services
), nrow=N_SECTEURS, ncol=N_SECTEURS, byrow=TRUE,
dimnames = list(SECTEURS, SECTEURS))

# Production totale par secteur (millions USD, Rwanda 2022)
# Calibrées sur Banque Mondiale : PIB Rwanda ~13 Md USD en 2022
production_totale <- c(
  Agriculture    = 2100,  # Café, thé, pyrèthre, cultures vivrières (principal secteur)
  Mines          = 280,   # Coltan, cassitérite, wolfram (3T : exportations majeures)
  Agro_industrie = 520,   # Transformation alimentaire, boissons, tabac
  Industrie      = 380,   # Textiles, ciment, matériaux de construction
  Construction   = 750,   # BTP, infrastructure (très actif : Vision 2050)
  Commerce       = 1100,  # Commerce de gros et de détail
  Transport      = 480,   # Transport routier, aérien, services logistiques
  Services       = 2200   # Finance, tourisme, services publics, éducation, santé
)

# Facteurs de conversion valeur → masse (tonnes par million USD) 
TONNES_PAR_musd <- c(
  Agriculture    = 8000,   # Produits bruts : lourds, faible valeur (bananes, céréales)
  Mines          = 3000,   # Minerais : denses, valeur croissante avec la transformation
  Agro_industrie = 4000,   # Produits transformés (huile, farine, sucre, conserves)
  Industrie      = 2000,   # Produits manufacturés intermédiaires
  Construction   = 10000,  # Ciment, gravier, acier : très lourds par rapport à la valeur
  Commerce       = 1500,   # Mix de biens distribués (alimentaire, électronique, textile)
  Transport      = 300,    # Services : peu de fret physique directement associé
  Services       = 100     # Quasi-immatériel (finance, éducation, santé, conseil)
)


# Chaque zone d'entreposage est caractérisée par :
#   - un profil sectoriel d'offre (ce qu'elle produit/exporte vers les autres zones)
#   - un profil sectoriel de demande (ce qu'elle consomme/importe des autres zones)
#   - une taille économique relative (Kigali Hub = référence à 1.0)
#
# Profils par type de zone :
#   hub       → fort en Commerce, Transport, Services (Kigali = centre économique)
#   sez       → fort en Industrie, Agro-industrie (production concentrée)
#   frontiere → fort en Commerce transfrontalier, Mines (export international)
#   ville     → profil équilibré, Commerce local dominant
#   marche    → fort en Agriculture et Agro-industrie locale (production agricole)


# Profils d'offre (ce que chaque type de zone produit et offre au marché)
# Chaque profil est un vecteur dont la somme des valeurs vaut 1 (100% de l'offre).
# Les valeurs indiquent la part de chaque secteur dans l'offre totale de la zone.
PROFILS_OFFRE <- list(
  hub      = c(Agriculture=0.02, Mines=0.01, Agro_industrie=0.15, Industrie=0.12,
               Construction=0.08, Commerce=0.25, Transport=0.20, Services=0.17),
  sez      = c(Agriculture=0.05, Mines=0.05, Agro_industrie=0.25, Industrie=0.30,
               Construction=0.10, Commerce=0.10, Transport=0.10, Services=0.05),
  frontiere= c(Agriculture=0.15, Mines=0.15, Agro_industrie=0.10, Industrie=0.08,
               Construction=0.05, Commerce=0.30, Transport=0.12, Services=0.05),
  ville    = c(Agriculture=0.12, Mines=0.02, Agro_industrie=0.08, Industrie=0.05,
               Construction=0.15, Commerce=0.30, Transport=0.10, Services=0.18),
  marche   = c(Agriculture=0.45, Mines=0.01, Agro_industrie=0.20, Industrie=0.03,
               Construction=0.02, Commerce=0.20, Transport=0.05, Services=0.04),
  industrie= c(Agriculture=0.02, Mines=0.05, Agro_industrie=0.10, Industrie=0.50,
               Construction=0.15, Commerce=0.08, Transport=0.07, Services=0.03)
)

# Profils de demande (ce que chaque type de zone consomme en provenance des autres)
PROFILS_DEMANDE <- list(
  hub      = c(Agriculture=0.05, Mines=0.02, Agro_industrie=0.20, Industrie=0.15,
               Construction=0.10, Commerce=0.20, Transport=0.15, Services=0.13),
  sez      = c(Agriculture=0.10, Mines=0.08, Agro_industrie=0.15, Industrie=0.25,
               Construction=0.15, Commerce=0.10, Transport=0.12, Services=0.05),
  frontiere= c(Agriculture=0.12, Mines=0.06, Agro_industrie=0.12, Industrie=0.12,
               Construction=0.06, Commerce=0.28, Transport=0.18, Services=0.06),
  ville    = c(Agriculture=0.15, Mines=0.02, Agro_industrie=0.18, Industrie=0.10,
               Construction=0.12, Commerce=0.22, Transport=0.08, Services=0.13),
  marche   = c(Agriculture=0.38, Mines=0.01, Agro_industrie=0.22, Industrie=0.06,
               Construction=0.04, Commerce=0.20, Transport=0.05, Services=0.04),
  industrie= c(Agriculture=0.05, Mines=0.10, Agro_industrie=0.10, Industrie=0.35,
               Construction=0.15, Commerce=0.08, Transport=0.12, Services=0.05)
)

# Importance relative de la richesse dans la masse économique.
# K_RWI_TAILLE = 0   → taille déterminée par la population seule
# K_RWI_TAILLE = 1   → une zone à p_rwi = 1 a 2× le poids par habitant
#                       d'une zone à p_rwi = 0  (rapport 2:1)
# K_RWI_TAILLE = 2   → rapport 3:1 entre la zone la plus riche et la plus
#                       pauvre de l'échantillon (plus discriminant)
# Valeur recommandée : 1.0 (équilibre entre les deux variables)
K_RWI_TAILLE <- 1.0

# Exposant appliqué au logarithme de la population.
# La population varie de ~20 000 à ~1 000 000 sur le Rwanda (rapport 50:1).
# Sans transformation, Kigali écraserait tout le reste.
# log10(pop) ramène le rapport à ~3.3:6.0 = 1.8:1 — trop peu discriminant.
# L'exposant ALPHA_LOG_POP étire ou compresse cette échelle log :
#   ALPHA_LOG_POP = 1.0 → échelle log (peu discriminante)
#   ALPHA_LOG_POP = 1.5 → intermédiaire (recommandé)
#   ALPHA_LOG_POP = 2.0 → se rapproche de la population brute
# Valeur recommandée : 1.5
ALPHA_LOG_POP <- 1.5

# Plafond de population pour les zones de type "industrie".
# Voir justification détaillée dans la transition IV.5 → V.
# Mettre Inf pour désactiver le cap.
CAP_POP_INDUSTRIE <- as.integer(CAP_EMPLOI_INDUSTRIE*2.5)

# ==============================================================================
# Paramètres de l'affectation All-or-Nothing
# ==============================================================================

# Flux minimum (en tonnes) pour qu'une paire OD soit affectée au réseau
# Les paires en dessous de ce seuil sont ignorées (flux négligeable)
SEUIL_FLUX_TONNES <- 50


# ==============================================================================
# Paramètres de l'analyse de vulnérabilité (Partie IX)
# ==============================================================================

# Paramètres du scénario de perturbation 
NOM_SCENARIO          <- "Inondation_RN1_Kigali_Huye"
DESCRIPTION_SCENARIO  <- "Rupture de la RN1 entre Kigali et Huye suite à une inondation"
DUREE_JOURS           <- 14
TYPE_EVENEMENT        <- "inondation"

# Choix du mode de création de scénario :
#     Mode A — Manuel       : liste d'osm_id ou de coordonnées GPS fournie à la main
#     Mode B — Buffer zone  : toutes les routes dans un rayon autour d'un point
#     Mode C — Raster risque: intersection avec un raster (grille) externe
# Si plusieurs méthodes sont choisies, les routes affectées seront celles de l'union des méthodes

# Mettre l'identifiant OSM de la ou des routes affectées
OSM_IDS_PERTURBES_MANUEL <- c(479687569 )
# Pour les activer : mettre TRUE, sinon : mettre FALSE
UTILISER_MODE_BUFFER        <- FALSE  
UTILISER_MODE_RASTER        <- FALSE

# Coordonnées du centre de la zone perturbée du mode buffer 
CENTRE_PERTURBATION_LON <- 29.950   # Est-Ouest
CENTRE_PERTURBATION_LAT <- -2.150   # Nord-Sud

# Nombre d'arêtes candidates testées pour l'analyse de criticité
N_TOP_ARETES_CRITIQUES <- 50

# Seuil de volume fret (tonnes) pour qu'une paire OD soit incluse
# dans le calcul de criticité (filtre pour accélérer le calcul)
SEUIL_PAIRES_CRITICITE <- 100

# Nombre d'arêtes critiques affichées sur la carte de criticité
N_ARETES_AFFICHEES_CRITICITE <- 20

# Rayon de la zone perturbée en mètres.
RAYON_PERTURBATION_M    <- 5000

# Types de routes inclus dans la perturbation (NULL = tous les types).
# Pour restreindre, utilisez par exemple : c("primary", "secondary") pour 
# ne perturber que les routes primaires et secondaires.
TYPES_ROUTES_PERTURBES  <- NULL  

# Seuil au-dessus duquel une route est considérée comme perturbée.
# Si le raster contient des probabilités (0-1) : un seuil de 0.5 signifie
# "routes dont plus de 50% de leur longueur est considérée comme à risque".
# Si le raster contient des profondeurs (cm) : un seuil de 30 signifie
# "routes à risque sont sous plus de 30cm d'eau" 
SEUIL_RISQUE_RASTER         <- 0.5

# Proportion minimale de la longueur d'une arête qui doit être en zone à risque
# pour que l'arête soit considérée comme perturbée.
# 0.3 = au moins 30% de la route doit être en zone à risque
PROPORTION_MIN_EXPOSEE      <- 0.3

# Proportion des routes exposées (dépassant les seuils) effectivement inondées.
# 1.0 = toutes les routes exposées sont coupées 
# 0.6 = 60% des routes exposées sont aléatoirement coupées
PROP_ROUTES_INONDEES_BUFFER <- 1
PROP_ROUTES_INONDEES_RASTER <- 0.7

# Graine aléatoire pour la sélection des routes inondées.
# Fixer cette valeur garantit la reproductibilité du scénario.
# Changer la graine = simuler un autre tirage du même événement.
SEED_INONDATION <- 42

# Nombre d'arêtes à tester pour la criticité.
# 50 = bon compromis rapidité / exhaustivité pour un premier diagnostic.
# Pour une analyse complète, augmenter à 200 ou 500 (plusieurs heures).
N_TOP_ARETES_CRITIQUES <- 50

# Pour accélérer le calcul de criticité, on ne recalcule que les paires OD avec un volume de fret
# supérieur à un seuil (SEUIL_PAIRES_CRITICITE), ce qui exclut les paires
# marginales qui ne changent pas le classement de criticité.
SEUIL_PAIRES_CRITICITE <- 100   # tonnes 

# ==============================================================================
# Paramètres du modèle ARIO-inventory
# ==============================================================================

# ── Paramètres de surproduction ───────────────────────────────────────────────
# α_base : surproduction initiale à l'équilibre (1.0 = pas de surproduction).
# α_max  : plafond de surproduction. Hallegatte 2014 utilise 1.25 (+25%).
# τ_α    : temps caractéristique d'ajustement de α (en jours).
#   1 an = 365 jours dans Hallegatte 2014. C'est lent volontairement : la mise
#   en place de capacités supplémentaires (heures sup, recrutement, imports)
#   prend du temps dans la réalité.
ARIO_ALPHA_BASE  <- 1.00
ARIO_ALPHA_MAX   <- 1.25
ARIO_TAU_ALPHA   <- 365

# ── Niveaux d'inventaire cibles (en jours de consommation) ────────────────────
# Hallegatte 2014 : 90 jours pour les biens stockables.
# Pour les biens non-stockables (transport notamment), max 3 jours (sinon
# instabilités numériques avec δt = 1 jour).
# La Construction est traitée comme "quasi-infinie" (365 jours) car le
# rationnement de ce secteur n'affecte pas immédiatement la production
# des autres secteurs.
ARIO_INV_DUREE_JOURS <- c(
  Agriculture    = 30,    # Périssables : stocks limités (céréales, etc.)
  Mines          = 90,    # Minerais : stockables longtemps
  Agro_industrie = 60,    # Produits transformés : stocks intermédiaires
  Industrie      = 90,    # Pièces, matériaux : stocks classiques
  Construction   = 365,   # ≈ infini (rationnement sans impact immédiat)
  Commerce       = 60,    # Biens de consommation : rotation moyenne
  Transport      = 3,     # Service non stockable
  Services       = 30     # Stocks de fournitures uniquement
)

# τ_s : temps caractéristique de restauration des inventaires (en jours).
# C'est la vitesse à laquelle les industries passent leurs commandes pour
# combler le déficit d'inventaire.
ARIO_TAU_S <- c(
  Agriculture    = 30,
  Mines          = 30,
  Agro_industrie = 30,
  Industrie      = 30,
  Construction   = 30,
  Commerce       = 30,
  Transport      = 1,     # Non-stockable
  Services       = 30
)

# ── Paramètre d'hétérogénéité ψ ───────────────────────────────────────────────
# ψ ∈ [0, 1] : sensibilité de la production aux ruptures d'inventaire.
#   ψ = 0   : biens parfaitement substituables au sein d'un secteur
#             → la production tient tant que le stock total > 0
#   ψ = 0.8 : valeur recommandée par Hallegatte 2014 (cas central)
#             → une baisse de stock de 20% commence à pénaliser la production
#   ψ = 1.0 : biens totalement spécialisés (production très fragile)
# Plus ψ est élevé, plus les pertes indirectes sont importantes.
# C'est le paramètre le plus sensible du modèle (cf. analyses Hallegatte).
ARIO_PSI <- 0.80

# ── Horizon de simulation et pas de temps ─────────────────────────────────────
# La perturbation dure DUREE_JOURS (paramètre Partie IX). On simule au-delà
# pour observer la phase de rétablissement (surproduction, reconstitution
# des stocks). Horizon par défaut : 2 × DUREE_JOURS, plafonné à 365 jours.
# Modifier ARIO_HORIZON_JOURS ci-dessous pour personnaliser.
ARIO_HORIZON_JOURS <- min(2 * DUREE_JOURS, 365)
ARIO_DT            <- 1   # Pas de temps en jours (Hallegatte 2014)

# ── Récupération exponentielle du choc de capacité Δ ──────────────────────────
# Une fois la perturbation passée (t > DUREE_JOURS), Δ_P décroît
# exponentiellement vers 0 avec un temps caractéristique τ_recup :
#   Δ_P(t) = Δ_P(t_0) × exp(-(t - DUREE_JOURS) / τ_recup)
# τ_recup = DUREE_JOURS/2 → récupération rapide une fois les routes rouvertes.
ARIO_TAU_RECUP <- DUREE_JOURS / 2

cat("✓ Paramètres globaux chargés\n\n")

# ==============================================================================
# RESET COMPLET — Suppression de tous les caches
# Mettre RESET_CACHES <- TRUE pour forcer un recalcul complet depuis zéro.
# Remettre à FALSE ensuite pour bénéficier des caches au prochain lancement.
# ==============================================================================

RESET_CACHES <- FALSE  # ← passer à TRUE pour tout recalculer

if (RESET_CACHES) {
  
  caches <- c(
    file.path(DIR_OUTPUT, "reseau_corrige_cache.rds"),
    file.path(DIR_OUTPUT, "pentes_cache.rds"),
    file.path(DIR_OUTPUT, "landuse_cache.rds"),
    file.path(DIR_OUTPUT, "od_cache.rds"),
    file.path(DIR_OUTPUT, "affectation_cache.rds")
  )
  
  cat("=== RESET COMPLET DES CACHES ===\n")
  
  for (f in caches) {
    if (file.exists(f)) {
      file.remove(f)
      cat("  ✓ Supprimé :", basename(f), "\n")
    } else {
      cat("  — Absent  :", basename(f), "\n")
    }
  }
  
  cat("\n⚠ RESET_CACHES = TRUE — pensez à le remettre à FALSE\n")
  cat("  Temps de recalcul estimé : 3-5h selon la machine\n\n")
  
} else {
  cat("  Caches conservés (RESET_CACHES = FALSE)\n\n")
}

# ==============================================================================
# I.2 bis : Environnement séparé pour les gros objets
# Crée un environnement R distinct de .GlobalEnv qui n'est PAS indexé par
# RStudio Server. Les gros objets (graphes, réseaux, rasters) y sont stockés
# pour éviter que RStudio ne fige la session en tentant d'afficher leur
# aperçu dans le panneau Environment.
# ==============================================================================

# Un "environnement" en R est un conteneur de variables, comme une boîte
# qui peut contenir des objets. .GlobalEnv est l'environnement par défaut,
# celui qu'on voit dans le panneau Environment de RStudio.
# new.env() crée un environnement séparé, parallèle à .GlobalEnv.
# RStudio n'indexe que .GlobalEnv automatiquement, donc les objets stockés
# dans env_lourds restent invisibles dans le panneau Environment.
# parent = emptyenv() : on isole complètement env_lourds (pas de chaîne de
# parents qui remonterait à .GlobalEnv). Ce n'est pas strictement nécessaire
# mais c'est plus propre et évite des résolutions de noms inattendues.
env_lourds <- new.env(parent = emptyenv())

# ── Fonctions utilitaires d'accès à env_lourds ────────────────────────────────
# Ces deux fonctions raccourcissent l'écriture pour stocker et récupérer
# des objets dans env_lourds. Sans elles, il faudrait à chaque fois écrire
# env_lourds$nom_objet, ce qui devient verbeux dans le code.

# stocker_lourd() : déplace un objet de .GlobalEnv vers env_lourds.
# Le paramètre nom est passé en chaîne pour permettre d'utiliser deparse(substitute())
# si on voulait passer l'objet directement, mais ici on reste explicite.
# La fonction supprime ensuite l'objet du global pour libérer la mémoire
# affichée par RStudio (l'objet n'apparaît plus dans le panneau Environment).
stocker_lourd <- function(nom, obj) {
  # assign() : place un objet dans un environnement spécifique sous un nom donné.
  # C'est l'équivalent fonctionnel de env_lourds[[nom]] <- obj.
  assign(nom, obj, envir = env_lourds)
  
  # On retourne invisible(NULL) pour que la fonction ne pollue pas la console
  # quand on l'appelle (ex : on ne veut pas voir le contenu s'afficher).
  invisible(NULL)
}

# recuperer_lourd() : récupère un objet depuis env_lourds.
# Plus court à écrire que env_lourds[[nom]] et plus explicite dans le code.
recuperer_lourd <- function(nom) {
  # get() : lit un objet depuis un environnement spécifique.
  # inherits = FALSE : ne cherche pas dans les environnements parents
  # (cohérent avec parent = emptyenv() utilisé plus haut).
  get(nom, envir = env_lourds, inherits = FALSE)
}

# lister_lourds() : affiche le contenu de env_lourds (pour diagnostic).
# Utile quand on veut vérifier quels objets ont été déplacés sans
# avoir à inspecter env_lourds dans le panneau Environment.
lister_lourds <- function() {
  noms <- ls(envir = env_lourds)
  if (length(noms) == 0) {
    cat("env_lourds est vide\n")
    return(invisible(NULL))
  }
  
  # Pour chaque objet, on affiche son nom et sa taille.
  # format() avec units = "auto" choisit l'unité la plus lisible (KB, MB, GB).
  for (n in noms) {
    obj  <- get(n, envir = env_lourds)
    taille <- format(object.size(obj), units = "auto")
    cat("  ", n, " : ", taille, "\n", sep = "")
  }
  invisible(noms)
}

cat("✓ Environnement env_lourds créé (objets non visibles dans RStudio)\n\n")

# ==============================================================================
# I.2 : Connexion DuckDB et fonctions utilitaires
# Ouvre la base analytique persistante et définit les raccourcis duck_write()
# et duck_query() utilisés dans toutes les parties suivantes.
# ==============================================================================

# DuckDB est une base de données SQL embarquée : elle fonctionne directement
# dans R sans avoir besoin d'un serveur séparé. On peut lui envoyer des requêtes 
# SQL pour manipuler des tableaux de données très efficacement — plus vite que 
# des boucles R sur de grands volumes. Le fichier "reseau_rwanda.duckdb" stocke 
# toutes les tables sur le disque, ce qui permet de reprendre le travail sans 
# recalculer depuis zéro.

# Fermeture propre de la connexion à DuckDB afin de la rouvrir ensuite proprement
if (exists("con")) {
  # tryCatch() : tente d'exécuter le code entre accolades ;
  # si une erreur survient, la fonction "error" l'attrape silencieusement
  # (NULL = ne rien faire). Cela évite que le script s'arrête si la connexion
  # n'existait pas encore.
  tryCatch(
    DBI::dbDisconnect(con, shutdown = TRUE),
    error = function(e) NULL
  )
}

cat("=== Connexion DuckDB ===\n")

# dbConnect() : ouvre une connexion à la base de données.
# duckdb() : indique à R quel type de base de données on utilise (le "pilote").
# dbdir = ":memory:" créerait une base en RAM uniquement (non persistante).
# Ici, on utilise un fichier sur le disque pour conserver les données entre sessions.
con <- dbConnect(duckdb(), dbdir = DB_PATH)

# Note : l'extension spatiale DuckDB existe mais n'est pas utilisée ici
# car sfnetworks ne sait pas lire depuis DuckDB spatial.
# Pour l'activer si besoin : dbExecute(con, "INSTALL spatial; LOAD spatial;")

cat("✓ DuckDB connecté :", DB_PATH, "\n\n")

# DIR_OUTPUT : dossier où seront sauvegardés tous les fichiers produits
# (cartes PNG, CSV, Parquet, GeoPackage).
# dir.create() le crée s'il n'existe pas encore.
# showWarnings = FALSE : n'affiche pas de message si le dossier existe déjà.
# recursive = TRUE : crée aussi les dossiers parents si nécessaire.
dir.create(DIR_OUTPUT, showWarnings = FALSE, recursive = TRUE)

# ── Fonctions utilitaires DuckDB ──────────────────────────────────────────────

# duck_write() : raccourci pour envoyer un tableau R vers DuckDB.
# dbWriteTable() copie un data.frame R dans une table DuckDB.
# overwrite = TRUE : si la table existe déjà, elle est remplacée (utile
# quand on relance le script sans vouloir d'erreur "table already exists").
# invisible(df) retourne df sans l'afficher dans la console, ce qui permet
# d'enchaîner les opérations avec l'opérateur %>%.
duck_write <- function(df, table_name) {
  dbWriteTable(con, table_name, df, overwrite = TRUE)
  invisible(df)  
}

# duck_query() : raccourci pour envoyer une requête SQL à DuckDB et récupérer
# le résultat sous forme de tableau R (data.frame).
# dbGetQuery() envoie le SQL, attend la réponse, et la renvoie en R.
duck_query <- function(sql) dbGetQuery(con, sql)

# ==============================================================================
# I.3 : Palettes de couleurs centralisées
# Définit toutes les couleurs utilisées dans les cartes. Modifier ici
# répercute les changements sur l'ensemble des visualisations du script.
# ==============================================================================

# ── Types de routes ───────────────────────────────────────────────────────────
PALETTE_ROAD_TYPE <- c(
  motorway     = "#E41A1C",   # Rouge vif    — autoroute
  trunk        = "#FF4400",   # Rouge-orange — route nationale principale
  primary      = "#FF7F00",   # Orange       — route primaire
  secondary    = "#E8A000",   # Jaune-ocre   — route secondaire
  tertiary     = "#999999",   # Gris moyen   — route tertiaire
  unclassified = "#CCCCCC"    # Gris clair   — route non classée
)

# ── Catégories de pente ───────────────────────────────────────────────────────
PALETTE_PENTE <- c(
  plat    = "#00AA00",   # Vert foncé  — pente < 2%
  legere  = "#AACC00",   # Vert-jaune  — pente 2-5%
  moderee = "#FF9900",   # Orange      — pente 5-8%
  forte   = "#FF0000"    # Rouge       — pente > 8%
)

# ── Types de zones (entrepôts) ────────────────────────────────────────────────
PALETTE_ZONE_TYPE <- c(
  hub       = "#000000",   # Noir        — hub central
  sez       = "#0055FF",   # Bleu vif    — zone économique spéciale
  marche    = "#00AA00",   # Vert        — marché
  frontiere = "#FF0000",   # Rouge       — poste frontière
  ville     = "#880088",   # Violet      — ville
  industrie = "#FF6600"    # Orange foncé— zone industrielle
)

# ── Coûts généralisés (gradient jaune pâle → bordeaux) ────────────────────────
PALETTE_COUTS <- c("#FFF7BC", "#FEC44F", "#D94701", "#7F0000")
# Lecture : faible coût = jaune pâle, coût élevé = bordeaux

# ── Ratio de coût entre véhicules (gradient rouge → jaune → vert) ─────────────
# Rouge = coût du véhicule au numérateur élevé relativement à celui au dénominateur ; vert = inverse
PALETTE_RATIO <- c("#D73027", "#FC8D59", "#FEE090", "#91CF60", "#1A9850")

# ── Volume de trafic fret (gradient bleu clair → violet) ──────────────────────
PALETTE_FRET <- c("#CCE5FF", "#6BAED6", "#2171B5", "#6A0DAD")
# Lecture : faible trafic = bleu clair, trafic intense = violet

# ── Flux commerciaux OD (gradient bleu) ───────────────────────────────────────
PALETTE_FLUX_OD <- c("#EFF3FF", "#BDD7E7", "#6BAED6", "#2171B5", "#084594")

# ── Catégories de trafic (pour légendes textuelles) ───────────────────────────
PALETTE_CLASSE_TRAFIC <- c(
  "Aucun"       = "#F0F0F0",   # Gris très clair
  "Très faible" = "#CCE5FF",   # Bleu très clair
  "Faible"      = "#6BAED6",   # Bleu moyen
  "Moyen"       = "#2171B5",   # Bleu foncé
  "Élevé"       = "#54278F",   # Violet foncé
  "Très élevé"  = "#6A0DAD"    # Violet intense
)

cat("✓ Palettes de couleurs définies\n\n")


# ==============================================================================
# I.4 : Paramètres de la flotte de véhicules
# Définit les 3 tables DuckDB décrivant la flotte (coûts, vitesses, pentes,
# transbordements, coûts pré-frontière). Pour ajouter un véhicule :
# modifier uniquement ce bloc, le reste du script s'adapte automatiquement.
# ==============================================================================

# Cette section crée cinq tableaux qui décrivent la flotte de véhicules utilisée
# dans le modèle. Chaque tableau est d'abord créé en R avec tribble() — une
# façon pratique de saisir un tableau ligne par ligne —, puis envoyé dans
# DuckDB avec duck_write() pour pouvoir être interrogé en SQL plus tard.

# ── Table 1 : paramètres scalaires par véhicule ───────────────────────────────
# Ce tableau contient les caractéristiques physiques et économiques de chaque
# type de véhicule : consommation de carburant, prix du carburant, valeur
# du temps du chauffeur, coûts d'usure selon le type de route, capacité de
# chargement, et pénalité en zone urbaine (congestion, restrictions de tonnage).
params_flotte_df <- tribble(
  ~vehicule_id,   ~nom,                    ~conso_base, ~facteur_paved, ~facteur_gravel, ~facteur_unpaved, ~facteur_conso_pente, ~prix_carburant, ~valeur_temps, ~usure_paved, ~usure_gravel, ~usure_unpaved, ~capacite_tonnes, ~facteur_urbain, ~facteur_emission_co2, ~facteur_emission_nox, ~facteur_emission_pm25,
  "camionnette",  "Camionnette (<3.5t)",    10,          1.00,           1.08,            1.18,             1.0,                  1.40,            4.5,           0.02,         0.04,          0.07,            3.0,              1.05,            2.68,                  0.25,                  0.040,
  "camion_moyen", "Camion moyen (5-10t)",   20,          1.00,           1.15,            1.30,             1.5,                  1.40,            7.5,           0.05,         0.08,          0.12,            7.5,              1.25,            2.68,                  0.50,                  0.065,
  "camion_lourd", "Camion lourd (>10t)",    35,          1.00,           1.25,            1.50,             2.0,                  1.40,            10.0,          0.08,         0.14,          0.22,            20.0,             1.60,            2.68,                  0.80,                  0.090
)
duck_write(params_flotte_df, "params_flotte")

# ── Table 2 : vitesses par véhicule × type de route × surface ─────────────────
# Chaque véhicule a ses propres vitesses de référence sur chaque combinaison.
# L'ajout d'un véhicule = ajouter 11 lignes avec son vehicule_id.
# Les vitesses sont en km/h et varient selon :
#   - le type de véhicule (un camion lourd ne peut pas aller aussi vite qu'une camionnette)
#   - le type de route (une autoroute permet des vitesses plus élevées qu'un chemin non classé)
#   - la surface (bitumée = rapide, piste en terre = lent)
vitesses_flotte_df <- tribble(
  ~vehicule_id,   ~road_type,      ~surface,   ~vitesse_kmh,
  # --- Camionnette ---
  "camionnette",  "motorway",      "paved",    120,
  "camionnette",  "trunk",         "paved",     90,
  "camionnette",  "trunk",         "gravel",    60,
  "camionnette",  "primary",       "paved",     80,
  "camionnette",  "primary",       "gravel",    55,
  "camionnette",  "secondary",     "paved",     70,
  "camionnette",  "secondary",     "gravel",    50,
  "camionnette",  "tertiary",      "paved",     60,
  "camionnette",  "tertiary",      "unpaved",   35,
  "camionnette",  "unclassified",  "gravel",    45,
  "camionnette",  "unclassified",  "unpaved",   28,
  # --- Camion moyen ---
  "camion_moyen", "motorway",      "paved",    100,
  "camion_moyen", "trunk",         "paved",     60,
  "camion_moyen", "trunk",         "gravel",    40,
  "camion_moyen", "primary",       "paved",     60,
  "camion_moyen", "primary",       "gravel",    40,
  "camion_moyen", "secondary",     "paved",     50,
  "camion_moyen", "secondary",     "gravel",    35,
  "camion_moyen", "tertiary",      "paved",     45,
  "camion_moyen", "tertiary",      "unpaved",   25,
  "camion_moyen", "unclassified",  "gravel",    30,
  "camion_moyen", "unclassified",  "unpaved",   20,
  # --- Camion lourd ---
  "camion_lourd", "motorway",      "paved",     80,
  "camion_lourd", "trunk",         "paved",     50,
  "camion_lourd", "trunk",         "gravel",    30,
  "camion_lourd", "primary",       "paved",     50,
  "camion_lourd", "primary",       "gravel",    30,
  "camion_lourd", "secondary",     "paved",     40,
  "camion_lourd", "secondary",     "gravel",    25,
  "camion_lourd", "tertiary",      "paved",     35,
  "camion_lourd", "tertiary",      "unpaved",   18,
  "camion_lourd", "unclassified",  "gravel",    22,
  "camion_lourd", "unclassified",  "unpaved",   14
)
duck_write(vitesses_flotte_df, "vitesses_flotte")

# ── Table 3 : facteurs de pente par véhicule × catégorie ──────────────────────
# Un camion chargé en côte monte beaucoup plus lentement qu'en terrain plat.
# Ces facteurs multiplicatifs réduisent la vitesse de référence en fonction
# de l'inclinaison de la route et du type de véhicule.
# Ex : facteur_pente = 0.45 pour camion_lourd en pente forte
#   → vitesse réelle = vitesse_base × 0.45 (55% de ralentissement !)
facteurs_pente_df <- tribble(
  ~vehicule_id,   ~slope_category, ~facteur_pente,
  "camionnette",  "plat",           1.00,
  "camionnette",  "legere",         0.95,
  "camionnette",  "moderee",        0.85,
  "camionnette",  "forte",          0.72,
  "camion_moyen", "plat",           1.00,
  "camion_moyen", "legere",         0.90,
  "camion_moyen", "moderee",        0.75,
  "camion_moyen", "forte",          0.60,
  "camion_lourd", "plat",           1.00,
  "camion_lourd", "legere",         0.82,
  "camion_lourd", "moderee",        0.62,
  "camion_lourd", "forte",          0.45
)
duck_write(facteurs_pente_df, "facteurs_pente_flotte")

# ── Table 4 : coûts de transbordement entre véhicules ─────────────────────────
# Coût fixe en USD pour transférer la cargaison d'un type de véhicule à un autre
# dans un entrepôt (manutention, attente, administration).
# Pour ajouter une combinaison : ajouter une ligne dans ce tribble.
# Ces coûts servent dans le graphe multi-modal (Partie V.2) pour décider
# si le surcoût du changement de véhicule est compensé par un itinéraire plus
# économique avec un autre type de camion.
couts_transbordement_df <- tribble(
  ~vehicule_origine,  ~vehicule_destination, ~cout_usd_fixe,
  "camion_lourd",     "camion_moyen",          25.0,
  "camion_lourd",     "camionnette",           40.0,
  "camion_moyen",     "camion_lourd",          25.0,
  "camion_moyen",     "camionnette",           15.0,
  "camionnette",      "camion_moyen",          15.0,
  "camionnette",      "camion_lourd",          40.0
)
duck_write(couts_transbordement_df, "couts_transbordement")

# ── Table 5 : coûts de transport pré-frontière par pays et par secteur ────────
# Ces coûts représentent le coût moyen de transport d'une marchandise
# depuis son point d'origine dans le pays étranger jusqu'à la frontière rwandaise.
# Ils s'ajoutent au coût de transport interne rwandais dans le modèle gravitaire.
# Source : estimations calibrées sur les données de coût de transport régional
# (Banque Mondiale, CPCS, données COMESA).
# Unité : USD par tonne
# La logique est simple : faire venir du café de Kampala (Ouganda) coûte moins
# cher que faire venir de l'acier de Dar es Salaam (Tanzanie) car la distance
# est bien plus courte et les routes sont meilleures.

couts_prebordure_df <- tribble(
  ~pays,       ~secteur,         ~cout_usd_tonne,
  # ── Ouganda (corridors Nord : Kampala → Gatuna/Kagitumba) ───────────────────
  # Distance moyenne Kampala-frontière Rwanda : ~500km, routes bitumées
  "Ouganda",   "Agriculture",     35.0,   
  "Ouganda",   "Mines",           25.0,
  "Ouganda",   "Agro_industrie",  30.0,
  "Ouganda",   "Industrie",       28.0,
  "Ouganda",   "Construction",    42.0,
  "Ouganda",   "Commerce",        26.0,
  "Ouganda",   "Transport",       18.0,
  "Ouganda",   "Services",         8.0,
  # ── Tanzanie (corridor Est : Dar es Salaam → Rusumo) ────────────────────────
  # Distance moyenne port Dar-frontière Rwanda : ~1300km
  # Coûts plus élevés car corridor plus long et qualité route variable
  "Tanzanie",  "Agriculture",     90.0,  
  "Tanzanie",  "Mines",           55.0,
  "Tanzanie",  "Agro_industrie",  75.0,
  "Tanzanie",  "Industrie",       70.0,
  "Tanzanie",  "Construction",   110.0,
  "Tanzanie",  "Commerce",        65.0,
  "Tanzanie",  "Transport",       45.0,
  "Tanzanie",  "Services",        12.0,
  # ── RDC (corridor Ouest : Goma → Rubavu) ────────────────────────────────────
  # Distance courte mais infrastructure très dégradée
  # Coûts élevés malgré la proximité géographique
  "RDC",       "Agriculture",     28.0,
  "RDC",       "Mines",           20.0,
  "RDC",       "Agro_industrie",  25.0,
  "RDC",       "Industrie",       30.0,
  "RDC",       "Construction",    38.0,
  "RDC",       "Commerce",        22.0,
  "RDC",       "Transport",       14.0,
  "RDC",       "Services",         5.0,
  # ── Burundi (corridor Sud : Bujumbura → Bugarama/Rusizi) ────────────────────
  # Distance moyenne Bujumbura-frontière Rwanda : ~150km
  # Infrastructure correcte sur axe principal
  "Burundi",   "Agriculture",     12.0,
  "Burundi",   "Mines",            9.0,
  "Burundi",   "Agro_industrie",  10.0,
  "Burundi",   "Industrie",       11.0,
  "Burundi",   "Construction",    16.0,
  "Burundi",   "Commerce",         9.0,
  "Burundi",   "Transport",        6.0,
  "Burundi",   "Services",         2.0
)
duck_write(couts_prebordure_df, "couts_prebordure")

cat("✓ Coûts pré-frontière chargés dans DuckDB :",
    nrow(couts_prebordure_df), "lignes\n\n")

# VEHICULE_REFERENCE : le type de camion utilisé par défaut pour calculer
# la matrice OD et alimenter le modèle gravitaire quand on n'a pas besoin
# de distinguer les véhicules.

# Véhicule de référence pour la matrice OD et le modèle gravitaire
VEHICULE_REFERENCE   <- "camion_moyen"

# Récupérer les ids pour les boucles de cartographie (Partie 10)
VEHICULES_IDS <- duck_query("SELECT vehicule_id, nom FROM params_flotte")

cat("✓ Flotte chargée dans DuckDB :",
    nrow(VEHICULES_IDS), "véhicules —",
    paste(VEHICULES_IDS$vehicule_id, collapse = ", "), "\n\n")

# ── Fonction de remappage multi-modal (définie ici car utilisée dans 02 et 03)
# n_noeuds doit être chargé avant d'appeler cette fonction
node_multi <- function(v_idx, n_id, n_noeuds_local) {
  as.integer((v_idx - 1L) * n_noeuds_local + n_id)
}

# ── Chemins des fichiers de persistance inter-scripts ─────────────────────────
PERSIST_GEODATA      <- file.path(DIR_OUTPUT, "persist_geodata.rds")
PERSIST_RESEAU_BASE  <- file.path(DIR_OUTPUT, "persist_reseau_base.rds")
PERSIST_ENTREPOSAGES <- file.path(DIR_OUTPUT, "persist_entreposages.rds")
PERSIST_RESEAU_COUTS <- file.path(DIR_OUTPUT, "persist_reseau_couts.rds")
PERSIST_GRAPHE_MM    <- file.path(DIR_OUTPUT, "persist_graphe_mm.rds")
PERSIST_MAPPING_MM   <- file.path(DIR_OUTPUT, "persist_mapping_mm.rds")
PERSIST_FLUX_FRET    <- file.path(DIR_OUTPUT, "persist_flux_fret.rds")
PERSIST_RESEAU_FRET  <- file.path(DIR_OUTPUT, "persist_reseau_fret.rds")
PERSIST_VULNERAB     <- file.path(DIR_OUTPUT, "persist_vulnerabilite.rds")
PERSIST_ARIO         <- file.path(DIR_OUTPUT, "persist_ario.rds")

cat("✓ 00_parametres.R chargé\n")