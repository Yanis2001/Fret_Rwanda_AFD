################################################################################
# 00_parametres.R
# RÔLE : Point d'entrée unique de la configuration.
#        Chargé en première ligne de TOUS les autres scripts via source().
#        Ne produit aucun fichier — configure uniquement l'environnement.
# USAGE : source("00_parametres.R")
################################################################################


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
  "duckdb",        # Base analytique embarquée — moteur SQL sans serveur
  "DBI",           # Interface R standard pour les bases de données (pilote DuckDB)
  "scales",        # Mise à l'échelle et formatage pour ggplot2 (rescale, percent…)
  "progress",      # Barre de progression
  "exactextractr", # Agrégation précise de rasters sur des polygones
  "digest",        # Génération d'empreinte numérique (hash) d'objets R
  "ggrepel",       # Étiquettes ggplot2 sans chevauchement (graphiques RWI, démographie)
  "ggalluvial",    # Diagrammes de Sankey pour les flux de fret (viz_fret.R)
  "RColorBrewer",  # Palettes de couleurs pour les cartes et graphiques sectoriels
  "wbstats"        # Accès à l'API Banque Mondiale 
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

# Charge un package ; si le chargement échoue (dépendance manquante, package
# corrompu…), le réinstalle avec toutes ses dépendances puis réessaie.
# Cela couvre le cas où un package est déjà présent dans installed.packages()
# mais dont une dépendance a été supprimée ou n'a jamais été installée
# (scénario courant en dehors des environnements conteneurisés comme Onyxia).
charger_package <- function(pkg) {
  tryCatch(
    library(pkg, character.only = TRUE),
    error = function(e) {
      message("⚠ Échec du chargement de '", pkg,
              "' — réinstallation avec dépendances…")
      install.packages(pkg, dependencies = TRUE)
      library(pkg, character.only = TRUE)
    }
  )
}

# invisible() évite d'afficher un message de confirmation dans la console pour
# chaque package chargé. lapply() est une boucle compacte qui applique la
# fonction charger_package() à chaque élément de la liste packages_requis.
invisible(lapply(packages_requis, charger_package))

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
# Sous-dossiers de sortie
DIR_CACHE   <- file.path(DIR_OUTPUT, "cache")
DIR_PERSIST <- file.path(DIR_OUTPUT, "persist")
DIR_CARTES  <- file.path(DIR_OUTPUT, "cartes")
DIR_EXPORTS <- file.path(DIR_OUTPUT, "exports")
DIR_RASTERS <- file.path(DIR_OUTPUT, "rasters")

# Création de tous les sous-dossiers
for (d in c(DIR_CACHE, DIR_PERSIST, DIR_CARTES, DIR_EXPORTS, DIR_RASTERS)) {
  dir.create(d, showWarnings = FALSE, recursive = TRUE)
}

# URL publique et stable pour le PBF Rwanda (date fixe = reproductibilité)
# Si on veut utiliser les données les plus à jour, utiliser le lien suivant : 
# https://download.geofabrik.de/africa/rwanda-latest.osm.pbf
GEOFABRIK_PBF_URL <- "https://download.geofabrik.de/africa/rwanda-260315.osm.pbf"

chemin_pbf     <- "rwanda-260315.osm.pbf"  # Nom local du fichier PBF après téléchargement

# WorldPop a réorganisé plusieurs fois son arborescence.
# On teste les URLs candidates dans l'ordre jusqu'à en trouver une valide.
# La première URL est la structure 2024 ; les suivantes sont des
# fallbacks vers les structures antérieures.
WORLDPOP_URLS_CANDIDATES <- c(
  # Structure actuelle — constrained, ajusté UN, 100m
  paste0("https://data.worldpop.org/GIS/Population/",
         "Global_2000_2020_Constrained/2020/BSGM/RWA/",
         "rwa_ppp_2020_UNadj_constrained.tif"),
  # Structure alternative — unconstrained
  paste0("https://data.worldpop.org/GIS/Population/",
         "Global_2000_2020/2020/RWA/rwa_ppp_2020_UNadj.tif"),
  # Structure via wopr (WorldPop Open Population Repository)
  paste0("https://wopr.worldpop.org/data/",
         "RWA/population/v1.0/",
         "RWA_population_v1_0_gridded.tif")
)

# Chemin local du raster WorldPop si déjà téléchargé 
WORLDPOP_LOCAL_PATH <- file.path(DIR_RASTERS, "worldpop_rwanda_100m.tif")

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

# Chemin local du ZIP et du CSV
RWI_CSV_LOCAL  <- "data/raw/rwa_relative_wealth_index.csv"   
RWI_ZIP_LOCAL  <- "data/raw/rwi_all_countries.zip"          

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
NISR_COL_DISTRICT  <- "ADM2_FR"   # Nom du district en français
NISR_COL_PROVINCE  <- "ADM1_FR"   # Nom de la province en français
NISR_COL_POP_TOTAL <- "T_TL"      # Population totale 

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

# Part du PIB considérée comme échangeable entre zones, par secteur.
# Plus la valeur est élevée, plus ce secteur génère de fret interzonal.
# Calibrage sur la structure économique rwandaise :
#   Agriculture (0.20)    : forte autoconsommation locale, marchés peu intégrés
#   Mines (0.70)          : quasi-totalité exportée vers Kigali ou à l'international
#   Construction (0.15)   : matériaux issus de carrières de proximité, très locaux
#   Industrie (0.55)      : manufactures légères distribuées à l'échelle nationale
#   Transport (0.60)      : service structurellement interzonal par définition
PART_ECHANGEABLE_SECTEUR <- c(
  Agriculture    = 0.20,
  Mines          = 0.70,
  Agro_industrie = 0.45,
  Industrie      = 0.55,
  Construction   = 0.15,
  Commerce       = 0.40,
  Transport      = 0.60,
  Services       = 0.30
)

# Facteurs multiplicatifs sur la part échangeable selon la composition du sol.
# Interprétation : une zone entièrement industrielle (p_ind = 1) voit sa part
# échangeable multipliée par FACTEUR_ECHANGEABLE_LANDUSE_INDUSTRIEL.
# L'effet est interpolé linéairement : p_ind = 0.30 → facteur = 1 + 0.30 × (1.30 − 1) = 1.09.
# Mettre à 1.0 pour désactiver la modulation par usage du sol.
FACTEUR_ECHANGEABLE_LANDUSE_INDUSTRIEL <- 1.30   # +30% pour zones purement industrielles
FACTEUR_ECHANGEABLE_LANDUSE_URBAIN     <- 1.15   # +15% pour zones purement urbaines

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

# ── Paramètres du modèle gravitaire doublement contraint ──────────────────────
# Ces paramètres contrôlent l'algorithme de Furness (IPF) qui calcule les
# facteurs d'équilibrage A_i et B_j permettant de respecter exactement les
# contraintes sur les flux sortants (offre) et entrants (demande).

# Nombre maximal d'itérations de l'algorithme de Furness.
# En pratique, la convergence est atteinte en 20-50 itérations sur des matrices
# bien conditionnées (pas de zones isolées avec offre ou demande nulle).
# 200 itérations est une sécurité pour les cas dégradés (matrices creuses).
FURNESS_MAX_ITER <- 200

# Seuil de convergence : erreur relative maximale tolérée sur les contraintes.
# L'algorithme s'arrête quand (|sum_j T_ij - target_O_i| / target_O_i) < TOL
# pour toutes les origines i, et idem pour les colonnes (destinations j).
# 1e-6 = 0.0001% d'erreur — suffisant pour que les flux soient économiquement
# indiscernables de la solution exacte.
FURNESS_TOL <- 1e-6

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

# ==============================================================================
# Prix du carburant — Source : RURA (Rwanda Utilities Regulatory Authority)
# RURA fixe le prix du diesel à la pompe chaque mois (www.rura.rw > Fuel Prices).
# Q2 2022 : 1 291 RWF/L ÷ 1 033 RWF/USD ≈ 1.25 USD/L
# Q4 2022 : 1 463 RWF/L ÷ 1 048 RWF/USD ≈ 1.40 USD/L  ← valeur retenue (moyenne annuelle)
# Ce paramètre est utilisé dans params_flotte_df (I.5) pour tous les types de véhicules.
# ==============================================================================

PRIX_CARBURANT_USD_L <- 1.40  # Diesel Rwanda, source : RURA 2022

# ==============================================================================
# Production totale par secteur — Source : Banque Mondiale (wbstats)
# production_totale = production BRUTE (output brut), calculée en divisant la
# valeur ajoutée (VA) sectorielle par la fraction de VA dans l'output :
#   output_j = VA_j / (1 - Σ_i A[i,j])
# Cette formule assure la cohérence avec la matrice A dans le modèle de Leontief :
#   A %*% output_brut donne les consommations intermédiaires attendues.
#
# Téléchargement automatique via wbstats (nécessite une connexion internet).
# En cas d'échec, les valeurs WB_VA_FALLBACK (BM Rwanda 2022 pré-calculées) sont utilisées.
#
# Indicateurs BM utilisés :
#   NV.AGR.TOTL.CD  : VA Agriculture, sylviculture, pêche (USD courants)
#   NV.IND.MANF.CD  : VA Industrie manufacturière (USD courants)
#   NV.IND.TOTL.CD  : VA Industrie totale incl. construction (USD courants)
#   NV.SRV.TOTL.CD  : VA Services (USD courants)
#   NY.GDP.MKTP.CD  : PIB (USD courants)
#
# Désagrégation appliquée (la BM ne publie pas ce niveau de détail) :
#   Manufacturier     → Agro_industrie (45%) + Industrie (55%) [parts RPHC5 2022]
#   Industrie_non_mfg → Mines (34%) + Construction (66%)
#     [parts calées sur Rwanda 2022 : Mines 245 M USD, Construction 474 M USD]
#   Services          → Commerce (22%) + Transport (12%) + Services_résiduel (66%)
#     [parts calées sur la structure EAC, Rwanda Economic Update 2022]
# ==============================================================================

# Chemin du cache local (évite de re-télécharger à chaque session)
WB_CACHE_PATH <- file.path(DIR_CACHE, "wb_rwanda_secteurs.rds")

# Valeurs de repli VA en M USD (Rwanda 2022, Banque Mondiale)
# Utilisées si wbstats est indisponible ou si le téléchargement échoue.
WB_VA_FALLBACK <- c(
  Agriculture    = 3294,  # NV.AGR.TOTL.CD Rwanda 2022
  Mines          =  245,  # (NV.IND.TOTL.CD − NV.IND.MANF.CD) × 0.34
  Agro_industrie =  340,  # NV.IND.MANF.CD (756 M USD) × 0.45 (RPHC5)
  Industrie      =  416,  # NV.IND.MANF.CD (756 M USD) × 0.55 (RPHC5)
  Construction   =  474,  # (NV.IND.TOTL.CD − NV.IND.MANF.CD) × 0.66
  Commerce       = 1399,  # NV.SRV.TOTL.CD (6 358 M USD) × 0.22 (structure EAC)
  Transport      =  763,  # NV.SRV.TOTL.CD × 0.12 (structure EAC)
  Services       = 4196   # NV.SRV.TOTL.CD × 0.66 (résiduel)
)

# Télécharge (ou recharge depuis le cache) les données VA sectorielles BM Rwanda.
# Retourne un data.frame avec une ligne par année, colonnes = indicateurs nommés.
telecharger_wb_va <- function() {
  if (file.exists(WB_CACHE_PATH)) {
    age_j <- as.numeric(difftime(Sys.time(), file.mtime(WB_CACHE_PATH), units = "days"))
    if (age_j < 90) {
      cat("    Cache BM utilisé (", round(age_j, 0), "j)\n", sep = "")
      return(readRDS(WB_CACHE_PATH))
    }
  }
  cat("    Téléchargement Banque Mondiale Rwanda…\n")
  res <- tryCatch(
    wbstats::wb_data(
      indicator  = c(
        agri  = "NV.AGR.TOTL.CD",
        manuf = "NV.IND.MANF.CD",
        indus = "NV.IND.TOTL.CD",
        serv  = "NV.SRV.TOTL.CD",
        gdp   = "NY.GDP.MKTP.CD"
      ),
      country    = "RWA",
      start_date = 2020, end_date = 2023
    ),
    error = function(e) { message("    ⚠ wbstats : ", conditionMessage(e)); NULL }
  )
  if (is.null(res)) return(NULL)
  res_ok <- res[!is.na(res$gdp), ]
  if (nrow(res_ok) == 0) return(NULL)
  res_ok <- res_ok[order(-res_ok$date), ][1L, ]
  saveRDS(res_ok, WB_CACHE_PATH)
  cat("    ✓ Rwanda ", res_ok$date, " — PIB ", round(res_ok$gdp / 1e9, 1), " Md USD\n", sep = "")
  res_ok
}

# Calcule l'output brut à partir des VA sectorielles via l'inverse de Leontief.
# Formule : output = (I - A)^{-1} × va
# Justification : le modèle (03_transport.R) calcule conso_interm = A %*% output
# et valeur_ajoutee = output - conso_interm = (I - A) × output.
# En posant (I - A) × output = va_cible, on obtient output = (I - A)^{-1} × va_cible,
# ce qui garantit que la BM et le modèle donnent exactement les mêmes VA sectorielles.
calculer_output_brut <- function(va) {
  leontief_inv <- solve(diag(N_SECTEURS) - A)
  setNames(round(as.vector(leontief_inv %*% va)), SECTEURS)
}

cat("  → Données Banque Mondiale (production_totale)…\n")
wb_rwa <- telecharger_wb_va()

if (!is.null(wb_rwa)) {
  agri_m       <- wb_rwa$agri[1]  / 1e6
  manuf_m      <- wb_rwa$manuf[1] / 1e6
  indus_m      <- wb_rwa$indus[1] / 1e6
  serv_m       <- wb_rwa$serv[1]  / 1e6

  # Industrie hors manufacturier = Mines + Construction + Utilities (résidu)
  # Parts calées sur Rwanda 2022 : Mines 34%, Construction 66%
  non_manuf_m  <- max(0, indus_m - manuf_m)
  mines_m      <- non_manuf_m * 0.34
  const_m      <- non_manuf_m * 0.66

  va_wb <- c(
    Agriculture    = agri_m,
    Mines          = mines_m,
    Agro_industrie = manuf_m * 0.45,
    Industrie      = manuf_m * 0.55,
    Construction   = const_m,
    Commerce       = serv_m * 0.22,
    Transport      = serv_m * 0.12,
    Services       = serv_m * 0.66
  )
  production_totale <- calculer_output_brut(va_wb)
  cat("  ✓ production_totale : Banque Mondiale", wb_rwa$date, "\n")
} else {
  production_totale <- calculer_output_brut(WB_VA_FALLBACK)
  cat("  ✓ production_totale : valeurs de repli BM Rwanda 2022\n")
}

# Facteurs de conversion output brut → masse de fret (tonnes par million USD)
# Calibrés sur :
#   Agriculture    : FAOSTAT Rwanda 2022 — ~7,2 Mt production totale / ~4 390 M USD output
#   Mines          : RMB Annual Report 2022 — ~730 kt (3T + carrières) / ~318 M USD output
#   Autres secteurs: rescaling cohérent avec les nouveaux outputs bruts BM 2022,
#                    visant à conserver une masse totale de fret plausible (~20 Mt/an).
TONNES_PAR_musd <- c(
  Agriculture    = 1600,   # FAOSTAT 2022 : ~7,2 Mt / ~4 390 M USD (bananes, céréales, café, thé)
  Mines          = 2300,   # RMB 2022 : ~730 kt (coltan, cassitérite, wolfram + carrières)
  Agro_industrie = 1800,   # Transformation alimentaire (farine, boissons, huiles, sucre)
  Industrie      = 1100,   # Manufactures légères (textiles, emballages, matériaux)
  Construction   = 9000,   # Agrégats, ciment, acier : matériaux très lourds / faible valeur
  Commerce       = 750,    # Mix de biens distribués ; valeur unitaire plus élevée que bruts
  Transport      = 130,    # Secteur de services : fret physique marginal
  Services       = 35      # Quasi-immatériel (finance, conseil, éducation, santé)
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
DESCRIPTION_SCENARIO  <- "Rupture de la RN1 entre Kigali et Huye suite à une inondation"
DUREE_JOURS           <- 14
TYPE_EVENEMENT        <- "inondation"

# Choix du mode de création de scénario :
#     Mode A — Manuel       : liste d'osm_id ou de coordonnées GPS fournie à la main
#     Mode B — Buffer zone  : toutes les routes dans un rayon autour d'un point
#     Mode C — Raster risque: intersection avec un raster (grille) externe
# Si plusieurs méthodes sont choisies, les routes affectées seront celles de l'union des méthodes

# Mettre l'identifiant OSM de la ou des routes affectées
OSM_IDS_PERTURBES_MANUEL <- c(479687569)

# Nom du scénario — généré automatiquement depuis les noms OSM des arêtes perturbées.
# Pour forcer un nom différent, remplacer NULL par une chaîne entre guillemets.
# Exemple : NOM_SCENARIO <- "Mon_scenario_custom"
#
# Fallback "Scenario_default" si DuckDB n'est pas encore disponible (session fraîche
# avant le premier lancement de 01_reseau.R).
NOM_SCENARIO <- tryCatch({
  if (!file.exists(DB_PATH)) stop("DuckDB absent")
  con_tmp <- DBI::dbConnect(duckdb::duckdb(), dbdir = DB_PATH, read_only = TRUE)
  # Pas de shutdown = TRUE ici : on ferme uniquement cette connexion temporaire,
  # sans arrêter l'instance DuckDB (con sera ouvert ensuite).
  on.exit(try(DBI::dbDisconnect(con_tmp), silent = TRUE), add = TRUE)
  noms <- DBI::dbGetQuery(
    con_tmp,
    sprintf(
      "SELECT DISTINCT name FROM routes_attrs_raw
       WHERE CAST(osm_id AS BIGINT) IN (%s) AND name IS NOT NULL AND name <> ''",
      paste(OSM_IDS_PERTURBES_MANUEL, collapse = ", ")
    )
  )$name
  DBI::dbDisconnect(con_tmp)
  if (length(noms) == 0) stop("aucun nom OSM trouvé")
  noms_clean <- gsub("[^[:alnum:]_]", "", gsub("\\s+", "_", trimws(noms)))
  paste(c(TYPE_EVENEMENT, noms_clean), collapse = "_")
}, error = function(e) "Scenario_default")
# Pour les activer : mettre TRUE, sinon : mettre FALSE
UTILISER_MODE_BUFFER        <- FALSE  
UTILISER_MODE_RASTER        <- FALSE

# Coordonnées du centre de la zone perturbée du mode buffer 
CENTRE_PERTURBATION_LON <- 29.950   # Est-Ouest
CENTRE_PERTURBATION_LAT <- -2.150   # Nord-Sud

# Nombre d'arêtes candidates testées pour l'analyse de criticité
N_TOP_ARETES_CRITIQUES <- 50

# Pour accélérer le calcul de criticité, on ne recalcule que les paires OD avec un volume de fret
# supérieur à un seuil (SEUIL_PAIRES_CRITICITE), ce qui exclut les paires
# marginales qui ne changent pas le classement de criticité.
SEUIL_PAIRES_CRITICITE <- 100   # tonnes 

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

RESET_CACHES <- FALSE  # ← passer à TRUE pour tout recalculer depuis zéro

if (RESET_CACHES) {
  
  caches <- c(
    file.path(DIR_CACHE, "reseau_corrige_cache.rds"),
    file.path(DIR_CACHE, "pentes_cache.rds"),
    file.path(DIR_CACHE, "landuse_cache.rds"),
    file.path(DIR_CACHE, "od_cache.rds"),
    file.path(DIR_CACHE, "affectation_cache.rds")
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
# I.2 : Environnement séparé pour les gros objets
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
# I.3 : Connexion DuckDB et fonctions utilitaires
# Ouvre la base analytique persistante et définit les raccourcis duck_write()
# et duck_query() utilisés dans toutes les parties suivantes.
# ==============================================================================

# DuckDB est une base de données SQL embarquée : elle fonctionne directement
# dans R sans avoir besoin d'un serveur séparé. On peut lui envoyer des requêtes 
# SQL pour manipuler des tableaux de données très efficacement — plus vite que 
# des boucles R sur de grands volumes. Le fichier "reseau_rwanda.duckdb" stocke 
# toutes les tables sur le disque, ce qui permet de reprendre le travail sans 
# recalculer depuis zéro.

# Fermeture propre de la connexion à DuckDB afin de la rouvrir ensuite proprement.
if (exists("con")) {
  tryCatch(
    DBI::dbDisconnect(con, shutdown = FALSE),
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
# I.4 : Palettes de couleurs centralisées
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

# ── Palette d'émissions (vert pâle → rouge foncé) ─────────────────────────────
# Rouge = route très émettrice (pente forte + mauvaise surface + véhicule lourd)
# Vert  = route peu émettrice (plat, bitumée, camion léger)
PALETTE_EMISSIONS <- c("#1A9850", "#91CF60", "#FEE08B", "#FC8D59", "#D73027")

# ── Secteurs économiques (Set2, dans l'ordre de SECTEURS) ─────────────────────
# Centralisé ici pour garantir que chaque secteur a toujours la même couleur
# dans tous les graphiques et cartes (barres, trajectoires, Sankey, carte dominante).
PALETTE_SECTEURS <- setNames(
  RColorBrewer::brewer.pal(N_SECTEURS, "Set2"),
  SECTEURS
)

cat("✓ Palettes de couleurs définies\n\n")


# ==============================================================================
# I.5 : Paramètres de la flotte de véhicules
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
  ~vehicule_id,   ~nom,                    ~conso_base, ~facteur_paved, ~facteur_gravel, ~facteur_unpaved, ~facteur_conso_pente, ~prix_carburant, ~valeur_temps, ~usure_paved, ~usure_gravel, ~usure_unpaved, ~capacite_tonnes, ~facteur_urbain, ~facteur_emission_co2, ~facteur_emission_nox, ~facteur_emission_pm25, ~cout_chargement_usd, ~cout_dechargement_usd,
  "camionnette",  "Camionnette (<3.5t)",    10,          1.00,           1.08,            1.18,             1.0,                  PRIX_CARBURANT_USD_L,            4.5,           0.02,         0.04,          0.07,            3.0,              1.05,            2.68,                  0.25,                  0.040,                  15,                   15,
  "camion_moyen", "Camion moyen (5-10t)",   20,          1.00,           1.15,            1.30,             1.5,                  PRIX_CARBURANT_USD_L,            7.5,           0.05,         0.08,          0.12,            7.5,              1.25,            2.68,                  0.50,                  0.065,                  25,                   25,
  "camion_lourd", "Camion lourd (>10t)",    35,          1.00,           1.25,            1.50,             2.0,                  PRIX_CARBURANT_USD_L,            10.0,          0.08,         0.14,          0.22,            20.0,             1.60,            2.68,                  0.80,                  0.090,                  40,                   40
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
VEHICULE_REFERENCE   <- "camion_lourd"

# Récupérer les ids pour les boucles de cartographie (Partie 10)
VEHICULES_IDS <- duck_query("SELECT vehicule_id, nom FROM params_flotte")

cat("✓ Flotte chargée dans DuckDB :",
    nrow(VEHICULES_IDS), "véhicules —",
    paste(VEHICULES_IDS$vehicule_id, collapse = ", "), "\n\n")

# ── Chemins des fichiers de persistance inter-scripts ─────────────────────────
PERSIST_GEODATA      <- file.path(DIR_PERSIST, "persist_geodata.rds")
PERSIST_RESEAU_BASE  <- file.path(DIR_PERSIST, "persist_reseau_base.rds")
PERSIST_ENTREPOSAGES <- file.path(DIR_PERSIST, "persist_entreposages.rds")
PERSIST_RESEAU_COUTS <- file.path(DIR_PERSIST, "persist_reseau_couts.rds")
PERSIST_GRAPHE_MM    <- file.path(DIR_PERSIST, "persist_graphe_mm.rds")
PERSIST_MAPPING_MM   <- file.path(DIR_PERSIST, "persist_mapping_mm.rds")
PERSIST_FLUX_FRET    <- file.path(DIR_PERSIST, "persist_flux_fret.rds")
PERSIST_RESEAU_FRET  <- file.path(DIR_PERSIST, "persist_reseau_fret.rds")
PERSIST_VULNERAB     <- file.path(DIR_PERSIST, "persist_vulnerabilite.rds")
PERSIST_ARIO         <- file.path(DIR_PERSIST, "persist_ario.rds")
PERSIST_DIAG_RES     <- file.path(DIR_PERSIST, "persist_diag_reseau.rds")

cat("✓ 00_parametres.R chargé\n")