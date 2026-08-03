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
  "ggpattern",     # Remplissages hachurés (composition sectorielle : exports/imports vs domestique)
  "RColorBrewer",  # Palettes de couleurs pour les cartes et graphiques sectoriels
  "readxl",        # Lecture des fichiers Excel (.xlsx) — utilisé pour la SAM IFPRI 2021
  "lhs"            # Plans d'expérience par hypercube latin (run_sensibilite.R)
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
# géographiques comme le DEM (modèle d'élévation) ou le PBF (données OSM du pays).
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
# CHOIX DU MODE
# Regroupe TOUS les interrupteurs de mode du modèle (quel comportement activer).
# ==============================================================================

# ── Affectation du fret (module 03) ───────────────────────────────────────────
# Congestion : TRUE = affectation à l'équilibre (BPR sur le temps + itérations
#   MSA, reroutage selon la saturation V/C) ; FALSE = All-or-Nothing
CONGESTION         <- TRUE

# Prise en compte de la congestion dans l'analyse de vulnérabilité (module 05) :
#   TRUE  = le réseau dégradé est RÉ-ÉQUILIBRÉ (méthode BPR/MSA) — le
#           trafic se reporte sur les routes restantes et les re-congestionne ;
#           la référence OD et l'analyse de criticité sont elles aussi évaluées
#           sur les poids congestionnés d'équilibre (congestion statique de base
#           pour la criticité, un re-MSA par arête testée étant infaisable).
#   FALSE = coûts LIBRES (charge nulle), référence = od_cache.
CONGESTION_VULNERABILITE <- TRUE

# ── Scénario de vulnérabilité (module 04) ─────────────────────────────────────
# Mode(s) de définition de la perturbation. On peut en activer plusieurs : les
#   arêtes perturbées sont l'UNION des modes actifs. Le mode manuel (liste
#   OSM_IDS_PERTURBES_MANUEL) est toujours appliqué en plus.
#     UTILISER_MODE_BUFFER : toutes les routes dans un rayon autour d'un point
#     UTILISER_MODE_RASTER : intersection avec un raster de risque externe
UTILISER_MODE_BUFFER <- FALSE
UTILISER_MODE_RASTER <- TRUE

# Mode de nommage du scénario : NULL = automatique (nom construit depuis les noms
#   OSM des arêtes perturbées) ; sinon une chaîne = nom manuel imposé.
NOM_SCENARIO_MANUEL  <- NULL

# ==============================================================================
# Pays étudié
# Utilisé dans les titres de cartes et messages console.
# ==============================================================================

NOM_PAYS <- "Rwanda"

# ==============================================================================
# TESTS DE SENSIBILITÉ — déclaration du scénario
# ==============================================================================
# OBJECTIF : pouvoir relancer tout le modèle en modifiant un ou plusieurs
# paramètres (betas gravitaires, valeur du temps, conversion valeur→tonnes…)
# SANS écraser les cartes, graphiques et exports du run de référence.
#
# FONCTIONNEMENT
#   - SCENARIO_ID       : identifiant technique. "reference" = run normal.
#                         Toute autre valeur bascule le run en mode sensibilité :
#                         les sorties partent dans un sous-dossier dédié et les
#                         figures reçoivent une mention "Test de sensibilité".
#   - SCENARIO_LIBELLE  : phrase lisible affichée sur les figures.
#   - SENSIBILITE       : liste nommée des paramètres à surcharger. La surcharge
#                         est appliquée TOUT À LA FIN de ce fichier (bloc final),
#                         une fois que tous les paramètres ont été définis.
#                         Chaque élément peut être :
#                           • une VALEUR      → remplace le paramètre
#                           • une FONCTION    → reçoit la valeur actuelle et
#                                               renvoie la nouvelle (pratique
#                                               pour les variations relatives)
#
# EXEMPLE (à écrire dans run_sensibilite.R, pas ici) :
#   SCENARIO_ID      <- "beta_plus20"
#   SCENARIO_LIBELLE <- "Betas gravitaires +20 %"
#   SENSIBILITE      <- list(BETA_SECTEUR = function(b) b * 1.2)
#
# Les trois objets ne sont définis ici QUE s'ils n'existent pas déjà : cela
# permet à run_sensibilite.R de les fixer AVANT de sourcer 00_parametres.R.
# ==============================================================================

if (!exists("SCENARIO_ID"))      SCENARIO_ID      <- "reference"
if (!exists("SCENARIO_LIBELLE")) SCENARIO_LIBELLE <- NULL
if (!exists("SENSIBILITE"))      SENSIBILITE      <- list()

# Drapeau utilisé partout en aval : TRUE dès que l'on n'est plus sur la référence
EST_SENSIBILITE <- !identical(SCENARIO_ID, "reference")

# Libellé de repli : si aucun libellé lisible n'est fourni, on affiche l'ID
if (EST_SENSIBILITE && is.null(SCENARIO_LIBELLE)) SCENARIO_LIBELLE <- SCENARIO_ID

# Suffixe ajouté au nom des fichiers de figures ("" en référence).
SUFFIXE_SCENARIO <- if (EST_SENSIBILITE) paste0("_", SCENARIO_ID) else ""

# Mention portée par toutes les figures du scénario (NULL en référence).
MENTION_SENSIBILITE <- if (EST_SENSIBILITE) {
  paste0("TEST DE SENSIBILITÉ — ", SCENARIO_LIBELLE,
         " (les valeurs diffèrent du run de référence)")
} else NULL

# ==============================================================================
# Chemins et fichiers
# ==============================================================================

DB_PATH        <- "reseau.duckdb"   # Fichier DuckDB persistant
DIR_OUTPUT     <- "outputs"                # Dossier de sortie de tous les fichiers
# Sous-dossiers de sortie
DIR_CACHE   <- file.path(DIR_OUTPUT, "cache")
DIR_PERSIST <- file.path(DIR_OUTPUT, "persist")
DIR_CARTES  <- file.path(DIR_OUTPUT, "cartes")
DIR_EXPORTS <- file.path(DIR_OUTPUT, "exports")
DIR_RASTERS <- file.path(DIR_OUTPUT, "rasters")

# ── Redirection des sorties en mode sensibilité ───────────────────────────────
# Tout ce qui DÉPEND des paramètres surchargés est isolé dans un sous-dossier
# propre au scénario, pour que le run de référence reste intact :
#   outputs/cartes/sensibilite/<id>/   figures et graphiques
#   outputs/exports/sensibilite/<id>/  exports CSV / Parquet / GeoPackage
#   outputs/persist/sensibilite/<id>/  objets intermédiaires entre modules
#   outputs/cache/sensibilite/<id>/    caches OD et affectation
#
# En revanche DIR_CACHE (caches lourds du module 01 : réseau corrigé, pentes,
# landuse, RWI) reste PARTAGÉ : ces caches décrivent la géographie du Rwanda et
# ne dépendent d'aucun paramètre économique. C'est ce partage qui rend un test
# de sensibilité rapide (~25 min économisées sur le module 01).
DIR_CACHE_SCENARIO <- DIR_CACHE   # caches OD / affectation (dépendants des paramètres)

if (EST_SENSIBILITE) {
  DIR_CARTES_REF     <- DIR_CARTES                                        # référence
  DIR_CARTES         <- file.path(DIR_CARTES,  "sensibilite", SCENARIO_ID)
  DIR_EXPORTS        <- file.path(DIR_EXPORTS, "sensibilite", SCENARIO_ID)
  DIR_PERSIST_REF    <- DIR_PERSIST                                        # référence
  DIR_PERSIST        <- file.path(DIR_PERSIST, "sensibilite", SCENARIO_ID)
  DIR_CACHE_SCENARIO <- file.path(DIR_CACHE,   "sensibilite", SCENARIO_ID)
}

# Création de tous les sous-dossiers
for (d in c(DIR_CACHE, DIR_CACHE_SCENARIO, DIR_PERSIST, DIR_CARTES,
            DIR_EXPORTS, DIR_RASTERS)) {
  dir.create(d, showWarnings = FALSE, recursive = TRUE)
}

# ── Amorçage du dossier persist du scénario ───────────────────────────────────
# Les modules lisent et écrivent les mêmes chemins PERSIST_*. Si l'on ne relance
# pas le module 01 (cas normal : la géographie ne change pas), les objets qu'il
# produit manqueraient au scénario. On copie donc depuis la référence tout
# fichier persist absent : les modules effectivement relancés (02→05) écraseront
# ensuite leur propre copie, sans jamais toucher aux fichiers de la référence.
if (EST_SENSIBILITE && dir.exists(DIR_PERSIST_REF)) {
  .a_copier <- setdiff(
    list.files(DIR_PERSIST_REF, pattern = "\\.rds$"),
    list.files(DIR_PERSIST,     pattern = "\\.rds$")
  )
  if (length(.a_copier) > 0) {
    file.copy(file.path(DIR_PERSIST_REF, .a_copier), DIR_PERSIST)
    cat("  ✓ Scénario", SCENARIO_ID, ": ", length(.a_copier),
        "fichier(s) persist copié(s) depuis la référence\n")
  }
  rm(.a_copier)

  # Cas particulier : persist_fond_carte.rds est le seul objet persistant écrit
  # par 01_reseau dans DIR_CARTES (et non DIR_PERSIST). Les scripts viz_*.R le
  # relisent depuis DIR_CARTES ; comme 01 n'est pas relancé en sensibilité et que
  # DIR_CARTES du scénario est créé vide, ce fichier manquerait et readRDS
  # échouerait ("cannot open the connection"). On le copie donc depuis la
  # référence s'il est absent — le fond de carte est purement géographique et
  # ne dépend d'aucun paramètre économique testé.
  .fond_ref <- file.path(DIR_CARTES_REF, "persist_fond_carte.rds")
  .fond_sc  <- file.path(DIR_CARTES,     "persist_fond_carte.rds")
  if (file.exists(.fond_ref) && !file.exists(.fond_sc)) {
    file.copy(.fond_ref, .fond_sc)
    cat("  ✓ Scénario", SCENARIO_ID, ": persist_fond_carte.rds copié depuis la référence\n")
  }
  rm(.fond_ref, .fond_sc)
}

# URL publique et stable du PBF OSM (date fixe = reproductibilité).
# ⚠ À adapter selon le pays : https://download.geofabrik.de/<continent>/<pays>.osm.pbf
# Version à jour : https://download.geofabrik.de/africa/rwanda-latest.osm.pbf
GEOFABRIK_PBF_URL <- "https://download.geofabrik.de/africa/rwanda-260315.osm.pbf"

chemin_pbf <- "rwanda-260315.osm.pbf"  # Nom local du fichier PBF — à adapter selon le pays

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
WORLDPOP_LOCAL_PATH <- file.path(DIR_RASTERS, "worldpop_100m.tif")

# Chemin du fichier NISR
NISR_CSV_PATH <- "data/raw/rwa_admpop_adm2_2023.csv"


# URL fichier RWI
RWI_ZIP_URL <- paste0(
  "https://data.humdata.org/dataset/",
  "76f2a2ea-ba50-40f5-b79c-db95d668b843/resource/",
  "de2f953e-940c-43bb-b1f8-4d02d28124b5/download/",
  "relative-wealth-index-april-2021.zip"
)

# Nom du fichier pays dans le ZIP (convention ISO3 en majuscules — à adapter selon le pays)
RWI_FICHIER <- "RWA_relative_wealth_index.csv"

# Chemin local du ZIP et du CSV
RWI_CSV_LOCAL  <- "data/raw/rwa_relative_wealth_index.csv"   
RWI_ZIP_LOCAL  <- "data/raw/rwi_all_countries.zip"          

# ── Raster d'aléa inondation (Mode C de l'analyse de vulnérabilité) ───────────
#
# Source : cartes d'aléa fluvial mondiales du JRC / Copernicus EMS (GloFAS),
#   produites par les modèles LISFLOOD (hydrologie) et LISFLOOD-FP (hydraulique)
#   sur MERIT-DEM. Résolution 3 arc-sec (~90 m), valeurs = hauteur d'eau en
#   MÈTRES, licence CC-BY 4.0.
#   Citation : Baugh, Colonese, D'Angelo, Dottori, Neal, Prudhomme, Salamon
#   (2024), « Modelled flood inundation for different return period scenarios
#   at the global scale », European Commission, Joint Research Centre.
#
# Les fichiers sont produits par preparer_raster_inondation.R, qui mosaïque les
# deux tuiles couvrant le Rwanda, découpe sur l'emprise du pays et retire les
# eaux permanentes (lac Kivu, lacs de l'Akagera) ainsi que les zones de
# profondeur aberrante signalées par le JRC.
#
# Période de retour retenue pour le scénario de rupture. Les trois valeurs
# disponibles définissent une gradation d'intensité :
#   10  → crue fréquente     (~163 arêtes exposées, ~101 km)
#   100 → crue centennale    (~211 arêtes exposées, ~143 km)
#   500 → crue extrême       (~231 arêtes exposées, ~160 km)
GLOFAS_PERIODE_RETOUR       <- 100

CHEMIN_RASTER_RISQUE        <- sprintf(
  "data/raw/zones_inondables_rwanda_glofas_rp%03d.tif", GLOFAS_PERIODE_RETOUR)

# ==============================================================================
# Paramètres DEM (Modèle Numérique de Terrain)
# ==============================================================================

DEM_ZOOM          <- 9      # Niveau de zoom elevatr (~300 m/pixel) 
DEM_ESPACEMENT_M  <- 100    # Pas d'échantillonnage le long des arêtes (mètres)
DEM_ALTITUDE_MIN  <- 800    # Altitude minimale réaliste dans le pays (m) — à adapter
DEM_ALTITUDE_MAX  <- 4600   # Altitude maximale réaliste dans le pays (m) — à adapter

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

# ── Rayon d'agglomération des entrepôts (m) ──────────────────────────────────
# Sert à DEUX usages :
#   (a) calcul de la population TEMPORAIRE de chaque point candidat (somme des
#       habitants dans un cercle de ce rayon) — utilisée uniquement pour classer
#       les points lors de la fusion ;
#   (b) seuil de FUSION : deux entrepôts distants de moins de ce rayon sont
#       agglomérés (priorité donnée à l'entrepôt le plus peuplé).
RAYON_AGGLO_ENTREPOT_M <- 4000

# Distance maximale pour écarter un point OSM trop proche d'un entrepôt déjà
# retenu LORS DE LA CONSTRUCTION du jeu de candidats (Partie II/IV.3). Distinct
# de RAYON_AGGLO_ENTREPOT_M, qui agit plus tard sur le jeu déjà assemblé.
DISTANCE_DEDUP_VILLES_M     <- 3000
DISTANCE_DEDUP_INDUSTRIEL_M <- 2000
DISTANCE_DEDUP_RETAIL_M     <- 1000

# Buffer autour de la frontière nationale pour inclure les villes frontalières (m)
BUFFER_FRONTIERE_VILLES_M <- 5000

# ── Entrepôts manuels ─────────────────────────────────────────────────────────
# Zones économiques positionnées manuellement par leurs coordonnées GPS
# (lon = longitude, lat = latitude en degrés décimaux WGS84).
# Ces zones sont les origines/destinations prioritaires du modèle de fret :
# hubs logistiques, postes frontières, villes structurantes, SEZ.
#
# pays = NA pour les zones internes au pays étudié, nom du pays pour les frontières.
# Utilisé en 03_transport.R pour associer les coûts pré-frontière.
# Les nœuds RoW (Rest of World) sont ajoutés séparément dans 03_transport.R
# et ne figurent pas ici car ils ne s'accrochent pas au réseau routier du pays.
entreposages_manuels <- tibble(
  nom  = c(
    "Kigali - Hub Central", "Kigali - SEZ Masoro", "Kigali - Marché Kimisagara",
    "Frontière Gatuna (Ouganda)", "Frontière Rusumo (Tanzanie)",
    "Frontière Rubavu/Goma (RDC)", "Frontière Kagitumba (Ouganda)",
    "Frontière Bugarama (Burundi)",
    "Huye (Butare) - Centre Sud", "Musanze - Centre Nord",
    "Rubavu - Centre Ouest", "Rusizi - Centre Sud-Ouest",
    "Bugesera SEZ (Agro-industrie)",
    "Muhanga", "Nyanza", "Rwamagana"
  ),
  type = c(
    "hub","sez","marche",
    "frontiere","frontiere","frontiere","frontiere",
    "frontiere","ville","ville","ville","ville",
    "sez","ville","ville","ville"
  ),
  # pays = NA pour les zones internes, nom du pays pour les frontières.
  # Utilisé pour associer les coûts pré-frontière en Partie 19
  pays = c(
    NA, NA, NA,
    "Ouganda", "Tanzanie", "RDC", "Ouganda",
    "Burundi",
    NA, NA, NA, NA,
    NA,
    NA, NA, NA
  ),
  lon = c(30.0619, 30.1300, 30.0588, 30.0890,
          30.7850, 29.2600, 30.7500, 29.0200,
          29.7388, 29.6333, 29.2650, 29.0100,
          30.1500, 29.7400, 29.7550, 30.4300),
  lat = c(-1.9536, -1.9000, -1.9700, -1.3800,
          -2.3800, -1.6667, -1.3100, -2.6200,
          -2.5965, -1.4992, -1.6750, -2.4900,
          -2.1000, -2.0850, -2.3500, -1.8700),
  source = "manuel"
)

# ==============================================================================
# Paramètres du graphe et de Dijkstra
# ==============================================================================

# Seuil de longueur minimale d'une arête pour ne pas être considérée dégénérée (m)
# Les arêtes en dessous de ce seuil sont supprimées après to_spatial_subdivision()
SEUIL_LONGUEUR_ARETE_M <- 0.5

# ==============================================================================
# Paramètres démographiques
# ==============================================================================

# Population minimale attribuée à une zone quand aucune des deux sources
# (WorldPop, NISR) ne fournit de valeur. Évite les zones à population = 0
# qui bloqueraient le modèle gravitaire (offre/demande nulle).
POP_FALLBACK_MIN <- 1000

# ── Zoom du raster WorldPop pour l'approche A ─────────────────────────────────
# Résolution disponible sur le portail WorldPop :
#   z=10 → ~100m/pixel (précis, fichier lourd ~200 Mo)
#   z=8  → ~400m/pixel (moins précis, fichier léger ~15 Mo)
WORLDPOP_ZOOM <- 9

# Noms attendus des colonnes dans le CSV NISR (à adapter selon le fichier réel)
# Ces noms correspondent au format typique des exports NISR data.gov.rw.
NISR_COL_DISTRICT  <- "ADM2_FR"   # Nom du district en français
NISR_COL_PROVINCE  <- "ADM1_FR"   # Nom de la province en français
NISR_COL_POP_TOTAL <- "T_TL"      # Population totale 

# ==============================================================================
# Paramètres RWI
# ==============================================================================

# ── Rayon d'estimation de la population d'une cellule RWI (m) ──────────────────
# Le RWI d'un nœud-entrepôt est la moyenne des cellules RWI tombant
# dans son polygone de Voronoï, PONDÉRÉE par la population de chaque cellule
# (cf. 01_reseau.R IV.5). Pour estimer cette population, on somme les pixels
# WorldPop dans un petit cercle autour de chaque point RWI. La maille RWI fait
# ~2,4 km ; on prend la demi-maille (~1,2 km) pour approcher la cellule sans
# empiéter sur les voisines.
BUFFER_POIDS_RWI_M <- 1200

# ==============================================================================
# Paramètres RPHC5 — Emploi sectoriel (profils d'offre empiriques)
# ==============================================================================

# Chemin vers le fichier d'emploi sectoriel par district.
RPHC5_EMPLOI_CSV_PATH   <- "data/raw/rwa_emploi_district_secteur_2022.csv"

# Second fichier d'emploi sectoriel par district, à la même structure mais
# d'une autre origine/vintage que RPHC5_EMPLOI_CSV_PATH. N'ALIMENTE AUCUN
# CALCUL du modèle (seul RPHC5_EMPLOI_CSV_PATH est lu par 01_reseau.R) — sert
# uniquement de point de comparaison dans viz_verif.R, les deux fichiers
# donnant des répartitions sectorielles très différentes par district et
# aucun n'étant une extraction confirmée du RPHC5 (cf. [[reference_sources_calibration]]).
RPHC5_EMPLOI_CSV_PATH_ALT <- "data/raw/rwa_emploi_district_secteur_2022_source_nationale.csv"

# Nom de la colonne "district" dans le fichier d'emploi (à adapter si besoin)
RPHC5_COL_DISTRICT_EMPLOI <- "District"

# Correspondance entre colonnes du CSV RPHC5 et secteurs du modèle.
# Format : "Nom_colonne_CSV" = list(Secteur_modele = part, ...)
# Les parts de chaque groupe doivent sommer à 1.
#
# Le RPHC5 ne distingue que 7 grands groupes d'emploi ; le modèle en utilise 11.
# Ventilation appliquée :
#   - Emploi_Agriculture → Agriculture vivrière + Cultures d'exportation (café/thé/tabac)
#   - Emploi_Industrie   → Agro-industrie + Manufactures + Chimie/pétrole + Énergie/eau
RPHC5_CORRESPONDANCE_SECTEURS <- list(
  Emploi_Agriculture  = list(Agriculture = 0.92, Cultures_export = 0.08),
  Emploi_Mines        = list(Mines = 1.0),
  Emploi_Industrie    = list(Agro_industrie = 0.45, Manufactures = 0.45,
                             Chimie_petrole = 0.05, Energie_eau = 0.05),
  Emploi_Construction = list(Construction = 1.0),
  Emploi_Commerce     = list(Commerce = 1.0),
  Emploi_Transport    = list(Transport = 1.0),
  Emploi_Services     = list(Services = 1.0)
)



# ==============================================================================
# Paramètres du modèle économique
# ==============================================================================

# Secteurs économiques modélisés (découpage orienté FRET, dérivé de la SAM).
# Ordre fixe. La matrice A et les agrégats sont recalculés automatiquement depuis
# la SAM via SAM_MAPPING_SECTEURS, donc changer cette liste impose seulement de
# maintenir cohérents : SAM_MAPPING_SECTEURS, VALEUR_RWF_PAR_TONNE,
# RPHC5_CORRESPONDANCE_SECTEURS, COMMERCE_EXTERIEUR_NISR, couts_prebordure_df.
SECTEURS <- c("Agriculture", "Cultures_export", "Mines", "Agro_industrie",
              "Chimie_petrole", "Manufactures", "Construction", "Commerce",
              "Transport", "Energie_eau", "Services")

N_SECTEURS <- length(SECTEURS)


# Chemin et feuille du fichier SAM
SAM_XLSX_PATH <- "data/raw/IFPRI_SAM_RWA_2021.xlsx"
SAM_FEUILLE   <- "SAM_2021"

# ── Correspondance des 41 comptes sectoriels de la SAM vers les 11 secteurs du modèle
# La clé est le suffixe à 4 lettres du code SAM (ex : « maiz » pour amaiz/cmaiz),
# commun aux comptes d'activités (préfixe « a ») et de commodités (préfixe « c »).
# Choix méthodologiques validés :
#   - Café/thé/cacao (coff) et Tabac (toba) → Cultures_export (cultures de rente).
#   - Chimie + pétrole (chem) → Chimie_petrole (vrac pétrolier importé).
#   - Textile/bois/minéraux non métalliques/métaux/machines/autres → Manufactures.
#   - Électricité/gaz (elec) et Eau/assainissement (watr) → Energie_eau (fret nul).
#   - Aliments transformés (food) et Boissons (beve) → Agro_industrie.
SAM_MAPPING_SECTEURS <- c(
  maiz = "Agriculture", rice = "Agriculture", ocer = "Agriculture", puls = "Agriculture",
  oils = "Agriculture", root = "Agriculture", vege = "Agriculture", sugr = "Agriculture",
  frui = "Agriculture", ocrp = "Agriculture", catt = "Agriculture", poul = "Agriculture",
  oliv = "Agriculture", fore = "Agriculture", fish = "Agriculture",
  coff = "Cultures_export", toba = "Cultures_export",
  mine = "Mines",
  food = "Agro_industrie", beve = "Agro_industrie",
  chem = "Chimie_petrole",
  text = "Manufactures", wood = "Manufactures", nmet = "Manufactures",
  metl = "Manufactures", mach = "Manufactures", oman = "Manufactures",
  cons = "Construction",
  trad = "Commerce",
  tran = "Transport",
  elec = "Energie_eau", watr = "Energie_eau",
  hotl = "Services", comm = "Services", fsrv = "Services", real = "Services",
  bsrv = "Services", padm = "Services", educ = "Services", heal = "Services",
  osrv = "Services"
)

# Comptes MÉNAGES de la SAM (consommation finale des ménages), ventilés par
# STRATE (r = rural, u = urbain) et QUINTILE de revenu (1 = plus pauvre … 5 = plus
# riche). Chaque compte « hhd-XY » porte un panier de consommation par commodité
# différent : les ménages ruraux pauvres consomment surtout de l'agriculture/
# agro-alimentaire, les ménages urbains aisés davantage de manufactures/services.
# Le suffixe « XY » (ex. « r1 », « u5 ») sert de clé de GROUPE dans la
# spatialisation de la demande finale par groupe (cf. 03_transport.R).
SAM_COMPTES_MENAGES <- c(
  "hhd-r1", "hhd-r2", "hhd-r3", "hhd-r4", "hhd-r5",
  "hhd-u1", "hhd-u2", "hhd-u3", "hhd-u4", "hhd-u5"
)

# Comptes de demande finale NON ménagère, SANS dimension revenu/urbain :
#   « gov » = consommation publique ; « s-i » = compte capital (FBCF + var. stocks).
SAM_COMPTES_DEMANDE_PUBLIQUE <- c("gov", "s-i")

# Comptes de la SAM lus en COLONNE et agrégés comme DEMANDE FINALE TOTALE :
# ménages + consommation publique (gov) + compte capital « s-i ».
# Les exportations (compte « row ») sont volontairement exclues — traitées via
# les entrepôts RoW (03_transport.R).
SAM_COMPTES_DEMANDE_FINALE <- c(SAM_COMPTES_MENAGES, SAM_COMPTES_DEMANDE_PUBLIQUE)

# Compte des MARGES de commerce et de transport de la SAM.
# Dans la SAM, l'offre d'une commodité (sa colonne) inclut une marge versée au
# compte « trc » ; ce compte reverse l'intégralité de ces marges à la commodité
# de Commerce (ctrad). Autrement dit, les marges = demande pour le secteur
# Commerce, et non pour la commodité sur laquelle elles portent.
SAM_COMPTE_MARGES <- "trc"

# Comptes de TAXES SUR LES PRODUITS de la SAM (lignes lues dans la colonne d'une
# commodité = taxe acquittée sur cette commodité). « stax » = taxe sur les ventes
# (TVA, accises), « mtax » = droits de douane à l'importation. Ces taxes sont un
# prélèvement fiscal (versé au gouvernement), pas un flux physique : elles sont
# exclues des flux de fret (cf. ramener la demande au prix de base ci-dessous).
SAM_COMPTES_TAXES_PRODUITS <- c("stax", "mtax")

# ── Fonction d'extraction de la SAM ───────────────────────────────────────────
# Lit le fichier Excel et agrège la matrice 110×110 vers les 11 secteurs du modèle.
#
# MÉTHODE (extraction sur le COMPTE COMMODITÉ, au prix de base) :
#   La SAM équilibre chaque commodité c entre son OFFRE (colonne) et sa
#   DEMANDE (ligne) :
#     OFFRE   = production domestique (activité→commodité) + imports
#               + marges de commerce/transport (trc) + taxes sur produits (stax+mtax)
#     DEMANDE = conso. intermédiaire (activités) + demande finale (ménages, gov, s-i)
#               + exports + marges reçues (uniquement le secteur Commerce)
#   Choix de modélisation (pour cohérence avec un modèle de FRET physique) :
#     1) La production retenue est l'offre du compte COMMODITÉ (activité→commodité),
#        et NON le total du compte activité. Cela exclut automatiquement
#        l'auto-consommation de subsistance (ventes directes activité→ménages,
#        ex. agriculture vivrière), qui ne circule pas sur les routes.
#     2) Les marges (trc) sont réaffectées en demande du secteur Commerce (service
#        réel), pas à la commodité sur laquelle elles portent.
#     3) Les taxes sur produits (stax, mtax) sont un prélèvement fiscal non
#        physique : on ramène la demande au PRIX DE BASE en retirant le « coin »
#        marges+taxes de chaque commodité, réparti au prorata de ses usages
#        physiques (intermédiaire + demande finale + exports).
#   Conséquence : par secteur, output + imports = conso_interm + demande_finale
#   + exports (bilan ressources-emplois équilibré, résidu ≈ 0).
#
# Retourne une liste contenant (toutes les valeurs en milliards de RWF) :
#   A              : matrice 11×11 des coefficients techniques a_ij = z_ij / output_j
#                    (z_ij = conso. intermédiaire de la commodité i par l'activité j,
#                     ramenée au prix de base)
#   output         : production domestique commercialisée par secteur (prix de base)
#   va             : valeur ajoutée par secteur (output − intrants intermédiaires)
#   demande_finale : demande finale par secteur, au prix de base, marges du
#                    Commerce incluses (= total : ménages + gov + s-i)
#   demande_finale_groupes  : matrice secteur × groupe-ménage (colonnes « r1 »…
#                    « u5 ») = panier de consommation de chaque groupe SAM, au prix
#                    de base. Σ_colonnes + demande_finale_publique = demande_finale.
#   demande_finale_publique : demande finale NON ménagère par secteur (gov + s-i),
#                    au prix de base, marges du Commerce incluses (résidu non
#                    ventilable par quintile/strate).
#   imports        : importations par secteur (flux compte « row » → commodité)
#   exports        : exportations par secteur (prix de base)
lire_sam <- function(chemin = SAM_XLSX_PATH, feuille = SAM_FEUILLE) {
  if (!file.exists(chemin)) {
    stop("Fichier SAM introuvable : ", chemin,
         "\n  → Le modèle économique exige la SAM. Placez le fichier dans data/raw/.")
  }
  # Lecture brute sans en-têtes : on récupère la grille telle quelle.
  brut <- as.data.frame(readxl::read_excel(chemin, sheet = feuille,
                                           col_names = FALSE, .name_repair = "minimal"))

  # Codes des comptes : 1re ligne = codes de colonne ; 2e colonne = codes de ligne.
  codes_col <- as.character(unlist(brut[1, ], use.names = FALSE))
  codes_row <- as.character(brut[[2]])

  # Helpers : position d'un compte par son code, et lecture numérique sécurisée
  # (les cellules vides ou textuelles sont traitées comme des zéros).
  idx_col <- function(code) which(codes_col == code)[1]
  idx_row <- function(code) which(codes_row == code)[1]
  num <- function(r, c) {
    if (is.na(r) || is.na(c)) return(0)
    v <- suppressWarnings(as.numeric(brut[r, c]))
    if (is.na(v)) 0 else v
  }

  SEC      <- SECTEURS
  suffixes <- names(SAM_MAPPING_SECTEURS)
  zero_sec <- function() setNames(numeric(length(SEC)), SEC)

  # Composantes brutes lues dans la SAM, avant retraitement (cf. méthode supra).
  dom       <- zero_sec()   # OFFRE : production domestique commercialisée (activité→commodité)
  imp       <- zero_sec()   # OFFRE : importations
  marg_paye <- zero_sec()   # OFFRE : marges trc portées par la commodité
  tax_paye  <- zero_sec()   # OFFRE : taxes sur produits (stax+mtax) sur la commodité
  fdem      <- zero_sec()   # DEMANDE : demande finale brute (ménages, gov, s-i)
  exp_brut  <- zero_sec()   # DEMANDE : exports bruts (prix d'acquisition)
  marg_recu <- zero_sec()   # DEMANDE : marges reçues (≠ 0 uniquement pour le Commerce)

  # Demande finale brute VENTILÉE par compte (secteur × compte de demande finale).
  # Permet, après passage au prix de base, de séparer les paniers de chaque groupe
  # de ménages (colonnes hhd-*) de la demande publique (gov, s-i). Σ_colonnes = fdem.
  fdem_comptes <- matrix(0, length(SEC), length(SAM_COMPTES_DEMANDE_FINALE),
                         dimnames = list(SEC, SAM_COMPTES_DEMANDE_FINALE))

  # Boucle 1 : grandeurs par commodité (offre et demande), agrégées aux secteurs.
  for (sf in suffixes) {
    s_grp  <- SAM_MAPPING_SECTEURS[[sf]]
    ra <- idx_row(paste0("a", sf))   # ligne de l'activité a{sf}
    rc <- idx_row(paste0("c", sf))   # ligne de la commodité c{sf} (demande)
    cc <- idx_col(paste0("c", sf))   # colonne de la commodité c{sf} (offre)

    # OFFRE (colonne de la commodité)
    dom[s_grp]       <- dom[s_grp]       + num(ra, cc)                           # production domestique commercialisée
    imp[s_grp]       <- imp[s_grp]       + num(idx_row("row"), cc)               # importations
    marg_paye[s_grp] <- marg_paye[s_grp] + num(idx_row(SAM_COMPTE_MARGES), cc)   # marges de commerce/transport
    for (tx in SAM_COMPTES_TAXES_PRODUITS)
      tax_paye[s_grp] <- tax_paye[s_grp] + num(idx_row(tx), cc)

    # DEMANDE (ligne de la commodité)
    for (dc in SAM_COMPTES_DEMANDE_FINALE) {
      v_dc <- num(rc, idx_col(dc))
      fdem[s_grp]            <- fdem[s_grp]            + v_dc   # total demande finale brute
      fdem_comptes[s_grp, dc] <- fdem_comptes[s_grp, dc] + v_dc # ventilation par compte (pour les groupes)
    }
    exp_brut[s_grp]  <- exp_brut[s_grp]  + num(rc, idx_col("row"))              # exports
    marg_recu[s_grp] <- marg_recu[s_grp] + num(rc, idx_col(SAM_COMPTE_MARGES))  # marges reçues (Commerce)
  }

  # Boucle 2 : consommations intermédiaires Z[i,j] = commodité i (ligne) achetée
  # par l'activité j (colonne), agrégées aux 11 secteurs.
  Z <- matrix(0, length(SEC), length(SEC), dimnames = list(SEC, SEC))
  for (sf_i in suffixes) {
    si <- SAM_MAPPING_SECTEURS[[sf_i]]; rc_i <- idx_row(paste0("c", sf_i))
    for (sf_j in suffixes) {
      sj <- SAM_MAPPING_SECTEURS[[sf_j]]; ca_j <- idx_col(paste0("a", sf_j))
      Z[si, sj] <- Z[si, sj] + num(rc_i, ca_j)
    }
  }
  inter <- rowSums(Z)   # conso. intermédiaire totale de chaque commodité i

  # ── Retraitement : passage au PRIX DE BASE ──────────────────────────────────
  # Le « coin » marges + taxes (wedge) gonfle la demande au prix d'acquisition
  # par rapport à l'offre physique au prix de base. On le retire au prorata des
  # usages physiques de chaque commodité (intermédiaire + demande finale + exports),
  # via un facteur k ∈ ]0,1]. Les marges reçues (Commerce) ne sont PAS un usage
  # physique : on les ajoute telles quelles à la demande finale du Commerce.
  usages_phys <- inter + fdem + exp_brut          # usages physiques au prix d'acquisition
  wedge       <- marg_paye + tax_paye             # coin marges + taxes à retirer
  k <- ifelse(usages_phys > 0, 1 - wedge / usages_phys, 1)
  names(k) <- SEC

  output         <- dom                            # production = offre domestique commercialisée
  imports        <- imp
  exports        <- exp_brut * k                   # exports ramenés au prix de base
  demande_finale <- fdem * k + marg_recu           # demande finale au prix de base + marges Commerce

  # ── Ventilation de la demande finale par groupe de ménages ──────────────────
  # On applique le même facteur prix de base k (par LIGNE = par secteur, comme
  # Z_base) à la demande finale ventilée par compte, puis on sépare :
  #   • les paniers ménages (colonnes hhd-*, renommées par leur clé de groupe
  #     « r1 »…« u5 ») → demande_finale_groupes (secteur × groupe) ;
  #   • la demande publique (gov + s-i) + les marges reçues du Commerce (flux
  #     dérivé non ventilable par quintile/strate) → demande_finale_publique.
  # Par construction : Σ_groupes + demande_finale_publique = demande_finale.
  fdem_comptes_base       <- fdem_comptes * k
  demande_finale_groupes  <- fdem_comptes_base[, SAM_COMPTES_MENAGES, drop = FALSE]
  colnames(demande_finale_groupes) <- sub("^hhd-", "", SAM_COMPTES_MENAGES)
  demande_finale_publique <- rowSums(
    fdem_comptes_base[, SAM_COMPTES_DEMANDE_PUBLIQUE, drop = FALSE]
  ) + marg_recu
  names(demande_finale_publique) <- SEC

  # Z au prix de base : on multiplie chaque LIGNE i (commodité vendue) par k[i].
  Z_base <- Z * k                                  # recyclage par ligne (k recyclé sur les colonnes)
  conso_interm <- rowSums(Z_base)                  # = inter * k (cohérent avec A %*% output)

  # Valeur ajoutée = production − intrants intermédiaires consommés (somme colonne).
  va <- output - colSums(Z_base)

  # Coefficients techniques : a_ij = z_ij / output_j (au prix de base).
  A_sam <- matrix(0, length(SEC), length(SEC), dimnames = list(SEC, SEC))
  for (sj in SEC) if (output[sj] > 0) A_sam[, sj] <- Z_base[, sj] / output[sj]

  # Garde-fous : le bilan doit être équilibré et les grandeurs économiquement valides.
  residu <- output + imports - conso_interm - demande_finale - exports
  stopifnot(
    all(k > 0 & k <= 1 + 1e-9),          # facteur prix de base valide
    all(demande_finale >= -1e-6),        # pas de demande finale négative
    max(abs(residu)) < 1e-6,             # bilan ressources-emplois équilibré (résidu ≈ 0)
    # la somme des paniers groupes + la demande publique reconstitue la demande totale
    max(abs(rowSums(demande_finale_groupes) + demande_finale_publique
            - demande_finale)) < 1e-6
  )

  list(A = A_sam, output = output[SEC], va = va[SEC],
       demande_finale = demande_finale[SEC],
       demande_finale_groupes  = demande_finale_groupes[SEC, , drop = FALSE],
       demande_finale_publique = demande_finale_publique[SEC],
       imports = imports[SEC], exports = exports[SEC])
}

cat("  → Lecture de la SAM…\n")
sam <- lire_sam()
cat("  ✓ SAM lue :", sum(sam$va), "mrd RWF de VA totale (",
    nrow(sam$A), "secteurs)\n")

# Demande finale nationale par secteur (MILLIARDS DE RWF), extraite de la SAM.
# Définition : F[s] = consommation des ménages + consommation publique
#              + investissement I (colonne « s-i » = FBCF + var. stocks, PAS l'épargne).
# NOTE : exportations/importations exclues — traitées via les entrepôts RoW et
#        leurs offre/demande sectorielles (03_transport.R).
DEMANDE_FINALE_SAM <- sam$demande_finale[SECTEURS]
stopifnot(all(names(DEMANDE_FINALE_SAM) %in% SECTEURS))

# ── Demande finale désagrégée par groupe de ménages ───────────────────────────
# Utilisées par 03_transport.R pour spatialiser la demande finale par groupe.
#   DEMANDE_FINALE_GROUPES_SAM  : matrice secteur × groupe (colonnes « r1 »…« u5 »),
#       panier de consommation de chaque groupe SAM (strate × quintile), mrd RWF.
#   DEMANDE_FINALE_PUBLIQUE_SAM : demande finale non ménagère (gov + s-i + marges
#       Commerce) par secteur, mrd RWF — spatialisée par pop × RWI.
# Identité préservée : rowSums(GROUPES) + PUBLIQUE = DEMANDE_FINALE_SAM.
DEMANDE_FINALE_GROUPES_SAM  <- sam$demande_finale_groupes[SECTEURS, , drop = FALSE]
DEMANDE_FINALE_PUBLIQUE_SAM <- sam$demande_finale_publique[SECTEURS]
stopifnot(
  setequal(colnames(DEMANDE_FINALE_GROUPES_SAM),
           sub("^hhd-", "", SAM_COMPTES_MENAGES)),
  max(abs(rowSums(DEMANDE_FINALE_GROUPES_SAM) + DEMANDE_FINALE_PUBLIQUE_SAM
          - DEMANDE_FINALE_SAM)) < 1e-6
)

# ── Quintiles de consommation de la SAM ───────────────────────────────────────
# QUANTILES_MENAGES_SAM : bornes des quintiles de consommation, en part cumulée
#   de la POPULATION. Les groupes de ménages de la SAM IFPRI 2021 (comptes
#   « hhd-r1 »…« hhd-u5 ») proviennent de l'enquête EICV5 (2016/17), qui définit
#   ses quintiles ainsi (EICV5 Main Indicators Report, p. iv ; formulation
#   identique dans EICV4, p. vi) : on trie les ménages par consommation annuelle
#   puis on découpe LA POPULATION en cinq parts égales.
#
#   DEUX CONSÉQUENCES, toutes deux appliquées par le modèle :
#     1) Le découpage est national : les groupes « u1 »…« u5 » sont le
#        croisement (strate × quintile national), et NON des quintiles calculés
#        à l'intérieur de la strate urbaine. Le groupe u1 est minuscule parce
#        qu'il y a très peu de citadins dans le quintile national le plus pauvre).
#     2) Le découpage porte sur les individus, pas sur les ménages 
QUANTILES_MENAGES_SAM <- c(0.2, 0.4, 0.6, 0.8)

# ── Repères de validation issus de l'EICV5 (2016/17) ──────────────────────────
# Valeurs publiées, utilisées UNIQUEMENT comme points de comparaison dans les
# diagnostics (elles n'entrent dans aucun calcul du modèle).
#   EICV5_PART_URBAINE_POP    : part de la population vivant en zone urbaine
#                               (Poverty Profile Report, tableau 10.2 : 18 %).
#   EICV5_CONSO_PAR_STRATE    : consommation annuelle par adulte-équivalent,
#                               milliers de RWF, prix de janvier 2014
#                               (Poverty Profile Report, tableau 7).
#   EICV5_CONSO_PAR_QUINTILE  : idem, par quintile national.
# Contrôle de cohérence interne de ces chiffres :
#   0,18 × 570 + 0,82 × 216 = 279,7 ≈ moyenne nationale publiée (279). ✓
EICV5_PART_URBAINE_POP   <- 0.18
EICV5_CONSO_PAR_STRATE   <- c(urbain = 570, rural = 216)
EICV5_CONSO_PAR_QUINTILE <- c(q1 = 86, q2 = 140, q3 = 192, q4 = 279, q5 = 699)

# ── Repère de validation issu du RPHC5 (2022) ──────────────────────────────────
# Part de la population classée urbaine sous la définition RÉVISÉE du recensement
# 2022 (postérieure à l'EICV5 et à sa définition villageoise de 2012). Utilisée
# UNIQUEMENT comme point de comparaison dans viz_verif.R, au même titre que le
# bloc EICV5 ci-dessus — n'entre dans aucun calcul du modèle. Cf. discussion
# détaillée sur l'écart avec l'EICV5 et avec PART_URBAINE_IMPLICITE_SAM plus bas
# (bloc CIBLE_PART_URBAINE_POP).
RPHC5_2022_PART_URBAINE_POP <- 0.279

# ── Tailles de groupes implicites de la SAM (cible de validation) ─────────────
# La SAM donne la consommation TOTALE de chaque groupe (C_g) mais pas sa
# POPULATION (N_g). On reconstruit N_g sous une hypothèse explicite :
#
#   HYPOTHÈSE : à l'intérieur d'un même quintile national, la consommation par
#   tête est la même en ville et à la campagne. C'est la conséquence directe de
#   la définition des quintiles — ils sont découpés sur un classement unique de
#   la consommation, donc deux ménages d'un même quintile ont, par construction,
#   des niveaux de consommation voisins quelle que soit leur strate.
#
#   Sous cette hypothèse, la part urbaine d'un quintile est sa part dans la
#   consommation du quintile, et chaque quintile pèse 20 % de la population :
#       part_pop[u,q] = 0,20 × C[u,q] / (C[u,q] + C[r,q])
#
# LIMITE : l'hypothèse est fausse pour le quintile 5, ouvert vers le haut (les
# riches urbains sont très au-dessus des riches ruraux). La part urbaine du Q5,
# et donc la part urbaine nationale, sont surestimées. La valeur obtenue est
# donc un MAJORANT — à lire comme tel dans les diagnostics.
PART_POP_GROUPE_SAM_CIBLE <- local({
  C  <- colSums(DEMANDE_FINALE_GROUPES_SAM)          # consommation totale par groupe
  qs <- as.character(1:5)
  parts <- setNames(numeric(10), c(paste0("r", qs), paste0("u", qs)))
  for (q in qs) {
    cu <- C[[paste0("u", q)]]; cr <- C[[paste0("r", q)]]
    parts[[paste0("u", q)]] <- 0.20 * cu / (cu + cr)
    parts[[paste0("r", q)]] <- 0.20 - parts[[paste0("u", q)]]
  }
  parts
})
PART_URBAINE_IMPLICITE_SAM <- sum(PART_POP_GROUPE_SAM_CIBLE[paste0("u", 1:5)])

# ── Classification géo-sociale au niveau du pixel (01_reseau.R IV.5.B) ────────
#
# CIBLE_PART_URBAINE_POP : part de la population nationale que le masque urbain
#   doit classer en « urbain ». Trois valeurs candidates, très différentes :
#
#     0,180  EICV5 2016/17 — l'enquête dont sont issus les groupes de la SAM,
#            sous la classification villageoise du recensement 2012.
#     0,279  RPHC5 2022 — définition révisée du recensement, postérieure à la SAM.
#     0,269  PART_URBAINE_IMPLICITE_SAM — ce que la SAM elle-même implique
#            (cf. supra), et qui tombe presque exactement sur le chiffre 2022.
#
#   POURQUOI PAS 0,18 : caler à 18 % imposerait au modèle un rapport de
#   consommation par tête urbain/rural de 5,6, contre 2,6 mesuré par l'EICV5 —
#   la SAM 2021 attribue 55 % de la consommation des ménages aux groupes urbains,
#   ce qui est incompatible avec une population urbaine de 18 %. L'écart
#   s'explique par les cinq années séparant l'enquête de la SAM, par le passage
#   aux prix courants (l'EICV déflate par un indice spatial qui écrase l'écart
#   ville/campagne) et par le calage de la SAM sur les comptes nationaux.
#   Le modèle doit être cohérent avec la SAM qu'il utilise, pas avec une enquête
#   antérieure : on retient donc la valeur implicite de la SAM.
CIBLE_PART_URBAINE_POP <- PART_URBAINE_IMPLICITE_SAM

# METHODE_MASQUE_URBAIN : comment désigner les pixels urbains.
#   "densite" — on classe les pixels par densité de population locale
#     décroissante et on retient les plus denses jusqu'à atteindre
#     CIBLE_PART_URBAINE_POP. Reproductible et calé sur une cible documentée.
#   "landuse" — appartenance à LANDUSE_URBAIN (OSM). Ancien comportement,
#     conservé pour comparaison ; le landuse OSM est très inégalement renseigné
#     hors de Kigali et ne permet aucun calage.
# NOTE : la définition officielle rwandaise est ADMINISTRATIVE (un code posé sur
#   chaque village lors de la cartographie censitaire de 2012) et ses critères ne
#   sont pas publiés — ni dans les rapports RPHC4/RPHC5, ni dans les notes
#   méthodologiques EICV. Elle n'est donc pas reproductible à partir de données
#   géographiques ouvertes : le masque ne peut être qu'un proxy calé.
METHODE_MASQUE_URBAIN <- "densite"

# RAYON_DENSITE_URBAINE_M : rayon du disque sur lequel on somme la population
#   pour mesurer la densité LOCALE d'un pixel (méthode "densite"). Un rayon trop
#   petit classerait urbains des pixels isolés très denses ; trop grand, il
#   diluerait les petits centres. 1 km ≈ échelle d'un quartier.
RAYON_DENSITE_URBAINE_M <- 1000

# AGREGATION_MASQUE_URBAIN_VIZ : facteur d'agrégation (en nombre de pixels
#   WorldPop par côté) appliqué à la carte de contrôle qui compare le masque
#   urbain du modèle au landuse urbain OSM (viz_verif.R). Le raster WorldPop
#   fait ~100 m de résolution ; le pays compte alors plusieurs millions de
#   pixels, trop pour un fichier de carte. Un facteur 10 ramène la carte à des
#   cellules d'environ 1 km de côté (résolution comparable à
#   RAYON_DENSITE_URBAINE_M), largement suffisante pour un diagnostic visuel.
AGREGATION_MASQUE_URBAIN_VIZ <- 10

# ── Pondération composite emploi × RWI dans le modèle MRIO ────────────────────
#
# ALPHA_EMPLOI_RWI : part de l'emploi dans le poids de production d'une zone.
#   w[i,s] = ALPHA_EMPLOI_RWI × (emp[i,s] / emp_national[s])
#           + (1 - ALPHA_EMPLOI_RWI) × (p_rwi[i] / Σ p_rwi)
#   α = 1 → pur emploi (comportement original)
#   α = 0 → pur RWI
#   Valeur recommandée : 0.7 (l'emploi reste le driver principal ; le RWI
#   corrige la productivité, mais ne varie pas par secteur).
ALPHA_EMPLOI_RWI <- 0.7

# EPSILON_RWI : décalage minimal appliqué à p_rwi avant le produit
#   z[i] = pop[i] × (p_rwi[i] + EPSILON_RWI)
#   Évite qu'une zone avec p_rwi ≈ 0 reçoive un poids nul en demande finale
#   même si elle est densément peuplée. 0.05 = 5% du range [0,1].
EPSILON_RWI <- 0.05

# Paramètres de friction par secteur (beta du modèle gravitaire)
#   Beta élevé = commerce très sensible au coût du transport rapporté à la valeur du bien
#               (biens lourds à faible valeur unitaire : agriculture, construction)
#   Beta faible = peu sensible (biens à haute valeur ajoutée)
BETA_SECTEUR <- c(
  Agriculture     = 2.3,   # Vivrier pondéreux, faible valeur unitaire → très sensible
  Cultures_export = 1.4,   # Café/thé/tabac : forte valeur → peu sensible au coût
  Mines           = 1.2,   # Minerai dense, forte valeur → peu sensible
  Agro_industrie  = 1.8,   # Produits transformés, valeur moyenne
  Chimie_petrole  = 1.5,   # Vrac pétrolier : lourd mais valeur élevée
  Manufactures    = 1.6,   # Mix textile/métaux/machines, valeur moyenne/haute
  Construction    = 2.5,   # Agrégats/ciment : très lourds, très faible valeur → très sensible
  Commerce        = 1.7    # Redistribution de biens
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

# Tolérance sur l'équilibre des marges avant lancement de l'IPF.
TOL_EQUILIBRE_MARGES <- FURNESS_TOL / 10

# Matrice des coefficients techniques A
A <- sam$A[SECTEURS, SECTEURS]

production_totale <- sam$output[SECTEURS]
cat("  ✓ production_totale : SAM IFPRI (",
    round(sum(production_totale)), "mrd RWF d'output total )\n")

# ── Valeur unitaire des marchandises (RWF PAR TONNE) ──────────────────────────
VALEUR_RWF_PAR_TONNE <- c(
  Agriculture     =   588000,  # Vivrier pondéreux : racines, bananes, céréales (FAOSTAT) — ≈ 1700 t/mrd
  Cultures_export =  3571000,  # Café/thé/tabac : forte valeur unitaire                   — ≈ 280 t/mrd
  Mines           =   435000,  # Minerai dense (3T) + carrières (RMB Annual Report 2022)  — ≈ 2300 t/mrd
  Agro_industrie  =   556000,  # Farine, boissons, huiles, sucre transformés              — ≈ 1800 t/mrd
  Chimie_petrole  =   625000,  # Produits pétroliers (vrac lourd) dominants à l'import    — ≈ 1600 t/mrd
  Manufactures    =   909000,  # Mix : ciment/verre lourds + machines/textile plus légers — ≈ 1100 t/mrd
  Construction    =   111000,  # Agrégats, ciment, acier : très lourds / faible valeur    — ≈ 9000 t/mrd
  Commerce        =  1316000,  # Biens redistribués, valeur unitaire plus élevée          — ≈ 760 t/mrd
  Transport       =      Inf,  # Service (cf. supra) — fret nul pour éviter le double comptage
  Energie_eau     =      Inf,  # Électricité + eau : aucun fret routier
  Services        =      Inf   # Immatériel (finance, ICT, éducation, santé, administration)
)
stopifnot(setequal(names(VALEUR_RWF_PAR_TONNE), SECTEURS))

# ── Densité physique DÉRIVÉE (tonnes par milliard de RWF) ──────────────────────
# NE PAS ÉDITER : obtenue par inversion de VALEUR_RWF_PAR_TONNE. Conservée sous ce
# nom car tout l'aval (03_transport.R, viz_fret.R) raisonne en tonnes/mrd RWF.
#   tonnes/mrd RWF = 1e9 RWF/mrd ÷ valeur (RWF/tonne)
#   valeur = Inf (services) → 0 tonne/mrd RWF (pas de fret) : le test « == 0 » en aval reste valide.
TONNES_PAR_mrd_RWF <- 1e9 / VALEUR_RWF_PAR_TONNE
stopifnot(setequal(names(TONNES_PAR_mrd_RWF), SECTEURS))

# ── Secteurs effectivement modélisés en FRET ──────────────────────────────────
# Sous-ensemble de SECTEURS avec du fret physique
# Les autres restent dans SECTEURS pour la comptabilité économique (matrice A,
# demande finale, SAM) mais sont EXCLUS du modèle gravitaire et de tous les
# tableaux/cartes de fret.
SECTEURS_FRET <- SECTEURS[TONNES_PAR_mrd_RWF[SECTEURS] > 0]

# ── Repère de change pour la validation externe (viz_verif.R) ─────────────────
# Taux de change moyen RWF/USD sur 2021, année de la SAM IFPRI utilisée par le
# modèle. Sert UNIQUEMENT à convertir la valeur ajoutée sectorielle de la SAM
# (mrd RWF) en USD courants pour la comparer à la valeur ajoutée sectorielle de
# la Banque Mondiale (World Development Indicators, en USD courants) — aucun
# calcul du modèle ne dépend de ce taux.
# Source : Banque Mondiale, "Official exchange rate (LCU per US$, period
# average)", Rwanda, 2021 (indicateur PA.NUS.FCRF) — moyenne annuelle ≈ 1001.
TAUX_CHANGE_RWF_USD_2021 <- 1001

# ── Correspondance SECTEURS → grandes catégories Banque Mondiale (diagnostic) ──
# La Banque Mondiale ne publie la valeur ajoutée que sous 4 grandes catégories
# (agriculture, industrie manufacturière, industrie y c. construction, services).
# Cette table ventile les 11 secteurs du modèle vers ces 4 catégories pour
# permettre une comparaison de parts sectorielles dans viz_verif.R. Ventilation
# approximative (affectation qualitative, pas de clé de pondération infra-
# sectorielle) — à lire comme un ordre de grandeur, pas une identité comptable.
CORRESPONDANCE_SECTEURS_BANQUE_MONDIALE <- c(
  Agriculture     = "agri",
  Cultures_export = "agri",
  Mines           = "indus",
  Agro_industrie  = "manuf",
  Chimie_petrole  = "manuf",
  Manufactures    = "manuf",
  Construction    = "indus",
  Commerce        = "serv",
  Transport       = "serv",
  Energie_eau     = "indus",
  Services        = "serv"
)
stopifnot(setequal(names(CORRESPONDANCE_SECTEURS_BANQUE_MONDIALE), SECTEURS))

# Garde-fou : BETA_SECTEUR doit couvrir exactement les secteurs de fret
stopifnot(setequal(names(BETA_SECTEUR), SECTEURS_FRET))

# ==============================================================================
# Paramètres des tests de sensibilité par HYPERCUBE LATIN (run_sensibilite.R)
# ==============================================================================
# Objectif : mesurer comment les résultats du modèle réagissent à l'incertitude
# sur les deux familles de paramètres les moins bien connues — les élasticités
# gravitaires (BETA_SECTEUR) et les valeurs unitaires (VALEUR_RWF_PAR_TONNE).
#
# Pourquoi un hypercube latin plutôt qu'un coefficient uniforme ?
#   Multiplier TOUS les betas (ou toutes les valeurs/tonne) par un même facteur
#   ne teste qu'une seule direction de variation et confond l'effet des
#   secteurs. Ici, CHAQUE secteur voit son beta ET sa valeur/tonne varier
#   INDÉPENDAMMENT. Le plan d'expérience est un hypercube latin
#   (lhs::randomLHS) : pour N tirages et d paramètres, chaque paramètre est
#   découpé en N intervalles de même probabilité et chacun n'est visité qu'une
#   fois. On obtient une couverture homogène de l'espace des paramètres avec
#   peu de tirages, là où un tirage purement aléatoire laisserait des trous.
#
# Les valeurs tirées sont des MULTIPLICATEURS appliqués aux valeurs de
# référence de ce fichier : 1 → inchangé, 1.3 → +30 %, 0.8 → −20 %.

# Nombre de tirages = nombre de scénarios de sensibilité générés. Chaque tirage
# relance les modules 02→05 + les visualisations (quelques minutes chacun) :
# 20 est un compromis raisonnable entre couverture de l'espace et temps total.
SENS_LHS_N <- 20

# Amplitude de variation RELATIVE (± autour de la référence), par famille.
#   0.30 → chaque beta de secteur est tiré dans [0.70 ; 1.30] × sa valeur de réf.
SENS_LHS_AMPLITUDE_BETA         <- 0.30
#   0.30 → chaque valeur unitaire (RWF/tonne) est tirée dans [0.70 ; 1.30].
SENS_LHS_AMPLITUDE_VALEUR_TONNE <- 0.30

# Graine aléatoire : rend le plan LHS reproductible d'une exécution à l'autre
# (mêmes scénarios → figures de synthèse comparables et réexécutables).
SENS_LHS_GRAINE <- 123

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
DESCRIPTION_SCENARIO  <- "Scénario de test"
DUREE_JOURS           <- 14
TYPE_EVENEMENT        <- "inondation"

# Définition des perturbations — trois méthodes combinables :
#     Mode A — Manuel       : liste d'osm_id fournie à la main (ci-dessous)
#     Mode B — Buffer zone  : toutes les routes dans un rayon autour d'un point
#     Mode C — Raster risque: intersection avec un raster (grille) externe
# L'activation des modes B/C se fait dans la section CHOIX DU MODE (tête de script).

# Mettre l'identifiant OSM de la ou des routes affectées (mode manuel)
OSM_IDS_PERTURBES_MANUEL <- c(479687569)

# Nom du scénario, construit selon NOM_SCENARIO_MANUEL (défini dans CHOIX DU MODE) :
#   NULL   → automatique : nom dérivé des noms OSM des arêtes perturbées
#            (ex. "inondation_RN1_Kigali") ; fallback "Scenario_default" si DuckDB
#            indisponible (session fraîche avant 01_reseau.R).
#   chaîne → manuel : ce nom est utilisé tel quel.
NOM_SCENARIO <- if (!is.null(NOM_SCENARIO_MANUEL)) {
  NOM_SCENARIO_MANUEL
} else {
  # Mode automatique : interroge DuckDB pour récupérer les noms OSM des arêtes
  # perturbées (table routes_attrs_raw, filtrée sur OSM_IDS_PERTURBES_MANUEL),
  # puis les concatène avec TYPE_EVENEMENT en un identifiant propre (sans espaces
  # ni caractères spéciaux). En cas d'échec (DuckDB absent, aucun nom trouvé),
  # bascule sur le fallback "Scenario_default".
  tryCatch({
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
}

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

# Seuil au-dessus duquel un point de la chaussée est considéré comme submergé.
# L'unité suit celle du raster pointé par CHEMIN_RASTER_RISQUE :
#   - raster GloFAS (celui utilisé ici) : hauteur d'eau en MÈTRES.
#     0.5 = « plus de 50 cm d'eau sur la chaussée », profondeur au-delà de
#     laquelle un poids lourd ne passe plus de façon fiable.
#   - raster de probabilité (0-1), ex. celui de creer_raster_test.R :
#     0.5 = « probabilité de submersion supérieure à 50 % ».
SEUIL_RISQUE_RASTER         <- 0.5

# Proportion minimale de la longueur d'une arête qui doit être en zone à risque
# pour que l'arête soit considérée comme perturbée.
# 0.3 = au moins 30% de la route doit être en zone à risque
PROPORTION_MIN_EXPOSEE      <- 0.3

# Proportion des routes exposées (dépassant les seuils) effectivement inondées.
# 1.0 = toutes les routes exposées sont coupées
# 0.6 = 60% des routes exposées sont aléatoirement coupées
PROP_ROUTES_INONDEES_BUFFER <- 1
PROP_ROUTES_INONDEES_RASTER <- 1

# Graine aléatoire pour la sélection des routes inondées.
# Fixer cette valeur garantit la reproductibilité du scénario.
# Changer la graine = simuler un autre tirage du même événement.
SEED_INONDATION <- 42


cat("✓ Paramètres globaux chargés\n\n")

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
# des boucles R sur de grands volumes. Le fichier "reseau.duckdb" stocke
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

# ── Catégories de saturation (encombrement V/C) ───────────────────────────────
# Carte des goulots d'étranglement : vert = fluide, rouge = saturé (V/C>1).
# Les niveaux correspondent à classe_saturation calculée dans 03_transport.R.
PALETTE_SATURATION <- c(
  "Fluide"            = "#1A9850",   # Vert        — V/C < 0,5
  "Dense"             = "#FEE08B",   # Jaune       — 0,5 ≤ V/C < 0,8
  "Proche saturation" = "#FC8D59",   # Orange      — 0,8 ≤ V/C < 1,0
  "Saturé"            = "#D73027",   # Rouge       — V/C ≥ 1,0
  "Inconnu"           = "#CCCCCC"    # Gris        — capacité non définie
)

# ── Palette d'émissions (vert pâle → rouge foncé) ─────────────────────────────
# Rouge = route très émettrice (pente forte + mauvaise surface + véhicule lourd)
# Vert  = route peu émettrice (plat, bitumée, camion léger)
PALETTE_EMISSIONS <- c("#1A9850", "#91CF60", "#FEE08B", "#FC8D59", "#D73027")

# ── Secteurs économiques (dans l'ordre de SECTEURS) ───────────────────────────
# Centralisé ici pour garantir que chaque secteur a toujours la même couleur
# dans tous les graphiques et cartes.
# L'association couleur↔secteur se fait par NOM : l'ordre d'empilement dans
# ggplot n'a donc aucune incidence sur les couleurs affichées.
.palette_secteurs_brut <- c(
  Agriculture     = "#2E7D32",  # vert
  Cultures_export = "#9ACD32",  # vert-olive (cultures de rente : café, thé, tabac)
  Mines           = "#8D6E63",  # brun
  Agro_industrie  = "#F57C00",  # orange
  Chimie_petrole  = "#C62828",  # rouge
  Manufactures    = "#7B1FA2",  # violet
  Construction    = "#607D8B",  # bleu-gris
  Commerce        = "#1565C0",  # bleu
  Transport       = "#00ACC1",  # cyan
  Energie_eau     = "#FDD835",  # jaune
  Services        = "#EC407A"   # rose
)
# Garde-fou : si un secteur de SECTEURS n'a pas de couleur attitrée (ajout d'un
# secteur sans mise à jour ici), on arrête avec un message explicite plutôt que
# de laisser ggplot afficher des barres grises silencieusement.
stopifnot(all(SECTEURS %in% names(.palette_secteurs_brut)))
PALETTE_SECTEURS <- .palette_secteurs_brut[SECTEURS]   # réordonne selon SECTEURS

# ── Source de la population par zone (diagnostic, viz_verif.R) ───────────────
# Couleur par méthode d'estimation retenue pour population_zone (hiérarchie
# WorldPop > NISR > plancher, cf. 01_reseau.R IV.6) : sert à cartographier où
# le modèle s'appuie sur une source dégradée.
PALETTE_SOURCE_POP <- c(
  "WorldPop"  = "#1A9850",   # Vert  — raster WorldPop, source la plus fine
  "NISR"      = "#FEE08B",   # Jaune — repli sur la population administrative par district
  "Fallback"  = "#D73027"    # Rouge — aucune donnée, plancher minimal appliqué
)

# ── Masque urbain du modèle vs landuse OSM (diagnostic, viz_verif.R) ─────────
# Couleur par catégorie de recouvrement entre le masque urbain retenu par le
# modèle (densité ou landuse, cf. METHODE_MASQUE_URBAIN) et le landuse urbain
# OSM : sert à voir où les deux méthodes s'accordent et où elles divergent.
PALETTE_MASQUE_URBAIN <- c(
  "Modèle seul"  = "#42A5F5",   # Bleu   — urbain pour le modèle, pas pour OSM
  "OSM seul"      = "#FFA726",   # Orange — urbain pour OSM, pas pour le modèle
  "Modèle + OSM" = "#1A9850"    # Vert   — les deux méthodes s'accordent
)

cat("✓ Palettes de couleurs définies\n\n")


# ==============================================================================
# I.5 : Paramètres de la flotte de véhicules
# Définit les tables DuckDB décrivant la flotte (coûts, vitesses,
# transbordements, coûts pré-frontière). Pour ajouter un véhicule :
# modifier uniquement ce bloc, le reste du script s'adapte automatiquement.
# ==============================================================================

# Cette section crée quatre tableaux qui décrivent la flotte de véhicules utilisée
# dans le modèle. Chaque tableau est d'abord créé en R avec tribble() — une
# façon pratique de saisir un tableau ligne par ligne —, puis envoyé dans
# DuckDB avec duck_write() pour pouvoir être interrogé en SQL plus tard.

# ── Table 1 : paramètres scalaires par véhicule ───────────────────────────────
# Ce tableau contient les caractéristiques physiques et économiques de chaque
# type de véhicule 
params_flotte_df <- tribble(
  ~vehicule_id,   ~nom,                    ~conso_base, ~facteur_conso_pente, ~prix_carburant, ~valeur_temps, ~capacite_tonnes, ~facteur_urbain, ~facteur_emission_co2, ~facteur_emission_nox, ~facteur_emission_pm25, ~cout_chargement_rwf, ~cout_dechargement_rwf, ~facteur_pcu,
  "camionnette",  "Camionnette (<3.5t)",    10,          1.0,                  1383.2,          4446,          3.0,              1.05,            2.68,                  0.25,                  0.040,                  14820,                14820,                1.5,
  "camion_moyen", "Camion moyen (5-10t)",   20,          1.5,                  1383.2,          7410,          7.5,              1.25,            2.68,                  0.50,                  0.065,                  24700,                24700,                2.0,
  "camion_lourd", "Camion lourd (>10t)",    35,          2.0,                  1383.2,          9880,          20.0,             1.60,            2.68,                  0.80,                  0.090,                  39520,                39520,                3.0
)
duck_write(params_flotte_df, "params_flotte")

# ── Table 1bis : capacité d'écoulement par type de route (congestion) ─────────
# OBJECTIF : borner le débit de chaque tronçon pour modéliser l'encombrement.
# La capacité est exprimée en PCU/jour ("Passenger Car Unit" = équivalent
# voiture particulière) 
# Elle dépend uniquement du TYPE de route, pas de sa longueur (c'est un débit
# de section). Ces valeurs sont des ordres de grandeur à caler sur des comptages
# réels (RTDA, corridor Nord) lorsqu'ils seront disponibles.
capacites_route_df <- tribble(
  ~road_type,     ~capacite_pcu_jour,
  "motorway",     30000,   # autoroute — débit très élevé
  "trunk",        15000,   # route nationale principale
  "primary",      10000,   # route primaire
  "secondary",     6000,   # route secondaire
  "tertiary",      3000,   # route tertiaire
  "unclassified",  1500    # route non classée / desserte locale
)

# ── Conversion tonnes/an → PCU/jour ───────────────────────────────────────────
# Le modèle affecte des TONNES/AN par véhicule ; la capacité est en PCU/JOUR.
# On convertit la charge d'une arête ainsi, pour chaque type de véhicule v :
#   trajets/an = tonnes_an / (capacite_tonnes[v] × TAUX_CHARGEMENT)
#   PCU/jour   = trajets/an / JOURS_TRAFIC_AN × facteur_pcu[v]
# puis on somme sur les véhicules pour obtenir la charge PCU/jour du tronçon.
TAUX_CHARGEMENT <- 0.7   # taux de remplissage moyen des camions (0–1)
JOURS_TRAFIC_AN <- 300   # nombre de jours ouvrables de trafic fret par an

# ── Fonction de congestion BPR (Bureau of Public Roads) ───────────────────────
# Fonction volume-délai appliquée au TEMPS de trajet (une route saturée ralentit) :
#   temps_congestionné = temps_libre × [ 1 + BPR_ALPHA × (V/C)^BPR_BETA ]
# où V = charge (PCU/jour) et C = capacité (PCU/jour) du tronçon. Seule la
# composante « temps » du coût généralisé enfle (carburant/usure inchangés), et le
# temps congestionné alimente le stock en transit de l'EOQ → un bien de valeur fuit
# les routes saturées (lien congestion → choix modal). Tant que V < C le surcoût
# reste faible ; au-delà il croît fortement, reportant le trafic. Classiques : 0,15 et 4.
BPR_ALPHA <- 0.15
BPR_BETA  <- 4

# ── Affectation à l'équilibre (Méthode des Moyennes Successives, MSA) ──────────
# Comme le coût dépend de la charge (et inversement), on itère :
#   1. affectation All-or-Nothing avec les coûts congestionnés courants
#   2. moyennage de la nouvelle charge avec la précédente (pas 1/n)
#   3. arrêt quand la charge ne varie quasiment plus (gap < MSA_TOL)
MSA_MAX_ITER <- 20     # nombre maximal d'itérations d'équilibre
MSA_TOL      <- 0.01   # convergence : variation relative L1 de la charge < 1 %
# (L'activation de la congestion se fait via CONGESTION, section CHOIX DU MODE.)

# ── Taux de détention du stock « r » (coût annuel de garder 1 RWF en stock) ───
# Décomposé en composantes EXPLICITES pour la lisibilité ; leur SOMME = r.
# Chaque composante est une fraction de la valeur de la marchandise, par an.
R_CAPITAL      <- 0.16   # coût d'opportunité du capital immobilisé (≈ taux prêteur BNR)
R_STOCKAGE     <- 0.03   # entreposage : espace, manutention, énergie
R_ASSURANCE    <- 0.01   # assurance des marchandises en stock
R_OBSOLESCENCE <- 0.03   # dépréciation, pertes, péremption (moyenne tous secteurs)
# r total utilisé par l'EOQ (≈ 0,23/an avec les valeurs ci-dessus)
TAUX_DETENTION_STOCK <- R_CAPITAL + R_STOCKAGE + R_ASSURANCE + R_OBSOLESCENCE

# Heures par an (calendaires) pour convertir le temps de trajet τ_v en fraction
# d'année : la marchandise « dort » dans le pipeline 24h/24 pendant le transit.
HEURES_PAR_AN <- 8760

# ── Plancher de remplissage des envois (fraction de la capacité du véhicule) ──
# La taille d'envoi optimale q* (Wilson) sert à la comptabilité logistique (coût de
# commande = Q/q* × coût fixe, stock cyclique = q*/2 × valeur × r). Sans borne basse,
# un bien de forte valeur à coût de commande faible donnerait un q* minuscule → un
# coût de commande explosif (Q/q* → ∞). On impose donc un remplissage minimal :
# q* ≥ EOQ_REMPLISSAGE_MIN × capacité. La valeur est aussi plafonnée en haut à la
# capacité (camion plein). Doit être > 0. À calibrer ; 0,5 = au moins un
# demi-chargement par envoi.
EOQ_REMPLISSAGE_MIN <- 0.5

# ── Table 2 : paramètres par véhicule × type de route × surface ───────────────
# Chaque véhicule a ses propres caractéristiques de circulation sur chaque
# combinaison (type de route, surface). L'ajout d'un véhicule = ajouter 11 lignes
# avec son vehicule_id. Trois colonnes :
#   - vitesse_kmh          : vitesse de référence (km/h), varie selon
#       le type de véhicule (un camion lourd ne va pas aussi vite qu'une camionnette),
#       le type de route (une autoroute permet plus de vitesse qu'un chemin non classé),
#       la surface (bitumée = rapide, piste en terre = lent).
#   - facteur_conso_route  : multiplicateur SANS UNITÉ appliqué à conso_base
#       (surconsommation de carburant due à la résistance au roulement : 1.00 sur
#       bitume, davantage sur latérite/terre, d'autant plus que le véhicule est lourd).
#   - usure_rwf_km         : coût d'usure du véhicule en RWF/km (pneus, suspension,
#       entretien), qui explose sur les mauvaises surfaces.
params_flotte_type_route_df <- tribble(
  ~vehicule_id,   ~road_type,      ~surface,   ~vitesse_kmh, ~facteur_conso_route, ~usure_rwf_km,
  # --- Camionnette ---
  "camionnette",  "motorway",      "paved",    120,          1.00,                  19.76,
  "camionnette",  "trunk",         "paved",     90,          1.00,                  19.76,
  "camionnette",  "trunk",         "gravel",    60,          1.08,                  39.52,
  "camionnette",  "primary",       "paved",     80,          1.00,                  19.76,
  "camionnette",  "primary",       "gravel",    55,          1.08,                  39.52,
  "camionnette",  "secondary",     "paved",     70,          1.00,                  19.76,
  "camionnette",  "secondary",     "gravel",    50,          1.08,                  39.52,
  "camionnette",  "tertiary",      "paved",     60,          1.00,                  19.76,
  "camionnette",  "tertiary",      "unpaved",   35,          1.18,                  69.16,
  "camionnette",  "unclassified",  "gravel",    45,          1.08,                  39.52,
  "camionnette",  "unclassified",  "unpaved",   28,          1.18,                  69.16,
  # --- Camion moyen ---
  "camion_moyen", "motorway",      "paved",    100,          1.00,                  49.40,
  "camion_moyen", "trunk",         "paved",     60,          1.00,                  49.40,
  "camion_moyen", "trunk",         "gravel",    40,          1.15,                  79.04,
  "camion_moyen", "primary",       "paved",     60,          1.00,                  49.40,
  "camion_moyen", "primary",       "gravel",    40,          1.15,                  79.04,
  "camion_moyen", "secondary",     "paved",     50,          1.00,                  49.40,
  "camion_moyen", "secondary",     "gravel",    35,          1.15,                  79.04,
  "camion_moyen", "tertiary",      "paved",     45,          1.00,                  49.40,
  "camion_moyen", "tertiary",      "unpaved",   25,          1.30,                 118.56,
  "camion_moyen", "unclassified",  "gravel",    30,          1.15,                  79.04,
  "camion_moyen", "unclassified",  "unpaved",   20,          1.30,                 118.56,
  # --- Camion lourd ---
  "camion_lourd", "motorway",      "paved",     80,          1.00,                  79.04,
  "camion_lourd", "trunk",         "paved",     50,          1.00,                  79.04,
  "camion_lourd", "trunk",         "gravel",    30,          1.25,                 138.32,
  "camion_lourd", "primary",       "paved",     50,          1.00,                  79.04,
  "camion_lourd", "primary",       "gravel",    30,          1.25,                 138.32,
  "camion_lourd", "secondary",     "paved",     40,          1.00,                  79.04,
  "camion_lourd", "secondary",     "gravel",    25,          1.25,                 138.32,
  "camion_lourd", "tertiary",      "paved",     35,          1.00,                  79.04,
  "camion_lourd", "tertiary",      "unpaved",   18,          1.50,                 217.36,
  "camion_lourd", "unclassified",  "gravel",    22,          1.25,                 138.32,
  "camion_lourd", "unclassified",  "unpaved",   14,          1.50,                 217.36
)
duck_write(params_flotte_type_route_df, "params_flotte_type_route")

# ── Table 3 : coûts de transbordement entre véhicules ─────────────────────────
# Coût fixe en RWF pour transférer la cargaison d'un type de véhicule à un autre
# dans un entrepôt (manutention, attente, administration).
# Pour ajouter une combinaison : ajouter une ligne dans ce tribble.
# Ces coûts servent dans le graphe multi-modal (Partie V.2) pour décider
# si le surcoût du changement de véhicule est compensé par un itinéraire plus
# économique avec un autre type de camion.
couts_transbordement_df <- tribble(
  ~vehicule_origine,  ~vehicule_destination, ~cout_rwf_fixe,
  "camion_lourd",     "camion_moyen",          24700,
  "camion_lourd",     "camionnette",           39520,
  "camion_moyen",     "camion_lourd",          24700,
  "camion_moyen",     "camionnette",           14820,
  "camionnette",      "camion_moyen",          14820,
  "camionnette",      "camion_lourd",          39520
)
duck_write(couts_transbordement_df, "couts_transbordement")

# ── Table 4 : coûts de transport pré-frontière par pays et par secteur ────────
# Ces coûts représentent le coût moyen de transport d'une marchandise
# depuis son point d'origine dans le pays étranger jusqu'à la frontière du pays étudié.
# Ils s'ajoutent au coût de transport interne dans le modèle gravitaire.
# Source : estimations calibrées sur les données de coût de transport régional
# (Banque Mondiale, CPCS, données COMESA).
# Unité : RWF par tonne
# La logique est simple : faire venir du café de Kampala (Ouganda) coûte moins
# cher que faire venir de l'acier de Dar es Salaam (Tanzanie) car la distance
# est bien plus courte et les routes sont meilleures.
#
# Ces coûts (comme le commerce extérieur) ne figurent PAS dans la SAM et sont issues
# de Claude. Ils sont saisis directement sur les 11 secteurs du
# modèle (un coût par tonne ne se « répartit » pas : Chimie_petrole, Manufactures
# et Energie_eau partagent le même niveau « industriel », et Cultures_export
# s'aligne sur Agriculture).
couts_prebordure_df <- tribble(
  ~pays,       ~secteur,          ~cout_rwf_tonne,
  # ── Ouganda (corridors Nord : Kampala → Gatuna/Kagitumba) ───────────────────
  # Distance moyenne Kampala-frontière du pays : ~500km, routes bitumées
  "Ouganda",   "Agriculture",      34580,
  "Ouganda",   "Cultures_export",  34580,
  "Ouganda",   "Mines",            24700,
  "Ouganda",   "Agro_industrie",   29640,
  "Ouganda",   "Chimie_petrole",   27664,
  "Ouganda",   "Manufactures",     27664,
  "Ouganda",   "Construction",     41496,
  "Ouganda",   "Commerce",         25688,
  "Ouganda",   "Transport",        17784,
  "Ouganda",   "Energie_eau",      27664,
  "Ouganda",   "Services",          7904,
  # ── Tanzanie (corridor Est : Dar es Salaam → Rusumo) ────────────────────────
  # Distance moyenne port Dar-frontière du pays : ~1300km
  # Coûts plus élevés car corridor plus long et qualité route variable
  "Tanzanie",  "Agriculture",      88920,
  "Tanzanie",  "Cultures_export",  88920,
  "Tanzanie",  "Mines",            54340,
  "Tanzanie",  "Agro_industrie",   74100,
  "Tanzanie",  "Chimie_petrole",   69160,
  "Tanzanie",  "Manufactures",     69160,
  "Tanzanie",  "Construction",    108680,
  "Tanzanie",  "Commerce",         64220,
  "Tanzanie",  "Transport",        44460,
  "Tanzanie",  "Energie_eau",      69160,
  "Tanzanie",  "Services",         11856,
  # ── RDC (corridor Ouest : Goma → Rubavu) ────────────────────────────────────
  # Distance courte mais infrastructure très dégradée
  # Coûts élevés malgré la proximité géographique
  "RDC",       "Agriculture",      27664,
  "RDC",       "Cultures_export",  27664,
  "RDC",       "Mines",            19760,
  "RDC",       "Agro_industrie",   24700,
  "RDC",       "Chimie_petrole",   29640,
  "RDC",       "Manufactures",     29640,
  "RDC",       "Construction",     37544,
  "RDC",       "Commerce",         21736,
  "RDC",       "Transport",        13832,
  "RDC",       "Energie_eau",      29640,
  "RDC",       "Services",          4940,
  # ── Burundi (corridor Sud : Bujumbura → Bugarama/Rusizi) ────────────────────
  # Distance moyenne Bujumbura-frontière du pays : ~150km
  # Infrastructure correcte sur axe principal
  "Burundi",   "Agriculture",      11856,
  "Burundi",   "Cultures_export",  11856,
  "Burundi",   "Mines",             8892,
  "Burundi",   "Agro_industrie",    9880,
  "Burundi",   "Chimie_petrole",   10868,
  "Burundi",   "Manufactures",     10868,
  "Burundi",   "Construction",     15808,
  "Burundi",   "Commerce",          8892,
  "Burundi",   "Transport",         5928,
  "Burundi",   "Energie_eau",      10868,
  "Burundi",   "Services",          1976
) %>%
  arrange(pays, secteur)
duck_write(couts_prebordure_df, "couts_prebordure")

cat("✓ Coûts pré-frontière chargés dans DuckDB :",
    nrow(couts_prebordure_df), "lignes\n\n")

# ==============================================================================
# Commerce extérieur du pays étudié par pays frontalier et par secteur (MILLIARDS DE RWF)
# Définitions :
#   imports_mrd_rwf : ce que le pays importe (= offre de l'entrepôt RoW
#                     vers les zones internes — fret entrant)
#   exports_mrd_rwf : ce que le pays exporte (= demande de l'entrepôt
#                     RoW sur la production locale — fret sortant)
# Ces valeurs alimentent les offre_zones / demande_zones des entrepôts RoW dans
# le modèle MRIO (03_transport.R, section VII.2).
#
# CONSTRUCTION (magnitude SAM × clé de répartition par pays) :
#   • MAGNITUDE : totaux imports/exports par secteur issus de la SAM IFPRI
#                 (sam$imports / $exports, en mrd RWF). C'est la source de
#                 vérité sur les VOLUMES échangés — désormais cohérente en unité
#                 avec les flux internes (tout est exprimé en mrd RWF).
#   • CLÉ PAYS  : la SAM ne ventile PAS par pays. On répartit donc chaque total
#                 sectoriel entre les 4 corridors frontaliers au prorata de la
#                 STRUCTURE géographique des estimations d'expert ci-dessous
#                 (NISR External Trade 2022 + RDB 2022). Concrètement :
#                   valeur[pays, s] = total_SAM[s] × part_pays[pays, s]
#                 où part_pays = (commerce du pays) / (commerce total du secteur),
#                 calculée séparément pour imports et exports, directement sur les
#                 11 secteurs du modèle.
# Pour changer la clé pays, il suffit de modifier les poids relatifs de la table
# CLE_REPARTITION_PAYS ci-dessous (les niveaux absolus n'ont pas d'importance,
# seules comptent les proportions entre pays au sein d'un secteur).
# ==============================================================================

# Note : les poids des secteurs « industriels » (Chimie_petrole, Manufactures,
# Energie_eau) partagent une même structure pays, et Cultures_export s'aligne sur
# Agriculture — faute d'estimations d'expert plus fines par pays.
# Lecture des grands équilibres encodés ici :
#   - Ouganda : 1er partenaire régional (corridor Nord) ; poids fort sur la plupart
#               des biens de consommation et agricoles.
#   - Tanzanie: corridor Est via Dar es Salaam → dominant pour le vrac industriel
#               importé (pétrole, machines) et l'export de minerais.
#   - RDC     : corridor Ouest (Rubavu/Goma) → débouché d'export notable (minerais,
#               agro, manufactures de réexport).
#   - Burundi : corridor Sud, volumes faibles.
CLE_REPARTITION_PAYS <- tribble(
  ~pays,      ~secteur,          ~poids_import, ~poids_export,
  # ── Ouganda ──────────────────────────────────────────────────────────────────
  "Ouganda",  "Agriculture",          110,           35,
  "Ouganda",  "Cultures_export",      110,           35,
  "Ouganda",  "Mines",                 12,           55,
  "Ouganda",  "Agro_industrie",        90,           25,
  "Ouganda",  "Chimie_petrole",        65,           12,
  "Ouganda",  "Manufactures",          65,           12,
  "Ouganda",  "Construction",          22,            2,
  "Ouganda",  "Commerce",              45,           18,
  "Ouganda",  "Transport",             10,            8,
  "Ouganda",  "Energie_eau",           65,           12,
  "Ouganda",  "Services",               5,            4,
  # ── Tanzanie ─────────────────────────────────────────────────────────────────
  "Tanzanie", "Agriculture",           55,           22,
  "Tanzanie", "Cultures_export",       55,           22,
  "Tanzanie", "Mines",                 35,          105,
  "Tanzanie", "Agro_industrie",       110,           12,
  "Tanzanie", "Chimie_petrole",       215,            9,
  "Tanzanie", "Manufactures",         215,            9,
  "Tanzanie", "Construction",          32,            1,
  "Tanzanie", "Commerce",              85,           12,
  "Tanzanie", "Transport",             22,            6,
  "Tanzanie", "Energie_eau",          215,            9,
  "Tanzanie", "Services",              12,            2,
  # ── RDC ──────────────────────────────────────────────────────────────────────
  "RDC",      "Agriculture",           42,           18,
  "RDC",      "Cultures_export",       42,           18,
  "RDC",      "Mines",                 32,           85,
  "RDC",      "Agro_industrie",        22,            9,
  "RDC",      "Chimie_petrole",        12,            6,
  "RDC",      "Manufactures",          12,            6,
  "RDC",      "Construction",           5,            1,
  "RDC",      "Commerce",              18,            9,
  "RDC",      "Transport",              5,            3,
  "RDC",      "Energie_eau",           12,            6,
  "RDC",      "Services",               2,            1,
  # ── Burundi ──────────────────────────────────────────────────────────────────
  "Burundi",  "Agriculture",           32,           12,
  "Burundi",  "Cultures_export",       32,           12,
  "Burundi",  "Mines",                  6,           30,
  "Burundi",  "Agro_industrie",        18,            6,
  "Burundi",  "Chimie_petrole",         9,            3,
  "Burundi",  "Manufactures",           9,            3,
  "Burundi",  "Construction",           3,            1,
  "Burundi",  "Commerce",              12,            5,
  "Burundi",  "Transport",              3,            2,
  "Burundi",  "Energie_eau",            9,            3,
  "Burundi",  "Services",               1,            1
)

# 1) Parts pays = proportions normalisées à 1 par secteur (imports/exports).
parts_pays <- CLE_REPARTITION_PAYS %>%
  group_by(secteur) %>%
  mutate(part_import = poids_import / sum(poids_import),
         part_export = poids_export / sum(poids_export)) %>%
  ungroup() %>%
  select(pays, secteur, part_import, part_export)

# 2) Totaux sectoriels SAM (mrd RWF) par secteur 
totaux_commerce_sam <- tibble(
  secteur     = SECTEURS,
  imports_sam = as.numeric(sam$imports[SECTEURS]),
  exports_sam = as.numeric(sam$exports[SECTEURS])
)

# 3) Application : magnitude SAM (secteur) × part pays (secteur).
COMMERCE_EXTERIEUR_NISR <- totaux_commerce_sam %>%
  inner_join(parts_pays, by = "secteur", relationship = "many-to-many") %>%
  transmute(
    pays,
    secteur,
    imports_mrd_rwf = imports_sam * part_import,
    exports_mrd_rwf = exports_sam * part_export
  ) %>%
  arrange(pays, secteur)

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
PERSIST_DIAG_RES     <- file.path(DIR_PERSIST, "persist_diag_reseau.rds")

# ==============================================================================
# TESTS DE SENSIBILITÉ — application des surcharges
# ==============================================================================
# Ce bloc est volontairement placé À LA FIN du fichier : tous les paramètres
# existent alors, et les surcharges écrasent la valeur de référence juste avant
# que les modules 01→05 ne commencent à les utiliser.
#
# Pour chaque entrée nommée de SENSIBILITE :
#   1. on vérifie que le paramètre EXISTE déjà (garde-fou contre les fautes de
#      frappe : sans cela, "BETA_SECTEURS" créerait silencieusement un objet
#      inutile et le test tournerait sur les valeurs de référence) ;
#   2. si la valeur fournie est une fonction, on l'applique à la valeur
#      courante (variations relatives : function(b) b * 1.2) ; sinon on
#      remplace directement ;
#   3. on trace dans la console l'ancienne et la nouvelle valeur.
# ==============================================================================

if (length(SENSIBILITE) > 0) {

  if (!EST_SENSIBILITE) {
    stop("SENSIBILITE est non vide mais SCENARIO_ID vaut encore \"reference\" : ",
         "les sorties du run de référence seraient écrasées par un test de ",
         "sensibilité. Donnez un SCENARIO_ID distinct.")
  }

  cat("\n=== TEST DE SENSIBILITÉ :", SCENARIO_ID, "===\n")
  cat("  Libellé :", SCENARIO_LIBELLE, "\n")

  for (.nom in names(SENSIBILITE)) {

    # 1. Garde-fou : le paramètre doit déjà exister dans 00_parametres.R
    if (!exists(.nom, envir = globalenv(), inherits = FALSE)) {
      stop("SENSIBILITE : le paramètre '", .nom, "' n'existe pas dans ",
           "00_parametres.R. Vérifiez l'orthographe.")
    }

    .ancien <- get(.nom, envir = globalenv())
    .modif  <- SENSIBILITE[[.nom]]

    # 2. Fonction = transformation de la valeur courante ; sinon remplacement
    .nouveau <- if (is.function(.modif)) .modif(.ancien) else .modif
    assign(.nom, .nouveau, envir = globalenv())

    # 3. Trace console (les objets volumineux ne sont pas affichés en entier)
    .apercu <- function(x) {
      if (is.numeric(x) && !is.null(names(x)) && length(x) <= 15) {
        paste(names(x), round(x, 4), sep = "=", collapse = ", ")
      } else if (is.numeric(x) && length(x) == 1) {
        as.character(round(x, 4))
      } else {
        .taille <- if (is.null(dim(x))) length(x) else dim(x)
        paste0("<", class(x)[1], " de dimension ",
               paste(.taille, collapse = "x"), ">")
      }
    }
    cat("  • ", .nom, "\n",
        "      avant : ", .apercu(.ancien),  "\n",
        "      après : ", .apercu(.nouveau), "\n", sep = "")
  }

  # ── Recalcul des grandeurs DÉRIVÉES ─────────────────────────────────────────
  # Certains paramètres en alimentent d'autres, calculés plus haut dans ce
  # fichier. Les surcharger sans recalculer les dérivés produirait un modèle
  # incohérent (ex. : nouvelles valeurs unitaires mais anciens tonnages).

  # VALEUR_RWF_PAR_TONNE → tonnes par milliard de RWF → liste des secteurs de fret
  if ("VALEUR_RWF_PAR_TONNE" %in% names(SENSIBILITE)) {
    TONNES_PAR_mrd_RWF <- 1e9 / VALEUR_RWF_PAR_TONNE
    SECTEURS_FRET      <- SECTEURS[TONNES_PAR_mrd_RWF[SECTEURS] > 0]
    stopifnot(setequal(names(BETA_SECTEUR), SECTEURS_FRET))
    cat("  ↳ dérivés recalculés : TONNES_PAR_mrd_RWF, SECTEURS_FRET\n")
  }

  # params_flotte_df (valeur du temps, consommation, capacités…) → table DuckDB
  # La table params_flotte est lue en SQL par 02_couts.R : il faut la réécrire.
  if ("params_flotte_df" %in% names(SENSIBILITE)) {
    duck_write(params_flotte_df, "params_flotte")
    VEHICULES_IDS <- duck_query("SELECT vehicule_id, nom FROM params_flotte")
    cat("  ↳ dérivés recalculés : table DuckDB params_flotte\n")
  }

  # BETA_SECTEUR : cohérence avec les secteurs effectivement transportés
  if ("BETA_SECTEUR" %in% names(SENSIBILITE)) {
    stopifnot(setequal(names(BETA_SECTEUR), SECTEURS_FRET))
  }

  rm(.nom, .ancien, .modif, .nouveau, .apercu)

  cat("  Sorties dirigées vers :", DIR_CARTES, "\n")
  cat("=== fin des surcharges ===\n\n")
}

# ==============================================================================
# TESTS DE SENSIBILITÉ — marquage automatique des figures
# ==============================================================================
# On redéfinit ggsave() dans l'environnement global. Comme les scripts viz_*.R
# sont sourcés dans ce même environnement, TOUS leurs appels à ggsave() passent
# désormais par cette version, sans avoir à modifier une seule ligne de viz.
#
# En mode sensibilité, chaque figure enregistrée reçoit :
#   - un suffixe de fichier "_<SCENARIO_ID>" (en plus du sous-dossier dédié),
#     pour rester identifiable si l'image est déplacée dans un rapport ;
#   - une mention en bas de graphique rappelant qu'il s'agit d'un test.
# En mode référence, la fonction se contente de relayer ggplot2::ggsave().
# ==============================================================================

ggsave <- function(filename, plot = ggplot2::last_plot(), ...) {

  if (EST_SENSIBILITE) {

    # ── Suffixe : "carte_x.png" → "carte_x_beta_plus20.png" (extension gardée)
    .rep <- dirname(filename)
    .fic <- basename(filename)
    .ext <- tools::file_ext(.fic)
    filename <- file.path(
      .rep,
      paste0(tools::file_path_sans_ext(.fic), SUFFIXE_SCENARIO,
             if (nzchar(.ext)) paste0(".", .ext) else "")
    )

    # ── Mention en bas de figure. Si le graphique porte déjà un caption
    #    (source des données…), on ajoute la mention à la ligne plutôt que de
    #    l'écraser.
    if (inherits(plot, "ggplot")) {
      .caption_actuel <- plot$labels$caption
      plot <- plot +
        ggplot2::labs(caption = if (is.null(.caption_actuel)) {
          MENTION_SENSIBILITE
        } else {
          paste0(.caption_actuel, "\n", MENTION_SENSIBILITE)
        }) +
        ggplot2::theme(
          plot.caption = ggplot2::element_text(
            hjust = 0, face = "bold", size = 8, colour = "#B22222"
          )
        )
    }
  }

  ggplot2::ggsave(filename = filename, plot = plot, ...)
}

# ==============================================================================
# NOTE DE LECTURE — helper partagé par tous les viz_*.R
# ==============================================================================
# note_lecture() construit le texte affiché sous chaque carte/graphique pour
# expliquer comment le lire (ce qui est comparé, ce qu'un écart signifierait),
# sans avoir à rouvrir le code. Utilisé comme caption= dans labs() (ggplot2) ou
# comme texte de tm_credits() (tmap).
#
# str_wrap() est indispensable : element_text() et tm_credits() ne font PAS de
# retour à la ligne automatique — seuls les "\n" explicites comptent — donc une
# note un peu longue déborde silencieusement du support et se retrouve tronquée
# par le PNG plutôt que de provoquer une erreur.
#
# largeur_car est en CARACTÈRES, pas en pouces : ≈ 12 caractères par pouce de
# largeur de figure à la taille de police par défaut (7.8 pt pour les captions
# ggplot2, 0.65 pour les credits tmap). Une marge de sécurité est incluse : au
# ras de la largeur réelle, un mot long isolé sur une ligne suffit à dépasser
# et à être tronqué. Choisir largeur_car ≈ largeur_pouces × 12 (7 po → 82,
# 8 po → 93, 9 po → 105) ; ne jamais dépasser ~12/po sans re-vérifier le rendu.
THEME_NOTE_LECTURE <- theme(
  plot.caption = element_text(hjust = 0, color = "#555555", size = 7.8,
                               lineheight = 1.15, margin = margin(t = 10))
)
note_lecture <- function(texte, largeur_car = 105) {
  str_wrap(paste0("Lecture : ", texte), width = largeur_car)
}

cat("✓ 00_parametres.R chargé\n")
if (EST_SENSIBILITE) {
  cat("⚠ MODE SENSIBILITÉ ACTIF —", SCENARIO_LIBELLE, "\n")
  cat("  Les sorties du run de référence ne seront PAS écrasées.\n\n")
}