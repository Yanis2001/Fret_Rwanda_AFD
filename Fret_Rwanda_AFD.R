################################################################################
# PROJET : Réseau Routier pour Modélisation du Commerce de Fret - Rwanda
# OBJECTIF : Construire un graphe routier pondéré par les coûts de transport
#            généralisés, puis modéliser les flux de fret entre zones via un
#            modèle gravitaire calibré sur une table Input-Output fictive.
# AUTEUR  : Yanis
# DATE    : Mars 2026
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
# ── POUR RETROUVER LE DÉPÔT GITHUB ───────────────────────────────────────────
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
  "progress"       # Barre de progression
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

# Chemin vers le fichier de base de données (créé s'il n'existe pas)
DB_PATH <- "reseau_rwanda.duckdb"

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
DIR_OUTPUT <- "outputs"
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

# ── Coûts généralisés (gradient jaune pâle → bordeaux) ───────────────────────
PALETTE_COUTS <- c("#FFF7BC", "#FEC44F", "#D94701", "#7F0000")
# Lecture : faible coût = jaune pâle, coût élevé = bordeaux

# ── Ratio de coût entre véhicules (gradient rouge → jaune → vert) ────────────
# Rouge = coût du véhicule au numérateur élevé relativement à celui au dénominateur ; vert = inverse
PALETTE_RATIO <- c("#D73027", "#FC8D59", "#FEE090", "#91CF60", "#1A9850")

# ── Volume de trafic fret (gradient bleu clair → violet) ─────────────────────
PALETTE_FRET <- c("#CCE5FF", "#6BAED6", "#2171B5", "#6A0DAD")
# Lecture : faible trafic = bleu clair, trafic intense = violet

# ── Flux commerciaux OD (gradient bleu) ──────────────────────────────────────
PALETTE_FLUX_OD <- c("#EFF3FF", "#BDD7E7", "#6BAED6", "#2171B5", "#084594")

# ── Catégories de trafic (pour légendes textuelles) ──────────────────────────
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

# ── Table 1 : paramètres scalaires par véhicule ──────────────────────────────
# Ce tableau contient les caractéristiques physiques et économiques de chaque
# type de véhicule : consommation de carburant, prix du carburant, valeur
# du temps du chauffeur, coûts d'usure selon le type de route, capacité de
# chargement, et pénalité en zone urbaine (congestion, restrictions de tonnage).
params_flotte_df <- tribble(
  ~vehicule_id,   ~nom,                    ~conso_base, ~facteur_paved, ~facteur_gravel, ~facteur_unpaved, ~facteur_conso_pente, ~prix_carburant, ~valeur_temps, ~usure_paved, ~usure_gravel, ~usure_unpaved, ~capacite_tonnes, ~facteur_urbain,
  "camionnette",  "Camionnette (<3.5t)",    10,          1.00,           1.08,            1.18,             1.0,                  1.40,            4.5,           0.02,         0.04,          0.07,            3.0,              1.05,
  "camion_moyen", "Camion moyen (5-10t)",   20,          1.00,           1.15,            1.30,             1.5,                  1.40,            7.5,           0.05,         0.08,          0.12,            7.5,              1.25,
  "camion_lourd", "Camion lourd (>10t)",    35,          1.00,           1.25,            1.50,             2.0,                  1.40,            10.0,          0.08,         0.14,          0.22,            20.0,             1.60
)
duck_write(params_flotte_df, "params_flotte")

# ── Table 2 : vitesses par véhicule × type de route × surface ────────────────
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

# ── Table 3 : facteurs de pente par véhicule × catégorie ─────────────────────
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

# ── Table 4 : coûts de transbordement entre véhicules ────────────────────────
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
  # ── Ouganda (corridors Nord : Kampala → Gatuna/Kagitumba) ─────────────────
  # Distance moyenne Kampala-frontière Rwanda : ~500km, routes bitumées
  "Ouganda",   "Agriculture",     35.0,   
  "Ouganda",   "Mines",           25.0,
  "Ouganda",   "Agro_industrie",  30.0,
  "Ouganda",   "Industrie",       28.0,
  "Ouganda",   "Construction",    42.0,
  "Ouganda",   "Commerce",        26.0,
  "Ouganda",   "Transport",       18.0,
  "Ouganda",   "Services",         8.0,
  # ── Tanzanie (corridor Est : Dar es Salaam → Rusumo) ─────────────────────
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
  # ── RDC (corridor Ouest : Goma → Rubavu) ─────────────────────────────────
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
  # ── Burundi (corridor Sud : Bujumbura → Bugarama/Rusizi) ─────────────────
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


################################################################################
# PARTIE II — ACQUISITION DES DONNÉES GÉOGRAPHIQUES
# Télécharge et charge en mémoire toutes les sources de données brutes.
# Aucun calcul ni transformation ici — uniquement du chargement.
# Si les fichiers sources changent (nouveau PBF, nouveau DEM), relancer
# cette partie invalide le cache des pentes (supprimer pentes_cache.rds).
################################################################################

# ==============================================================================
# II.1 : Données routières (PBF)
# Télécharge le fichier PBF depuis MinIO et charge les segments routiers
# utiles au fret via une requête GDAL filtrée sur les types de routes.
# ==============================================================================

# ── Téléchargement depuis MinIO (SSP Cloud) ───────────────────────────────────
# Le fichier PBF (Protocolbuffer Binary Format) est le format natif d'OpenStreetMap.
# Il est stocké sur le bucket S3 personnel sur la plateforme SSP Cloud.
# save_object() télécharge l'objet S3 vers le répertoire de travail local.
# Un "bucket S3" est un espace de stockage dans le cloud, similaire à un dossier
# Google Drive ou Dropbox, mais accessible via une API (interface programmatique).
# MinIO est une implémentation open-source compatible avec l'API Amazon S3,
# utilisée sur la plateforme SSP Cloud de l'INSEE/CASD.
save_object(
  object    = "data/raw/rwanda-260315.osm.pbf",  # Chemin dans le bucket S3
  bucket    = "yanisdumas",                        # Nom du bucket MinIO
  file      = "rwanda-260315.osm.pbf",            # Nom du fichier local après téléchargement
  region    = "",                                  # Région vide pour MinIO (non AWS standard)
  use_https = TRUE,
  base_url  = "minio.lab.sspcloud.fr"             # Point d'accès MinIO SSP Cloud
)

chemin_pbf <- "rwanda-260315.osm.pbf"

# Vérification de l'existence du fichier avant de continuer.
# stop() interrompt le script avec un message d'erreur explicite.
if (!file.exists(chemin_pbf)) stop("Fichier PBF introuvable.")

# ── Lecture sélective du fichier PBF ─────────────────────────────────────────
# st_read() peut lire directement un fichier PBF via le driver GDAL/OGR.
# On ne charge que la couche "lines" (routes linéaires) et uniquement
# les types de routes utiles pour le fret (pas les chemins piétons, pistes cyclables…).
# La clause WHERE est exécutée au niveau du driver GDAL : seuls les segments
# pertinents sont chargés en mémoire (gain mémoire important sur un pays entier).
# "highway IN ('motorway', ...)" : dans OSM, l'attribut "highway" classe le type
# de route. On ne garde que les routes sur lesquelles un camion peut circuler.
routes_rwanda_raw <- st_read(
  chemin_pbf,
  layer = "lines",
  query = "SELECT * FROM lines
           WHERE highway IN
           ('motorway','trunk','primary','secondary','tertiary','unclassified')",
  quiet = FALSE  # Afficher les informations de chargement
)

cat("✓ Données chargées :", nrow(routes_rwanda_raw), "segments\n\n")

# Vérification des landuse disponibles
# "landuse" est un attribut OSM qui décrit l'utilisation du sol :
# résidentiel, commercial, industriel, etc. On sonde ici ce qui est
# disponible dans le fichier PBF avant de l'utiliser pour tagger les zones.
landuse_test <- st_read(
  chemin_pbf, layer = "multipolygons",
  query = "SELECT landuse FROM multipolygons
           WHERE landuse IN ('residential','commercial','industrial','retail')",
  quiet = TRUE
)
cat("Zones par landuse :\n")
print(table(landuse_test$landuse))

# Vérification des place disponibles
# "place" est un attribut OSM qui désigne le type de localité humaine :
# ville (city), bourg (town), village, quartier (suburb), etc.
place_test <- st_read(
  chemin_pbf, layer = "multipolygons",
  query = "SELECT place FROM multipolygons
           WHERE place IN ('city','town','village','suburb','neighbourhood')",
  quiet = TRUE
)
cat("\nZones par place :\n")
print(table(place_test$place))

# ==============================================================================
# II.2 : Nettoyage des attributs routiers
# Extrait les tags OSM (surface, vitesse, sens unique) via regex, puis
# harmonise les valeurs hétérogènes en SQL via DuckDB (CASE WHEN).
# ==============================================================================


# Vérifier le nom de la colonne géométrie dans la couche points
# La couche "points" du PBF contient les lieux ponctuels (villes, POI…).
# On récupère les villes et les bourgs pour les intégrer plus tard
# comme zones d'entreposage potentielles.
villes_raw <- st_read(
  chemin_pbf, layer = "points",
  query = "SELECT name, place FROM points
           WHERE place IN ('city','town')",
  quiet = TRUE
)
cat("Colonnes disponibles :\n")
print(names(villes_raw))

# On charge maintenant les villes/bourgs en objet sf exploitable :
# st_as_sf() : s'assure que c'est bien un objet géospatial R.
# st_transform(crs = 32735) : reprojette en UTM Zone 35S (coordonnées métriques
# adaptées au Rwanda, permettant de mesurer des distances en mètres).
# filter(!is.na(name)) : supprime les lieux sans nom dans OSM.
# mutate(type = ...) : crée une colonne "type" — les villes (city) deviennent
# des "hub", les bourgs (town) deviennent des "ville".
villes_osm <- st_read(
  chemin_pbf, layer = "points",
  query = "SELECT name, place FROM points
           WHERE place IN ('city','town')",
  quiet = TRUE
) %>%
  st_as_sf() %>%
  st_transform(crs = 32735) %>%
  filter(!is.na(name)) %>%
  mutate(type = if_else(place == "city", "hub", "ville"))

cat("Villes récupérées :", nrow(villes_osm), "\n")
print(villes_osm %>% st_drop_geometry() %>% select(name, place))


# ── Extraction des tags OSM depuis la colonne other_tags ──────────────────────
# Dans les fichiers PBF, les attributs secondaires (surface, vitesse max, etc.)
# sont stockés dans une colonne texte "other_tags" au format :
#   "clé1"=>"valeur1","clé2"=>"valeur2",...
# Cette fonction extrait la valeur associée à une clé donnée via une regex.
# Une "regex" (expression régulière) est un pattern de recherche de texte.
# Ici, on cherche par exemple le pattern "surface"=>"<valeur>" pour extraire
# uniquement <valeur> (ex : "asphalt", "gravel", "unpaved").
extraire_tag <- function(other_tags, cle) {
  if (is.na(other_tags)) return(NA_character_)
  
  # Pattern : cherche "cle"=>"valeur" où valeur ne contient pas de guillemets
  pattern <- paste0('"', cle, '"=>"([^"]*)"')
  match   <- regmatches(other_tags, regexec(pattern, other_tags))
  
  # regexec retourne une liste :
  #   [[1]][1] = match global (toute la chaîne)
  #   [[1]][2] = groupe capturant (la valeur entre guillemets)
  if (length(match[[1]]) > 1) match[[1]][2] else NA_character_
}

# ── Étape 3a : extraction des tags (nécessite R, non vectorisable en SQL) ─────
# sapply() applique extraire_tag() sur chaque ligne de la colonne other_tags.
# C'est l'opération la plus lente de cette partie (boucle implicite sur ~10 000 lignes).
# Pour chaque segment routier, on extrait 4 attributs :
#   surface  : type de revêtement ("asphalt", "gravel", "dirt"…)
#   maxspeed : vitesse maximale autorisée (en km/h)
#   lanes    : nombre de voies
#   oneway   : sens unique ("yes" ou "no")
routes_attrs_raw <- routes_rwanda_raw %>%
  rename(geometry = `_ogr_geometry_`) %>%    # Normalisation du nom de la colonne géométrie
  mutate(
    surface  = sapply(other_tags, extraire_tag, cle = "surface"),
    maxspeed = sapply(other_tags, extraire_tag, cle = "maxspeed"),
    lanes    = sapply(other_tags, extraire_tag, cle = "lanes"),
    oneway   = sapply(other_tags, extraire_tag, cle = "oneway")
  ) %>%
  select(osm_id, name, highway, surface, maxspeed, lanes, oneway, geometry) %>%
  rename(road_type = highway) %>%   # Renommage pour cohérence avec le reste du modèle
  st_as_sf() %>%
  filter(st_is_valid(geometry)) %>% # Supprimer les géométries invalides (auto-intersections…)
  st_make_valid()                   # Tenter de réparer les géométries invalides restantes

# ── Étape 3b : harmonisation de la surface via DuckDB SQL ─────────────────────
# On détache la géométrie (non stockable dans DuckDB standard) et on charge
# uniquement les attributs textuels dans DuckDB.
# st_drop_geometry() : retire la colonne de coordonnées géographiques du tableau
# (DuckDB ne sait pas stocker des géométries spatiales dans sa version standard).
attrs_df <- routes_attrs_raw %>% st_drop_geometry()
duck_write(attrs_df, "routes_attrs_raw")

# La requête CASE WHEN harmonise les valeurs OSM hétérogènes de "surface"
# (ex : "asphalt", "concrete", "paved" → tous ramenés à "paved")
# puis impute les valeurs manquantes selon le type de route :
#   - Les routes nationales (trunk, primary) sont supposées bitumées au Rwanda
#   - Les routes secondaires : gravier (fréquent hors Kigali)
#   - Les routes tertiaires et non classées : piste en terre par défaut
# CASE WHEN en SQL est l'équivalent du "si...alors...sinon" dans d'autres langages.
# La structure est : CASE WHEN condition THEN résultat WHEN ... ELSE résultat_par_défaut END
attrs_clean <- duck_query("
  SELECT
    osm_id,
    name,
    road_type,
    maxspeed,
    lanes,
    oneway,
    CASE
      -- Valeurs OSM synonymes de 'bitumé'
      WHEN surface IN ('paved','asphalt','concrete')          THEN 'paved'
      -- Valeurs OSM synonymes de 'gravier compacté'
      WHEN surface IN ('gravel','compacted','fine_gravel')    THEN 'gravel'
      -- Valeurs OSM synonymes de 'piste en terre'
      WHEN surface IN ('unpaved','dirt','earth','ground')     THEN 'unpaved'
      -- Imputation selon le type de route si la surface est manquante dans OSM
      WHEN surface IS NULL
       AND road_type IN ('motorway','trunk','primary')        THEN 'paved'
      WHEN surface IS NULL
       AND road_type = 'secondary'                            THEN 'gravel'
      WHEN surface IS NULL
       AND road_type IN ('tertiary','unclassified')           THEN 'unpaved'
      ELSE 'unpaved'   -- Valeur par défaut pour les cas non couverts ci-dessus
    END AS surface
  FROM routes_attrs_raw
")

# Réintégration de la géométrie sf (impossible à stocker dans DuckDB standard)
# par jointure sur osm_id. On conserve ainsi la colonne géométrie de routes_attrs_raw.
# left_join() : fusionne deux tableaux en conservant toutes les lignes du tableau
# de gauche (routes_attrs_raw) et en y ajoutant les colonnes du tableau de droite
# (attrs_clean), en faisant correspondre les lignes via la colonne osm_id.
routes_rwanda <- routes_attrs_raw %>%
  select(osm_id, geometry) %>%
  left_join(attrs_clean, by = "osm_id") %>%
  st_as_sf() %>%
  # CRS 32735 = WGS 84 / UTM Zone 35S : projection métrique adaptée au Rwanda
  # Nécessaire pour calculer des longueurs en mètres et des pentes en %
  st_transform(crs = 32735)

cat("✓ Nettoyage terminé :", nrow(routes_rwanda), "segments — surface harmonisée via DuckDB\n\n")

# ==============================================================================
# II.3 : Couches administratives et fond de carte
# Extrait frontières, provinces, lacs et parcs depuis le PBF.
# Définit fond_carte(), la fonction réutilisée dans toutes les cartes du script.
# ==============================================================================

# ── Frontière nationale (admin_level = 2) ─────────────────────────────────────
# Dans OSM, admin_level = 2 désigne les frontières nationales.
# st_union() fusionne tous les polygones de la couche en un seul polygone,
# ce qui est utile pour tracer la frontière du Rwanda d'un seul tenant.
rwanda_boundary <- st_read(
  chemin_pbf, layer = "multipolygons",
  query = "SELECT * FROM multipolygons WHERE admin_level = '2'",
  quiet = TRUE
) %>%
  rename(geometry = `_ogr_geometry_`) %>%
  st_as_sf() %>%
  st_make_valid() %>%
  st_transform(crs = 32735)

rwanda_national <- rwanda_boundary %>%
  st_union() %>%
  st_as_sf() %>%
  st_make_valid()

# ── Provinces (admin_level = 4) ───────────────────────────────────────────────
# Dans OSM, admin_level = 4 correspond aux subdivisions de premier niveau
# (provinces au Rwanda). On filtre ensuite pour ne garder que les géométries
# de type POLYGON ou MULTIPOLYGON (et non des lignes ou des points).
rwanda_provinces <- st_read(
  chemin_pbf, layer = "multipolygons",
  query = "SELECT * FROM multipolygons WHERE admin_level = '4'",
  quiet = TRUE
) %>%
  rename(geometry = `_ogr_geometry_`) %>%
  st_as_sf() %>%
  st_make_valid() %>%
  filter(st_geometry_type(geometry) %in% c("POLYGON", "MULTIPOLYGON")) %>%
  st_transform(crs = 32735)

# Fallback : utiliser la frontière nationale si les provinces sont absentes du PBF
if (nrow(rwanda_provinces) == 0) rwanda_provinces <- rwanda_national

cat("✓ Couches administratives extraites\n")

# ── Lacs depuis le PBF ────────────────────────────────────────────────────────
# Filtrage sur > 1 km² pour ne conserver que les lacs significatifs
# (lac Kivu, lac Rweru, lac Muhazi…). tryCatch gère l'absence de données.

lacs_ok <- FALSE
# tryCatch() permet de continuer le script si le téléchargement échoue
# Si la lecture du PBF échoue (erreur GDAL, données manquantes…), le script
# ne s'arrête pas : il affiche juste un avertissement et continue sans les lacs.
tryCatch({
  lacs_raw <- st_read(
    chemin_pbf, layer = "multipolygons",
    query = "SELECT * FROM multipolygons
             WHERE natural = 'water'
             OR other_tags LIKE '%\"natural\"=>\"water\"%'",
    quiet = TRUE
  ) %>%
    rename(geometry = `_ogr_geometry_`) %>%
    st_as_sf() %>%
    st_make_valid() %>%
    filter(st_geometry_type(geometry) %in% c("POLYGON", "MULTIPOLYGON")) %>%
    st_transform(crs = 32735) %>%
    mutate(aire_km2 = as.numeric(st_area(geometry)) / 1e6) %>% # Crée la colonne aire km2
    filter(aire_km2 > 1)
  if (nrow(lacs_raw) > 0) lacs_ok <- TRUE
  cat("  Lacs chargés :", nrow(lacs_raw), "\n")
}, error = function(e) cat("  ⚠ Lacs non disponibles dans le PBF\n"))


# ── Parcs naturels depuis le PBF ──────────────────────────────────────────────
# Les parcs sont tagués de trois façons dans OSM :
#   boundary = national_park      → parcs nationaux officiels
#   boundary = protected_area     → zones protégées (réserves, sanctuaires)
#   leisure  = nature_reserve     → réserves naturelles

parcs_ok <- FALSE

tryCatch({
  parcs_raw <- st_read(
    chemin_pbf, layer = "multipolygons",
    query = "SELECT * FROM multipolygons
             WHERE boundary IN ('national_park', 'protected_area')
             OR    leisure   =  'nature_reserve'",
    quiet = TRUE
  ) %>%
    rename(geometry = `_ogr_geometry_`) %>%
    st_as_sf() %>%
    st_make_valid() %>%
    filter(st_geometry_type(geometry) %in% c("POLYGON", "MULTIPOLYGON")) %>%
    st_transform(crs = 32735) %>%
    mutate(
      aire_km2 = as.numeric(st_area(geometry)) / 1e6,
      # Récupérer le nom anglais depuis other_tags si disponible
      nom_en   = sapply(other_tags, extraire_tag, cle = "name:en"),
      nom_parc = if_else(!is.na(nom_en) & nom_en != "", nom_en, name)
    ) %>%
    filter(aire_km2 > 5)   # Exclure les micro-zones (< 5 km²)
  
  if (nrow(parcs_raw) > 0) {
    parcs_ok <- TRUE
    cat("  Parcs naturels chargés :", nrow(parcs_raw), "\n")
    cat("  Noms :", paste(parcs_raw$nom_parc, collapse = ", "), "\n")
  } else {
    cat("  ⚠ Aucun parc trouvé dans le PBF\n")
  }
  
}, error = function(e) {
  cat("  ⚠ Parcs non disponibles dans le PBF :", conditionMessage(e), "\n")
})

# ── Zone d'affichage (bbox 250km × 250km centrée sur le Rwanda) ───────────────
# Buffer de 125km de chaque côté du centroïde pour afficher les frontières voisines
# Cette zone d'affichage légèrement plus grande que le Rwanda permet de voir
# les pays voisins sur les cartes (Ouganda, Tanzanie, RDC, Burundi).

# 1. Calcul du centroïde du Rwanda (point central)
centre_rwanda <- rwanda_national %>% st_centroid() %>% st_coordinates()
centre_x <- centre_rwanda[1, "X"]  # Coordonnée X (Est-Ouest) du centroïde
centre_y <- centre_rwanda[1, "Y"]  # Coordonnée Y (Nord-Sud) du centroïde

# 2. Définition du buffer de 125 km 
buffer_km <- 125000 

# 3. Construction manuelle d'un polygone carré (bbox) autour du centroïde
#    Les coordonnées sont calculées en ajoutant/soustrayant buffer_km aux coordonnées du centroïde
#    Format : liste de points dans l'ordre (coin bas-gauche → coin bas-droit → coin haut-droit → coin haut-gauche → retour au coin bas-gauche)
#    st_sfc() encapsule la géométrie dans un objet sf reconnu par les fonctions de cartographie.
bbox_poly <- st_sfc(st_polygon(list(rbind(
  # Coin Sud-Ouest : c(X,Y) = c(Ouest, Sud)
  c(centre_x - buffer_km, centre_y - buffer_km),
  
  # Coin Sud-Est : c(X,Y) = c(Est, Sud)
  c(centre_x + buffer_km, centre_y - buffer_km),
  
  # Coin Nord-Est : c(X,Y) = c(Est, Nord)
  c(centre_x + buffer_km, centre_y + buffer_km),
  
  # Coin Nord-Ouest : c(X,Y) = c(Ouest, Nord)
  c(centre_x - buffer_km, centre_y + buffer_km),
  
  # Retour au coin Sud-Ouest pour fermer le polygone
  c(centre_x - buffer_km, centre_y - buffer_km)
))), crs = 32735) %>% st_as_sf()

# 4. Extraction de la bbox (coordonnées min/max) pour utilisation dans tmap
#    st_bbox() retourne un vecteur [xmin, ymin, xmax, ymax]
bbox_carto <- st_bbox(bbox_poly)
#   - xmin = centre_x - buffer_km (Ouest)
#   - ymin = centre_y - buffer_km (Sud)
#   - xmax = centre_x + buffer_km (Est)
#   - ymax = centre_y + buffer_km (Nord)


# ── Fonction de fond de carte réutilisable ────────────────────────────────────
# fond_carte() : une fonction R qui crée les couches cartographiques de base
# (provinces en fond gris, frontière nationale, parcs en vert, lacs en bleu).
# Toutes les cartes thématiques du script commencent par fond_carte() puis
# ajoutent leur couche spécifique avec l'opérateur "+".
# Cela évite de répéter le même code de fond à chaque nouvelle carte.

# Crée les couches de base (provinces, frontière, lacs) communes à toutes les cartes.
# Retourne un objet tmap auquel on ajoute des couches thématiques avec +.

fond_carte <- function() {
  
  # tm_shape() : déclare la couche spatiale à représenter.
  # tm_polygons() : dessine des polygones remplis.
  # fill = "#F5F5F0" : couleur de remplissage (gris très pâle pour le fond).
  # col = "#AAAAAA" : couleur des bordures (gris moyen pour les limites de provinces).
  # lwd : épaisseur du trait de bordure.
  carte <- tm_shape(rwanda_provinces, bbox = bbox_carto) +
    tm_polygons(
      fill = "#F5F5F0",
      col  = "#AAAAAA",
      lwd  = 0.8,
      fill.legend = tm_legend(show = FALSE)
    ) +
    tm_shape(rwanda_national) +
    tm_borders(col = "#222222", lwd = 2.5)
  
  # ── Parcs naturels (sous les lacs pour ne pas les masquer) ────────────────
  # fill_alpha = 0.45 : transparence à 45%, pour voir les routes par-dessous.
  if (parcs_ok) carte <- carte +
      tm_shape(parcs_raw) +
      tm_polygons(
        fill        = "#A8D5A2",     # Vert pâle caractéristique des zones protégées
        col         = "#5A9E52",     # Bordure vert plus soutenu
        lwd         = 1.2,
        fill_alpha  = 0.45,          # Semi-transparent pour voir les routes dessous
        fill.legend = tm_legend(show = FALSE)
      )
  
  # ── Lacs ──────────────────────────────────────────────────────────────────
  if (lacs_ok) carte <- carte +
      tm_shape(lacs_raw) +
      tm_polygons(
        fill        = "#A8C8E8",
        col         = "#7AAAC8",
        lwd         = 0.5,
        fill.legend = tm_legend(show = FALSE)
      )
  
  carte
}

# ── Carte 1 : vérification post-nettoyage ──────────────────────────────────────
# Cette carte est générée pour vérifier visuellement que le réseau routier
# a été correctement chargé et nettoyé. Chaque type de route apparaît dans
# une couleur différente (définie dans PALETTE_ROAD_TYPE).
# tm_lines() : représente les lignes (routes) avec une couleur selon "road_type".
# tm_scale() : définit comment mapper les valeurs de "road_type" aux couleurs.
carte_verif_routes <- fond_carte() +
  tm_shape(routes_rwanda) +
  tm_lines(
    col       = "road_type",
    col.scale = tm_scale(values = PALETTE_ROAD_TYPE),
    col.legend = tm_legend(title = "Type de route"),
    lwd = 1.2
  ) +
  tm_title("Réseau Routier du Rwanda\nContrôle post-nettoyage (Partie 3)") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position = c("right", "top"))

# tmap_save() exporte la carte en fichier PNG haute résolution.
# width, height : dimensions en pixels. dpi = 300 : résolution pour impression.
tmap_save(carte_verif_routes,
          file.path(DIR_OUTPUT, "carte_verif_routes_partie3.png"),
          width = 3000, height = 2400, dpi = 300)

# Ce bloc if (FALSE) est volontairement désactivé (FALSE = ne jamais s'exécuter).
# Pour l'activer temporairement et afficher la carte interactive dans RStudio,
# il suffit de remplacer FALSE par TRUE et de relancer ce bloc.
# tmap_mode("view") active le mode interactif (carte zoomable dans le Viewer).
# tmap_mode("plot") remet le mode statique pour les exports PNG.
if (FALSE) {
  tmap_mode("view")
  print(carte_verif_routes)
  tmap_mode("plot")   # Remettre en mode statique pour la suite du script
}

cat("✓ Carte de vérification générée\n\n")

# ==============================================================================
# II.4 : Modèle Numérique de Terrain (DEM) 
# Télécharge le DEM SRTM depuis AWS via elevatr. En cas d'échec, génère 
# un DEM fictif calibré sur la topographie réelle du Rwanda. 
# Utilisé uniquement en Partie IV.2 pour le calcul des pentes.
# ==============================================================================

# Le DEM (Digital Elevation Model) est une grille de pixels où chaque valeur
# représente l'altitude en mètres au-dessus du niveau de la mer.
# Il sera utilisé pour calculer la pente de chaque segment routier
# (ratio dénivelé/longueur × 100 = pourcentage de pente).
# Le Rwanda est très montagneux (surnommé "le pays des mille collines"),
# ce qui rend ce calcul crucial pour estimer les coûts de transport.

# Créer l'emprise géographique à partir de la bbox des routes
# pour ne télécharger que la zone d'intérêt (Rwanda uniquement)
bbox_routes <- st_bbox(routes_rwanda)
emprise_points <- data.frame(
  x = c(bbox_routes["xmin"], bbox_routes["xmax"]),
  y = c(bbox_routes["ymin"], bbox_routes["ymax"])
)

# Reconvertir en objet sf en WGS84 (elevatr attend des coordonnées géographiques)
# Le système WGS84 (EPSG:4326) utilise latitude/longitude en degrés décimaux.
# C'est le système utilisé par les GPS grand public.
emprise_sf <- st_as_sf(emprise_points, coords = c("x","y"), crs = 32735) %>%
  st_transform(crs = 4326)

tryCatch({
  # z = 9 correspond à un zoom de ~300m/pixel, suffisant pour des routes
  # clip = "locations" : on ne récupère les données que dans l'emprise fournie
  dem_rwanda <- get_elev_raster(emprise_sf, z = 9, clip = "locations")
  dem_rwanda <- rast(dem_rwanda)   # Conversion raster R → terra SpatRaster
  # Reprojection en UTM 35S pour cohérence avec les routes
  # method = "bilinear" : interpolation bilinéaire (meilleure qualité que "nearest")
  # L'interpolation bilinéaire calcule la valeur d'un pixel en faisant une
  # moyenne pondérée de ses 4 voisins les plus proches, ce qui donne des
  # transitions d'altitude plus douces que le simple voisin le plus proche.
  dem_rwanda <- project(dem_rwanda, "EPSG:32735", method = "bilinear")
  cat("✓ DEM téléchargé et reprojeté\n")
  
}, error = function(e) {
  cat("⚠ Téléchargement DEM échoué — création d'un DEM fictif réaliste\n")
  
  # DEM fictif calibré sur la réalité physique du Rwanda :
  # - Altitude min : ~950m (vallées de l'Est et du Sud)
  # - Altitude max : ~2 500m (région volcanique non modélisée intégralement)
  # - Gradient Ouest-Est : le Rwanda est plus élevé à l'Ouest (dorsale Congo-Nil)
  ext_utm <- ext(bbox_routes["xmin"], bbox_routes["xmax"],
                 bbox_routes["ymin"], bbox_routes["ymax"])
  
  # Raster vide avec résolution ~90m (comparable au SRTM niveau 3)
  # resolution = 90 signifie que chaque pixel représente 90m × 90m sur le terrain.
  dem_rwanda <<- rast(ext_utm, resolution = 90, crs = "EPSG:32735")
  
  set.seed(123)   # Graine pour reproductibilité du bruit aléatoire
  n_cells    <- ncell(dem_rwanda)  # Nombre total de pixels dans le raster
  
  # xFromCell() retourne la coordonnée X (longitude UTM) du centre de chaque cellule
  x_coords <- xFromCell(dem_rwanda, 1:n_cells)
  
  # Gradient d'élévation : 1 500m à l'Est → 2 300m à l'Ouest
  # La formule normalise x_coords entre 0 (Est) et 1 (Ouest) puis multiplie par 800m
  # Cette formule simule la dorsale Congo-Nil qui traverse le Rwanda du Nord au Sud.
  base_elevation <- 1500 + (max(x_coords) - x_coords) /
    (max(x_coords) - min(x_coords)) * 800
  
  # Ajout d'un bruit gaussien (sd=150m) pour simuler collines et vallées
  # rnorm(n, 0, 150) génère n valeurs aléatoires suivant une loi normale
  # de moyenne 0 et d'écart-type 150m.
  # pmax/pmin bornent les valeurs entre 950m et 2 500m
  values(dem_rwanda) <<- pmax(950, pmin(2500, base_elevation + rnorm(n_cells, 0, 150)))
  
  cat("✓ DEM fictif créé\n")
})


# Découpe le raster dem_rwanda pour ne garder que la zone qui chevauche le 
# polygone rwanda_boundary pour éviter de traiter des données hors de la zone d'intérêt
# crop() : réduit le raster à l'emprise rectangulaire d'un polygone.
dem_rwanda <- crop(dem_rwanda, vect(rwanda_boundary)) 

# Masque les pixels du raster qui ne sont pas à l'intérieur du polygone rwanda_boundary
# définis comme NA
# mask() : met à NA tous les pixels hors du polygone. Ainsi les pixels
# des pays voisins (Ouganda, RDC…) sont exclus du calcul des pentes.
dem_rwanda <- mask(dem_rwanda, vect(rwanda_boundary))

# Limite les valeurs du raster à un intervalle donné et remplace les valeurs hors seuil par NA
# Valeurs < 800m ou > 4600m sont irréalistes pour le Rwanda : on les supprime.
dem_rwanda <- clamp(dem_rwanda, lower = 800, upper = 4600, values = NA)

cat("  Élévation min :", round(global(dem_rwanda, "min", na.rm = TRUE)[,1]), "m\n")
cat("  Élévation max :", round(global(dem_rwanda, "max", na.rm = TRUE)[,1]), "m\n\n")

# Cartographier rapidement pour identifier visuellement les anomalies
# plot() (de terra) affiche le raster en nuances de couleur dans la fenêtre R.
# add = TRUE superpose la frontière en rouge par-dessus le raster.
plot(dem_rwanda, main = "DEM Rwanda — vérification")
plot(st_geometry(rwanda_boundary), add = TRUE, border = "red")


################################################################################
# PARTIE III — CONSTRUCTION ET CORRECTION DU RÉSEAU ROUTIER
# Transforme les segments OSM bruts en graphe sfnetworks topologiquement
# cohérent, puis extrait la composante géante (réseau principal connecté).
# Toute modification ici invalide le cache des pentes.
#
# ── MISE EN CACHE ────────────────────────────────────────────────────────────
# Les corrections topologiques (subdivision aux intersections, suppression
# des pseudo-nœuds, extraction de la composante géante) prennent ~3-5 min.
# Le résultat est mis en cache dans outputs/reseau_corrige_cache.rds.
# Le cache est invalidé automatiquement si :
#   - le fichier PBF a changé (détection par taille de fichier)
#   - le nombre de segments d'entrée a changé
# Pour forcer un recalcul : supprimer le fichier .rds.
################################################################################

CACHE_RESEAU     <- file.path(DIR_OUTPUT, "reseau_corrige_cache.rds")
cache_reseau_valide <- FALSE

# Empreinte du PBF : la taille du fichier est un proxy simple et rapide
# (quelques ms) pour détecter une modification de la source.
# Pour une validation plus stricte, on pourrait utiliser digest::digest(file = ...)
# mais ça prendrait ~1s pour un PBF de 50 Mo, ce qui n'apporte rien en pratique.
pbf_size_actuelle     <- file.size(chemin_pbf)
n_segments_entree_act <- nrow(routes_rwanda)

# ── Tentative de chargement du cache ──────────────────────────────────────────
if (file.exists(CACHE_RESEAU)) {
  
  cat("=== PARTIE III — Tentative de chargement du cache réseau ===\n")
  cache_reseau <- readRDS(CACHE_RESEAU)
  
  # Double vérification : le cache n'est valide que si le PBF ET le nombre
  # de segments d'entrée correspondent exactement à la session actuelle.
  # Si l'une des deux conditions change, on rejette le cache.
  if (!is.null(cache_reseau$pbf_size) &&
      !is.null(cache_reseau$n_segments_entree) &&
      cache_reseau$pbf_size          == pbf_size_actuelle &&
      cache_reseau$n_segments_entree == n_segments_entree_act) {
    
    reseau_rwanda       <- cache_reseau$reseau_rwanda
    cache_reseau_valide <- TRUE
    
    cat("  ✓ Cache réseau valide\n")
    cat("    Nœuds  :", igraph::vcount(reseau_rwanda), "\n")
    cat("    Arêtes :", igraph::ecount(reseau_rwanda), "\n")
    cat("    → Corrections topologiques ignorées (~3-5 min gagnées)\n\n")
    
  } else {
    cat("  ⚠ Cache réseau invalide (PBF ou segments modifiés) — recalcul\n")
    cat("    Cache : pbf_size =", cache_reseau$pbf_size,
        "| n_segments =", cache_reseau$n_segments_entree, "\n")
    cat("    Actuel: pbf_size =", pbf_size_actuelle,
        "| n_segments =", n_segments_entree_act, "\n\n")
  }
}

# ══════════════════════════════════════════════════════════════════════════════
# BLOC CONDITIONNEL : III.1 à III.3 ne s'exécutent que si pas de cache valide
# ══════════════════════════════════════════════════════════════════════════════
if (!cache_reseau_valide) {

# ==============================================================================
# III.1 : Création du graphe sfnetworks
# Convertit les LINESTRING en réseau nœuds/arêtes non orienté.
# Nœuds = intersections et extrémités ; arêtes = segments de route.
# ==============================================================================

# sfnetworks représente le réseau routier comme un graphe topologique où :
#   - les NŒUDS sont les intersections et extrémités de routes
#   - les ARÊTES sont les segments de route entre deux nœuds
# Ce graphe servira ensuite à igraph pour le calcul de plus courts chemins.

# ── Homogénéisation des types de géométrie ───────────────────────────────────
# Le fichier PBF peut contenir des MULTILINESTRING (plusieurs lignes groupées)
# que sfnetworks ne sait pas gérer. st_cast() les éclate en LINESTRING simples.
# Un LINESTRING est une séquence de points formant une ligne.
# Un MULTILINESTRING est un groupe de plusieurs lignes, comme si une route
# était découpée en morceaux non contigus — sfnetworks ne peut pas en faire
# un segment de graphe cohérent.
routes_rwanda_clean <- routes_rwanda %>%
  st_cast("LINESTRING", warn = FALSE) %>%
  filter(st_geometry_type(.) == "LINESTRING") %>%  # Supprimer les types non conformes
  st_make_valid()

# as_sfnetwork() convertit le sf en réseau non orienté (directed = FALSE):
# un segment peut être parcouru dans les deux sens (routes bidirectionnelles).
# Les routes à sens unique seraient gérées avec directed = TRUE + attribut oneway.
reseau_rwanda <- as_sfnetwork(routes_rwanda_clean, directed = FALSE) 

cat("✓ Réseau initial — nœuds :", igraph::vcount(reseau_rwanda),
    "— arêtes :", igraph::ecount(reseau_rwanda), "\n\n")


# ==============================================================================
# III.2 : Corrections topologiques
# Subdivision aux intersections, suppression des pseudo-nœuds.
# Résout la fragmentation du réseau OSM (routes qui se croisent sans nœud).
# ==============================================================================

# Les données OSM contiennent fréquemment des erreurs topologiques :
#   1. Routes qui se croisent sans nœud d'intersection (pont raté, erreur de saisie)
#   2. Nœuds intermédiaires inutiles (points au milieu d'une ligne droite)
# Ces erreurs créent des composantes connexes multiples (le réseau est "fragmenté")
# et empêchent les algorithmes de plus court chemin de trouver des itinéraires.
# Imaginez une carte routière papier où certaines routes semblent se croiser
# mais n'ont pas d'échangeur : le GPS ne peut pas vous faire passer de l'une
# à l'autre même si elles se touchent visuellement.

# ── Étape 1 : Subdivision aux intersections ───────────────────────────────────
# to_spatial_subdivision() détecte les croisements de routes sans nœud commun
# et crée des nœuds aux points d'intersection. C'est l'opération fondamentale
# pour connecter des routes qui se croisent physiquement.

cat("  Étape 1/4 : subdivision aux intersections...\n")

reseau_subdivise <- reseau_rwanda %>%
  convert(to_spatial_subdivision)

cat("  → ", igraph::count_components(reseau_subdivise), "composantes après subdivision\n")

# ── Étape 2 : Suppression des pseudo-nœuds ────────────────────────────────────
# Un pseudo-nœud (degré 2) est un nœud connecté à exactement 2 arêtes.
# Il n'est pas topologiquement nécessaire (pas une vraie intersection) et alourdit
# le graphe. to_spatial_smooth() les supprime et fusionne les arêtes adjacentes.
# Exemple : une route droite avec 50 nœuds intermédiaires (à chaque virage OSM)
# devient une seule arête après lissage — bien plus efficace pour Dijkstra.

cat("  Étape 2/4 : suppression des pseudo-nœuds...\n")

reseau_lisse <- reseau_subdivise %>%
  convert(to_spatial_smooth)

cat("  → ", igraph::count_components(reseau_lisse), "composantes après lissage\n")


# Remplacer FALSE par TRUE si on veut activer cette partie du code : ⚠ ~1 jour de calcul
if(FALSE) {
  # ── Étape 3 : snapping ciblé post-topologie ───────────────────────────────────
  # Maintenant que la topologie est propre, un snapping léger (5m seulement)
  # connecte les extrémités quasi-jointives.
  # Les gaps < 5m sont rarissimes dans les PBF OSM Rwanda bien maintenus.
  # La subdivision (étape 1) règle déjà l'essentiel des problèmes de connectivité.
  # À réactiver uniquement sur un sous-réseau local si des composantes isolées
  # persistent après l'étape 4.
  # Le "snapping" consiste à "aimanter" les extrémités de routes qui sont très
  # proches mais pas exactement connectées (écart de quelques mètres dû à
  # des imprécisions de saisie dans OSM).
  
  cat("  Étape 3/4 : snapping léger (5m)...\n")
  
  tryCatch({
    aretes_sf     <- reseau_lisse %>% 
      activate("edges") %>%             # Active la table des arêtes (segments routiers)
      st_as_sf()                        # Convertit en objet sf
    n_aretes_snap <- nrow(aretes_sf)    # Nombre total d'arêtes à traiter
    
    
    # Initialisation d'une barre de progression pour suivre l'avancement
    pb_snap <- progress_bar$new(
      format = "  Snapping   [:bar] :percent | écoulé : :elapsed | ETA : :eta",
      total  = n_aretes_snap,
      clear  = FALSE,
      width  = 70
    )
    
    # Initialisation d'une liste pour stocker les géométries "snappées"
    # Chaque élément de la liste correspondra à une arête du réseau.  
    geoms_snapped <- vector("list", n_aretes_snap)
    
    
    for (i in seq_len(n_aretes_snap)) {
      # Applique st_snap() à l'arête i :
      #   - géométrie source : aretes_sf$geometry[i] (l'arête courante)
      #   - cible : aretes_sf$geometry (toutes les autres arêtes du réseau)
      #   - tolerance = 5 : distance maximale (en mètres) pour le snapping.
      #     Si une extrémité de l'arête i est à ≤5m d'une autre géométrie, elle sera "aimantée".
      geoms_snapped[[i]] <- st_snap(
        aretes_sf$geometry[i],   
        aretes_sf$geometry,      
        tolerance = 5            
      )
      pb_snap$tick()             # barre de progression
    }
    
    
    # Reconstruction du réseau après snapping
    aretes_snap <- aretes_sf %>%
      mutate(geometry = do.call(c, geoms_snapped)) %>% # Combine toutes les géométries de la liste en un seul vecteur
      st_make_valid() %>%                              # Corrige les géométries invalides
      filter(                                          # Garde uniquement les géométries de type LINESTRING et non vides.
        st_geometry_type(geometry) == "LINESTRING",
        !st_is_empty(geometry)
      )
    
    # Reconstruction du réseau sous forme de sfnetwork
    reseau_lisse <- as_sfnetwork(aretes_snap, directed = FALSE) %>%
      activate("edges") %>%
      mutate(longueur_m = as.numeric(st_length(geometry))) %>% # Recalcule la longueur des arêtes après snapping
      convert(to_spatial_subdivision)                          #  Reconnecte les intersections (au cas où le snapping a créé des croisements non nodaux)
    
    cat("  →", igraph::count_components(reseau_lisse), "composantes après snapping\n")
    
  }, error = function(e) {
    cat("  ⚠ Snapping échoué, on continue sans :", conditionMessage(e), "\n")
  })
}
# ── Étape 4 : Extraction de la composante géante ──────────────────────────────
# Même après corrections, le réseau peut rester fragmenté (routes isolées,
# pistes sans connexion au réseau principal). On conserve uniquement la plus
# grande composante connexe (composante géante), qui couvre la quasi-totalité
# du territoire national.
# La "composante géante" est l'ensemble des nœuds et arêtes qui forment un
# réseau interconnecté d'un seul tenant : on peut aller de n'importe quel
# nœud à n'importe quel autre nœud. Les petits fragments isolés (une piste
# de quelques km sans connexion) en sont exclus.

cat("  Étape 4/4 : extraction de la composante géante...\n")

# - `as_tbl_graph()` : Convertit le réseau sfnetwork en un graphe tidygraph/igraph.
#   Cela permet d'utiliser les fonctions d'analyse de graphe d'igraph.
# - `igraph::components()` : Identifie toutes les composantes connexes du graphe.
# - Résultat : `composantes_finales` est une liste avec deux éléments :
#    - $membership : Vecteur indiquant à quelle composante appartient chaque nœud.
#    - $csize : Vecteur indiquant la taille (nombre de nœuds) de chaque composante.
composantes_finales <- igraph::components(reseau_lisse %>% as_tbl_graph())   

# Identification de la composante géante
# which.max() renvoie l'indice de la valeur maximale dans un vecteur.
# Ici, on cherche l'identifiant de la composante qui contient le plus de nœuds.
id_geante           <- which.max(composantes_finales$csize)

# Extraction des nœuds appartenant à la composante géante
# which() renvoie les indices des éléments d'un vecteur logique qui sont TRUE.
noeuds_geante       <- which(composantes_finales$membership == id_geante)

# Calcul du pourcentage de nœuds dans la composante géante
pct_noeuds          <- round(length(noeuds_geante) / igraph::vcount(reseau_lisse) * 100, 1)

cat("  Composante géante :", length(noeuds_geante), "nœuds (", pct_noeuds, "% du réseau)\n")

# ==============================================================================
# III.3 : Diagnostic et extraction de la composante géante
# Analyse les arêtes exclues (type, surface, province), génère la carte
# de diagnostic, puis filtre le réseau sur la composante géante uniquement.
# ==============================================================================

# Vérifier les colonnes disponibles dans rwanda_provinces
cat("Colonnes de rwanda_provinces :\n")
print(names(rwanda_provinces))

# ── Récupérer les arêtes du réseau AVANT filtrage (reseau_lisse) ─────────────
# activate("edges") : dans sfnetworks, le réseau a deux "tables" — une pour les
# nœuds et une pour les arêtes. activate() bascule entre les deux.
aretes_lisse <- reseau_lisse %>% activate("edges") %>% st_as_sf() %>%
  mutate(longueur_m = as.numeric(st_length(geometry)))  
noeuds_lisse <- reseau_lisse %>% activate("nodes") %>% st_as_sf()

# Appartenance de chaque nœud à une composante (calculée sur reseau_lisse)
comp_lisse <- igraph::components(reseau_lisse %>% as_tbl_graph())
# $membership : pour chaque nœud, son numéro de composante.
# $csize : taille (en nœuds) de chaque composante.
noeuds_lisse$composante  <- comp_lisse$membership
noeuds_lisse$taille_comp <- comp_lisse$csize[comp_lisse$membership]

# Identifier les nœuds hors composante géante
id_geante_lisse    <- which.max(comp_lisse$csize)
noeuds_hors_geante <- noeuds_lisse %>%
  filter(composante != id_geante_lisse)

# ── Joindre l'info composante aux arêtes via leurs nœuds extrémité ───────────
# Une arête est "hors géante" si au moins un de ses nœuds l'est.
# from et to sont les indices des nœuds aux extrémités de chaque arête.
# comp_lisse$membership[from] : numéro de composante du nœud de départ.
# pmin() : pour chaque arête, prend la plus petite des deux tailles de composante.
aretes_lisse <- aretes_lisse %>%
  mutate(
    comp_from      = comp_lisse$membership[from],
    comp_to        = comp_lisse$membership[to],
    taille_comp    = pmin(
      comp_lisse$csize[comp_from],
      comp_lisse$csize[comp_to]
    ),
    hors_geante    = (comp_from != id_geante_lisse) | (comp_to != id_geante_lisse)
  )

aretes_perdues <- aretes_lisse %>% filter(hors_geante)

cat("Arêtes totales (avant filtrage) :", nrow(aretes_lisse), "\n")
cat("Arêtes perdues (hors composante géante) :", nrow(aretes_perdues),
    "(", round(nrow(aretes_perdues)/nrow(aretes_lisse)*100,1), "%)\n\n")

# ── 1. Distribution par type de route ─────────────────────────────────────────
# Ce diagnostic vérifie si les arêtes exclues sont surtout des routes importantes
# (problème grave) ou des pistes non classées (moins critique).
if ("road_type" %in% names(aretes_perdues)) {
  distrib_road_type <- aretes_perdues %>%
    st_drop_geometry() %>%
    group_by(road_type) %>%
    summarise(
      n_aretes      = n(),
      longueur_km   = round(sum(longueur_m, na.rm = TRUE) / 1000, 1),
      pct_sur_total = round(n() / nrow(aretes_perdues) * 100, 1)
    ) %>%
    arrange(desc(n_aretes))
  
  cat("── Distribution par type de route ──────────────────────────────────\n")
  print(distrib_road_type)
  cat("\n")
}

# ── 2. Distribution par surface ───────────────────────────────────────────────
if ("surface" %in% names(aretes_perdues)) {
  distrib_surface <- aretes_perdues %>%
    st_drop_geometry() %>%
    group_by(surface) %>%
    summarise(
      n_aretes    = n(),
      longueur_km = round(sum(longueur_m, na.rm = TRUE) / 1000, 1),
      pct         = round(n() / nrow(aretes_perdues) * 100, 1)
    ) %>%
    arrange(desc(n_aretes))
  
  cat("── Distribution par surface ─────────────────────────────────────────\n")
  print(distrib_surface)
  cat("\n")
}

# ── 3. Distribution par taille de composante (isolats vs petits fragments) ────
# "Isolat" = un nœud seul, sans aucune connexion.
# "Micro" = 2 à 5 nœuds (quelques segments isolés).
# Ces fragments sont généralement des routes mal dessinées dans OSM
# qui ne rejoignent jamais le réseau principal.
distrib_taille <- aretes_perdues %>%
  st_drop_geometry() %>%
  mutate(
    categorie_comp = case_when(
      taille_comp == 1  ~ "Isolat (1 nœud)",
      taille_comp <= 5  ~ "Micro (2–5 nœuds)",
      taille_comp <= 20 ~ "Petit (6–20 nœuds)",
      TRUE              ~ "Moyen (>20 nœuds)"
    )
  ) %>%
  group_by(categorie_comp) %>%
  summarise(
    n_aretes    = n(),
    longueur_km = round(sum(longueur_m, na.rm = TRUE) / 1000, 1),
    pct         = round(n() / nrow(aretes_perdues) * 100, 1)
  ) %>%
  arrange(desc(n_aretes))

cat("── Distribution par taille de composante ────────────────────────────\n")
print(distrib_taille)
cat("\n")

# ── 4. Localisation géographique (province la plus touchée) ───────────────────
# On spatialise les arêtes perdues et on les intersecte avec les provinces
if (nrow(aretes_perdues) > 0 && nrow(rwanda_provinces) > 0) {
  
  # Renommage AVANT la jointure pour éviter le conflit avec la colonne
  # "name" des arêtes (nom de route OSM)
  provinces_join <- rwanda_provinces %>%
    select(nom_province = name)
  
  # st_centroid() calcule le point central de chaque arête.
  # st_join() avec st_within() associe chaque centroïde à la province
  # dans laquelle il se trouve (jointure spatiale).
  centroides_perdues <- aretes_perdues %>%
    st_centroid(of_largest_polygon = FALSE) %>%
    st_join(provinces_join, join = st_within)
  
  distrib_province <- centroides_perdues %>%
    st_drop_geometry() %>%
    group_by(Province = nom_province) %>%
    summarise(
      n_aretes    = n(),
      longueur_km = round(sum(longueur_m, na.rm = TRUE) / 1000, 1),
      pct         = round(n() / nrow(aretes_perdues) * 100, 1)
    ) %>%
    arrange(desc(n_aretes))
  
  cat("── Localisation par province ────────────────────────────────────────\n")
  print(distrib_province)
  cat("\n")
}

# ── 5. Carte des arêtes perdues ───────────────────────────────────────────────
cat("Génération de la carte des arêtes perdues...\n")

# Palette par type de route (cohérente avec la carte de vérification Partie 3)

carte_aretes_perdues <- fond_carte() +
  
  
  # Arêtes perdues colorées par type de route
  tm_shape(aretes_perdues) +
  tm_lines(
    col       = "road_type",
    col.scale = tm_scale(values = PALETTE_ROAD_TYPE),
    col.legend = tm_legend(title = "Type de route\n(arêtes perdues)"),
    lwd = 3
  ) +
  
  # Nœuds hors géante (points rouges) pour visualiser les isolats
  tm_shape(noeuds_hors_geante) +
  tm_dots(fill = "#CC0000", size = 0.2, fill_alpha = 0.5) +
  
  tm_title(paste0("Arêtes exclues de la composante géante\n(",
                  round(nrow(aretes_perdues) / nrow(aretes_lisse %>% activate("edges") %>% st_as_sf()) * 100, 1),
                  "% du réseau)")) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_aretes_perdues,
  file.path(DIR_OUTPUT, "carte_aretes_perdues.png"),
  width = 3000, height = 2400, dpi = 300
)
if (FALSE) {
  tmap_mode("view")
  print(carte_aretes_perdues)
  tmap_mode("plot")
}

cat("✓ Carte des arêtes perdues sauvegardée\n\n")

# progress_bar$new() : crée une barre de progression qui s'affiche dans la console.
# total = length(noeuds_geante) : nombre d'itérations attendues.
# Avance la barre d'un pas à chaque itération.
pb_geante <- progress_bar$new(
  format = "  Filtrage   [:bar] :percent | durée : :elapsed",
  total  = length(noeuds_geante),
  clear  = FALSE,
  width  = 60
)

# Création du réseau avec uniquement la composante géante.
# filter() sur les nœuds : ne garde que les nœuds dont l'indice est dans noeuds_geante.
# row_number() génère les indices 1, 2, 3, ... pour chaque nœud.
# %in% vérifie l'appartenance : row_number() %in% noeuds_geante = TRUE si ce nœud
# fait partie de la composante géante.
reseau_rwanda <- reseau_lisse %>%
  activate("nodes") %>%
  filter({
    pb_geante$tick()
    row_number() %in% noeuds_geante
  }) %>%
  mutate(node_id = row_number())

# st_length() : calcule la longueur de chaque arête en mètres à partir de sa géométrie.
# as.numeric() : convertit le résultat (objet "units") en nombre ordinaire.
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(longueur_m = as.numeric(st_length(geometry)))

# Vérification immédiate
n_na_longueur <- reseau_rwanda %>%
  activate("edges") %>% st_as_sf() %>%
  pull(longueur_m) %>%
  { sum(is.na(.) | . == 0) }

cat("✓ longueur_m recalculée sur toutes les arêtes\n")
cat("  Arêtes avec longueur_m = 0 ou NA :", n_na_longueur, "(doit être 0)\n\n")

# to_spatial_subdivision() crée des fragments de longueur nulle aux intersections
# quand deux nœuds sont géométriquement confondus. On les élimine ici pour éviter 
# toute propagation de NA en aval.
n_avant_filtre <- igraph::ecount(reseau_rwanda)

reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(longueur_m_brute = as.numeric(st_length(geometry))) %>%
  filter(longueur_m_brute > 0.5) %>%         # Seuil 0.5m
  select(-longueur_m_brute)                  # Colonne temporaire, on la retire

n_apres_filtre <- igraph::ecount(reseau_rwanda)
cat("Arêtes dégénérées supprimées :", n_avant_filtre - n_apres_filtre,
    "(", round((n_avant_filtre - n_apres_filtre)/n_avant_filtre*100, 1), "% du réseau)\n")
cat("Arêtes conservées            :", n_apres_filtre, "\n\n")

cat("✓ Réseau corrigé —",
    igraph::vcount(reseau_rwanda), "nœuds,",
    igraph::ecount(reseau_rwanda), "arêtes\n\n")


# ── Diagnostic complet de la fragmentation ───────────────────────────────────
# On recalcule les composantes connexes sur le réseau final pour vérifier
# qu'il est bien dominé par une seule grande composante.

composantes_finales <- igraph::components(reseau_rwanda %>% as_tbl_graph())
sizes <- sort(composantes_finales$csize, decreasing = TRUE) # trie les tailles des composantes connexes du réseau par ordre décroissant

cat("=== Diagnostic de fragmentation ===\n\n")

cat("Distribution des composantes :\n")
cat("  >= 1000 noeuds :", sum(sizes >= 1000), "composantes\n")
cat("  100–999 noeuds :", sum(sizes >= 100 & sizes < 1000), "composantes\n")
cat("  10–99  noeuds  :", sum(sizes >= 10  & sizes < 100),  "composantes\n")
cat("  2–9    noeuds  :", sum(sizes >= 2   & sizes < 10),   "composantes\n")
cat("  1      noeud   :", sum(sizes == 1),                  "composantes\n")

cat("Nombre de nœuds dans reseau_rwanda :", igraph::vcount(reseau_rwanda), "\n")
cat("Nombre d'arêtes dans reseau_rwanda :", igraph::ecount(reseau_rwanda), "\n")

rm(composantes_finales)

# ════════════════════════════════════════════════════════════════════════════
# SAUVEGARDE DU CACHE
# ════════════════════════════════════════════════════════════════════════════

cat("=== Sauvegarde du cache réseau ===\n")

saveRDS(
  list(
    reseau_rwanda      = reseau_rwanda,
    pbf_size           = pbf_size_actuelle,
    n_segments_entree  = n_segments_entree_act,
    n_noeuds           = igraph::vcount(reseau_rwanda),
    n_aretes           = igraph::ecount(reseau_rwanda),
    date_creation      = Sys.time()
  ),
  CACHE_RESEAU
)

cat("  ✓ Cache sauvegardé :", CACHE_RESEAU, "\n")
cat("  → Au prochain lancement, la Partie III s'exécutera en <1s\n\n")

}  # fin du if (!cache_reseau_valide)


# ── Vérifications communes (toujours exécutées, qu'on ait un cache ou non) ──
# Ces vérifications sont rapides et permettent de détecter tôt un problème
# de cohérence avec le reste du script (ex : composante non connectée).
cat("=== Vérifications post-Partie III ===\n")
cat("  Nœuds dans reseau_rwanda  :", igraph::vcount(reseau_rwanda), "\n")
cat("  Arêtes dans reseau_rwanda :", igraph::ecount(reseau_rwanda), "\n")

n_composantes <- igraph::count_components(reseau_rwanda %>% as_tbl_graph())
if (n_composantes != 1) {
  warning("  ⚠ Le réseau a ", n_composantes, " composantes (attendu : 1)\n")
} else {
  cat("  ✓ Réseau entièrement connecté (1 composante)\n")
}

n_na_longueur <- reseau_rwanda %>%
  activate("edges") %>% st_as_sf() %>%
  pull(longueur_m) %>%
  { sum(is.na(.) | . == 0) }

cat("  Arêtes avec longueur_m = 0 ou NA :", n_na_longueur,
    "(doit être 0)\n\n")

################################################################################
# PARTIE IV — ENRICHISSEMENT DU RÉSEAU
# Ajoute trois couches d'information au réseau routier :
#   - zones urbaines (pénalité sur les poids lourds en ville)
#   - pentes (impact sur vitesse et consommation)
#   - entrepôts (origines/destinations du modèle de fret)
# Les Parties V à VIII dépendent de ces attributs mais pas les unes des autres.
################################################################################

# ==============================================================================
# IV.1 : Zones d'usage du sol
# Charge les zones résidentielles, commerciales, industrielles et retail depuis le PBF.
# Tague chaque arête du réseau avec zone_urbaine = TRUE/FALSE via centroïde.
# ==============================================================================

# L'objectif est de savoir quelles routes traversent des zones urbanisées.
# En zone urbaine, les camions sont pénalisés (congestion, restrictions de
# circulation, limitation de vitesse). On va donc "étiqueter" chaque segment
# routier avec zone_urbaine = TRUE ou FALSE selon qu'il passe par une zone
# résidentielle, commerciale ou industrielle.

# ── Chargement des zones résidentielles et commerciales ──
zones_urbaines <- st_read(
  chemin_pbf, layer = "multipolygons",
  query = "SELECT * FROM multipolygons
           WHERE landuse IN ('residential','commercial','retail')",
  quiet = TRUE
) %>%
  rename(geometry = `_ogr_geometry_`) %>%
  st_as_sf() %>%
  st_make_valid() %>%
  filter(st_geometry_type(geometry) %in% c("POLYGON","MULTIPOLYGON")) %>%
  st_transform(crs = 32735)

# ── Chargement des zones industrielles ──
# On filtre les zones industrielles de plus de 0.01 km² (100m × 100m)
# pour exclure les petits bâtiments isolés taggés comme "industrial" par erreur.
zones_industrielles <- st_read(
  chemin_pbf, layer = "multipolygons",
  query = "SELECT * FROM multipolygons
           WHERE landuse = 'industrial'",
  quiet = TRUE
) %>%
  rename(geometry = `_ogr_geometry_`) %>%
  st_as_sf() %>%
  st_make_valid() %>%
  filter(st_geometry_type(geometry) %in% c("POLYGON","MULTIPOLYGON")) %>%
  st_transform(crs = 32735) %>%
  mutate(aire_km2 = as.numeric(st_area(geometry)) / 1e6) %>%
  filter(aire_km2 > 0.01)

# ── Extraction des zones retail depuis zones_urbaines (déjà chargées) ─────────
# On extrait les zones retail (commerces de détail) qui étaient incluses dans
# les zones_urbaines mais méritent une colonne distincte pour les analyses.
zones_retail <- zones_urbaines %>%
  filter(landuse == "retail") %>%
  mutate(aire_km2 = as.numeric(st_area(geometry)) / 1e6) %>%
  filter(aire_km2 > 0.005)

cat("  Zones retail :", nrow(zones_retail), "\n\n")

# ── Taguage des arêtes : zone_urbaine = TRUE si l'arête traverse une zone dense ──
# On utilise le centroïde de chaque arête pour l'intersection (plus rapide
# qu'une intersection complète ligne × polygone sur 29 000 arêtes)
cat("  Taguage des arêtes du réseau...\n")

# st_centroid() : calcule le point central de chaque arête (un point par ligne).
# Tester si UN POINT est dans un polygone est bien plus rapide que tester si
# UNE LIGNE croise un polygone — gain de temps significatif sur ~30 000 arêtes.
aretes_centroides <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  st_centroid(of_largest_polygon = FALSE) %>%
  mutate(arete_idx = row_number())

# Union de toutes les zones urbaines pour une seule opération d'intersection.
# st_union() fusionne tous les polygones en un seul grand polygone.
# C'est plus rapide de tester l'intersection avec 1 polygone qu'avec 1000.
zones_urbaines_union <- zones_urbaines %>%
  st_union() %>%
  st_make_valid()

# st_intersects retourne une liste de vecteurs d'indices — lengths() > 0 = intersection.
# Pour chaque centroïde d'arête, on vérifie s'il est dans une zone urbaine.
# lengths() > 0 : TRUE si le centroïde intersecte au moins une zone urbaine.
in_urbain <- lengths(st_intersects(aretes_centroides, zones_urbaines_union)) > 0

# Intégration dans le réseau : on ajoute une colonne booléenne (TRUE/FALSE)
# à la table des arêtes du réseau sfnetworks.
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(zone_urbaine = in_urbain)

n_urbain <- sum(in_urbain)
cat("  Arêtes en zone urbaine :", n_urbain,
    "(", round(n_urbain / igraph::ecount(reseau_rwanda) * 100, 1), "% du réseau)\n\n")

# Stocker dans DuckDB pour usage dans la table des coûts 
duck_write(
  tibble(
    zone_urbaine    = c(TRUE, FALSE),
    label_zone      = c("urbaine", "rurale")
  ),
  "ref_zones"
)

cat("✓ Zones d'usage du sol chargées et arêtes taguées\n\n")

# ==============================================================================
# IV.2 : Calcul des pentes (avec cache)
# Échantillonne des points d'élévation le long de chaque arête depuis le DEM.
# Résultat mis en cache dans outputs/pentes_cache.rds — recalcul uniquement
# si le nombre d'arêtes change. 
# Pour forcer le recalcul : supprimer le fichier .rds.
# ==============================================================================

# Le calcul des pentes est une opération particulièrement longue (~30 min).
# Pour ne pas le refaire à chaque exécution, on sauvegarde le résultat dans
# un fichier "cache" (.rds = format binaire R). À la prochaine exécution,
# si le réseau n'a pas changé, on charge directement le cache.
# Si le réseau a changé (nouvelles corrections topo, nouveau PBF),
# le cache est automatiquement invalidé et le calcul repart de zéro.

CACHE_PENTES <- file.path(DIR_OUTPUT, "pentes_cache.rds")

aretes_avec_geom <- reseau_rwanda %>% activate("edges") %>% st_as_sf()
n_aretes         <- nrow(aretes_avec_geom)

# ── Tentative de chargement du cache ──────────────────────────────────────────
cache_valide <- FALSE

if (file.exists(CACHE_PENTES)) {
  
  cat("  Cache trouvé :", CACHE_PENTES, "\n")
  cache <- readRDS(CACHE_PENTES)
  
  # Contrôle de validité : le nombre d'arêtes doit correspondre
  # Si le réseau a été modifié (nouvelles corrections topo, nouveau PBF…),
  # le cache est rejeté et le calcul repart de zéro.
  if (!is.null(cache$n_aretes) && cache$n_aretes == n_aretes) {
    pentes_df    <- cache$pentes_df
    cache_valide <- TRUE
    cat("  ✓ Cache valide (", n_aretes, "arêtes) — calcul des pentes ignoré\n\n")
  } else {
    cat("  ⚠ Cache invalide : réseau modifié (",
        cache$n_aretes, "arêtes en cache vs",
        n_aretes, "arêtes actuelles) — recalcul...\n")
  }
}

# ── Calcul si pas de cache valide ─────────────────────────────────────────────
if (!cache_valide) {
  
  cat("  Calcul des pentes pour", n_aretes, "arêtes...\n")
  
  # calculer_pente_arete() : calcule les indicateurs d'élévation d'une arête.
  # Pour chaque arête, on échantillonne des points tous les "espacement" mètres,
  # on extrait l'altitude à chaque point depuis le DEM, puis on calcule :
  #   slope_mean     : pente moyenne (dénivelé net / longueur × 100) en %
  #   elevation_gain : cumul des montées (en mètres)
  #   elevation_loss : cumul des descentes (en mètres, valeur positive)
  #   rugosity       : (montées + descentes) / longueur = irrégularité du profil
  calculer_pente_arete <- function(ligne_geom, dem, espacement = 100) {
    
    longueur <- as.numeric(st_length(ligne_geom))
    n_points <- max(2, floor(longueur / espacement))
    # Si la route est très courte (< 2× l'espacement), on prend juste 2 points
    # (début et fin). Sinon on échantillonne régulièrement.
    points   <- if (longueur < espacement * 2)
      st_line_sample(ligne_geom, n = 2, type = "regular")
    else
      st_line_sample(ligne_geom, n = n_points, type = "regular")
    
    # st_cast() : convertit la géométrie MULTIPOINT en POINT individuels.
    # terra::extract() : extrait la valeur du raster DEM à chaque point.
    # method = "bilinear" : interpolation bilinéaire pour plus de précision.
    points_sf   <- st_cast(points, "POINT")
    elevations  <- terra::extract(dem, vect(points_sf), method = "bilinear")
    elev_values <- elevations[, 2]
    
    if (any(is.na(elev_values)) || length(elev_values) < 2)
      return(list(slope_mean=0, elevation_gain=0, elevation_loss=0, rugosity=0))
    
    # Pente nette = (altitude finale - altitude initiale) / longueur × 100
    denivele_net   <- elev_values[length(elev_values)] - elev_values[1]
    slope_mean_pct <- (denivele_net / longueur) * 100
    # diff() : calcule les différences entre valeurs consécutives.
    # Ex : c(100, 105, 102, 108) → diff = c(5, -3, 6)
    differences    <- diff(elev_values)
    # On ne garde que les valeurs positives (montées) pour elevation_gain
    elevation_gain <- sum(differences[differences > 0], na.rm = TRUE)
    # abs() : valeur absolue — on veut une distance positive pour les descentes
    elevation_loss <- abs(sum(differences[differences < 0], na.rm = TRUE))
    rugosity       <- (elevation_gain + elevation_loss) / longueur
    
    list(slope_mean     = slope_mean_pct,
         elevation_gain = elevation_gain,
         elevation_loss = elevation_loss,
         rugosity       = rugosity)
  }
  
  # Initialisation d'une liste vide pour stocker les résultats de chaque arête.
  # vector("list", n) crée une liste de n éléments vides — plus efficace que
  # de faire grandir une liste dynamiquement avec c() dans la boucle.
  resultats_pentes <- vector("list", n_aretes)
  
  for (i in seq_len(n_aretes)) {
    if (i %% 500 == 0 || i == n_aretes)
      cat("  Pentes :", round(i / n_aretes * 100, 1), "%\n")
    resultats_pentes[[i]] <- calculer_pente_arete(
      aretes_avec_geom$geometry[i],
      dem_rwanda,
      espacement = 100
    )
  }
  
  # bind_rows() : transforme une liste de listes en un data.frame R.
  # Chaque élément de la liste devient une ligne du tableau.
  pentes_df <- bind_rows(resultats_pentes)
  
  # ── Sauvegarde du cache ──────────────────────────────────────────────────────
  # On sauvegarde pentes_df + le nombre d'arêtes pour validation future
  # saveRDS() : sauvegarde un objet R arbitraire dans un fichier binaire.
  # On sauvegarde une liste avec deux éléments : le tableau des pentes
  # et le nombre d'arêtes (pour la vérification de validité au prochain chargement).
  saveRDS(
    list(pentes_df = pentes_df, n_aretes = n_aretes),
    CACHE_PENTES
  )
  cat("  ✓ Cache sauvegardé :", CACHE_PENTES, "\n\n")
}

# ── Intégration des pentes dans le réseau ─────────────────────────────────────
# case_when() : équivalent de plusieurs if/else imbriqués — catégorise la pente
# en 4 classes selon sa valeur absolue (abs() = ignore le signe montée/descente).
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    slope_mean      = pentes_df$slope_mean,
    elevation_gain  = pentes_df$elevation_gain,
    elevation_loss  = pentes_df$elevation_loss,
    rugosity        = pentes_df$rugosity,
    slope_category  = case_when(
      abs(slope_mean) < 2 ~ "plat",
      abs(slope_mean) < 5 ~ "legere",
      abs(slope_mean) < 8 ~ "moderee",
      TRUE                ~ "forte"
    )
  )

cat("✓ Pentes intégrées dans le réseau\n\n")

# ==============================================================================
# IV.3 : Nœuds d'entreposage
# Construit la liste des zones économiques (manuelles + OSM city/town +
# zones industrielles), les dédoublonne, les snappe sur le réseau routier
# et les intègre comme attributs des nœuds (is_warehouse, warehouse_type…).
# ==============================================================================

# Les "entrepôts" (warehouses) sont les origines et destinations du modèle de fret.
# Ils représentent les lieux entre lesquels les marchandises circulent :
# Kigali Hub, postes frontières, villes importantes, zones industrielles.
# Chaque entrepôt sera "accroché" au nœud du réseau routier le plus proche
# (snapping), ce qui permettra de calculer des itinéraires entre eux.

# Les nœuds d'entreposage sont les origines/destinations du modèle de fret.
# Ils représentent des zones économiques importantes (hub, SEZ, frontières…).

# ── Entrepôts manuels  ────────────────────────────────────────────────────────
# Ces entrepôts ont été positionnés manuellement avec leurs coordonnées GPS
# (lon = longitude, lat = latitude en degrés décimaux WGS84).
# "pays" = NULL pour les zones internes au Rwanda, nom du pays pour les frontières
# (utilisé pour associer les coûts pré-frontière dans le modèle gravitaire).
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
  # pays = NULL pour les zones internes, nom du pays pour les frontières
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

# Conversion des entrepôts manuels en sf pour la comparaison spatiale
manuels_sf <- entreposages_manuels %>%
  st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
  st_transform(crs = 32735)

# ── Entrepôts depuis city/town OSM ───────────────────────────────────────────
# Filtrer uniquement les villes dans le territoire rwandais
# Évite que les villes des pays voisins se snappent toutes sur les mêmes nœuds frontières
# st_filter() : ne garde que les géométries qui intersectent le polygone donné.
# st_buffer(dist = 5000) : élargit la frontière de 5km pour inclure les villes
# rwandaises situées exactement sur la frontière.
villes_osm <- villes_osm %>%
  st_filter(rwanda_national %>% st_buffer(dist = 5000))
# Buffer de 5km pour garder les villes très proches de la frontière
cat("  Villes OSM dans ou proches du Rwanda :", nrow(villes_osm), "\n")

# Identifier les villes OSM non dupliquées avec les entrepôts manuels
# lengths(idx_proches) == 0 : sélectionne les villes OSM qui ne sont proches
# d'AUCUN entrepôt manuel (distances > 3km pour tous).
idx_proches <- st_is_within_distance(villes_osm, manuels_sf, dist = 3000)
villes_nouvelles <- villes_osm[lengths(idx_proches) == 0, ] %>%
  st_transform(crs = 4326) %>%
  mutate(
    nom    = paste0(name, " (OSM)"),
    type   = if_else(place == "city", "hub", "ville"),
    lon    = st_coordinates(geometry)[,1],
    lat    = st_coordinates(geometry)[,2],
    source = "osm_place"
  ) %>%
  st_drop_geometry() %>%
  select(nom, type, lon, lat, source)

cat("  Villes OSM city/town nouvelles (non dupliquées) :",
    nrow(villes_nouvelles), "\n")

# ── Entrepôts depuis zones industrielles (origines de fret) ──────────────────
# Les grandes zones industrielles sont d'importants générateurs de fret.
# On calcule leur centroïde et on les ajoute comme entrepôts potentiels,
# en excluant celles qui sont déjà trop proches des entrepôts existants.
if (nrow(zones_industrielles) > 0) {
  
  # Centroïdes des zones industrielles significatives (> 0.05 km²)
  centroides_indus <- zones_industrielles %>%
    filter(aire_km2 > 0.05) %>%
    st_centroid(of_largest_polygon = FALSE) %>%
    st_transform(crs = 4326) %>%
    mutate(
      lon = st_coordinates(geometry)[,1],
      lat = st_coordinates(geometry)[,2],
      nom = paste0("Zone industrielle ", row_number()),
      type   = "industrie",
      source = "osm_industrial"
    ) %>%
    st_drop_geometry() %>%
    select(nom, type, lon, lat, source)
  
  # Dédoublonnage : supprimer les zones trop proches d'un entrepôt existant
  # bind_rows() : empile verticalement deux tableaux ayant les mêmes colonnes.
  centroides_indus_sf <- centroides_indus %>%
    st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
    st_transform(crs = 32735)
  
  tous_existants <- bind_rows(
    entreposages_manuels %>%
      st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
      st_transform(crs = 32735),
    villes_nouvelles %>%
      st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
      st_transform(crs = 32735)
  )
  
  idx_indus_proches <- st_is_within_distance(centroides_indus_sf,
                                             tous_existants, dist = 2000)
  zones_indus_nouvelles <- centroides_indus[lengths(idx_indus_proches) == 0, ]
  
  cat("  Zones industrielles nouvelles :", nrow(zones_indus_nouvelles), "\n")
} else {
  zones_indus_nouvelles <- tibble(nom=character(), type=character(),
                                  lon=numeric(), lat=numeric(), source=character())
}

# ── Entrepôts depuis zones retail (destinations commerciales) ─────────────────
# Les grandes zones commerciales (centres commerciaux, marchés) sont d'importantes
# destinations de fret. On les ajoute de la même façon que les zones industrielles.
if (nrow(zones_retail) > 0) {
  
  centroides_retail <- zones_retail %>%
    filter(aire_km2 > 0.01) %>%
    st_centroid(of_largest_polygon = FALSE) %>%
    mutate(
      lon    = st_coordinates(geometry)[,1],
      lat    = st_coordinates(geometry)[,2],
      nom    = paste0("Zone retail ", row_number()),
      type   = "marche",
      source = "osm_retail"
    ) %>%
    st_drop_geometry() %>%
    select(nom, type, lon, lat, source)
  
  # Dédoublonnage
  centroides_retail_sf <- centroides_retail %>%
    st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
    st_transform(crs = 32735)
  
  tous_existants2 <- bind_rows(
    entreposages_manuels %>%
      st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
      st_transform(crs = 32735),
    villes_nouvelles %>%
      st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
      st_transform(crs = 32735),
    zones_indus_nouvelles %>%
      st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
      st_transform(crs = 32735)
  )
  
  idx_retail_proches <- st_is_within_distance(centroides_retail_sf,
                                              tous_existants2, dist = 1000)
  zones_retail_nouvelles <- centroides_retail[lengths(idx_retail_proches) == 0, ]
  
  cat("  Zones retail nouvelles :", nrow(zones_retail_nouvelles), "\n")
} else {
  zones_retail_nouvelles <- tibble(nom=character(), type=character(),
                                   lon=numeric(), lat=numeric(), source=character())
}

# ── Assemblage final ──────────────────────────────────────────────────────────
# bind_rows() empile les 4 sources d'entrepôts (manuels, OSM villes,
# industriels, retail) en un seul tableau.
# mutate(pays = NA_character_) : les entrepôts OSM n'ont pas de pays associé
# (ils sont tous internes au Rwanda).
# distinct(lon, lat) : supprime les doublons résiduels ayant exactement
# les mêmes coordonnées.
# Dans le bloc d'assemblage final, ajouter pays dans le select
entreposages_fictifs <- bind_rows(
  entreposages_manuels,
  villes_nouvelles %>% mutate(pays = NA_character_),
  zones_indus_nouvelles %>% mutate(pays = NA_character_),
  zones_retail_nouvelles %>% mutate(pays = NA_character_)
) %>%
  # Supprimer les éventuels doublons résiduels sur les coordonnées
  distinct(lon, lat, .keep_all = TRUE)

cat("\n✓ Entrepôts totaux :", nrow(entreposages_fictifs), "\n")
cat("  dont manuels    :", sum(entreposages_fictifs$source == "manuel"), "\n")
cat("  dont OSM villes :", sum(entreposages_fictifs$source == "osm_place"), "\n")
cat("  dont industriels:", sum(entreposages_fictifs$source == "osm_industrial"), "\n")
cat("  dont retail     :", sum(entreposages_fictifs$source == "osm_retail"), "\n\n")

# Ajout de la colonne source dans la table DuckDB
duck_write(entreposages_fictifs, "zones_entreposage")

# Conversion en objet sf et reprojection en UTM 35S (même CRS que le réseau)
# st_as_sf() avec coords = c("lon","lat") : crée un objet de points géospatiaux
# à partir des colonnes de coordonnées lon et lat.
entreposages_sf <- entreposages_fictifs %>%
  st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
  st_transform(crs = 32735)

# Création des buffers circulaires de 2km autour de chaque entrepôt.
entreposages_buffer <- entreposages_sf %>%
  st_buffer(dist = 2000)

# ── Accrochage (snapping) des entrepôts au réseau ────────────────────────────
# Les coordonnées des entrepôts ne tombent pas exactement sur le réseau routier.
# st_nearest_feature() trouve pour chaque entrepôt le nœud du réseau le plus proche.
# C'est le "snapping" : on "accroche" chaque entrepôt au nœud routier le plus proche.
# Sans ce snapping, Dijkstra ne pourrait pas partir d'un entrepôt car il ne serait
# pas sur le graphe. Avec le snapping, l'entrepôt devient synonyme du nœud voisin.
noeuds_reseau <- reseau_rwanda %>% activate("nodes") %>% st_as_sf()

entreposages_avec_snap <- entreposages_sf %>%
  mutate(
    noeud_proche_id = st_nearest_feature(geometry, noeuds_reseau),
    # Calcul de la distance d'accrochage pour contrôle qualité
    # (une distance > 2km indiquerait un entrepôt mal positionné)
    # st_distance() par_element = TRUE : calcule la distance entre le point i
    # de la première couche et le point i de la deuxième couche (pas toutes les paires).
    distance_snap   = as.numeric(
      st_distance(geometry, noeuds_reseau[noeud_proche_id,], by_element = TRUE)
    )
  ) %>%
  # ── Garder un seul entrepôt par nœud : priorité aux OSM villes, puis manuels,
  #    puis industriels (order de source dans entreposages_fictifs)
  #    arrange() les ranges dans l'ordre et distinct() ne garde que la première
  #    occurrance de noeud_proche_id
  arrange(match(source, c("osm_place", "manuel","osm_industrial","osm_retail"))) %>%
  distinct(noeud_proche_id, .keep_all = TRUE)

cat("  Entrepôts après dédoublonnage par nœud :", nrow(entreposages_avec_snap), "\n")

# Associe le noeud le plus proche à chaque entrepot ainsi que son type.
# match(A, B) : pour chaque élément de A, trouve sa position dans B.
# Utilisé ici pour retrouver le nom/type/pays de l'entrepôt associé à chaque nœud.
reseau_rwanda <- reseau_rwanda %>%
  activate("nodes") %>%
  mutate(
    node_id        = row_number(),
    is_warehouse   = node_id %in% entreposages_avec_snap$noeud_proche_id,  # TRUE si le nœud est proche d'un entrepôt
    warehouse_name = if_else(                                              # Nom de l'entrepôt associé (si is_warehouse = TRUE), sinon NA
      is_warehouse,
      entreposages_avec_snap$nom[match(node_id, entreposages_avec_snap$noeud_proche_id)], 
      # match () cherche la position de chaque élément de node_id dans le vecteur entreposages_avec_snap$noeud_proche_id
      # Cette ligne permet de trouver le nom d'un entrepôt associé à un identifiant de nœud
      NA_character_
    ),
    warehouse_type = if_else(                                              # Type de l'entrepôt (ex: "marche", "ville", "centre industriel"), sinon NA
      is_warehouse,
      entreposages_avec_snap$type[match(node_id, entreposages_avec_snap$noeud_proche_id)],  # Cette ligne permet de trouver le type d'un entrepôt associé à un identifiant de nœud
      NA_character_
    ),
    # ── pays d'origine pour les points frontière ──────────────────────────────
    warehouse_pays = if_else(
      is_warehouse,
      entreposages_avec_snap$pays[match(node_id, entreposages_avec_snap$noeud_proche_id)],
      NA_character_
    )
  )

cat("✓", nrow(entreposages_avec_snap), "entreposages intégrés au réseau\n\n")


################################################################################
# PARTIE V — CALCUL DES COÛTS DE TRANSPORT
# Calcule les coûts généralisés (USD/tkm) pour chaque arête × véhicule via
# une requête SQL DuckDB, puis assemble le graphe multi-modal à 3 couches
# (une par véhicule) avec arêtes de transbordement aux entrepôts.
# Dépend de la Partie IV complète. Les Parties VI et VII en dépendent.
################################################################################

# ==============================================================================
# V.1 : Coûts généralisés par véhicule (SQL DuckDB)
# Requête SQL chaînée en 5 CTEs : vitesse → pente → consommation → coût.
# Produit la table aretes_couts_tous (N_arêtes × N_véhicules lignes).
# cost_per_tkm = (carburant + usure × facteur_urbain + temps × facteur_urbain)
#                / (capacite_tonnes × length_km)
# ==============================================================================

# Formules appliquées :
#   speed_kmh     = vitesse_base × facteur_pente
#   conso (L/100km) = conso_base × facteur_surface × (1 + slope × FACTEUR / 100)
#   cost_fuel     = (length_km × conso/100) × prix_carburant
#   cost_wear     = length_km × usure_usd_km
#   cost_time     = (length_km / speed_kmh) × valeur_temps
#
# L'unité finale (USD/tkm) permet de comparer des routes de longueurs différentes.

# st_drop_geometry() : nécessaire car DuckDB ne peut pas stocker des colonnes
# géométriques sf. On extrait uniquement les attributs tabulaires.
aretes_df <- reseau_rwanda %>%
  activate("edges") %>% st_as_sf() %>% st_drop_geometry() %>%
  mutate(arete_id = row_number())
duck_write(aretes_df, "aretes_base")

# Cette longue requête SQL utilise des CTEs (Common Table Expressions).
# Une CTE (définie avec WITH nom AS (...)) est une sous-requête nommée
# qu'on peut réutiliser dans la même requête. C'est comme créer des étapes
# intermédiaires dans un calcul en chaîne, chaque étape utilisant le résultat
# de la précédente.
# CREATE OR REPLACE TABLE : crée une nouvelle table dans DuckDB, ou remplace
# la table existante si elle existe déjà.
duck_query("
  CREATE OR REPLACE TABLE aretes_couts_tous AS

  WITH

  -- Étape 1 : combinaison de chaque arête avec chaque véhicule de la flotte
  -- CROSS JOIN : N_arêtes × N_véhicules lignes (ex : 15 000 × 3 = 45 000 lignes)
  -- CROSS JOIN produit le produit cartésien de deux tables : chaque ligne de la
  -- table A est combinée avec chaque ligne de la table B.
  -- Ici : chaque segment de route est combiné avec chaque type de véhicule.
  aretes_x_vehicules AS (
    SELECT
      a.*,
      f.vehicule_id,
      f.nom                AS vehicule_nom,
      f.conso_base,
      f.facteur_conso_pente,
      f.prix_carburant,
      f.valeur_temps,
      f.facteur_urbain,
      -- Application de la pénalité urbaine :
      -- En zone résidentielle/commerciale, les poids lourds sont pénalisés
      -- via un multiplicateur sur le coût du temps et l'usure
      CASE WHEN a.zone_urbaine = TRUE THEN f.facteur_urbain ELSE 1.0 END
        AS facteur_urbain_applique,
      f.capacite_tonnes,
      -- Facteur surface et coût d'usure : résolu ici par CASE (fonction si-alors-sinon) pour éviter
      -- une jointure supplémentaire (params_surface n'est plus une table séparée)
      CASE a.surface
        WHEN 'paved'   THEN f.facteur_paved
        WHEN 'gravel'  THEN f.facteur_gravel
        WHEN 'unpaved' THEN f.facteur_unpaved
        ELSE f.facteur_unpaved
      END AS facteur_surface,
      CASE a.surface
        WHEN 'paved'   THEN f.usure_paved
        WHEN 'gravel'  THEN f.usure_gravel
        WHEN 'unpaved' THEN f.usure_unpaved
        ELSE f.usure_unpaved
      END AS usure_usd_km
    FROM aretes_base a
    CROSS JOIN params_flotte f
  ),

  -- Étape 2 : jointure avec la table de vitesses (vehicule_id + road_type + surface)
  -- LEFT JOIN : garde toutes les lignes de la table de gauche (aretes_x_vehicules)
  -- et ajoute les colonnes de la table de droite (vitesses_flotte) quand la
  -- condition ON est vraie. Si aucune vitesse n'est trouvée, NULL est retourné.
  -- COALESCE(valeur, 30) : si la vitesse est NULL (non trouvée), on utilise 30 km/h
  -- comme valeur par défaut raisonnable.
  avec_vitesse AS (
    SELECT
      ax.*,
      COALESCE(v.vitesse_kmh, 30) AS vitesse_base -- COALESCE : remplace les valeurs NULL par 30
    FROM aretes_x_vehicules ax
    -- Rajoût de la colonne vitesse en appariant en fonction de l'identifiant 
    -- du véhicule, du type de route et de la surface
    LEFT JOIN vitesses_flotte v
      ON  ax.vehicule_id = v.vehicule_id
      AND ax.road_type   = v.road_type
      AND ax.surface     = v.surface
  ),

  -- Étape 3 : application du facteur de pente sur la vitesse
  -- vitesse_effective = vitesse_base × facteur_pente
  -- Ex : sur une pente forte, un camion lourd va à 0.45 × sa vitesse de base.
  avec_vitesse_pente AS (
     SELECT
      av.*,
      av.vitesse_base * COALESCE(pp.facteur_pente, 1.0) AS speed_kmh
      -- COALESCE : si slope_category est NULL (arête topologique),
      -- facteur_pente = 1.0 (pas de modification de vitesse)
     FROM avec_vitesse av
     LEFT JOIN facteurs_pente_flotte pp
       ON  av.vehicule_id    = pp.vehicule_id
       AND av.slope_category = pp.slope_category
    ),

  -- Étape 4 : consommation de carburant (surconso en montée uniquement)
  -- La surconsommation s'applique uniquement quand slope_mean > 0 (montée).
  -- En descente, le moteur freine légèrement mais on ne modélise pas de gain.
  avec_conso AS (
    SELECT
      *,
      conso_base
        * facteur_surface
        * CASE
            WHEN slope_mean > 0
            THEN 1.0 + (slope_mean * facteur_conso_pente / 100.0)
            ELSE 1.0
          END AS conso_L_per_100km
    FROM avec_vitesse_pente
  ),

-- Étape 5 : conversion unités + calcul des composantes de coût
-- NULLIF(x, 0) : renvoie NULL si x vaut 0, sinon x.
-- Cela évite les divisions par zéro (ex : longueur_m = 0 → length_km = NULL).
  avec_couts AS (
    SELECT
      *,
       NULLIF(longueur_m, 0) / 1000.0                                  AS length_km,
      (NULLIF(longueur_m, 0) / 1000.0) / NULLIF(speed_kmh, 0)          AS travel_time_h,
      (NULLIF(longueur_m, 0) / 1000.0) * (conso_L_per_100km / 100.0)   AS fuel_consumption_L
    FROM avec_conso
  )

  -- Sélection finale : toutes les colonnes utiles + coût généralisé
  SELECT
    vehicule_id,
    vehicule_nom,
    arete_id,
    road_type,
    surface,
    slope_mean,
    slope_category,
    elevation_gain,
    elevation_loss,
    longueur_m,
    length_km,
    speed_kmh,
    conso_L_per_100km,
    fuel_consumption_L,
    fuel_consumption_L * prix_carburant             AS cost_fuel_usd,
    length_km * usure_usd_km                        AS cost_wear_usd,
    travel_time_h * valeur_temps                    AS cost_time_usd,
    travel_time_h,
    -- Coût par tkm avec pénalité urbaine sur le temps et l'usure
    -- Formule : (carburant + usure_pénalisée + temps_pénalisé) / distance / capacité
    (cost_fuel_usd
      + cost_wear_usd * facteur_urbain_applique
      + cost_time_usd * facteur_urbain_applique)
      / (NULLIF(length_km, 0)
      * NULLIF(capacite_tonnes, 0))                           AS cost_per_tkm
  FROM avec_couts
")

# Stats récapitulatives par véhicule depuis DuckDB
# AVG() : moyenne arithmétique
# SUM() : somme
# ROUND(x, n) : arrondit x à n décimales
# GROUP BY : regroupe les lignes par valeur d'une colonne, puis calcule les agrégats
#            par groupe. Ici, on calcule des stats séparément pour chaque véhicule.
stats_flotte <- duck_query("
  SELECT
    vehicule_id,
    vehicule_nom,
    ROUND(AVG(cost_per_tkm), 3) AS cout_par_tkm_moyen,
    ROUND(AVG(cost_fuel_usd / NULLIF(cost_fuel_usd + cost_wear_usd + cost_time_usd, 0)) * 100, 1) AS part_carburant_pct,
    ROUND(AVG(cost_time_usd / NULLIF(cost_fuel_usd + cost_wear_usd + cost_time_usd, 0)) * 100, 1) AS part_temps_pct,
    ROUND(AVG(cost_wear_usd / NULLIF(cost_fuel_usd + cost_wear_usd + cost_time_usd, 0)) * 100, 1) AS part_usure_pct
  FROM aretes_couts_tous
  GROUP BY vehicule_id, vehicule_nom
  ORDER BY cout_par_tkm_moyen
")
print(stats_flotte)

# Export Parquet de la table consolidée
# COPY TO : commande DuckDB pour exporter une table dans un fichier.
# FORMAT PARQUET : format de fichier colonnaire compressé, très efficace pour
# les grands tableaux analytiques. Lisible avec pandas en Python ou arrow en R.
dbExecute(con, paste0(
  "COPY (SELECT * FROM aretes_couts_tous) TO '",
  file.path(DIR_OUTPUT, "aretes_couts_tous_vehicules.parquet"),
  "' (FORMAT PARQUET)"
))
# Le chiffre qu'il y a à la suite de la commande précédente correspond au nombre de lignes bien exportées

# ── Réintégration dans sfnetworks pour le véhicule de référence ───────────────
# On récupère les coûts calculés dans DuckDB pour le véhicule de référence
# (camion_moyen) et on les ajoute comme attributs des arêtes dans le réseau sf.
# glue::glue() : interpolation de chaînes de caractères — remplace {VEHICULE_REFERENCE}
# par la valeur de la variable R VEHICULE_REFERENCE dans la requête SQL.
aretes_ref <- duck_query(glue::glue("
  SELECT * FROM aretes_couts_tous
  WHERE vehicule_id = '{VEHICULE_REFERENCE}'
  ORDER BY arete_id
"))

reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    length_km            = aretes_ref$length_km,
    speed_kmh            = aretes_ref$speed_kmh,
    travel_time_h        = aretes_ref$travel_time_h,
    conso_L_per_100km    = aretes_ref$conso_L_per_100km,
    fuel_consumption_L   = aretes_ref$fuel_consumption_L,
    cost_fuel_usd        = aretes_ref$cost_fuel_usd,
    cost_wear_usd        = aretes_ref$cost_wear_usd,
    cost_time_usd        = aretes_ref$cost_time_usd,
    cost_per_tkm         = aretes_ref$cost_per_tkm
  )

cat("✓ Table aretes_couts_tous créée dans DuckDB\n")
cat("  Lignes :", duck_query("SELECT COUNT(*) AS n FROM aretes_couts_tous")$n,
    "(arêtes × véhicules)\n\n")

# ── Vérification finale des colonnes critiques pour Dijkstra ──────────────────
# Un NA ou un Inf dans les poids de Dijkstra provoquerait des résultats erronés
# (chemins infinis, nœuds non atteignables). On vérifie ici qu'il n'y en a pas.
# is.nan() : Not a Number — résultat de 0/0 ou Inf - Inf par exemple.
# is.infinite() : valeur infinie (comme Inf ou -Inf en R).
aretes_check <- reseau_rwanda %>% activate("edges") %>% st_as_sf()

verif <- tibble(
  colonne = c("length_km", "speed_kmh", "travel_time_h", "cost_per_tkm"),
  n_na    = c(
    sum(is.na(aretes_check$length_km)            | is.nan(aretes_check$length_km)),
    sum(is.na(aretes_check$speed_kmh)            | is.nan(aretes_check$speed_kmh)),
    sum(is.na(aretes_check$travel_time_h)        | is.nan(aretes_check$travel_time_h)),
    sum(is.na(aretes_check$cost_per_tkm) | is.nan(aretes_check$cost_per_tkm) |
          is.infinite(aretes_check$cost_per_tkm))
  )
)
print(verif)
cat("  Total arêtes pathologiques :", sum(verif$n_na), "(doit être 0)\n\n")

# ── Diagnostic approfondi ─────────────────────────────────────────────────────
# Ce diagnostic vérifie la cohérence entre le réseau R (sfnetworks) et
# la table DuckDB. Un désalignement (nombre d'arêtes différent) causerait
# une réintégration incorrecte des coûts dans le réseau.

aretes_diag <- reseau_rwanda %>% activate("edges") %>% st_as_sf()

cat("=== Diagnostic désalignement indices ===\n\n")

# 1. Nombre d'arêtes dans le réseau vs dans DuckDB
n_reseau <- nrow(aretes_diag)
n_duckdb <- duck_query(glue::glue(
  "SELECT COUNT(*) AS n FROM aretes_couts_tous WHERE vehicule_id = '{VEHICULE_REFERENCE}'"
))$n

cat("Arêtes dans reseau_rwanda :", n_reseau, "\n")
cat("Arêtes dans DuckDB        :", n_duckdb, "\n")
cat("Écart                     :", n_duckdb - n_reseau, "\n\n")

# 2. Longueur brute depuis la géométrie vs longueur_m stockée
aretes_diag <- aretes_diag %>%
  mutate(
    longueur_geom   = as.numeric(st_length(geometry)),
    longueur_stored = longueur_m,
    ecart           = abs(longueur_geom - longueur_stored)
  )

cat("longueur_m = 0 ou NA (stockée)  :", 
    sum(is.na(aretes_diag$longueur_stored) | aretes_diag$longueur_stored == 0), "\n")
cat("longueur_geom = 0 ou NA (géométrie):", 
    sum(is.na(aretes_diag$longueur_geom)  | aretes_diag$longueur_geom  == 0), "\n\n")

# 3. Vérifier si length_km dans reseau_rwanda correspond à longueur_m / 1000
cat("length_km NA dans reseau_rwanda  :", 
    sum(is.na(aretes_diag$length_km)), "\n")

# 4. Chercher si aretes_base dans DuckDB a des longueur_m = 0
zero_duckdb <- duck_query("
  SELECT COUNT(*) AS n_zero, MIN(longueur_m) AS min_l, MAX(longueur_m) AS max_l
  FROM aretes_base
  WHERE longueur_m = 0 OR longueur_m IS NULL
")
cat("Arêtes longueur_m = 0 ou NULL dans aretes_base (DuckDB) :", zero_duckdb$n_zero, "\n")

# 5. Vérifier si length_km = NA dans DuckDB correspond à longueur_m = 0
na_duckdb <- duck_query(glue::glue("
  SELECT COUNT(*) AS n_na, MIN(longueur_m) AS min_l, AVG(longueur_m) AS avg_l
  FROM aretes_couts_tous
  WHERE vehicule_id = '{VEHICULE_REFERENCE}'
    AND length_km IS NULL
"))
cat("Arêtes length_km NULL dans DuckDB :", na_duckdb$n_na, "\n")
cat("  longueur_m min sur ces arêtes   :", na_duckdb$min_l, "\n")
cat("  longueur_m moy sur ces arêtes   :", round(na_duckdb$avg_l, 4), "\n\n")

# 6. Vérifier l'alignement arete_id
# Les arete_id dans DuckDB doivent être exactement 1, 2, …, n_reseau.
# Si le max est différent, il y a un décalage d'indice entre R et DuckDB.
arete_ids_duckdb <- duck_query(glue::glue("
  SELECT arete_id FROM aretes_couts_tous
  WHERE vehicule_id = '{VEHICULE_REFERENCE}'
  ORDER BY arete_id
"))
cat("arete_id max dans DuckDB :", max(arete_ids_duckdb$arete_id), "\n")
cat("Nb arêtes réseau         :", n_reseau, "\n")
cat("Correspondance parfaite  :", max(arete_ids_duckdb$arete_id) == n_reseau, "\n")

# ==============================================================================
# V.2 : Graphe multi-modal avec transbordements
# Réplique le réseau en 3 couches (une par véhicule) et ajoute des arêtes
# de transbordement aux entrepôts uniquement. Le Dijkstra sur ce graphe
# trouve automatiquement la combinaison optimale de véhicules pour chaque OD.
# ==============================================================================

# Le graphe multi-modal est une extension du réseau routier classique :
# au lieu d'avoir une seule "couche" de routes, on en a une par véhicule.
# Chaque nœud existe donc en 3 exemplaires : un par véhicule (camionnette,
# camion_moyen, camion_lourd).
# Des arêtes de "transbordement" relient les nœuds d'entrepôt entre couches :
# elles représentent le changement de véhicule, à un coût fixe.
# L'algorithme de Dijkstra sur ce graphe étendu trouvera automatiquement
# si un trajet est moins cher en commençant par camion_lourd et en finissant
# par camionnette (avec un transbordement intermédiaire) ou tout en camion_moyen.

# ── Paramètres de base ────────────────────────────────────────────────────────
n_vehicules <- nrow(VEHICULES_IDS)
graphe_base <- reseau_rwanda %>% as_tbl_graph()
n_noeuds    <- igraph::vcount(graphe_base)

# Fonction de remappage : nœud n dans la couche du véhicule v_idx.
# Dans le graphe multi-modal, les nœuds sont numérotés ainsi :
#   Couche 1 (camionnette)  : nœuds 1 .. n_noeuds
#   Couche 2 (camion_moyen) : nœuds n_noeuds+1 .. 2×n_noeuds
#   Couche 3 (camion_lourd) : nœuds 2×n_noeuds+1 .. 3×n_noeuds
# node_multi(v_idx, n_id) donne l'indice global du nœud n_id dans la couche v_idx.
node_multi <- function(v_idx, n_id) as.integer((v_idx - 1L) * n_noeuds + n_id)

cat("  Nœuds de base :", n_noeuds, "\n")
cat("  Véhicules     :", n_vehicules, "\n")
cat("  Nœuds total   :", n_noeuds * n_vehicules, "\n\n")

# ── Récupération des arêtes de base (from/to = indices igraph) ────────────────
aretes_base_tbl <- reseau_rwanda %>%
  activate("edges") %>%
  as_tibble() %>%
  mutate(arete_id = row_number())

# ── 1. Arêtes intra-couche (routes, une couche par véhicule) ──────────────────
# Pour chaque véhicule, on crée une copie de toutes les arêtes du réseau
# avec les coûts spécifiques à ce véhicule. Les indices from/to sont remappés
# dans la couche correspondante du graphe multi-modal.
edges_intra <- list()

for (v_idx in seq_len(n_vehicules)) {
  id_veh <- VEHICULES_IDS$vehicule_id[v_idx]
  
  # Coûts et attributs depuis DuckDB pour ce véhicule
  couts_veh <- duck_query(glue::glue("
    SELECT arete_id, cost_per_tkm, length_km, travel_time_h
    FROM aretes_couts_tous
    WHERE vehicule_id = '{id_veh}'
    ORDER BY arete_id
  "))
  
  # weight : poids de Dijkstra = coût total de traverser cette arête avec ce véhicule
  # = cost_per_tkm × length_km (coût par tonne-kilomètre × distance en km)
  edges_intra[[v_idx]] <- tibble(
    from          = node_multi(v_idx, aretes_base_tbl$from),
    to            = node_multi(v_idx, aretes_base_tbl$to),
    weight        = couts_veh$cost_per_tkm * couts_veh$length_km,
    length_km     = couts_veh$length_km,
    travel_time_h = couts_veh$travel_time_h,
    vehicule_id   = id_veh,
    type          = "route"
  ) %>%
    filter(!is.na(weight), weight > 0)
  
  cat("  Couche", id_veh, ":", nrow(edges_intra[[v_idx]]), "arêtes\n")
}

# ── 2. Arêtes de transbordement aux entrepôts (inter-couches) ─────────────────
# Uniquement aux nœuds d'entrepôt — pas de transbordement en bord de route
# On récupère les indices des nœuds d'entrepôt dans le graphe de base.
couts_transb       <- duck_query("SELECT * FROM couts_transbordement")
warehouse_nodes_base <- which(igraph::V(graphe_base)$is_warehouse)

cat("\n  Entrepôts disponibles pour transbordement :",
    length(warehouse_nodes_base), "\n")

edges_transb <- list()
k <- 0

# Pour chaque entrepôt et chaque paire de véhicules (origine → destination),
# on crée une arête de transbordement reliant le nœud-entrepôt dans la
# couche du véhicule d'origine au même nœud dans la couche du véhicule de destination.
for (wh_node in warehouse_nodes_base) {
  for (r in seq_len(nrow(couts_transb))) {
    
    # match() : trouve la position du nom du véhicule dans la liste VEHICULES_IDS
    v_orig <- match(couts_transb$vehicule_origine[r],     VEHICULES_IDS$vehicule_id)
    v_dest <- match(couts_transb$vehicule_destination[r], VEHICULES_IDS$vehicule_id)
    if (is.na(v_orig) || is.na(v_dest)) next
    
    k <- k + 1
    edges_transb[[k]] <- tibble(
      from          = node_multi(v_orig, wh_node),
      to            = node_multi(v_dest, wh_node),
      weight        = couts_transb$cout_usd_fixe[r],
      length_km     = 0,    # Pas de distance physique au transbordement
      travel_time_h = 0,    # Temps de manutention non modélisé ici
      vehicule_id   = paste0(couts_transb$vehicule_origine[r],
                             "->",
                             couts_transb$vehicule_destination[r]),
      type          = "transbordement"
    )
  }
}

cat("  Arêtes de transbordement créées :", k, "\n\n")

# ── Assemblage du graphe multi-modal ──────────────────────────────────────────
# bind_rows() empile toutes les arêtes (intra-couche + transbordements)
# en un seul tableau. c() combine une liste de listes en une liste plate.
all_edges_mm <- bind_rows(c(edges_intra, edges_transb))

# ── Table de mapping : arête multi-modale → arête physique + véhicule ─────────
# Nécessaire pour l'affectation All-or-Nothing en Partie 20
# Chaque arête intra-couche du graphe multi-modal est associée à :
#   - son indice dans le réseau physique (arete_physique_idx)
#   - son véhicule (vehicule_id)
#   - son type (route ou transbordement)
# Ce mapping permettra, après avoir trouvé un chemin optimal dans le graphe
# multi-modal, de reconstituer quelles routes physiques ont été empruntées
# et par quel type de véhicule.

n_aretes_physiques <- nrow(aretes_base_tbl)

# Les arêtes intra-couche sont numérotées de 1 à N_vehicules × N_aretes_physiques
# dans l'ordre : couche 1 (arêtes 1..N), couche 2 (arêtes N+1..2N), etc.
# Les arêtes de transbordement viennent ensuite.

mapping_aretes_mm <- bind_rows(
  # Arêtes intra-couche (routes)
  lapply(seq_len(n_vehicules), function(v_idx) {
    tibble(
      idx_mm           = seq_len(n_aretes_physiques) + (v_idx - 1) * n_aretes_physiques,
      arete_physique_idx = seq_len(n_aretes_physiques),
      vehicule_id      = VEHICULES_IDS$vehicule_id[v_idx],
      type             = "route"
    )
  }),
  # Arêtes de transbordement
  tibble(
    idx_mm             = seq(
      n_vehicules * n_aretes_physiques + 1,
      n_vehicules * n_aretes_physiques + length(edges_transb)
    ),
    arete_physique_idx = NA_integer_,
    vehicule_id        = NA_character_,
    type               = "transbordement"
  )
)

# ── Vecteurs d'accès direct pour le remappage  ────────────────────────────────
# Ces trois vecteurs permettent de retrouver en O(1) (accès direct par indice)
# le type, l'indice physique et le véhicule d'une arête multi-modale.
# O(1) signifie que le temps d'accès est constant, peu importe la taille du vecteur.
# C'est beaucoup plus rapide qu'une recherche dans un tableau (O(n)).
# Indexés par idx_mm → accès en O(1) au lieu de O(n) par recherche dans le tibble
# Taille = n_vehicules × n_aretes_physiques + n_transbordements

# Initialisation à la bonne taille des vecteur lookup
max_idx_mm <- max(mapping_aretes_mm$idx_mm)
lookup_type     <- character(max_idx_mm)
lookup_physique <- integer(max_idx_mm)
lookup_vehicule <- character(max_idx_mm)

# A l'indice idx_mm, on associe si c'est une route ou non, le numéro de l'arête dans le réseau et le véhicule
lookup_type[mapping_aretes_mm$idx_mm]     <- mapping_aretes_mm$type
lookup_physique[mapping_aretes_mm$idx_mm] <- 
  ifelse(is.na(mapping_aretes_mm$arete_physique_idx), 
         0L, 
         mapping_aretes_mm$arete_physique_idx)
lookup_vehicule[mapping_aretes_mm$idx_mm] <- 
  ifelse(is.na(mapping_aretes_mm$vehicule_id), 
         "", 
         mapping_aretes_mm$vehicule_id)

cat("  Vecteurs de lookup construits — taille :", max_idx_mm, "\n\n")

cat("  Table de mapping créée :", nrow(mapping_aretes_mm), "arêtes\n")
cat("  dont routes        :", sum(mapping_aretes_mm$type == "route"), "\n")
cat("  dont transbordements:", sum(mapping_aretes_mm$type == "transbordement"), "\n\n")

# Table des nœuds : chaque nœud de base existe en N_vehicules exemplaires
# rep(x, n) : répète le vecteur x, n fois.
# rep(x, each = n) : répète chaque élément de x, n fois.
vertices_mm <- tibble(
  name      = seq_len(n_noeuds * n_vehicules),
  node_base = rep(seq_len(n_noeuds), n_vehicules),
  vehicule  = rep(VEHICULES_IDS$vehicule_id, each = n_noeuds)
)

# igraph::graph_from_data_frame() : construit un objet igraph à partir d'un
# tableau d'arêtes (colonnes "from" et "to" obligatoires) et d'un tableau
# de nœuds (colonne "name" obligatoire).
# directed = FALSE : graphe non orienté (on peut aller dans les deux sens).
graphe_multimodal <- igraph::graph_from_data_frame(
  all_edges_mm,
  directed = FALSE,
  vertices = vertices_mm
)

cat("✓ Graphe multi-modal construit\n")
cat("  Nœuds  :", igraph::vcount(graphe_multimodal),
    "(", n_noeuds, "×", n_vehicules, "couches)\n")
cat("  Arêtes :", igraph::ecount(graphe_multimodal),
    "dont", k, "transbordements\n\n")


# ==============================================================================
# V.3 : Cartes de coûts et de pentes
# Génère une carte de cost_per_tkm par véhicule, deux cartes de ratio
# (lourd/camionnette, moyen/camionnette) et la carte des pentes.
# ==============================================================================

# On génère une carte par véhicule montrant le coût de transport sur chaque
# segment routier. Les segments rouges/bordeaux sont les plus coûteux
# (routes en mauvais état, pentes importantes, zone urbaine).

for (i in seq_len(nrow(VEHICULES_IDS))) {
  
  id_veh  <- VEHICULES_IDS$vehicule_id[i]
  nom_veh <- VEHICULES_IDS$nom[i]
  
  # Récupérer les coûts depuis DuckDB (une requête SQL, pas de R intermédiaire)
  couts_veh <- duck_query(glue::glue("
    SELECT arete_id, cost_per_tkm, speed_kmh
    FROM aretes_couts_tous
    WHERE vehicule_id = '{id_veh}'
    ORDER BY arete_id
  "))
  
  reseau_tmp <- reseau_rwanda %>%
    activate("edges") %>%
    mutate(
      cost_per_tkm          = couts_veh$cost_per_tkm,
      speed_kmh            = couts_veh$speed_kmh
    )
  
  # tm_scale_intervals() : découpe la variable continue (cost_per_tkm) en
  # intervalles discrets pour la légende.
  # style="quantile" : intervalles de taille égale en nombre d'observations
  # (chaque classe contient le même nombre d'arêtes).
  carte <- fond_carte() +
    tm_shape(reseau_tmp %>% activate("edges") %>% st_as_sf()) +
    tm_lines(
      col       = "cost_per_tkm",
      col.scale = tm_scale_intervals(style="quantile", n=4, values=PALETTE_COUTS),
      col.legend = tm_legend(title = "Coût (USD/tkm)"),
      lwd = 1.5
    ) +
    tm_shape(entreposages_sf) + tm_dots(fill="black", size=0.2) +
    tm_title(paste("Coûts de Transport —", nom_veh)) +
    tm_layout(legend.outside=TRUE, frame=TRUE) +
    tm_scalebar(position=c("left","bottom")) +
    tm_compass(position=c("right","top"))
  
  nom_fichier <- paste0("carte_couts_", id_veh, ".png")
  tmap_save(carte, file.path(DIR_OUTPUT, nom_fichier), width=3000, height=2400, dpi=300)
  cat("  ✓", nom_fichier, "\n")
}


# On génère une deuxième série de cartes dans un format différent (pour le Viewer)
# en stockant les objets tmap dans une liste pour un affichage interactif optionnel.
cartes_vehicules <- list()

for (i in seq_len(nrow(VEHICULES_IDS))) {
  id_veh  <- VEHICULES_IDS$vehicule_id[i]
  nom_veh <- VEHICULES_IDS$nom[i]
  cat(id_veh, "\n")
  cat(nom_veh, "\n")
  
  couts_veh <- duck_query(glue::glue("
    SELECT arete_id, cost_per_tkm, speed_kmh
    FROM aretes_couts_tous
    WHERE vehicule_id = '{id_veh}'
    ORDER BY arete_id
  "))
  cat("Lignes récupérées :", nrow(couts_veh), "\n")
  
  reseau_tmp <- reseau_rwanda %>%
    activate("edges") %>%
    mutate(
      cost_per_tkm          = couts_veh$cost_per_tkm,
      speed_kmh            = couts_veh$speed_kmh
    )
  cat("reseau_tmp créé\n")
  
  # tm_scale(values.range = c(0.5, 5)) : remplace la légende de la largeur de ligne
  # par une échelle continue entre 0.5 (fin) et 5 (épais).
  cartes_vehicules[[id_veh]] <- fond_carte() +
    tm_shape(reseau_tmp %>% activate("edges") %>% st_as_sf()) +
    tm_lines(
      col        = "cost_per_tkm",
      col.scale  = tm_scale_intervals(style="quantile", n=5, values="brewer.yl_or_rd"),
      col.legend = tm_legend(title = "Coût (USD/km)"),
      lwd = 1.5
    ) +
    tm_shape(entreposages_sf) + tm_dots(fill="black", size=0.2) +
    tm_title(paste("Coûts de Transport —", nom_veh))
}
if (FALSE){
  tmap_mode("view")
  cat("✓ Cartes créées :", paste(names(cartes_vehicules), collapse=", "), "\n")
  cat("  Pour afficher, entrer dans la console : print(cartes_vehicules[['camionnette']])\n")
  cat("                                          print(cartes_vehicules[['camion_moyen']])\n")
  cat("                                          print(cartes_vehicules[['camion_lourd']])\n")
  tmap_mode("plot")
}

# ── Carte comparative : ratio coût par km camion lourd vs camionnette ─────────
# Cette carte montre où le camion lourd est relativement plus avantageux (vert)
# ou plus désavantageux (rouge) par rapport à la camionnette.
# Ratio > 1 : le camion lourd coûte plus cher par tkm (pentes fortes, routes dégradées)
# Ratio < 1 : le camion lourd est plus avantageux (économies d'échelle sur grande route)
# Requête SQL directe : le calcul du ratio se fait entièrement dans DuckDB
ratio_df <- duck_query("
  SELECT
    a.arete_id,
    a.cost_per_tkm / NULLIF(b.cost_per_tkm, 0) AS ratio_lourd_vs_legere
  FROM
    (SELECT arete_id, cost_per_tkm FROM aretes_couts_tous WHERE vehicule_id = 'camion_lourd')  a
  JOIN
    (SELECT arete_id, cost_per_tkm FROM aretes_couts_tous WHERE vehicule_id = 'camionnette') b
  USING (arete_id)
  ORDER BY arete_id
")

if (nrow(ratio_df) > 0) {
  reseau_ratio <- reseau_rwanda %>%
    activate("edges") %>%
    mutate(ratio_lourd_vs_legere = ratio_df$ratio_lourd_vs_legere)
  
  carte_ratio <- fond_carte() +
    tm_shape(reseau_ratio %>% activate("edges") %>% st_as_sf()) +
    tm_lines(
      col       = "ratio_lourd_vs_legere",
      col.scale = tm_scale_intervals(style="quantile", n=5, values=PALETTE_RATIO),
      col.legend = tm_legend(title="Ratio coût\nlourd / camionnette"),
      lwd = 1.5
    ) +
    tm_title("Surcoût relatif — Camion lourd vs Camionnette") +
    tm_layout(legend.outside=TRUE, frame=TRUE) +
    tm_scalebar(position=c("left","bottom")) +
    tm_compass(position=c("right","top"))
  
  tmap_save(carte_ratio, file.path(DIR_OUTPUT,"carte_ratio_vehicules.png"),
            width=3000, height=2400, dpi=300)
  cat("  ✓ carte_ratio_vehicules.png\n")
}

cat("✓", nrow(VEHICULES_IDS), "cartes + 1 carte comparative générées\n\n")

if (FALSE){
  tmap_mode("view")
  print(carte_ratio)
  tmap_mode("plot")
}

# ── Carte comparative : ratio coût par tkm camion moyen vs camionnette ────────
ratio_moyen_df <- duck_query("
  SELECT
    a.arete_id,
    a.cost_per_tkm / NULLIF(b.cost_per_tkm, 0) AS ratio_moyen_vs_camionnette
  FROM
    (SELECT arete_id, cost_per_tkm FROM aretes_couts_tous WHERE vehicule_id = 'camion_moyen') a
  JOIN
    (SELECT arete_id, cost_per_tkm FROM aretes_couts_tous WHERE vehicule_id = 'camionnette') b
  USING (arete_id)
  ORDER BY arete_id
")

if (nrow(ratio_moyen_df) > 0) {
  reseau_ratio_moyen <- reseau_rwanda %>%
    activate("edges") %>%
    mutate(ratio_moyen_vs_camionnette = ratio_moyen_df$ratio_moyen_vs_camionnette)
  
  carte_ratio_moyen <- fond_carte() +
    tm_shape(reseau_ratio_moyen %>% activate("edges") %>% st_as_sf()) +
    tm_lines(
      col       = "ratio_moyen_vs_camionnette",
      col.scale = tm_scale_intervals(style="quantile", n=5, values=PALETTE_RATIO),
      col.legend = tm_legend(title="Ratio coût\nmoyen / camionnette"),
      lwd = 1.5
    ) +
    tm_title("Surcoût relatif — Camion moyen vs Camionnette") +
    tm_layout(legend.outside=TRUE, frame=TRUE) +
    tm_scalebar(position=c("left","bottom")) +
    tm_compass(position=c("right","top"))
  
  tmap_save(carte_ratio_moyen,
            file.path(DIR_OUTPUT,"carte_ratio_moyen_camionnette.png"),
            width=3000, height=2400, dpi=300)
  cat("  ✓ carte_ratio_moyen_camionnette.png\n")
}

if (FALSE) {
  tmap_mode("view")
  print(carte_ratio_moyen)
  tmap_mode("plot")
}

# ── Carte des pentes (indépendante du véhicule) ───────────────────────────────
# Cette carte ne dépend pas du type de véhicule : elle montre juste l'inclinaison
# du terrain sur chaque segment routier. Elle permet d'identifier visuellement
# les zones montagneuses (routes en rouge = pente forte > 8%).
carte_pentes <- fond_carte() +
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col="slope_category",
           col.scale = tm_scale(values = PALETTE_PENTE),
           col.legend=tm_legend(title="Catégorie de pente"), lwd=1.5) +
  tm_title("Pentes du Réseau Routier") +
  tm_layout(legend.outside=TRUE, frame=TRUE) +
  tm_scalebar(position=c("left","bottom")) +
  tm_compass(position=c("right","top"))

tmap_save(carte_pentes, file.path(DIR_OUTPUT,"carte_pentes_rwanda.png"),
          width=3000, height=2400, dpi=300)
cat("  ✓ carte_pentes_rwanda.png\n")

if (FALSE) {
  tmap_mode("view")
  print(carte_pentes)
  tmap_mode("plot")
}

################################################################################
# PARTIE VI — MATRICE ORIGINE-DESTINATION
# Calcule les coûts de transport optimaux entre toutes les paires d'entrepôts
# via Dijkstra multi-modal, stocke la matrice OD dans DuckDB et exporte
# le réseau enrichi (GeoPackage, CSV, Parquet).
################################################################################

# ==============================================================================
# VI.1 : Dijkstra et matrice OD
# Pour chaque paire d'entrepôts, cherche le chemin de moindre coût dans le
# graphe multi-modal. Stocke les résultats (coût, distance, temps, véhicules
# utilisés, transbordements) dans la table DuckDB matrice_od.
# ==============================================================================

# L'algorithme de Dijkstra est l'algorithme classique de "plus court chemin"
# dans un graphe pondéré. "Court" ici ne signifie pas physiquement court,
# mais de moindre coût (le "poids" de chaque arête est son coût de transport).
# Pour chaque paire (entrepôt i, entrepôt j), on cherche le chemin qui
# minimise le coût total, potentiellement en changeant de véhicule.
# La "matrice OD" (Origine-Destination) est le tableau carré n×n qui contient
# le coût optimal pour aller de chaque entrepôt vers chaque autre entrepôt.

# Extraction des nœuds identifiés comme entrepôts dans le réseau
noeuds_entreposage <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  as_tibble() %>%
  mutate(warehouse_id = row_number()) # Donne un identifiant unique

n_warehouses <- nrow(noeuds_entreposage)
cat("Entreposages :", n_warehouses, "\n")

# ── Préparation du graphe igraph pour Dijkstra ────────────────────────────────
# as_tbl_graph() convertit sfnetworks en tidygraph/igraph tout en conservant
# les attributs des nœuds et des arêtes.
graphe_igraph      <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(weight = cost_per_tkm * length_km) %>%   # weight = métrique de coût pour Dijkstra
  as_tbl_graph()

# which() : renvoie les indices (positions) des éléments TRUE dans un vecteur logique.
# igraph::V() : récupère tous les sommets (vertices = nœuds) du graphe.
# $is_warehouse : attribut is_warehouse de chaque nœud (TRUE/FALSE).
warehouse_node_ids <- which(igraph::V(graphe_igraph)$is_warehouse)

# ── Calcul des plus courts chemins multi-modaux par Dijkstra ──────────────────
# Pour chaque paire d'entrepôts (i,j) :
#   1. Tester tous les véhicules de départ possibles en i
#   2. Tester tous les véhicules d'arrivée possibles en j
#   3. Le chemin optimal dans le graphe multi-modal donne automatiquement
#      la meilleure combinaison, y compris avec transbordements intermédiaires

od_rows <- list()
idx     <- 0

# ── Mise en cache de la matrice OD ────────────────────────────────────────────
# Le calcul de Dijkstra multi-modal est très long (plusieurs dizaines de minutes
# pour 120 zones). Pour éviter de le refaire à chaque exécution du script,
# on sauvegarde le résultat dans un fichier ".rds" (format binaire R).
# À la prochaine exécution, si le réseau et le nombre de zones n'ont pas changé,
# on charge directement le fichier sauvegardé — le calcul est alors instantané.
# Pour forcer un recalcul complet (ex : après avoir ajouté des zones ou modifié
# le réseau), il suffit de supprimer le fichier "outputs/od_cache.rds".
CACHE_OD <- file.path(DIR_OUTPUT, "od_cache.rds")
cache_od_valide <- FALSE

# file.exists() : vérifie si le fichier cache existe déjà sur le disque.
if (file.exists(CACHE_OD)) {
  cache_od <- readRDS(CACHE_OD)  # Charge le fichier sauvegardé en mémoire R
  
  # Double vérification : le cache n'est valide que si le nombre de zones ET
  # le nombre d'arêtes correspondent exactement à la session actuelle.
  # Si l'un ou l'autre a changé (ex : nouveau PBF, nouvelles zones ajoutées),
  # le cache est rejeté et le calcul repart de zéro automatiquement.
  if (!is.null(cache_od$n_warehouses) && cache_od$n_warehouses == n_warehouses &&
      !is.null(cache_od$n_aretes)     && cache_od$n_aretes     == n_aretes_physiques) {
    od_long         <- cache_od$od_long
    cache_od_valide <- TRUE
    cat("  ✓ Cache OD valide (", n_warehouses, "zones ×",
        n_aretes_physiques, "arêtes) — calcul Dijkstra ignoré\n\n")
  } else {
    cat("  ⚠ Cache OD invalide — recalcul Dijkstra...\n")
  }
}

# Si pas de cache valide, on lance le calcul complet puis on sauvegarde.
if (!cache_od_valide) {
  od_rows <- list()
  idx     <- 0

  for (i in seq_along(warehouse_nodes_base)) {
    
    # Nettoyage mémoire périodique : libère les objets R inutilisés tous les
    # 10 passages. Sans cela, les matrices de distances accumulées en mémoire
    # au fil des itérations peuvent saturer la RAM et provoquer un crash.
    if (i %% 10 == 0) gc()
    
    # sources_i : indices des nœuds dans le graphe multi-modal correspondant
    # à l'entrepôt i dans chacune des 3 couches véhicule.
    sources_i <- sapply(seq_len(n_vehicules),
                        function(v) node_multi(v, warehouse_nodes_base[i]))
  
  # Tous les nœuds destination (toutes couches × tous entrepôts) en une seule passe
  targets_all <- as.vector(sapply(
    seq_len(n_vehicules),
    function(v) node_multi(v, warehouse_nodes_base)
  ))
  
  # igraph::distances() : calcule les distances de Dijkstra depuis plusieurs
  # sources vers plusieurs cibles en une seule passe.
  # weights = E(graphe)$weight : utilise la colonne "weight" comme poids des arêtes.
  # Résultat : matrice n_sources × n_targets de coûts optimaux.
  # Inf dans la matrice = impossible d'atteindre la cible depuis la source.
  dists_all <- igraph::distances(
    graphe_multimodal,
    v       = sources_i,
    to      = targets_all,
    weights = igraph::E(graphe_multimodal)$weight
  )
  
  for (j in seq_along(warehouse_nodes_base)) {
    if (i == j) next
    
    # cols_j : colonnes de la matrice dists_all correspondant à l'entrepôt j
    # dans toutes les couches véhicule.
    cols_j   <- j + (seq_len(n_vehicules) - 1) * length(warehouse_nodes_base)
    min_cout <- min(dists_all[, cols_j], na.rm = TRUE)
    if (is.infinite(min_cout)) next  # Entrepôts non connectés → pas de flux
    
    # Identifier la meilleure combinaison de véhicules
    # which(... arr.ind = TRUE) : renvoie les indices ligne ET colonne du minimum.
    best_idx  <- which(dists_all[, cols_j] == min_cout, arr.ind = TRUE)[1, ]
    best_from <- sources_i[best_idx[1]]
    best_to   <- targets_all[cols_j[best_idx[2]]]
    
    # igraph::shortest_paths() : récupère le chemin lui-même (pas seulement le coût).
    # output = "epath" : retourne les indices des arêtes empruntées (edge path).
    # C'est différent de "vpath" qui retourne les nœuds.
    path_obj  <- igraph::shortest_paths(
      graphe_multimodal,
      from    = best_from,
      to      = best_to,
      weights = igraph::E(graphe_multimodal)$weight,
      output  = "epath"
    )
    edges_path <- path_obj$epath[[1]]
    edge_data  <- igraph::edge_attr(graphe_multimodal)
    
    idx <- idx + 1
    od_rows[[idx]] <- list(
      id_origine        = i,
      id_destination    = j,
      nom_origine       = noeuds_entreposage$warehouse_name[i],
      nom_destination   = noeuds_entreposage$warehouse_name[j],
      cout_usd          = min_cout,
      distance_km       = sum(edge_data$length_km[edges_path],     na.rm = TRUE),
      temps_h           = sum(edge_data$travel_time_h[edges_path], na.rm = TRUE),
      vehicule_depart   = VEHICULES_IDS$vehicule_id[best_idx[1]],
      vehicule_arrivee  = VEHICULES_IDS$vehicule_id[best_idx[2]],
      n_transbordements = sum(edge_data$type[edges_path] == "transbordement")
    )
  }
  if (i %% 3 == 0) cat("  OD multi-modal :", round(i/n_warehouses*100,1), "%\n")
}

od_long <- bind_rows(od_rows)

# saveRDS() : sauvegarde l'objet R dans un fichier binaire sur le disque.
# On sauvegarde à la fois les résultats (od_long) et les paramètres de
# validation (n_warehouses, n_aretes) pour pouvoir vérifier la validité
# du cache lors des prochaines exécutions.
saveRDS(
  list(od_long = od_long, n_warehouses = n_warehouses,
       n_aretes = n_aretes_physiques),
  CACHE_OD
)
cat("  ✓ Cache OD sauvegardé :", CACHE_OD, "\n\n")
}
duck_write(od_long, "matrice_od")

# Statistiques enrichies incluant les transbordements
od_stats <- duck_query("
  SELECT
    COUNT(*)                          AS n_paires,
    ROUND(AVG(cout_usd), 2)           AS cout_moyen_usd,
    ROUND(AVG(distance_km), 1)        AS dist_moyenne_km,
    SUM(n_transbordements > 0)        AS paires_avec_transbordement,
    ROUND(AVG(n_transbordements), 2)  AS transbordements_moyens
  FROM matrice_od
")

cat("✓ Matrice OD multi-modale stockée dans DuckDB\n")
cat("  Paires connectées            :", od_stats$n_paires, "(sur ",
    n_warehouses * (n_warehouses - 1),"paires possibles", 
    round(od_stats$n_paires / (n_warehouses * (n_warehouses - 1)) * 100, 1),
    "% de connectivité)\n")
cat("  Coût moyen                   :", od_stats$cout_moyen_usd, "USD\n")
cat("  Paires avec transbordement   :", od_stats$paires_avec_transbordement, "\n")
cat("  Transbordements moyens/trajet:", od_stats$transbordements_moyens, "\n\n")


# ==============================================================================
# VI.2 : Exports du réseau (GeoPackage, CSV, Parquet)
# Exporte le réseau routier enrichi (coûts par véhicule, pentes, topologie)
# et la matrice OD dans tous les formats de sortie via DuckDB COPY TO.
# ==============================================================================

# COPY TO est la commande DuckDB pour exporter des tables vers des fichiers.
# Avantages sur write.csv() :
#   - Parquet : format colonnaire compressé (~10× plus compact que CSV)
#   - Vitesse : écriture multithread native de DuckDB
#   - SQL : filtrer/transformer les données à l'export sans créer de df R intermédiaire


# ── Récupération des coûts de tous les véhicules depuis DuckDB ────────────────
# MAX(CASE WHEN ...) est un idiome SQL pour "pivoter" une table longue en large.
# La table aretes_couts_tous est en format long : chaque arête a 3 lignes (une par véhicule).
# On veut une table large : chaque arête a 1 ligne avec 3 colonnes de coût.
# MAX() est utilisé comme agrégateur car chaque arête a exactement une valeur
# non-NULL par véhicule dans le CASE WHEN.
couts_wide <- duck_query("
  SELECT
    arete_id,
    MAX(CASE WHEN vehicule_id = 'camionnette'  THEN cost_per_tkm          END) AS cost_tkm_camionnette,
    MAX(CASE WHEN vehicule_id = 'camion_moyen' THEN cost_per_tkm          END) AS cost_tkm_camion_moyen,
    MAX(CASE WHEN vehicule_id = 'camion_lourd' THEN cost_per_tkm          END) AS cost_tkm_camion_lourd
  FROM aretes_couts_tous
  GROUP BY arete_id
  ORDER BY arete_id
")

# ── Construction de la table des arêtes finales enrichie ─────────────────────
aretes_finales <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  mutate(
    cost_tkm_camionnette  = couts_wide$cost_tkm_camionnette,
    cost_tkm_camion_moyen = couts_wide$cost_tkm_camion_moyen,
    cost_tkm_camion_lourd = couts_wide$cost_tkm_camion_lourd
  ) %>%
  select(osm_id, name, road_type, surface, length_km, slope_mean,
         elevation_gain, elevation_loss,
         cost_tkm_camionnette, cost_tkm_camion_moyen, cost_tkm_camion_lourd)

duck_write(aretes_finales %>% st_drop_geometry(), "aretes_finales")

# Export GeoPackage (format géospatial ouvert, compatible QGIS/ArcGIS/GRASS)
# Le GeoPackage est un format de fichier standard pour les données géospatiales.
# Il peut contenir des géométries (points, lignes, polygones) + leurs attributs.
# Compatible avec QGIS (logiciel SIG libre) et ArcGIS (logiciel SIG commercial).
# Seul st_write() peut exporter des géométries (DuckDB ne les supporte pas encore)
st_write(aretes_finales, file.path(DIR_OUTPUT,"reseau_rwanda_aretes.gpkg"),
         delete_dsn=TRUE, quiet=TRUE)

noeuds_finaux <- reseau_rwanda %>%
  activate("nodes") %>%
  st_as_sf() %>%
  select(node_id, is_warehouse, warehouse_name, warehouse_type)
st_write(noeuds_finaux, file.path(DIR_OUTPUT, "reseau_rwanda_noeuds.gpkg"),
         delete_dsn=TRUE, quiet=TRUE)

# Exports CSV depuis DuckDB via COPY TO
# HEADER = TRUE : inclure les noms de colonnes en première ligne du fichier
dbExecute(con, paste0(
  "COPY (SELECT * FROM matrice_od) TO '",
  file.path(DIR_OUTPUT, "matrice_od_long.csv"),
  "' (FORMAT CSV, HEADER)"
))
dbExecute(con, paste0(
  "COPY (SELECT * FROM aretes_finales) TO '",
  file.path(DIR_OUTPUT,'aretes_finales.csv'),
  "'(FORMAT CSV, HEADER)"
))
dbExecute(con, paste0(
  "COPY (SELECT * FROM couts_prebordure) TO '",
  file.path(DIR_OUTPUT, "couts_prebordure.csv"),
  "' (FORMAT CSV, HEADER)"
))
cat("  ✓ couts_prebordure.csv\n")

# Exports Parquet depuis DuckDB
# Lisible directement avec : Python → pd.read_parquet() ; R → arrow::read_parquet()
dbExecute(con, paste0(
  "COPY (SELECT * FROM aretes_finales) TO '",
  file.path(DIR_OUTPUT, 'aretes_finales.parquet'), 
  "'(FORMAT PARQUET)"
))
dbExecute(con, paste0(
  "COPY (SELECT * FROM matrice_od) TO '",
  file.path(DIR_OUTPUT, 'matrice_od.parquet'), 
  "'(FORMAT PARQUET)"
))

cat("✓ Exports CSV + Parquet via DuckDB COPY TO\n\n")


################################################################################
# PARTIE VII — MODÈLE ÉCONOMIQUE
# Construit la chaîne économique complète :
#   Table IO → multiplicateurs Leontief → offres/demandes par zone
#   → modèle gravitaire avec friction sur coûts de transport (C_ij)
#     et coûts pré-frontière (C_prebordure).
# Dépend de la Partie VI (matrice OD) pour construire C_ij.
################################################################################

# ==============================================================================
# VII.1 : Table Input-Output de Leontief
# Définit les 8 secteurs, la matrice des coefficients techniques A,
# les productions totales et les facteurs de conversion valeur → tonnes.
# Calcule les multiplicateurs de Leontief (I-A)^(-1) et stocke dans DuckDB.
# NOTE : données fictives calibrées sur le Rwanda 2022. Pour utiliser les
# données NISR réelles, remplacer A et production_totale ici uniquement.
# ==============================================================================

# La table Input-Output de Leontief modélise les interdépendances sectorielles :
#   a_ij = part de la production du secteur j consommée en intrant par le secteur i
#   Production totale : X = (I - A)^(-1) × D  [équation de Leontief]
#   où D = demande finale et (I - A)^(-1) = matrice des multiplicateurs de Leontief
#
# Interprétation de (I - A)^(-1) :
#   Un élément [i,j] donne l'augmentation de production du secteur i nécessaire
#   pour satisfaire une augmentation de 1 USD de demande finale dans le secteur j.
#
# En termes concrets : si les ménages rwandais dépensent 1 USD de plus en
# produits alimentaires (Agro_industrie), combien cela génère-t-il de production
# supplémentaire dans l'Agriculture (pour fournir les matières premières) ?
# C'est ce que calcule la matrice de Leontief.


# 8 secteurs représentatifs de l'économie rwandaise en 2022
SECTEURS   <- c("Agriculture","Mines","Agro_industrie","Industrie",
                "Construction","Commerce","Transport","Services")
N_SECTEURS <- length(SECTEURS)

# ── Matrice des coefficients techniques A ─────────────────────────────────────
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

# ── Production totale par secteur (millions USD, Rwanda 2022) ─────────────────
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

# ── Facteurs de conversion valeur → masse (tonnes par million USD) ────────────
# Ces facteurs permettent de convertir les flux monétaires (M USD) en tonnes physiques.
# Un secteur à ratio élevé (Construction) génère beaucoup de tonnes par USD
# (ciment, gravier = produits lourds et peu chers).
# À l'inverse, les Services génèrent très peu de fret physique.
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

# ── Grandeurs dérivées de la table IO ─────────────────────────────────────────
# %*% : produit matriciel en R (différent de * qui est une multiplication élément par élément).
# A %*% x donne le vecteur des consommations intermédiaires : pour chaque secteur i,
# la somme de a_ij × production_j sur tous les secteurs j fournisseurs.
conso_interm   <- as.vector(A %*% production_totale)
# Valeur ajoutée = production - consommations intermédiaires 
valeur_ajoutee <- production_totale - conso_interm
# Demande finale ≈ 85% de la valeur ajoutée 
demande_finale <- valeur_ajoutee * 0.85

# ── Stockage dans DuckDB ──────────────────────────────────────────────────────
io_table <- tibble(
  secteur             = SECTEURS,
  production_musd     = production_totale,
  conso_interm_musd   = conso_interm,
  valeur_ajoutee_musd = valeur_ajoutee,
  demande_finale_musd = demande_finale,
  tonnes_par_musd     = TONNES_PAR_musd
)
duck_write(io_table, "io_table")

# Récapitulatif IO
recap_io <- io_table

# Matrice A en format long pour des requêtes SQL sur les coefficients individuels
# rownames_to_column() : transforme les noms de lignes en colonne "secteur_input".
# pivot_longer() : transforme un tableau large (colonnes = secteurs) en tableau long
# (une ligne = un coefficient, avec colonnes "secteur_output" et "coef_a").
A_long <- as.data.frame(A) %>%
  rownames_to_column("secteur_input") %>%
  pivot_longer(-secteur_input, names_to="secteur_output", values_to="coef_a")
duck_write(A_long, "matrice_a_long")

# ── Multiplicateurs de Leontief ───────────────────────────────────────────────
# L = (I - A)^(-1) : la matrice inverse de Leontief
# L[i,j] = augmentation de production du secteur i nécessaire pour fournir 1 USD
# de demande finale supplémentaire dans le secteur j (effets directs + indirects)
# diag(N_SECTEURS) : matrice identité de taille N×N (1 sur la diagonale, 0 ailleurs).
# solve() : calcule l'inverse d'une matrice carrée.
# I - A : donne la matrice de "production nette" (production moins ce qui est
# réinjecté comme intrant dans le circuit).
leontief <- solve(diag(N_SECTEURS) - A)
leontief_long <- as.data.frame(leontief) %>%
  setNames(SECTEURS) %>%
  mutate(secteur_demande = SECTEURS) %>%
  pivot_longer(-secteur_demande, names_to="secteur_production", values_to="multiplicateur")
duck_write(leontief_long, "multiplicateurs_leontief")

cat("✓ Table IO + multiplicateurs de Leontief chargés dans DuckDB\n\n")


# ==============================================================================
# VII.2 : Offres et demandes par zone
# Affecte à chaque zone un profil sectoriel d'offre et de demande via une
# moyenne pondérée entre le profil de base (déterminé par le type de zone)
# et les profils des types de zones correspondant aux usages du sol environnants.
#
# FORMULE :
#   profil_final = (profil_base * 1 + profil_industrie * part_ind 
#                  + profil_hub * part_urb)
#                / (1 + part_ind + part_urb)
#
# Les profils landuse réutilisent PROFILS_OFFRE et PROFILS_DEMANDE déjà
# définis : les zones industrielles environnantes contribuent via le profil
# "industrie", les zones urbaines via le profil "urbain" (le type de zone le
# plus représentatif d'un environnement urbain dense).
#
# Cette interpolation convexe garantit que :
#   1. La somme des parts sectorielles reste toujours égale à 1
#   2. L'identité structurelle de la zone n'est jamais effacée par son
#      contexte local (le profil de base a toujours un poids de 1)
#   3. Les poids sont directement interprétables comme des proportions
#      de surface dans le buffer de 2km
#   4. Pas besoin de pmax(), renormalisation, plafonds ou bruit aléatoire
# ==============================================================================

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

# Taille économique relative de chaque zone
# (détermine le volume absolu des échanges générés)
# Kigali = 1.0 (référence). Un hub à 0.10 génère 10× moins d'échanges que Kigali.
TAILLE_ZONE <- c(
  "Kigali - Hub Central"          = 1.00,  # Référence : hub dominant du pays
  "Kigali - SEZ Masoro"           = 0.35,  # Zone industrielle intégrée à Kigali
  "Kigali - Marché Kimisagara"    = 0.18,  # Grand marché de gros populaire
  "Frontière Gatuna (Ouganda)"    = 0.25,  # Principal corridor Nord (vers Kampala)
  "Frontière Rusumo (Tanzanie)"   = 0.20,  # Corridor Est (vers Dar es Salaam, mer)
  "Frontière Rubavu/Goma (RDC)"   = 0.30,  # Corridor Ouest (très actif avec la RDC)
  "Frontière Kagitumba (Ouganda)" = 0.15,  # Frontière secondaire Nord-Est
  "Huye (Butare) - Centre Sud"    = 0.18,  # 2e ville : université, industrie
  "Musanze - Centre Nord"         = 0.15,  # Zone touristique volcanique
  "Rubavu - Centre Ouest"         = 0.14,  # Ville du lac Kivu
  "Rusizi - Centre Sud-Ouest"     = 0.12,  # Frontalière RDC/Burundi
  "Bugesera SEZ (Agro-industrie)" = 0.22,  # SEZ en développement près de l'aéroport
  "Muhanga"                       = 0.08,  # Carrefour de transit
  "Nyanza"                        = 0.07,  # Ancienne capitale royale
  "Rwamagana"                     = 0.09,  # Capitale de la Province de l'Est
  "Frontière Bugarama (Burundi)"  = 0.12   # Corridor Sud modéré
)

# ── Correspondance entre types de landuse et profils de zone ─────────────────
# Les zones industrielles dans le buffer sont représentées par le profil
# "industrie" déjà défini dans PROFILS_OFFRE/DEMANDE.
# Les zones urbaines sont représentées par le profil "hub" — le type de zone
# dont la structure économique est la plus proche d'un environnement urbain dense.
# Ce choix est explicite et discutable : on pourrait utiliser "ville" pour
# les zones résidentielles si on disposait de données plus granulaires.
PROFIL_OFFRE_LANDUSE_INDUSTRIEL   <- PROFILS_OFFRE[["industrie"]]
PROFIL_DEMANDE_LANDUSE_INDUSTRIEL <- PROFILS_DEMANDE[["industrie"]]
PROFIL_OFFRE_LANDUSE_URBAIN       <- PROFILS_OFFRE[["hub"]]
PROFIL_DEMANDE_LANDUSE_URBAIN     <- PROFILS_DEMANDE[["hub"]]

cat("✓ Profils landuse définis\n\n")

# Part du PIB qui "voyage" entre zones (le reste est consommé localement)
# 35% est une hypothèse conservatrice pour un pays enclavé comme le Rwanda
PART_ECHANGEABLE <- 0.35
echelle_offre    <- sum(production_totale) * PART_ECHANGEABLE
echelle_demande  <- sum(demande_finale)    * PART_ECHANGEABLE

# Génération des matrices offre et demande (lignes = zones, colonnes = secteurs)
# matrix(0, n, m) : crée une matrice de zéros de dimensions n×m.
# dimnames : noms des lignes (zones) et colonnes (secteurs) pour lisibilité.
offre_zones   <- matrix(0, n_warehouses, N_SECTEURS,
                        dimnames=list(noeuds_entreposage$warehouse_name, SECTEURS))
demande_zones <- matrix(0, n_warehouses, N_SECTEURS,
                        dimnames=list(noeuds_entreposage$warehouse_name, SECTEURS))

# ── Calcul de la composition d'usage du sol autour de chaque entrepôt ─────────
# Pour chaque entrepôt, on calcule la part de chaque landuse dans un buffer
# de 2km. Cette composition module les profils d'offre/demande.
# Une zone industrielle entourée de grandes zones industrielles aura un profil
# d'offre encore plus orienté "Industrie" que la moyenne de son type.

# calc_part_landuse() : calcule la proportion de la surface d'un buffer
# qui est couverte par des polygones d'usage du sol (zones urbaines ou industrielles).
#
# Paramètres :
#   buffer_geom — géométrie sf d'un seul buffer circulaire (autour d'un entrepôt)
#   zones_sf    — objet sf contenant les polygones de landuse à tester
#                 (zones_urbaines ou zones_industrielles selon l'appel)
#
# Retourne :
#   Un nombre entre 0 et 1 :
#     0   = aucune zone de ce type dans le buffer
#     0.4 = 40% de la surface du buffer est couverte par ce type de zone
#     1   = le buffer est entièrement dans une zone de ce type
#
# Exemple d'interprétation :
#   calc_part_landuse(buf, zones_industrielles) = 0.35
#   → 35% de la zone dans un rayon de 2km autour de l'entrepôt est industrielle
#   → son profil d'offre sera davantage orienté "Industrie" et "Construction"

calc_part_landuse <- function(buffer_geom, zones_sf) {
  
  # Vérification préalable : si la couche de zones est vide (ex : pas de zones
  # industrielles dans le PBF), on retourne directement 0 sans calcul.
  if (nrow(zones_sf) == 0) return(0)
  
  # Encapsulation de la géométrie brute dans un objet sf complet avec son CRS.
  # st_sfc() : crée une colonne géométrique à partir d'une géométrie brute.
  # st_as_sf() : transforme en objet sf manipulable par les fonctions spatiales.
  # Le CRS 32735 (UTM Zone 35S) est celui de tout le réseau routier — il est
  # indispensable de le spécifier ici car buffer_geom est une géométrie brute
  # extraite d'un objet sf, qui a perdu son CRS au passage.
  buffer_sf <- st_as_sf(st_sfc(buffer_geom, crs = 32735))
  
  # st_intersection() : calcule la géométrie commune entre le buffer et les zones.
  # Résultat : les fragments des polygones de landuse qui se trouvent à l'intérieur
  # du buffer circulaire de 2km autour de l'entrepôt.
  # Si aucun polygone ne chevauche le buffer, st_intersection retourne un sf vide.
  # suppressWarnings() : évite les messages d'avertissement sur les géométries
  # complexes (lignes de bord, coins de polygones) qui n'impactent pas le résultat.
  intersection <- suppressWarnings(st_intersection(zones_sf, buffer_sf))
  
  # Si l'intersection est vide (aucune zone de ce type dans le buffer),
  # on retourne 0 immédiatement sans calculer d'aire.
  if (nrow(intersection) == 0) return(0)
  
  # Calcul de l'aire totale des fragments d'intersection en mètres carrés.
  # st_area() calcule l'aire de chaque polygone résultant de l'intersectionen m² ;
  # as.numeric() le convertit en nombre ordinaire pour les opérations arithmétiques.
  # sum() additionne toutes les surfaces si plusieurs polygones se chevauchent
  # avec le buffer.
  aire_intersection <- sum(as.numeric(st_area(intersection)), na.rm = TRUE)
  
  # Calcul de l'aire totale du buffer de référence (cercle de 2km de rayon).
  # Cette valeur est la même pour tous les entrepôts (même rayon) mais on la
  # recalcule ici pour que la fonction soit générique (indépendante du rayon).
  aire_buffer <- as.numeric(st_area(buffer_sf))
  
  # Protection contre une division par zéro si le buffer a une aire nulle
  # (ne devrait pas arriver avec des coordonnées valides, mais par sécurité).
  if (aire_buffer == 0) return(0)
  
  # Calcul de la proportion et plafonnement à 1.
  # min(..., 1) : évite d'obtenir une valeur > 1 en cas d'artefacts géométriques
  # (ex : légers chevauchements de polygones qui gonflent artificiellement l'aire).
  min(aire_intersection / aire_buffer, 1)
}

# ── Mise en cache du calcul de composition landuse ────────────────────────────
# Pour chaque zone d'entreposage, on calcule la part de surface urbanisée et
# industrielle dans un rayon de 2km. Ce calcul nécessite des intersections
# géométriques entre les buffers de chaque zone et les polygones de landuse,
# ce qui peut prendre plusieurs minutes.
# Comme pour les pentes et la matrice OD, on met le résultat en cache pour
# éviter de le recalculer inutilement à chaque exécution.
# Le cache est invalidé si le nombre de zones change (nouvelle zone ajoutée).
CACHE_LANDUSE <- file.path(DIR_OUTPUT, "landuse_cache.rds")
cache_landuse_valide <- FALSE

if (file.exists(CACHE_LANDUSE)) {
  cache_lu <- readRDS(CACHE_LANDUSE)
  # On vérifie que le nombre de zones est identique à la session actuelle.
  # Si de nouvelles zones ont été ajoutées, le cache est rejeté.
  if (!is.null(cache_lu$n_warehouses) && cache_lu$n_warehouses == n_warehouses) {
    part_urbain     <- cache_lu$part_urbain
    part_industriel <- cache_lu$part_industriel
    cache_landuse_valide <- TRUE
    cat("  ✓ Cache landuse valide (", n_warehouses, "zones) — calcul ignoré\n\n")
  } else {
    cat("  ⚠ Cache landuse invalide — recalcul...\n")
  }
}

if (!cache_landuse_valide) {
  cat("  Calcul de la composition landuse par zone...\n")
  
  # Deux vecteurs numériques initialisés à zéro, un par type de landuse.
  # numeric(n) crée un vecteur de n zéros — on les remplira zone par zone.
  part_urbain     <- numeric(n_warehouses)
  part_industriel <- numeric(n_warehouses)
  
  for (i in seq_len(n_warehouses)) {
    buf <- entreposages_buffer[i, ]$geometry
    part_urbain[i]     <- calc_part_landuse(buf, zones_urbaines)
    part_industriel[i] <- calc_part_landuse(buf, zones_industrielles)
    if (i %% 5 == 0) cat("  Landuse par zone :", round(i/n_warehouses*100), "%\n")
  }
  
  # Sauvegarde des deux vecteurs + le nombre de zones pour validation future
  saveRDS(
    list(part_urbain = part_urbain, part_industriel = part_industriel,
         n_warehouses = n_warehouses),
    CACHE_LANDUSE
  )
  cat("  ✓ Cache landuse sauvegardé\n\n")
}

cat("✓ Composition landuse calculée\n\n")

taille_default <- 0.10  # Valeur par défaut pour les zones OSM

tailles_toutes_zones <- sapply(
  noeuds_entreposage$warehouse_name,
  function(nom) {
    t <- TAILLE_ZONE[nom]
    if (is.na(t)) taille_default else t   
  }
)
somme_tailles <- sum(tailles_toutes_zones)

cat("  Somme des tailles (", n_warehouses, "zones) :", round(somme_tailles, 2), "\n")
cat("  dont manuelles :", round(sum(TAILLE_ZONE), 2), "\n")
cat("  dont OSM (défaut) :", round(somme_tailles - sum(TAILLE_ZONE), 2), "\n\n")

# ── Modification des profils selon la composition landuse ─────────────────────
# Principe : plus une zone est industrielle, plus son profil d'offre favorise
# l'Industrie et la Construction ; plus elle est urbaine, plus elle favorise
# le Commerce et les Services.

for (i in 1:n_warehouses) {
  nom_zone  <- noeuds_entreposage$warehouse_name[i]
  type_zone <- noeuds_entreposage$warehouse_type[i]
  taille    <- TAILLE_ZONE[nom_zone]
  if (is.na(taille)) taille <- taille_default    
  
  # ── Récupération du profil de base selon le type de zone ──────────────────
  # C'est l'identité structurelle de la zone : une frontière reste une frontière
  # indépendamment de ce qui l'entoure géographiquement.
  profil_o_base <- PROFILS_OFFRE[[type_zone]]
  profil_d_base <- PROFILS_DEMANDE[[type_zone]]
  
  # ── Parts landuse dans le buffer de 2km ───────────────────────────────────
  # Ces valeurs ont été calculées et mises en cache avant cette boucle.
  # Elles déterminent le poids des profils landuse dans la moyenne pondérée.
  p_ind <- part_industriel[i]   # Entre 0 et 1 — pas de plafond nécessaire
  p_urb <- part_urbain[i]       # car la formule est une interpolation convexe
  
  # ── Moyenne pondérée des profils (interpolation convexe) ──────────────────
  # Numérateur   : somme pondérée des profils de base et des profils landuse
  # Dénominateur : somme des poids (garantit que le résultat somme à 1)
  #
  # Exemple pour Rubavu (p_ind=0.08, p_urb=0.35) :
  #   profil_o_final = (1 * profil_frontiere + 0.08 * profil_industriel
  #                     + 0.35 * profil_urbain) / (1 + 0.08 + 0.35)
  #
  # Le profil de base (poids = 1) ne peut jamais être complètement écrasé
  # par les profils landuse, même si p_ind + p_urb > 1.
  
  poids_base <- 1
  
  denominateur_o <- poids_base + p_ind + p_urb
  denominateur_d <- poids_base + p_ind + p_urb
  
  profil_o_final <- (profil_o_base                    * poids_base     +
                       PROFIL_OFFRE_LANDUSE_INDUSTRIEL   * p_ind +
                       PROFIL_OFFRE_LANDUSE_URBAIN       * p_urb) / denominateur_o
  
  profil_d_final <- (profil_d_base                    * poids_base     +
                       PROFIL_DEMANDE_LANDUSE_INDUSTRIEL * p_ind +
                       PROFIL_DEMANDE_LANDUSE_URBAIN     * p_urb) / denominateur_d
  
  # ── Volume final = profil × taille × échelle nationale ────────────────────
  # taille          : poids économique relatif de la zone (Kigali = 1.0)
  # echelle_offre   : volume total échangeable à l'échelle nationale (M USD)
  # sum(TAILLE_ZONE): normalisation pour que la somme des parts fasse 1
  # Pas de bruit aléatoire : l'hétérogénéité est entièrement portée par
  # la composition landuse, ce qui est plus défendable empiriquement.
  offre_zones[i,]   <- profil_o_final * taille * echelle_offre   / somme_tailles
  demande_zones[i,] <- profil_d_final * taille * echelle_demande / somme_tailles
}

# ── Stockage dans DuckDB en format long ───────────────────────────────────────
# Format long (1 ligne = 1 zone × 1 secteur) plus adapté aux jointures SQL
offre_long_df <- as.data.frame(offre_zones) %>%
  rownames_to_column("zone") %>%
  pivot_longer(-zone, names_to = "secteur", values_to = "offre_musd")
duck_write(offre_long_df, "offre_zones")

demande_long_df <- as.data.frame(demande_zones) %>%
  rownames_to_column("zone") %>%
  pivot_longer(-zone, names_to = "secteur", values_to = "demande_musd")
duck_write(demande_long_df, "demande_zones")

# Bilan par zone calculé directement en SQL
recap_zones <- duck_query("
  SELECT
    o.zone,
    ROUND(SUM(o.offre_musd), 2)                  AS offre_totale_musd,
    ROUND(SUM(d.demande_musd), 2)                AS demande_totale_musd,
    ROUND(SUM(o.offre_musd - d.demande_musd), 2) AS solde_musd
  FROM offre_zones o
  JOIN demande_zones d ON o.zone = d.zone AND o.secteur = d.secteur
  GROUP BY o.zone
  ORDER BY offre_totale_musd DESC
")

cat("✓ Offres et demandes par zone stockées dans DuckDB\n\n")

# ==============================================================================
# VII.3 : Modèle gravitaire
# T_ij^s = K^s × O_i^s × D_j^s × (C_ij + C_prebordure_ij^s)^(-beta_s)
# C_ij        = coût réseau interne (matrice OD Partie VI)
# C_prebordure = coût de transport depuis le pays étranger jusqu'à la frontière
# beta_s      = sensibilité au coût par secteur (2.5 Construction, 0.9 Services)
# ==============================================================================


# Le modèle gravitaire estime les flux commerciaux bilatéraux :
#
#   T_ij^s = K^s * O_i^s * D_j^s *(C_ij + C_prebordure_ij^s)^(-beta_s)
#
# où :
#   T_ij^s            = flux du secteur s de la zone i vers la zone j (M USD)
#   O_i^s             = offre du secteur s en zone i
#   D_j^s             = demande du secteur s en zone j
#   F_ij              = facteur de friction = C_ij^(-beta)
#   C_ij              = coût généralisé de transport (USD / tonne) entre i et j
#   C_prebordure_ij^s = coût généralisé de transport (USD / tonne) entre i et j lorsque i est une ville frontalière (intègre le coût de transport intra-pays frontaliers)
#   beta_s            = paramètre de friction (sensibilité au coût) par secteur
#   K^s               = constante de calibration par secteur
#
# Intuition : les zones proches (coût faible) échangent plus que les zones
# lointaines. Le paramètre beta modèle à quel point la distance/coût freine
# les échanges. Agriculture : très sensible (produits frais, lourds → beta élevé).
# Services : peu sensible (transactions financières → beta faible).

cat("Paramètres du modèle gravitaire:\n")

# --- Paramètres de friction par secteur ---
# Beta élevé = très sensible au coût (produits lourds/périssables)
# Beta faible = peu sensible (haute valeur, export international)
BETA_SECTEUR <- c(
  Agriculture    = 2.2,  # Très sensible (périssable, faible valeur/tonne)
  Mines          = 1.2,  # Peu sensible (haute valeur, destinations fixes)
  Agro_industrie = 1.8,
  Industrie      = 1.6,
  Construction   = 2.5,  # Très sensible (matériaux très lourds)
  Commerce       = 1.7,
  Transport      = 1.3,
  Services       = 0.9   # Peu sensible (services quasi-immatériels)
)

for (s in SECTEURS) {
  cat("  β(", s, ") =", BETA_SECTEUR[s], "\n")
}
cat("\n")

# ── Reconstruction de la matrice coûts en R carrée ──────────────────────────
# On passe de la matrice OD format long (DuckDB, 1 ligne = 1 paire OD)
# au format matriciel carré (R, n_zones × n_zones) pour le calcul gravitaire.
matrice_couts     <- matrix(0, n_warehouses, n_warehouses,
                            dimnames = list(noeuds_entreposage$warehouse_name, noeuds_entreposage$warehouse_name))

for (r in seq_len(nrow(od_long))) {
  i <- od_long$id_origine[r]; j <- od_long$id_destination[r]
  matrice_couts[i, j]     <- od_long$cout_usd[r]
}

# --- Préparation de la matrice de coûts ---
C_ij <- matrice_couts
diag(C_ij) <- NA          # Pas d'échange intrazone (une zone n'échange pas avec elle-même)
C_ij[C_ij == 0] <- NA     # Zones non connectées → pas de flux

# ── Récupération des coûts pré-frontière depuis DuckDB ────────────────────────
couts_prebordure <- duck_query("SELECT * FROM couts_prebordure")

# ── Identification des entrepôts frontière et de leur pays ────────────────────
# left_join() : fusionne noeuds_entreposage avec entreposages_fictifs pour
# récupérer le pays associé à chaque entrepôt frontière.
entrepots_frontiere <- noeuds_entreposage %>%
  filter(warehouse_type == "frontiere") %>%
  left_join(
    entreposages_fictifs %>% select(nom, pays),
    by = c("warehouse_name" = "nom")
  )

cat("  Entrepôts frontière avec pays :\n")
print(entrepots_frontiere %>% select(warehouse_name, pays))

# ── Construction d'une matrice de coûts pré-frontière par secteur ─────────────
# Dimensions : n_warehouses × n_warehouses × N_SECTEURS
# C_prebordure[i, j, s] = coût pré-frontière si i est une frontière, 0 sinon
# Note : le coût pré-frontière s'applique sur l'axe des origines (i)
# car c'est la marchandise qui arrive de l'étranger vers le Rwanda
# array() : crée un tableau à 3 dimensions (matrice × secteur).
C_prebordure <- array(
  0,
  dim      = c(n_warehouses, n_warehouses, N_SECTEURS),
  dimnames = list(
    noeuds_entreposage$warehouse_name,
    noeuds_entreposage$warehouse_name,
    SECTEURS
  )
)

for (i in seq_len(n_warehouses)) {
  nom_zone  <- noeuds_entreposage$warehouse_name[i]
  type_zone <- noeuds_entreposage$warehouse_type[i]
  
  if (type_zone != "frontiere") next  # next : passe directement à l'itération suivante
  
  # Récupérer le pays de ce point frontière
  pays_zone <- entrepots_frontiere$pays[
    entrepots_frontiere$warehouse_name == nom_zone
    ]
  if (length(pays_zone) == 0 || is.na(pays_zone)) next
  
  # Récupérer les coûts pré-frontière pour ce pays
  couts_pays <- couts_prebordure %>%
    filter(pays == pays_zone)
  
  for (s in SECTEURS) {
    cout_s <- couts_pays$cout_usd_tonne[couts_pays$secteur == s]
    if (length(cout_s) == 0) next
    
    # Affecter à toutes les destinations j depuis ce point frontière i
    C_prebordure[i, , s] <- cout_s
  }
}

cat("✓ Matrice de coûts pré-frontière construite\n\n")


# --- Calcul des flux gravitaires par secteur ---
cat("Calcul des flux gravitaires...\n")

flux_gravitaire <- list()   # Matrice de flux par secteur (M USD)

# Création de la matrice de flux total avec des noms de zones UNIQUES.
# noeuds_entreposage$warehouse_name peut contenir des doublons si deux zones
# portent le même nom (ex : deux zones OSM appelées "Kigali").
# make.unique() règle ce problème une fois pour toutes ici, ce qui évite
# les erreurs "must have unique names" lors de tous les pivot_longer(),
# rownames_to_column() et write.csv() qui utilisent flux_total plus loin.
noms_zones_uniques <- make.unique(noeuds_entreposage$warehouse_name, sep = "_")

flux_total <- matrix(0, nrow = n_warehouses, ncol = n_warehouses,
                     dimnames = list(noms_zones_uniques,
                                     noms_zones_uniques))
for (s in SECTEURS) {
  beta_s <- BETA_SECTEUR[s]
  
  # C_ij effective = coût réseau interne + coût pré-frontière
  C_ij_effectif <- C_ij + C_prebordure[, , s]
  C_ij_effectif[is.na(C_ij_effectif)] <- NA  # Conserver les NA (zones non connectées)
  
  # Friction : zones proches ont plus d'échanges
  friction <- C_ij_effectif^(-beta_s)
  friction[is.na(friction)] <- 0
  
  # Offres et demandes sectorielles
  O_s <- offre_zones[, s]
  D_s <- demande_zones[, s]
  
  # Flux gravitaire brut : T_ij = O_i * D_j * F_ij
  # outer(x, y) : produit extérieur — crée une matrice n×m où chaque élément [i,j] = x[i] * y[j]
  flux_brut <- outer(O_s, D_s) * friction
  diag(flux_brut) <- 0  # Une zone ne peut pas s'échanger avec elle-même
  
  # Calibration : normalisation pour respecter les totaux
  # sqrt(sum_O × sum_D) est une cible géométrique qui équilibre offre et demande.
  # On multiplie tous les flux par un facteur K tel que leur somme = cible.
  if (sum(flux_brut) > 0) {
    cible     <- sqrt(sum(O_s) * sum(D_s))
    facteur_k <- cible / sum(flux_brut)
    flux_calibre <- flux_brut * facteur_k
  } else {
    flux_calibre <- flux_brut
  }
  
  flux_gravitaire[[s]] <- flux_calibre
  flux_total <- flux_total + flux_calibre
}

cat("✓ Flux gravitaires calculés pour", length(SECTEURS), "secteurs\n")

# --- Résultats globaux ---
# sapply() : applique une fonction à chaque élément d'un vecteur et retourne
# un vecteur (ou matrice) de résultats.
flux_par_secteur_df <- tibble(
  Secteur          = SECTEURS,
  Beta             = unname(BETA_SECTEUR),
  Flux_total_musd  = sapply(SECTEURS, function(s) round(sum(flux_gravitaire[[s]]), 1)),
  Flux_moyen_musd  = sapply(SECTEURS, function(s) {
    f <- flux_gravitaire[[s]]
    round(mean(f[f > 0]), 3)
  })
)

cat("\nFlux par secteur:\n")
print(flux_par_secteur_df)

# Vérifier les doublons dans les noms de zones
cat("Doublons dans warehouse_name :\n")
print(noeuds_entreposage$warehouse_name[duplicated(noeuds_entreposage$warehouse_name)])

# Top 10 des paires OD
# pivot_longer() : transforme la matrice carrée en format long (1 ligne = 1 paire OD).
# filter(flux_musd > 0.01) : exclut les flux négligeables (< 10 000 USD).
# arrange(desc()) : trie par ordre décroissant de flux.
flux_total_long <- flux_total %>%
  as.data.frame() %>%
  setNames(make.unique(colnames(.), sep = "_")) %>%
  rownames_to_column("Origine") %>%
  pivot_longer(-Origine, names_to = "Destination", values_to = "flux_musd") %>%
  filter(flux_musd > 0.01) %>%
  arrange(desc(flux_musd))

cat("\nTop 10 des flux commerciaux bilatéraux (M USD):\n")
print(head(flux_total_long, 10))
cat("\n")
cat("✓ Flux total modélisé:", round(sum(flux_total), 1), "M USD\n")
cat("  Nombre de paires actives:", nrow(flux_total_long), "\n\n")


################################################################################
# PARTIE VIII — AFFECTATION DU FRET ET RÉSULTATS
# Convertit les flux monétaires (M USD) en tonnes, affecte chaque flux OD
# au chemin optimal du graphe multi-modal (All-or-Nothing), puis produit
# l'ensemble des visualisations et exports finaux.
################################################################################

# ==============================================================================
# VIII.1 : Conversion et affectation All-or-Nothing
# VERSION OPTIMISÉE MÉMOIRE pour Onyxia SSP Cloud
#
#   1. Nettoyage explicite des objets intermédiaires devenus inutiles
#   2. gc() plus agressif (à chaque itération au lieu de toutes les 10)
#   3. Remplacement de shortest_paths(output="epath") par une reconstruction
#      plus légère via distances() + vecteur prédécesseurs
#   4. Vectorisation de l'affectation des volumes (suppression boucle interne)
#   5. Monitoring de la RAM pour détecter les problèmes en amont
# ==============================================================================

# ── ÉTAPE 0 : Nettoyage agressif de la mémoire avant de commencer ────────────
# On supprime tous les objets intermédiaires qui ne serviront plus.

cat("── Nettoyage mémoire avant Partie VIII.1 ──────────────────────────────\n")

objets_a_supprimer <- c(
  # Réseaux intermédiaires de la Partie III
  "reseau_lisse", "reseau_subdivise", "graphe_base", "graphe_igraph",
  "aretes_lisse", "aretes_perdues", "aretes_avec_geom", "aretes_diag",
  "aretes_check", "aretes_centroides", "noeuds_lisse", "noeuds_hors_geante",
  "noeuds_sf", "composantes_finales", "comp_lisse",
  
  # Données brutes OSM
  "routes_rwanda_raw", "routes_attrs_raw", "routes_rwanda_clean",
  "attrs_df", "attrs_clean", "landuse_test", "place_test", "villes_raw",
  
  # Couches géographiques lourdes
  "dem_rwanda", "zones_urbaines_union",
  "zones_urbaines", "zones_industrielles", "zones_retail",
  "centroides_indus", "centroides_indus_sf", "centroides_retail",
  "centroides_retail_sf", "tous_existants", "tous_existants2",
  "bbox_poly", "emprise_sf", "emprise_points",
  
  # Grandes tables intermédiaires
  "edges_intra", "edges_transb", "all_edges_mm", "mapping_aretes_mm",
  "vertices_mm", "aretes_base_tbl", "aretes_df", "aretes_ref",
  "couts_wide", "couts_veh", "ratio_df", "ratio_moyen_df",
  
  # Objets tmap (peuvent être très lourds)
  "carte_verif_routes", "carte_aretes_perdues", "carte_ratio",
  "carte_ratio_moyen", "carte_pentes", "cartes_vehicules",
  "reseau_tmp", "reseau_ratio", "reseau_ratio_moyen",
  
  # Caches déjà intégrés
  "pentes_df", "cache", "cache_od", "cache_lu",
  
  # Diagnostics
  "distrib_road_type", "distrib_surface", "distrib_taille",
  "distrib_province", "centroides_perdues", "provinces_join",
  "verif", "zero_duckdb", "na_duckdb", "arete_ids_duckdb"
)

# On supprime uniquement les objets qui existent (évite les warnings)
objets_existants <- objets_a_supprimer[
  sapply(objets_a_supprimer, exists, envir = .GlobalEnv)
]
if (length(objets_existants) > 0) {
  rm(list = objets_existants, envir = .GlobalEnv)
  cat("  Supprimés :", length(objets_existants), "objets\n")
}

# Fonction utilitaire pour afficher la RAM utilisée
afficher_ram <- function(etape = "") {
  ram_mb <- round(sum(gc()[, 2]), 1)
  cat("  [RAM ", etape, "] ", ram_mb, " MB utilisés\n", sep = "")
}

# Double gc() pour forcer la libération complète (premier passage marque,
# deuxième passage collecte réellement)
invisible(gc(full = TRUE))
invisible(gc(full = TRUE))
afficher_ram("après nettoyage")

# ── ÉTAPE 1 : Conversion des flux monétaires en tonnes ───────────────────────
cat("\nConversion des flux en tonnes...\n")

flux_tonnes_total <- matrix(
  0,
  nrow = n_warehouses, ncol = n_warehouses,
  dimnames = list(noms_zones_uniques, noms_zones_uniques)
)

for (s in SECTEURS) {
  flux_tonnes_total <- flux_tonnes_total + flux_gravitaire[[s]] * TONNES_PAR_musd[s]
}

tonnage_total <- sum(flux_tonnes_total)
cat("  Tonnage total modélisé:",
    format(round(tonnage_total), big.mark = " "), "tonnes\n\n")

# ── ÉTAPE 2 : Pré-filtrage des paires OD à traiter ────────────────────────────
# Plutôt que de boucler sur n_warehouses² paires puis de filtrer par seuil,
# on construit d'abord la liste des paires pertinentes. Ça permet aussi
# d'avoir une barre de progression exacte et de mieux répartir les gc().

SEUIL_FLUX_TONNES <- 50

CACHE_AFFECTATION      <- file.path(DIR_OUTPUT, "affectation_cache.rds")
cache_affectation_valide <- FALSE

# Empreinte (hash) des entrées du calcul. Si l'une d'entre elles change,
# le hash change, et le cache est automatiquement rejeté.
# digest::digest() produit une empreinte stable de n'importe quel objet R :
# deux objets identiques donnent le même hash, deux objets différents donnent
# des hashs différents. On combine plusieurs entrées dans une liste.
if (!requireNamespace("digest", quietly = TRUE)) {
  install.packages("digest")
}

empreinte_entrees <- digest::digest(
  list(
    flux_tonnes_total = flux_tonnes_total,    # dépend de BETA, TONNES, PART_ECHANGEABLE
    seuil             = SEUIL_FLUX_TONNES,
    n_aretes          = n_aretes_physiques,
    n_warehouses      = n_warehouses,
    n_vehicules       = n_vehicules,
    n_aretes_mm       = igraph::ecount(graphe_multimodal)
  ),
  algo = "xxhash64"
)

if (file.exists(CACHE_AFFECTATION)) {
  
  cat("── Tentative de chargement du cache d'affectation ─────────────────────\n")
  cache_aff <- readRDS(CACHE_AFFECTATION)
  
  # Le cache est valide si l'empreinte des entrées correspond.
  # Une empreinte différente signifie qu'au moins une entrée a changé
  # (PBF modifié, paramètres économiques modifiés, etc.).
  if (!is.null(cache_aff$empreinte) &&
      cache_aff$empreinte == empreinte_entrees) {
    
    volume_trafic_mm         <- cache_aff$volume_trafic_mm
    paires_traitees          <- cache_aff$paires_traitees
    paires_non_connectees    <- cache_aff$paires_non_connectees
    cache_affectation_valide <- TRUE
    
    cat("  ✓ Cache d'affectation valide\n")
    cat("    Paires traitées      :", paires_traitees, "\n")
    cat("    Paires non connectées:", paires_non_connectees, "\n")
    cat("    → Affectation ignorée (~2-5 min gagnées)\n\n")
    
  } else {
    cat("  ⚠ Cache d'affectation invalide (entrées modifiées) — recalcul\n\n")
  }
}

# ══════════════════════════════════════════════════════════════════════════════
# BLOC CONDITIONNEL : l'affectation ne s'exécute que si pas de cache valide
# ══════════════════════════════════════════════════════════════════════════════
if (!cache_affectation_valide) {

# ── ÉTAPE 3 : Pré-filtrage des paires OD à traiter ────────────────────────────

# which(..., arr.ind = TRUE) renvoie les indices (i, j) des éléments qui
# satisfont la condition sous forme de matrice à 2 colonnes.
paires_actives <- which(flux_tonnes_total > SEUIL_FLUX_TONNES, arr.ind = TRUE)
# On exclut la diagonale (i == j)
paires_actives <- paires_actives[paires_actives[, 1] != paires_actives[, 2], ]

n_paires <- nrow(paires_actives)
cat("  Paires OD à traiter :", format(n_paires, big.mark = " "),
    "(sur", format(n_warehouses^2 - n_warehouses, big.mark = " "),
    "possibles)\n\n")

# ── ÉTAPE 4 : Préparation des matrices de résultats ──────────────────────────
# Tableau 3D (arêtes × véhicules × secteurs) pour conserver l'information sectorielle.
# Chaque "tranche" du tableau correspond à un secteur économique.
# Exemple de lecture : volume_trafic_mm_s[500, "camion_moyen", "Agriculture"]
# = tonnes d'Agriculture transportées par camion moyen sur l'arête n°500
volume_trafic_mm_s <- array(
  0,
  dim      = c(n_aretes_physiques, n_vehicules, N_SECTEURS),
  dimnames = list(NULL, VEHICULES_IDS$vehicule_id, SECTEURS)
)

paires_traitees       <- 0
paires_non_connectees <- 0

# Récupération du vecteur de poids une seule fois hors boucle
# (évite l'accès répété à E(graphe_multimodal)$weight qui est coûteux)
poids_mm <- igraph::E(graphe_multimodal)$weight

# ── ÉTAPE 5 : Boucle principale par zone origine ─────────────────────────────
# On parcourt les zones origine une par une. Pour chaque origine i, on calcule
# en UNE SEULE fois les distances vers toutes les destinations (bien plus
# efficace qu'une requête par paire).

cat("Affectation du fret au réseau (All-or-Nothing multi-modal)...\n")

# Pré-calcul : indices globaux de toutes les destinations dans les 3 couches
targets_all_global <- as.vector(sapply(
  seq_len(n_vehicules),
  function(v) node_multi(v, warehouse_nodes_base)
))

# On regroupe les paires actives par origine pour traiter toute une origine
# en une passe Dijkstra.
paires_par_origine <- split(
  paires_actives[, 2],       # destinations
  paires_actives[, 1]        # origines (clé du split)
)
origines_a_traiter <- as.integer(names(paires_par_origine))

n_origines <- length(origines_a_traiter)

# Barre de progression
pb_aff <- progress_bar$new(
  format = "  Affectation [:bar] :percent | ETA: :eta | :current/:total",
  total  = n_origines,
  clear  = FALSE,
  width  = 70
)

for (idx_i in seq_along(origines_a_traiter)) {
  
  i <- origines_a_traiter[idx_i]
  destinations_i <- paires_par_origine[[as.character(i)]]
  
  # Indices globaux des sources pour cette origine (3 couches véhicule)
  sources_i <- as.integer(sapply(
    seq_len(n_vehicules),
    function(v) node_multi(v, warehouse_nodes_base[i])
  ))
  
  # Dijkstra en une passe : depuis les 3 sources vers toutes les destinations.
  # Résultat : matrice 3 × (n_warehouses * n_vehicules)
  dists_all <- igraph::distances(
    graphe_multimodal,
    v       = sources_i,
    to      = targets_all_global,
    weights = poids_mm
  )
  
  # Pour chaque destination j active avec i, on identifie la meilleure
  # combinaison (couche de départ, couche d'arrivée) puis on reconstruit
  # le chemin et on affecte le volume.
  
  for (j in destinations_i) {
    
    # ── Dijkstra : calcul du chemin optimal (une seule fois pour la paire i,j) ──
    # On cherche le chemin de moindre coût entre i et j, indépendamment
    # de la nature de la marchandise. Ce chemin sera ensuite utilisé pour
    # ventiler les volumes de TOUS les secteurs — c'est l'hypothèse centrale :
    # le routage physique ne dépend pas du type de marchandise, seulement
    # des coûts de transport (surface, pente, véhicule).
    # Cette hypothèse est cohérente avec la structure du modèle : les
    # différences sectorielles interviennent dans la GÉNÉRATION des flux
    # (via beta et profils d'offre/demande) mais pas dans le ROUTAGE.
    
    cols_j   <- j + (seq_len(n_vehicules) - 1) * n_warehouses
    min_cout <- min(dists_all[, cols_j], na.rm = TRUE)
    
    if (is.infinite(min_cout)) {
      paires_non_connectees <- paires_non_connectees + 1
      next
    }
    
    # Identification de la meilleure combinaison de couches véhicule
    best_idx_mat <- which(dists_all[, cols_j] == min_cout, arr.ind = TRUE)
    if (!is.matrix(best_idx_mat)) best_idx_mat <- matrix(best_idx_mat, nrow = 1)
    best_from <- sources_i[best_idx_mat[1, 1]]
    best_to   <- targets_all_global[cols_j[best_idx_mat[1, 2]]]
    
    # Reconstruction du chemin optimal (liste des arêtes empruntées)
    path_obj <- igraph::shortest_paths(
      graphe_multimodal,
      from    = best_from,
      to      = best_to,
      weights = poids_mm,
      output  = "epath"
    )
    edges_path_mm <- as.integer(path_obj$epath[[1]])
    rm(path_obj)
    
    if (length(edges_path_mm) == 0) {
      paires_non_connectees <- paires_non_connectees + 1
      next
    }
    
    # ── Identification des arêtes physiques valides sur ce chemin ────────────
    # On filtre les arêtes "route" (pas les transbordements entre véhicules)
    # et on récupère leur indice physique et leur véhicule associé.
    edges_valides <- edges_path_mm[edges_path_mm <= max_idx_mm]
    types_e       <- lookup_type[edges_valides]
    edges_routes  <- edges_valides[types_e == "route"]
    
    if (length(edges_routes) == 0) {
      paires_traitees <- paires_traitees + 1
      next
    }
    
    idx_phys_vec <- lookup_physique[edges_routes]
    veh_id_vec   <- lookup_vehicule[edges_routes]
    
    # On ne garde que les arêtes avec un indice physique et un véhicule valides
    valides <- idx_phys_vec >= 1 &
      idx_phys_vec <= n_aretes_physiques &
      veh_id_vec != ""
    
    if (!any(valides)) {
      paires_traitees <- paires_traitees + 1
      next
    }
    
    idx_phys_vec <- idx_phys_vec[valides]
    veh_id_vec   <- veh_id_vec[valides]
    col_veh_vec  <- match(veh_id_vec, VEHICULES_IDS$vehicule_id)
    
    # ── Ventilation sectorielle sur le chemin trouvé ─────────────────────────
    # Le chemin est le même pour tous les secteurs (hypothèse de routage unique).
    # On affecte maintenant le volume de CHAQUE secteur séparément sur ce chemin.
    # Cela permet de savoir, arête par arête, combien de tonnes d'Agriculture,
    # de Mines, d'Industrie, etc. y transitent — sans recalculer Dijkstra.
    for (s in SECTEURS) {
      
      # Volume en tonnes pour ce secteur entre i et j
      # flux_gravitaire[[s]] : matrice n_zones × n_zones des flux en M USD
      # TONNES_PAR_musd[s]   : facteur de conversion M USD → tonnes pour ce secteur
      flux_ij_s <- flux_gravitaire[[s]][i, j] * TONNES_PAR_musd[s]
      
      # Si le flux sectoriel est négligeable, on passe au secteur suivant
      # pour ne pas alourdir inutilement les calculs
      if (is.na(flux_ij_s) || flux_ij_s < 1) next
      
      # Affectation vectorisée : on ajoute le volume sectoriel à chaque arête
      # du chemin, dans la colonne correspondant au véhicule utilisé,
      # et dans la tranche correspondant au secteur s.
      # cbind() construit un index à 2 colonnes pour adresser la matrice 2D
      # (arête, véhicule) dans la tranche sectorielle.
      indices_2d <- cbind(idx_phys_vec, col_veh_vec)
      volume_trafic_mm_s[indices_2d, s] <-
        volume_trafic_mm_s[indices_2d, s] + flux_ij_s
    }
    
    paires_traitees <- paires_traitees + 1
  }
  
  # Nettoyage explicite de dists_all avant l'itération suivante
  rm(dists_all)
  
  # ### FIX : gc() à chaque itération pour éviter l'accumulation
  # Le coût de gc() est négligeable (~50-100ms) comparé à Dijkstra (~1-5s),
  # mais la libération continue évite les pics de RAM qui font crasher R.
  if (idx_i %% 5 == 0) {
    invisible(gc(verbose = FALSE))
  }
  
  pb_aff$tick()
}

# ── Reconstruction des matrices agrégées pour compatibilité avec la suite ──
# On recalcule les totaux par véhicule et le total global en sommant
# sur la dimension sectorielle (3ème dimension du tableau 3D).
# apply(X, c(1,2), sum) : pour chaque combinaison (arête, véhicule),
# somme sur tous les secteurs → redonne une matrice 2D comme avant.
volume_trafic_mm  <- apply(volume_trafic_mm_s, c(1, 2), sum)
volume_trafic     <- rowSums(volume_trafic_mm)

# ── Table sectorielle par arête pour export ───────────────────────────────
# On calcule aussi le volume total par secteur sur chaque arête,
# toutes couches véhicule confondues. C'est la donnée la plus utile
# pour les analyses sectorielles en aval (quelle route porte quel secteur ?).
# apply(X, c(1,3), sum) : pour chaque combinaison (arête, secteur),
# somme sur tous les véhicules → matrice n_aretes × N_SECTEURS
volume_par_secteur <- apply(volume_trafic_mm_s, c(1, 3), sum)
# On convertit en data.frame pour faciliter l'export CSV
volume_par_secteur_df <- as.data.frame(volume_par_secteur)
colnames(volume_par_secteur_df) <- paste0("vol_t_", SECTEURS)

cat("\n✓ Affectation multi-modale terminée\n")
cat("  Paires traitées      :", paires_traitees, "\n")
cat("  Paires non connectées:", paires_non_connectees, "\n\n")

# ── SAUVEGARDE DU CACHE ────────────────────────────────────────────────────
cat("=== Sauvegarde du cache d'affectation ===\n")

saveRDS(
  list(
    volume_trafic_mm      = volume_trafic_mm,
    paires_traitees       = paires_traitees,
    paires_non_connectees = paires_non_connectees,
    empreinte             = empreinte_entrees,
    date_creation         = Sys.time()
  ),
  CACHE_AFFECTATION
)

cat("  ✓ Cache sauvegardé :", CACHE_AFFECTATION, "\n")
cat("  → Au prochain lancement (sans changement), l'affectation sera ignorée\n\n")

}  # fin du if (!cache_affectation_valide)

# ══════════════════════════════════════════════════════════════════════════════
# SUITE : calculs rapides toujours exécutés (qu'on ait un cache ou non)
# ══════════════════════════════════════════════════════════════════════════════

# ── Statistiques de répartition modale ───────────────────────────────────────
cat("Répartition modale du trafic (tonnes × km) :\n")

longueurs_km <- reseau_rwanda %>%
  activate("edges") %>%
  as_tibble() %>%
  pull(length_km)

tkm_total <- sum(volume_trafic * longueurs_km, na.rm = TRUE)

for (v in seq_len(n_vehicules)) {
  veh_nom <- VEHICULES_IDS$nom[v]
  tkm_veh <- sum(volume_trafic_mm[, v] * longueurs_km, na.rm = TRUE)
  pct     <- if (tkm_total > 0) round(tkm_veh / tkm_total * 100, 1) else 0
  cat("  ", veh_nom, ":", format(round(tkm_veh), big.mark = " "),
      "t×km (", pct, "%)\n")
}
cat("\n")

# ── Étape 6 : Intégration des volumes au réseau ──────────────────────────────
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    volume_tonnes       = volume_trafic,
    volume_camionnette  = volume_trafic_mm[, "camionnette"],
    volume_camion_moyen = volume_trafic_mm[, "camion_moyen"],
    volume_camion_lourd = volume_trafic_mm[, "camion_lourd"],
    part_camion_lourd   = if_else(
      volume_tonnes > 0,
      round(volume_camion_lourd / volume_tonnes * 100, 1),
      0
    ),
    classe_trafic = case_when(
      volume_tonnes == 0     ~ "Aucun",
      volume_tonnes < 500    ~ "Très faible",
      volume_tonnes < 5000   ~ "Faible",
      volume_tonnes < 25000  ~ "Moyen",
      volume_tonnes < 100000 ~ "Élevé",
      TRUE                   ~ "Très élevé"
    ),
    classe_trafic = factor(classe_trafic,
                           levels = c("Aucun", "Très faible", "Faible",
                                      "Moyen", "Élevé", "Très élevé"))
  )

# Nettoyage final
invisible(gc(full = TRUE))
cat("✓ Partie VIII.1 terminée\n\n")

# ==============================================================================
# TRANSITION VIII.1 → VIII.2
# Recrée les objets de statistiques (volumes_par_zone, stats_trafic) qui
# avaient été supprimés du patch de VIII.1 et qui sont requis en VIII.2.
# ==============================================================================

# ── Statistiques de trafic sur le réseau ─────────────────────────────────────
stats_trafic <- reseau_rwanda %>%
  activate("edges") %>%
  as_tibble() %>%
  filter(volume_tonnes > 0) %>%
  summarise(
    n_aretes_actives = n(),
    volume_max_t     = max(volume_tonnes),
    volume_moyen_t   = mean(volume_tonnes),
    volume_median_t  = median(volume_tonnes)
  )

cat("Statistiques du trafic fret sur le réseau:\n")
cat("  Arêtes avec trafic  :",
    format(stats_trafic$n_aretes_actives, big.mark = " "), "\n")
cat("  Volume max (arête)  :",
    format(round(stats_trafic$volume_max_t), big.mark = " "), "tonnes\n")
cat("  Volume moyen (actif):",
    format(round(stats_trafic$volume_moyen_t), big.mark = " "), "tonnes\n\n")

# ── Zones les plus actives (origines + destinations cumulées) ────────────────
# volumes_par_zone est nécessaire en VIII.2 pour dimensionner les points
# des zones sur les cartes (taille_point ∝ log10(offre + demande)).
cat("Activité fret par zone (origines + destinations):\n")

volumes_par_zone <- tibble(
  Zone       = noeuds_entreposage$warehouse_name,
  Type       = noeuds_entreposage$warehouse_type,
  Offre_kt   = round(rowSums(flux_tonnes_total) / 1000, 1),
  Demande_kt = round(colSums(flux_tonnes_total) / 1000, 1)
) %>%
  mutate(Total_kt = Offre_kt + Demande_kt) %>%
  arrange(desc(Total_kt))

print(head(volumes_par_zone, 15))
cat("\n")

# ── Diagnostic de connectivité (optionnel mais utile pour debug) ─────────────
# Ce diagnostic reconstruit brièvement graphe_igraph pour vérifier la 
# connectivité des entrepôts. 

cat("=== Diagnostic de connectivité des entrepôts ===\n\n")

# Reconstruction LÉGÈRE du graphe igraph (on l'avait supprimé en VIII.1)
graphe_igraph <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(weight = cost_per_tkm * length_km) %>%
  as_tbl_graph()

warehouse_node_ids <- which(igraph::V(graphe_igraph)$is_warehouse)

composantes <- igraph::components(graphe_igraph)
cat("Nombre de composantes connexes:", composantes$no, "\n")
cat("Taille de la plus grande composante:", max(composantes$csize), "nœuds\n\n")

# On ne liste que les 10 premiers entrepôts pour ne pas polluer le log
cat("Composante de chaque entrepôt (10 premiers) :\n")
for (i in seq_len(min(10, length(warehouse_node_ids)))) {
  node_id <- warehouse_node_ids[i]
  comp    <- composantes$membership[node_id]
  taille  <- composantes$csize[comp]
  cat("  [", i, "]", noeuds_entreposage$warehouse_name[i],
      "→ composante", comp, "(", taille, "nœuds)\n")
}

# Libération du graphe igraph après diagnostic (on en a fini avec lui)
rm(graphe_igraph, composantes)
invisible(gc(verbose = FALSE))

cat("\n✓ Transition VIII.1 → VIII.2 terminée\n\n")

# ==============================================================================
# VIII.2 : Visualisations
# Génère 5 sorties graphiques : carte du trafic fret, carte de répartition
# modale, desire lines OD, graphiques sectoriels et heatmap de la matrice OD.
# ==============================================================================

# --- Préparation des couches spatiales ---

# Forcer les colonnes numériques explicitement
# rescale() (du package scales) : normalise une variable entre deux valeurs cibles.
# Ici, log10(volume) est mis à l'échelle entre 0.5 et 5 pour définir
# l'épaisseur des lignes sur la carte (lignes plus épaisses = trafic plus élevé).
aretes_fret <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  filter(volume_tonnes > 0) %>%
  mutate(
    volume_tonnes = as.numeric(volume_tonnes),
    volume_log    = as.numeric(log10(volume_tonnes + 1)),
    lwd_val       = as.numeric(rescale(log10(volume_tonnes + 1), to = c(0.5, 5)))
  )

# Coordonnées des zones
coords_zones_sf <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  st_as_sf()

# match() : associe chaque nœud d'entrepôt à son index dans noeuds_entreposage
# pour récupérer les volumes de trafic.
coords_zones_sf <- coords_zones_sf %>%
  mutate(match_idx = match(warehouse_name, noeuds_entreposage$warehouse_name)) %>%
  filter(!is.na(match_idx)) %>%
  arrange(match_idx)

# Taille des points proportionnelle au volume total de la zone (en scale log)
coords_zones_sf <- coords_zones_sf %>%
  mutate(
    offre_kt      = as.numeric(volumes_par_zone$Offre_kt[
      match(warehouse_name, volumes_par_zone$Zone)]),
    demande_kt    = as.numeric(volumes_par_zone$Demande_kt[
      match(warehouse_name, volumes_par_zone$Zone)]),
    total_kt      = offre_kt + demande_kt,
    taille_point  = as.numeric(rescale(log10(total_kt + 1), to = c(0.3, 1.8)))
  )

cat("✓ Couches préparées\n")
cat("  Arêtes fret actives:", nrow(aretes_fret), "\n")
cat("  Volume min:", round(min(aretes_fret$volume_tonnes)), "t\n")
cat("  Volume max:", round(max(aretes_fret$volume_tonnes)), "t\n\n")


# ============================================================
# CARTE 4 : Intensité du trafic fret sur le réseau routier
# tm_scale_continuous() au lieu de tm_scale_intervals()
# ============================================================

cat("Génération de la carte du trafic fret...\n")

# La largeur de ligne est proportionnelle au volume de trafic (échelle log).
# Les nœuds sont colorés selon le type de zone et dimensionnés selon le
# volume total généré/consommé.
carte_fret <- fond_carte() +
  
  # Réseau de base en gris très clair
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.3) +
  
  # Arêtes avec trafic
  tm_shape(aretes_fret) +
  tm_lines(
    col = "volume_tonnes",
    col.scale = tm_scale_intervals(style="quantile", n=4, values=PALETTE_FRET),
    col.legend = tm_legend(title = "Volume fret\n(tonnes)"),
    lwd = "lwd_val",
    lwd.scale = tm_scale(values.range = c(0.4, 5)),
    lwd.legend = tm_legend(show = FALSE)
  ) +
  
  # Points des zones
  tm_shape(coords_zones_sf) +
  tm_dots(
    fill = "warehouse_type",
    fill.scale = tm_scale(values = PALETTE_ZONE_TYPE),
    fill.legend = tm_legend(title = "Type de zone"),
    size = "taille_point",
    size.scale = tm_scale(values.range = c(0.3, 1.8)),
    size.legend = tm_legend(show = FALSE)
  ) +
  
  tm_title("Intensité du Trafic Fret\nModèle gravitaire - Rwanda") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position = c("right", "top"))

tmap_save(carte_fret,
          file.path(DIR_OUTPUT,"carte_trafic_fret.png"),
          width = 3000, height = 2400, dpi = 300)
cat("✓ Carte trafic fret sauvegardée\n")


# ── Carte : part du camion lourd par arête ────────────────────────────────────
aretes_avec_trafic <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  filter(volume_tonnes > 0)

carte_modal <- fond_carte() +
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.3) +
  tm_shape(aretes_avec_trafic) +
  tm_lines(
    col       = "part_camion_lourd",
    col.scale = tm_scale_intervals(
      style  = "fixed",
      breaks = c(0, 25, 50, 75, 100),
      values = c("#1A9850","#FEE090","#FC8D59","#D73027")
    ),
    col.legend = tm_legend(title = "Part camion\nlourd (%)"),
    lwd = 1.5
  ) +
  tm_title("Répartition modale — Part du camion lourd") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left","bottom")) +
  tm_compass(position  = c("right","top"))

tmap_save(carte_modal,
          file.path(DIR_OUTPUT,"carte_repartition_modale.png"),
          width = 3000, height = 2400, dpi = 300)
cat("✓ Carte répartition modale sauvegardée\n")


# ============================================================
# CARTE 5 : Flux OD (desire lines)
# ============================================================

cat("Génération de la carte des flux OD (desire lines)...\n")

SEUIL_DESIRE <- quantile(flux_total[flux_total > 0], 0.40)

# ── Pré-calcul des coordonnées (hors boucle) ──────────────────────────────────
# Évite les which() répétés dans la double boucle
coords_lookup <- coords_zones_sf %>%
  st_drop_geometry() %>%
  mutate(
    X = st_coordinates(coords_zones_sf)[, "X"],
    Y = st_coordinates(coords_zones_sf)[, "Y"]
  ) %>%
  select(warehouse_name, X, Y)

# Index nommé pour accès O(1) au lieu de which() O(n)
coords_X <- setNames(coords_lookup$X, coords_lookup$warehouse_name)
coords_Y <- setNames(coords_lookup$Y, coords_lookup$warehouse_name)

# ── Passage de la matrice de flux au format long (1 ligne = 1 paire OD) ───────
# La matrice flux_total_fixe est en format "large" : N_zones lignes × N_zones
# colonnes. pivot_longer() la transforme en format "long" : 1 ligne par paire
# (Origine, Destination), ce qui est bien plus facile à filtrer et à joindre.
# On filtre dès ici pour ne conserver que les flux significatifs
# (>= SEUIL_DESIRE, calculé comme le 40e percentile des flux non nuls) afin de
# ne pas surcharger la carte avec des milliers de lignes quasi invisibles.
flux_long <- flux_total %>%
  as.data.frame() %>%
  rownames_to_column("Origine") %>%        # Les noms de lignes deviennent une colonne
  pivot_longer(-Origine, names_to = "Destination", values_to = "flux_musd") %>%
  filter(
    Origine != Destination,                # Exclure les flux intrazone (diagonale)
    !is.na(flux_musd),                     # Supprimer les paires sans données
    flux_musd >= SEUIL_DESIRE              # Ne garder que les flux suffisamment importants
  ) %>%
  # Ajout des tonnes correspondantes depuis flux_tonnes_total.
  # On fait une jointure sur les deux clés (Origine, Destination) pour récupérer
  # le volume physique (tonnes) associé au flux monétaire (M USD) de chaque paire.
  left_join(
    flux_tonnes_total %>%
      as.data.frame() %>%
      rownames_to_column("Origine") %>%
      pivot_longer(-Origine, names_to = "Destination", values_to = "flux_tonnes"),
    by = c("Origine", "Destination")
  ) %>%
  mutate(flux_kt = flux_tonnes / 1000) %>% # Conversion tonnes → milliers de tonnes (kt)
  # Sécurité finale : ne garder que les paires dont les deux zones ont des
  # coordonnées connues dans coords_X/Y. Sans ce filtre, la boucle de création
  # des géométries produirait des points NA qui feraient planter st_sfc().
  filter(
    Origine     %in% names(coords_X),
    Destination %in% names(coords_X)
  )

cat("  Paires OD à représenter :", nrow(flux_long), "\n")

# ── Création des géométries de desire lines (une par paire OD) ────────────────
# Une "desire line" est un segment rectiligne reliant le centroïde de la zone
# d'origine au centroïde de la zone de destination. Elle représente visuellement
# l'intensité du flux commercial entre deux zones, indépendamment du chemin
# réellement emprunté sur le réseau routier.
# On utilise une boucle for plutôt que mapply() (version précédente) car mapply()
# allouait toutes les géométries simultanément en RAM, ce qui provoquait un crash
# de RStudio sur les sessions avec peu de mémoire disponible. La boucle for crée
# les géométries une par une : l'empreinte mémoire instantanée reste constante
# quelle que soit la taille de flux_long.
# vector("list", n) : initialise une liste vide de n éléments. C'est plus
# efficace que de faire grandir la liste dynamiquement avec c() à chaque tour
# de boucle, car R n'a pas besoin de réallouer la mémoire à chaque itération.
if (nrow(flux_long) > 0) {
  
  geoms <- vector("list", nrow(flux_long))
  
  for (k in seq_len(nrow(flux_long))) {
    ori <- flux_long$Origine[k]       # Identifiant de la zone d'origine
    dst <- flux_long$Destination[k]   # Identifiant de la zone de destination
    # st_linestring() crée un segment géométrique à partir de deux points.
    # rbind() empile les coordonnées [X_origine, Y_origine] et [X_dest, Y_dest]
    # en une matrice 2×2 que st_linestring() interprète comme début et fin du segment.
    geoms[[k]] <- st_linestring(rbind(
      c(coords_X[ori], coords_Y[ori]),   # Point de départ : centroïde de l'origine
      c(coords_X[dst], coords_Y[dst])    # Point d'arrivée : centroïde de la destination
    ))
  }
  
  desire_sf <- st_sf(
    Origine     = flux_long$Origine,
    Destination = flux_long$Destination,
    flux_musd   = as.numeric(flux_long$flux_musd),
    flux_kt     = as.numeric(flux_long$flux_kt),
    geometry    = st_sfc(geoms, crs = 32735)
  ) %>%
    mutate(flux_log = as.numeric(log10(flux_musd + 0.01)))
  
  cat("  Desire lines créées :", nrow(desire_sf), "\n")
  cat("  flux_musd range :", round(min(desire_sf$flux_musd), 3),
      "-", round(max(desire_sf$flux_musd), 3), "\n")
  
  # ── Carte (résolution réduite si trop lente) ──────────────────────────────
  carte_od <- fond_carte() +
    tm_shape(desire_sf) +
    tm_lines(
      col        = "flux_musd",
      col.scale  = tm_scale_intervals(style = "quantile", n = 5,
                                      values = PALETTE_FLUX_OD),
      col.legend = tm_legend(title = "Flux commercial\n(M USD)"),
      lwd        = "flux_log",
      lwd.scale  = tm_scale(values.range = c(0.5, 5)),
      lwd.legend = tm_legend(show = FALSE),
      col_alpha  = 0.65
    ) +
    tm_shape(coords_zones_sf) +
    tm_dots(
      fill        = "warehouse_type",
      fill.scale  = tm_scale(values = PALETTE_ZONE_TYPE),
      fill.legend = tm_legend(title = "Type de zone"),
      size        = 0.45
    ) +
    tm_title("Flux Commerciaux Interzonaux\nModèle gravitaire - Rwanda") +
    tm_layout(legend.outside = TRUE, frame = TRUE) +
    tm_scalebar(position = c("left", "bottom")) +
    tm_compass(position  = c("right", "top"))
  
  tmap_save(carte_od,
            file.path(DIR_OUTPUT, "carte_flux_od.png"),
            width = 2000, height = 1600, dpi = 200)  # Résolution réduite
  cat("✓ Carte flux OD sauvegardée\n")
  
} else {
  cat("⚠ Aucune desire line à représenter\n")
}


# ============================================================
# GRAPHIQUE 1 : Flux par secteur économique
# ============================================================

cat("Génération des graphiques statistiques...\n")

g1 <- flux_par_secteur_df %>%
  ggplot(aes(x = reorder(Secteur, Flux_total_musd),
             y = Flux_total_musd,
             fill = Secteur)) +
  geom_col(show.legend = FALSE, width = 0.75) +
  geom_text(aes(label = paste0(Flux_total_musd, " M$")),
            hjust = -0.1, size = 3.5, color = "#333333") +
  coord_flip(clip = "off") +
  scale_fill_brewer(palette = "Set2") +
  scale_y_continuous(expand = expansion(mult = c(0, 0.15))) +
  labs(
    title    = "Flux commerciaux interzonaux par secteur",
    subtitle = "Modèle gravitaire - Rwanda (données fictives réalistes)",
    x        = NULL,
    y        = "Flux total inter-zones (millions USD)"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    plot.title         = element_text(face = "bold", size = 15),
    plot.subtitle      = element_text(color = "#666666"),
    panel.grid.major.y = element_blank(),
    panel.grid.minor   = element_blank()
  )

ggsave(file.path(DIR_OUTPUT,"graphique_flux_secteurs.png"),
       g1, width = 11, height = 6, dpi = 300)
cat("✓ Graphique flux secteurs sauvegardé\n")


# ============================================================
# GRAPHIQUE 2 : Offre vs Demande par zone
# ============================================================

g2 <- recap_zones %>%
  pivot_longer(
    cols      = c(offre_totale_musd, demande_totale_musd),
    names_to  = "Type_flux",
    values_to = "Valeur"
  ) %>%
  mutate(
    Zone_court = str_trunc(zone, 28),
    Type_flux  = recode(Type_flux,
                        "offre_totale_musd"   = "offre",
                        "demande_totale_musd" = "demande")
  ) %>%
  ggplot(aes(x = reorder(Zone_court, Valeur),
             y = Valeur,
             fill = Type_flux)) +
  geom_col(position = "dodge", width = 0.7) +
  coord_flip() +
  scale_fill_manual(values = c("offre" = "#1976D2", "demande" = "#D32F2F")) +
  labs(
    title    = "Offre et Demande par zone économique",
    subtitle = "Modèle gravitaire - Rwanda (données fictives réalistes)",
    x        = NULL,
    y        = "Valeur (millions USD)",
    fill     = NULL
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title         = element_text(face = "bold", size = 14),
    plot.subtitle      = element_text(color = "#666666"),
    legend.position    = "top",
    panel.grid.major.y = element_blank()
  )

ggsave(file.path(DIR_OUTPUT,"graphique_offre_demande.png"),
       g2, width = 13, height = 8, dpi = 300)
cat("✓ Graphique offre/demande sauvegardé\n")


# ============================================================
# GRAPHIQUE 3 : Heatmap de la matrice OD
# Noms courts uniques via make.unique()
# ============================================================

noms_courts_raw <- noeuds_entreposage$warehouse_name %>%
  str_remove(" - .*") %>%
  str_remove(" \\(.*") %>%
  str_trunc(18)

noms_courts <- make.unique(noms_courts_raw, sep = "_")  # Kigali, Kigali_1, Kigali_2

flux_heatmap <- flux_total %>%
  as.data.frame() %>%
  setNames(noms_courts) %>%
  mutate(Origine = noms_courts) %>%
  pivot_longer(-Origine, names_to = "Destination", values_to = "Flux") %>%
  mutate(
    Flux_log    = ifelse(Flux > 0, log10(Flux), NA),
    Origine     = factor(Origine,     levels = rev(noms_courts)),
    Destination = factor(Destination, levels = noms_courts)
  )

g3 <- ggplot(flux_heatmap,
             aes(x = Destination, y = Origine, fill = Flux_log)) +
  geom_tile(color = "white", linewidth = 0.4) +
  scale_fill_gradient(
    low      = "#FFF7EC",
    high     = "#7F0000",
    na.value = "#F5F5F5",
    name     = "log₁₀\n(M USD)"
  ) +
  labs(
    title    = "Matrice des flux commerciaux interzonaux",
    subtitle = "Modèle gravitaire - Rwanda (log₁₀ M USD)",
    x        = "Destination",
    y        = "Origine"
  ) +
  theme_minimal(base_size = 10) +
  theme(
    axis.text.x     = element_text(angle = 45, hjust = 1, size = 8),
    axis.text.y     = element_text(size = 8),
    plot.title      = element_text(face = "bold", size = 13),
    plot.subtitle   = element_text(color = "#666666"),
    panel.grid      = element_blank(),
    legend.position = "right"
  )

ggsave(file.path(DIR_OUTPUT,"heatmap_flux_od.png"),
       g3, width = 13, height = 11, dpi = 300)
cat("✓ Heatmap flux OD sauvegardée\n")


# ============================================================
# GRAPHIQUE 4 : Composition sectorielle des flux par zone
# ============================================================

offre_long <- as.data.frame(offre_zones) %>%
  rownames_to_column("Zone") %>%
  pivot_longer(-Zone, names_to = "Secteur", values_to = "Offre_musd") %>%
  mutate(Zone_court = str_trunc(str_remove(Zone, " - .*"), 22))

g4 <- offre_long %>%
  group_by(Zone_court) %>%
  mutate(Part_pct = Offre_musd / sum(Offre_musd) * 100) %>%
  ungroup() %>%
  ggplot(aes(x = reorder(Zone_court, -Offre_musd),
             y = Part_pct,
             fill = Secteur)) +
  geom_col(width = 0.8) +
  coord_flip() +
  scale_fill_brewer(palette = "Set2") +
  scale_y_continuous(labels = scales::percent_format(scale = 1)) +
  labs(
    title    = "Composition sectorielle de l'offre par zone",
    subtitle = "Modèle gravitaire - Rwanda",
    x        = NULL,
    y        = "Part dans l'offre totale (%)",
    fill     = "Secteur"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title      = element_text(face = "bold", size = 13),
    legend.position = "right"
  )

ggsave(file.path(DIR_OUTPUT,"graphique_composition_sectorielle.png"),
       g4, width = 14, height = 8, dpi = 300)
cat("✓ Graphique composition sectorielle sauvegardé\n\n")


# ==============================================================================
# VIII.3 : Exports finaux
# Exporte toutes les matrices (flux M USD, flux tonnes, offre/demande, IO)
# en CSV et le réseau complet avec volumes fret en GeoPackage.
# ==============================================================================

cat("Export des données du modèle de fret...\n")

write.csv(as.data.frame(A),
          file.path(DIR_OUTPUT,"table_io_coefficients.csv"),
          row.names = TRUE)
write.csv(recap_io,
          file.path(DIR_OUTPUT,"table_io_recap.csv"),
          row.names = FALSE)
write.csv(as.data.frame(flux_total) %>% rownames_to_column("Zone"),
          file.path(DIR_OUTPUT,"matrice_flux_gravitaire_musd.csv"),
          row.names = FALSE)
write.csv(as.data.frame(flux_tonnes_total) %>% rownames_to_column("Zone"),
          file.path(DIR_OUTPUT,"matrice_flux_fret_tonnes.csv"),
          row.names = FALSE)
write.csv(recap_zones,
          file.path(DIR_OUTPUT,"offre_demande_zones.csv"),
          row.names = FALSE)

# ── Export complémentaire : réseau avec volumes fret ─────────────────────────
aretes_fret_export <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  select(osm_id, name, road_type, surface,
         length_km, speed_kmh, cost_per_tkm,
         volume_tonnes, classe_trafic,
         volume_camionnette, volume_camion_moyen, volume_camion_lourd,
         part_camion_lourd)

st_write(aretes_fret_export,
         file.path(DIR_OUTPUT, "reseau_rwanda_avec_fret.gpkg"),
         delete_dsn = TRUE, quiet = TRUE)
cat("✓ GeoPackage avec volumes fret exporté\n")

# Export du détail sectoriel par arête
# Ce fichier permet de répondre à des questions comme :
# "Quelle part du trafic sur la RN1 est de l'Agriculture ?"
aretes_fret_sectoriel <- aretes_fret_export %>%
  st_drop_geometry() %>%
  bind_cols(volume_par_secteur_df)

dbExecute(con, paste0(
  "COPY (SELECT * FROM aretes_fret_sectoriel) TO '",
  file.path(DIR_OUTPUT, "volumes_fret_par_secteur.csv"),
  "' (FORMAT CSV, HEADER)"
))
cat("✓ Export sectoriel par arête sauvegardé\n")

################################################################################
# PARTIE IX — ANALYSE DE VULNÉRABILITÉ ET DE CONTOURNEMENT
#
# OBJECTIF : Simuler la suppression d'une ou plusieurs arêtes du réseau
#            (routes inondées, glissements de terrain, etc.) et mesurer
#            l'impact sur les coûts de transport entre toutes les paires OD.
#
# STRUCTURE :
#   IX.1 — Chargement des perturbations (manuel ou raster de risque)
#   IX.2 — Identification des arêtes perturbées
#   IX.3 — Recalcul de la matrice OD sur le réseau dégradé
#   IX.4 — Calcul des surcoûts et classification des impacts
#   IX.5 — Identification des arêtes critiques (analyse de sensibilité)
#   IX.6 — Cartes et exports
#
# DESIGN POUR L'AVENIR :
#   Ce script est conçu pour accepter, à terme, des cartes raster de zones
#   inondables (ex : sorties de modèles hydrologiques HEC-RAS, HAND, JRC)
#   ou de glissements de terrain (ex : données NASA LSAF, susceptibilité USGS).
#   Pour l'instant, trois modes de définition des perturbations sont disponibles :
#     Mode A — Manuel       : liste d'osm_id ou de coordonnées GPS fournie à la main
#     Mode B — Buffer zone  : toutes les routes dans un rayon autour d'un point
#     Mode C — Raster risque: (PRÊT À BRANCHER) intersection avec un raster externe
#
# DÉPENDANCES :
#   Ce bloc dépend des objets construits dans les Parties I à VIII :
#     reseau_rwanda      — réseau sfnetworks avec coûts et volumes
#     graphe_multimodal  — graphe igraph multi-modal (3 couches véhicules)
#     od_long            — matrice OD de référence (avant perturbation)
#     noeuds_entreposage — liste et indices des zones d'entrepôt
#     warehouse_nodes_base — indices des entrepôts dans le graphe de base
#     VEHICULES_IDS      — tableau des types de véhicules
#     node_multi()       — fonction de remappage multi-modal
#     fond_carte()       — fonction de fond de carte réutilisable
#     DIR_OUTPUT         — dossier de sortie
################################################################################


################################################################################
# PARTIE IX.1 — DÉFINITION DE LA PERTURBATION
#
# Cette section centralise TOUT ce que l'utilisateur doit modifier pour
# changer de scénario. Il ne devrait pas avoir besoin de toucher au reste
# du code pour tester un nouvel événement climatique.
#
# TROIS MODES sont disponibles. Activez-en UN SEUL à la fois en mettant
# les deux autres en commentaire (en ajoutant # au début de chaque ligne).
################################################################################

cat("==========================================================\n")
cat("  PARTIE IX — ANALYSE DE VULNÉRABILITÉ ET CONTOURNEMENT\n")
cat("==========================================================\n\n")

# ==============================================================================
# IX.1.A : Mode Manuel — liste d'identifiants OSM ou de noms de routes
# À utiliser quand on connaît précisément quelle route est affectée.
# Un osm_id est l'identifiant unique d'un segment dans OpenStreetMap.
# Vous pouvez le trouver en cliquant sur une route dans OpenStreetMap.org.
# ==============================================================================

# ── Paramètres du scénario ────────────────────────────────────────────────────
# Modifiez ces valeurs pour changer de scénario sans toucher au reste du code.

NOM_SCENARIO       <- "Inondation_RN1_Kigali_Huye"  # Nom court, sans espaces (utilisé dans les noms de fichiers)
DESCRIPTION_SCENARIO <- "Rupture de la RN1 entre Kigali et Huye suite à une inondation"
DUREE_JOURS        <- 14        # Durée estimée de la perturbation (en jours)
TYPE_EVENEMENT     <- "inondation"  # "inondation", "glissement", "pont_effondre", etc.

# Identifiants OSM des segments à supprimer.
# Laissez vide (c()) si vous utilisez le Mode B ou C à la place.
# Pour trouver un osm_id : allez sur openstreetmap.org, cliquez sur une route,
# cliquez sur "Plus de détails" → l'identifiant apparaît dans l'URL.
OSM_IDS_PERTURBES_MANUEL <- c()  # Exemple : c("12345678", "87654321")

# ==============================================================================
# IX.1.B : Mode Buffer — toutes les routes dans un rayon autour d'un point GPS
# À utiliser pour simuler un événement localisé (ex : pont effondré, zone
# inondée d'environ X km²) quand on ne connaît pas les osm_id précis.
# ==============================================================================

# Activez ce mode en passant UTILISER_MODE_BUFFER à TRUE.
# Les coordonnées sont en degrés décimaux (système GPS standard).
UTILISER_MODE_BUFFER <- TRUE    # TRUE = activer ce mode, FALSE = désactiver

# Centre de la zone perturbée (longitude, latitude en WGS84)
CENTRE_PERTURBATION_LON <- 29.950   # Longitude du centre (Est-Ouest)
CENTRE_PERTURBATION_LAT <- -2.150   # Latitude du centre (Nord-Sud, négatif = Sud)

# Rayon de la zone perturbée en mètres.
# 5 000m = zone de 5km de rayon ≈ surface d'environ 78 km²
RAYON_PERTURBATION_M    <- 5000

# Types de routes inclus dans la perturbation (NULL = tous les types).
# Utile pour simuler une inondation qui ne touche que les routes en fond de vallée
# (souvent des routes secondaires et tertiaires) mais pas les routes en hauteur.
# NULL signifie "toutes les routes" ; pour restreindre, utilisez par exemple :
# c("primary", "secondary") pour ne perturber que les routes primaires et secondaires.
TYPES_ROUTES_PERTURBES  <- NULL  # NULL = tous les types, ou c("primary","secondary",...)

# ==============================================================================
# IX.1.C : Mode Raster — intersection avec un raster de risque externe
# PRÊT À BRANCHER : cette section est préparée pour accueillir un raster
# de zones inondables (GeoTIFF) dès que vous en disposez.
# Le raster doit contenir des valeurs de risque (ex : profondeur d'eau en cm,
# probabilité d'inondation en %, susceptibilité de glissement 0-1).
# ==============================================================================

# Activez ce mode en passant UTILISER_MODE_RASTER à TRUE.
# ATTENTION : si Mode Buffer ET Mode Raster sont tous les deux TRUE,
# les deux sources sont combinées (union des routes perturbées).
UTILISER_MODE_RASTER    <- FALSE   # TRUE = activer, FALSE = désactiver (futur)

# Chemin vers le fichier raster de risque (GeoTIFF ou format terra-compatible).
# Exemples de sources de données :
#   - JRC Global Surface Water  : https://global-surface-water.appspot.com/
#   - HAND (Height Above Nearest Drainage) : https://www.earthenv.org/
#   - NASA LSAF (glissements)   : https://pmm.nasa.gov/landslides
#   - Modèles hydrologiques locaux (HEC-RAS, LISFLOOD-FP, etc.)
CHEMIN_RASTER_RISQUE    <- "data/raw/zones_inondables_rwanda.tif"  # À modifier

# Seuil au-dessus duquel une route est considérée comme perturbée.
# Si le raster contient des probabilités (0-1) : un seuil de 0.5 signifie
# "routes dont plus de 50% de leur longueur est dans une zone à risque > 50%".
# Si le raster contient des profondeurs (cm) : un seuil de 30 signifie
# "routes submergées sous plus de 30cm d'eau" (seuil de praticabilité standard).
SEUIL_RISQUE_RASTER     <- 0.5

# Proportion minimale de la longueur d'une arête qui doit être en zone à risque
# pour que l'arête soit considérée comme perturbée.
# 0.3 = au moins 30% de la route doit être en zone inondable.
PROPORTION_MIN_EXPOSEE  <- 0.3

cat("✓ Paramètres du scénario chargés :\n")
cat("  Nom          :", NOM_SCENARIO, "\n")
cat("  Description  :", DESCRIPTION_SCENARIO, "\n")
cat("  Durée        :", DUREE_JOURS, "jours\n")
cat("  Type         :", TYPE_EVENEMENT, "\n")
cat("  Mode buffer  :", UTILISER_MODE_BUFFER, "\n")
cat("  Mode raster  :", UTILISER_MODE_RASTER, "\n\n")


################################################################################
# PARTIE IX.2 — IDENTIFICATION DES ARÊTES PERTURBÉES
#
# Cette section traduit les paramètres de IX.1 en une liste concrète
# d'identifiants d'arêtes dans le graphe igraph/sfnetworks.
# C'est ici que les trois modes sont fusionnés en un seul ensemble d'arêtes.
################################################################################

cat("── Identification des arêtes perturbées ──────────────────────────────\n\n")

# On commence avec un ensemble vide d'osm_id perturbés.
# character(0) est un vecteur de chaînes de caractères vide en R.
osm_ids_perturbes <- character(0)

# ── Mode A : identifiants manuels ─────────────────────────────────────────────
if (length(OSM_IDS_PERTURBES_MANUEL) > 0) {
  # as.character() : s'assure que les identifiants sont des chaînes de texte
  # (les osm_id peuvent parfois être chargés comme des entiers numériques)
  osm_ids_perturbes <- union(osm_ids_perturbes,
                             as.character(OSM_IDS_PERTURBES_MANUEL))
  cat("  Mode A (manuel) :", length(OSM_IDS_PERTURBES_MANUEL),
      "osm_id fournis\n")
}

# ── Mode B : buffer géographique ─────────────────────────────────────────────
if (UTILISER_MODE_BUFFER) {
  
  # Création du point central de la perturbation en WGS84 (GPS)
  # puis reprojection en UTM 35S (même CRS que le réseau) pour que
  # le buffer soit exprimé en mètres et non en degrés.
  point_perturbation <- st_sfc(
    st_point(c(CENTRE_PERTURBATION_LON, CENTRE_PERTURBATION_LAT)),
    crs = 4326    # WGS84 = système GPS standard
  ) %>%
    st_transform(crs = 32735)    # UTM Zone 35S = système métrique Rwanda
  
  # st_buffer() crée un cercle de rayon RAYON_PERTURBATION_M autour du point.
  # Ce cercle représente la zone géographique affectée par l'événement.
  zone_perturbation_buffer <- st_buffer(point_perturbation,
                                        dist = RAYON_PERTURBATION_M)
  
  # Récupération de toutes les arêtes du réseau sous forme sf
  aretes_sf_mode_b <- reseau_rwanda %>%
    activate("edges") %>%
    st_as_sf()
  
  # Filtrage optionnel par type de route
  if (!is.null(TYPES_ROUTES_PERTURBES)) {
    aretes_sf_mode_b <- aretes_sf_mode_b %>%
      filter(road_type %in% TYPES_ROUTES_PERTURBES)
  }
  
  # st_intersects() : teste si chaque arête croise la zone de perturbation.
  # lengths() > 0 : TRUE si l'arête intersecte au moins partiellement la zone.
  # On utilise l'intersection (et non l'inclusion totale) car une route peut
  # n'être que partiellement dans la zone inondée mais quand même bloquée.
  dans_buffer <- lengths(st_intersects(aretes_sf_mode_b,
                                       zone_perturbation_buffer)) > 0
  
  # Récupération des osm_id des arêtes dans le buffer
  # as.character() : conversion en texte pour la cohérence avec les autres modes
  ids_mode_b <- as.character(aretes_sf_mode_b$osm_id[dans_buffer])
  ids_mode_b <- ids_mode_b[!is.na(ids_mode_b)]  # Supprimer les NA éventuels
  
  # union() fusionne deux vecteurs sans doublons
  osm_ids_perturbes <- union(osm_ids_perturbes, ids_mode_b)
  
  cat("  Mode B (buffer", RAYON_PERTURBATION_M / 1000, "km) :",
      length(ids_mode_b), "arêtes dans la zone\n")
  cat("    Centre :", CENTRE_PERTURBATION_LON, "E /",
      abs(CENTRE_PERTURBATION_LAT), "S\n")
}

# ── Mode C : raster de risque (PRÊT À BRANCHER) ───────────────────────────────
# Ce bloc est entièrement préparé mais ne s'exécutera que si :
#   1. UTILISER_MODE_RASTER == TRUE
#   2. Le fichier CHEMIN_RASTER_RISQUE existe sur le disque
# Pour l'activer : mettre UTILISER_MODE_RASTER <- TRUE en IX.1.C
# et fournir un fichier GeoTIFF valide.
if (UTILISER_MODE_RASTER) {
  
  if (!file.exists(CHEMIN_RASTER_RISQUE)) {
    # warning() affiche un message d'avertissement sans arrêter le script
    # (contrairement à stop() qui arrêterait l'exécution)
    warning("  ⚠ Mode C activé mais fichier raster introuvable : ",
            CHEMIN_RASTER_RISQUE, "\n  Mode C ignoré.\n")
  } else {
    
    cat("  Mode C (raster) : chargement de", CHEMIN_RASTER_RISQUE, "...\n")
    
    # Chargement du raster de risque avec terra
    raster_risque <- rast(CHEMIN_RASTER_RISQUE)
    
    # Reprojection en UTM 35S pour cohérence avec le réseau routier
    raster_risque <- project(raster_risque, "EPSG:32735", method = "bilinear")
    
    # Récupération des arêtes du réseau
    aretes_sf_mode_c <- reseau_rwanda %>%
      activate("edges") %>%
      st_as_sf() %>%
      mutate(arete_idx_c = row_number())
    
    # Pour chaque arête, on calcule la proportion de sa longueur
    # qui se trouve dans une zone à risque supérieur au seuil.
    #
    # Approche :
    #   1. Échantillonner des points le long de chaque arête (tous les 100m)
    #   2. Extraire la valeur du raster à chaque point
    #   3. Calculer la proportion de points avec valeur > SEUIL_RISQUE_RASTER
    #   4. Si proportion > PROPORTION_MIN_EXPOSEE → arête perturbée
    
    cat("    Échantillonnage des points le long des arêtes...\n")
    
    # calculer_exposition_raster() : fonction qui calcule pour une arête donnée
    # la proportion de sa longueur exposée au risque selon le raster.
    calculer_exposition_raster <- function(ligne_geom, raster, seuil, espacement = 100) {
      
      longueur <- as.numeric(st_length(ligne_geom))
      
      # Au moins 2 points ; plus de points pour les longues arêtes
      n_pts <- max(2, floor(longueur / espacement))
      
      pts <- st_line_sample(ligne_geom, n = n_pts, type = "regular") %>%
        st_cast("POINT")
      
      # terra::extract() : lit la valeur du raster à chaque point
      valeurs <- terra::extract(raster, vect(pts))[, 2]
      
      # proportion de points dépassant le seuil de risque
      if (all(is.na(valeurs))) return(0)
      mean(valeurs > seuil, na.rm = TRUE)
    }
    
    # Application à toutes les arêtes avec barre de progression
    n_aretes_c <- nrow(aretes_sf_mode_c)
    proportions_exposees <- numeric(n_aretes_c)
    
    pb_raster <- progress_bar$new(
      format = "  Raster exposition [:bar] :percent | ETA: :eta",
      total  = n_aretes_c, clear = FALSE, width = 60
    )
    
    for (i in seq_len(n_aretes_c)) {
      proportions_exposees[i] <- calculer_exposition_raster(
        aretes_sf_mode_c$geometry[i],
        raster_risque,
        SEUIL_RISQUE_RASTER
      )
      pb_raster$tick()
    }
    
    # Sélection des arêtes suffisamment exposées
    ids_mode_c <- as.character(
      aretes_sf_mode_c$osm_id[proportions_exposees >= PROPORTION_MIN_EXPOSEE]
    )
    ids_mode_c <- ids_mode_c[!is.na(ids_mode_c)]
    
    osm_ids_perturbes <- union(osm_ids_perturbes, ids_mode_c)
    
    cat("  Mode C (raster, seuil", SEUIL_RISQUE_RASTER, ") :",
        length(ids_mode_c), "arêtes exposées\n")
  }
}

# ── Bilan : arêtes effectivement perturbées ────────────────────────────────────
# On traduit maintenant les osm_id en indices d'arêtes dans le graphe igraph.
# Ce sont ces indices qui seront utilisés pour supprimer les arêtes.
aretes_reseau_sf <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  mutate(arete_idx = row_number())

# match() : pour chaque osm_id perturbé, trouve son indice de ligne dans le réseau
# !is.na() : supprime les osm_id non trouvés (hors réseau, déjà supprimés, etc.)
indices_aretes_perturbees <- aretes_reseau_sf$arete_idx[
  aretes_reseau_sf$osm_id %in% osm_ids_perturbes
]
indices_aretes_perturbees <- indices_aretes_perturbees[
  !is.na(indices_aretes_perturbees)
]

n_perturb <- length(indices_aretes_perturbees)

if (n_perturb == 0) {
  # Si aucune arête n'est trouvée, on arrête avec un message explicatif
  stop("⚠ Aucune arête perturbée identifiée. Vérifiez les paramètres du scénario.\n",
       "  → Mode Buffer : les coordonnées GPS sont-elles dans le Rwanda ?\n",
       "  → Mode Manuel : les osm_id existent-ils dans le réseau ?\n",
       "  → Mode Raster : le seuil est-il trop élevé ?\n")
}

cat("\n✓ Arêtes perturbées identifiées :", n_perturb, "\n")

# Synthèse attributaire des arêtes perturbées (pour le rapport)
resume_perturb <- aretes_reseau_sf %>%
  filter(arete_idx %in% indices_aretes_perturbees) %>%
  st_drop_geometry() %>%
  summarise(
    n_aretes         = n(),
    longueur_km      = round(sum(longueur_m, na.rm = TRUE) / 1000, 1),
    pct_du_reseau    = round(n() / nrow(aretes_reseau_sf) * 100, 2),
    road_types       = paste(sort(unique(road_type)), collapse = ", "),
    surfaces         = paste(sort(unique(surface)),   collapse = ", ")
  )

cat("  Longueur totale   :", resume_perturb$longueur_km, "km\n")
cat("  Part du réseau    :", resume_perturb$pct_du_reseau, "%\n")
cat("  Types de routes   :", resume_perturb$road_types, "\n")
cat("  Surfaces          :", resume_perturb$surfaces, "\n\n")


################################################################################
# PARTIE IX.3 — RECALCUL DE LA MATRICE OD SUR LE RÉSEAU DÉGRADÉ
#
# On reconstruit le graphe multi-modal en retirant les arêtes perturbées,
# puis on recalcule les distances OD optimales sur ce réseau dégradé.
# La comparaison avant/après donne les surcoûts de transport.
#
# NOTE SUR LA STRATÉGIE DE SUPPRESSION :
#   On ne supprime pas physiquement les arêtes du réseau sfnetworks
#   (ce serait difficile à annuler proprement). À la place, on leur donne
#   un poids infini dans igraph : Dijkstra ne les empruntera jamais,
#   ce qui revient à les supprimer logiquement du réseau.
#   Pour restaurer le réseau, il suffit de remettre les poids d'origine.
################################################################################

cat("── Reconstruction du graphe dégradé ──────────────────────────────────\n\n")

# ── Récupération des poids originaux du graphe multi-modal ────────────────────
# igraph::E() : accède aux arêtes (edges) du graphe.
# $weight : attribut "weight" de chaque arête (coût de transport en USD).
poids_originaux <- igraph::E(graphe_multimodal)$weight

# On travaille sur une COPIE du graphe multi-modal pour ne pas altérer l'original.
# Le graphe original (graphe_multimodal) reste intact et servira de référence.
graphe_degrade <- graphe_multimodal

# ── Mise à l'infini des arêtes perturbées dans TOUTES les couches véhicule ────
# Dans le graphe multi-modal, chaque arête physique existe en N_vehicules
# exemplaires (une par couche). On doit bloquer l'arête dans TOUTES les couches.
#
# La correspondance entre arête physique (indice 1..n_aretes) et arêtes
# multi-modales (une par couche) est donnée par le vecteur lookup construit en V.2.

# Toutes les arêtes multi-modales de type "route" (pas de transbordement)
# dont l'indice physique est dans la liste des arêtes perturbées
indices_mm_perturbes <- which(
  lookup_type     == "route" &
    lookup_physique %in% indices_aretes_perturbees
)

cat("  Arêtes multi-modales à bloquer :", length(indices_mm_perturbes),
    "(", n_perturb, "arêtes physiques ×", n_vehicules, "couches véhicules)\n")

# Attribution d'un poids infini aux arêtes perturbées.
# Inf en R est la valeur "infini" — Dijkstra ne traversera jamais une arête
# de poids infini car il existerait toujours un chemin de moindre coût.
# C'est mathématiquement équivalent à supprimer les arêtes du graphe.
igraph::E(graphe_degrade)$weight[indices_mm_perturbes] <- Inf

cat("  ✓ Graphe dégradé construit (arêtes bloquées avec poids = Inf)\n\n")

# ── Recalcul de la matrice OD sur le réseau dégradé ───────────────────────────
cat("  Recalcul des distances OD sur le réseau dégradé...\n")

# On stocke les résultats dans une liste, puis on l'assemble en data.frame.
# La structure est identique à od_long (Partie VI) pour faciliter la comparaison.
od_rows_degrade <- list()
idx_deg         <- 0

for (i in seq_along(warehouse_nodes_base)) {
  
  sources_i <- sapply(seq_len(n_vehicules),
                      function(v) node_multi(v, warehouse_nodes_base[i]))
  
  targets_all <- as.vector(sapply(
    seq_len(n_vehicules),
    function(v) node_multi(v, warehouse_nodes_base)
  ))
  
  # Calcul des distances depuis l'entrepôt i vers tous les autres entrepôts
  # dans le graphe DÉGRADÉ (routes bloquées = poids infini).
  # La syntaxe est identique au Dijkstra de la Partie VI, seul le graphe change.
  dists_deg <- igraph::distances(
    graphe_degrade,             # ← graphe dégradé au lieu de graphe_multimodal
    v       = sources_i,
    to      = targets_all,
    weights = igraph::E(graphe_degrade)$weight
  )
  
  for (j in seq_along(warehouse_nodes_base)) {
    if (i == j) next
    
    cols_j      <- j + (seq_len(n_vehicules) - 1) * length(warehouse_nodes_base)
    min_cout_deg <- min(dists_deg[, cols_j], na.rm = TRUE)
    
    idx_deg <- idx_deg + 1
    od_rows_degrade[[idx_deg]] <- list(
      id_origine      = i,
      id_destination  = j,
      nom_origine     = noeuds_entreposage$warehouse_name[i],
      nom_destination = noeuds_entreposage$warehouse_name[j],
      cout_degrade    = min_cout_deg,   # Inf si plus de chemin possible
      connecte        = !is.infinite(min_cout_deg)  # FALSE = zones déconnectées
    )
  }
  
  if (i %% 5 == 0 || i == length(warehouse_nodes_base))
    cat("  OD dégradé :", round(i / length(warehouse_nodes_base) * 100, 1), "%\n")
}

od_degrade <- bind_rows(od_rows_degrade)

cat("✓ Matrice OD dégradée calculée\n\n")


################################################################################
# PARTIE IX.4 — CALCUL DES SURCOÛTS ET CLASSIFICATION DES IMPACTS
#
# On compare les deux matrices OD (avant / après perturbation) pour calculer :
#   - Le surcoût absolu (USD supplémentaires par trajet)
#   - Le surcoût relatif (% d'augmentation)
#   - Le type d'impact (détour, déconnexion, inchangé)
#   - Les zones les plus touchées en cumulant leurs surcoûts
################################################################################

cat("── Calcul des surcoûts ────────────────────────────────────────────────\n\n")

# ── Fusion des deux matrices OD (référence + dégradée) ────────────────────────
# left_join() : pour chaque paire OD dans la matrice de référence, on récupère
# le coût dégradé correspondant. Les colonnes by = sont les clés de jointure.
od_compare <- od_long %>%
  left_join(
    od_degrade %>%
      select(id_origine, id_destination, cout_degrade, connecte),
    by = c("id_origine", "id_destination")
  ) %>%
  mutate(
    
    # Surcoût absolu : différence de coût entre la situation dégradée et normale.
    # Si la route est coupée (cout_degrade = Inf), le surcoût est NA
    # (on le traite séparément dans la variable "type_impact").
    surcout_absolu_usd  = if_else(
      connecte,
      cout_degrade - cout_usd,
      NA_real_
    ),
    
    # Surcoût relatif : augmentation en % par rapport au coût de référence.
    # NULLIF équivalent en R : on évite la division par zéro si cout_usd = 0.
    surcout_relatif_pct = if_else(
      connecte & cout_usd > 0,
      round((cout_degrade - cout_usd) / cout_usd * 100, 1),
      NA_real_
    ),
    
    # Classification du type d'impact pour chaque paire OD.
    # case_when() : équivalent R de if / else if / else.
    # L'ordre des conditions compte : la première condition vraie est retenue.
    type_impact = case_when(
      is.na(connecte) | !connecte   ~ "deconnecte",   # Plus aucun chemin possible
      surcout_absolu_usd  == 0      ~ "inchange",     # Le chemin optimal ne passe pas par la zone perturbée
      surcout_relatif_pct < 10      ~ "faible",       # Détour minime (< 10%)
      surcout_relatif_pct < 50      ~ "modere",       # Détour notable (10-50%)
      surcout_relatif_pct < 100     ~ "fort",         # Détour majeur (50-100%)
      TRUE                          ~ "tres_fort"     # Doublement ou plus du coût
    ),
    
    # Conversion de type_impact en facteur ordonné pour les graphiques
    type_impact = factor(
      type_impact,
      levels = c("inchange", "faible", "modere", "fort", "tres_fort", "deconnecte")
    )
  )

# ── Sauvegarde dans DuckDB ────────────────────────────────────────────────────
# On stocke la table de comparaison dans DuckDB pour des requêtes SQL ultérieures.
# Le nom de la table inclut le nom du scénario pour permettre de stocker
# plusieurs scénarios simultanément.
nom_table_impact <- paste0("impact_", NOM_SCENARIO)
duck_write(od_compare, nom_table_impact)

# ── Statistiques globales d'impact ────────────────────────────────────────────
stats_impact <- od_compare %>%
  group_by(type_impact) %>%
  summarise(
    n_paires       = n(),
    pct_paires     = round(n() / nrow(od_compare) * 100, 1),
    surcout_moy    = round(mean(surcout_absolu_usd,  na.rm = TRUE), 2),
    surcout_median = round(median(surcout_absolu_usd, na.rm = TRUE), 2),
    .groups        = "drop"
  )

cat("Distribution des impacts par type :\n")
print(stats_impact)

# ── Zones les plus touchées ────────────────────────────────────────────────────
# Pour chaque zone, on cumule les surcoûts sur tous ses trajets (comme origine
# ET comme destination) pour mesurer son exposition totale à la perturbation.
surcouts_par_zone <- od_compare %>%
  filter(type_impact != "inchange") %>%
  group_by(Zone = nom_origine) %>%
  summarise(
    surcout_total_usd  = round(sum(surcout_absolu_usd,  na.rm = TRUE), 1),
    surcout_moyen_usd  = round(mean(surcout_absolu_usd, na.rm = TRUE), 2),
    n_paires_touchees  = n(),
    n_deconnexions     = sum(type_impact == "deconnecte"),
    pct_surcout_moyen  = round(mean(surcout_relatif_pct, na.rm = TRUE), 1),
    .groups            = "drop"
  ) %>%
  arrange(desc(surcout_total_usd))

cat("\nTop 10 des zones les plus touchées (en tant qu'origine) :\n")
print(head(surcouts_par_zone, 10))
cat("\n")

# Même calcul côté destination (quelles zones reçoivent moins de fret ?)
surcouts_par_destination <- od_compare %>%
  filter(type_impact != "inchange") %>%
  group_by(Zone = nom_destination) %>%
  summarise(
    surcout_total_usd = round(sum(surcout_absolu_usd,  na.rm = TRUE), 1),
    n_deconnexions    = sum(type_impact == "deconnecte"),
    .groups           = "drop"
  ) %>%
  arrange(desc(surcout_total_usd))

cat("Top 5 des zones les plus isolées (en tant que destination) :\n")
print(head(surcouts_par_destination, 5))
cat("\n")


################################################################################
# PARTIE IX.5 — IDENTIFICATION DES ARÊTES CRITIQUES
#
# OBJECTIF : Trouver les arêtes qui, si elles sont supprimées individuellement,
#            causent le plus grand surcoût agrégé sur le réseau.
# MÉTHODE  : Pour chaque arête candidate, on simule sa suppression isolée
#            et on calcule le surcoût OD total. On classe ensuite les arêtes
#            par ordre décroissant de criticité.
#
# REMARQUE SUR LE TEMPS DE CALCUL :
#   Tester TOUTES les arêtes du réseau serait trop long (Dijkstra × 30 000 arêtes).
#   On se restreint aux arêtes "candidates" selon deux filtres :
#     1. Les arêtes sur le chemin optimal d'au moins une paire OD (utiles)
#     2. Parmi elles, les arêtes de fort volume de trafic (impactantes)
#   Ce sous-ensemble représente typiquement 5-15% du réseau total,
#   ce qui rend le calcul faisable en moins d'une heure.
#
# PARAMÈTRE :
#   N_TOP_ARETES_CRITIQUES — nombre d'arêtes à tester (par ordre de volume)
#   Augmenter ce nombre = analyse plus complète mais plus lente.
################################################################################

cat("── Analyse de criticité des arêtes ───────────────────────────────────\n\n")

# Nombre d'arêtes à tester pour la criticité.
# 50 = bon compromis rapidité / exhaustivité pour un premier diagnostic.
# Pour une analyse complète, augmenter à 200 ou 500 (plusieurs heures).
N_TOP_ARETES_CRITIQUES <- 50

# ── Sélection des arêtes candidates ───────────────────────────────────────────
# On prend les N arêtes avec le plus gros volume de trafic fret,
# car ce sont les candidates les plus susceptibles d'être critiques.
# Les arêtes sans trafic (routes jamais empruntées dans le modèle) sont exclues.
aretes_candidates <- aretes_reseau_sf %>%
  filter(!is.na(volume_tonnes), volume_tonnes > 0) %>%
  arrange(desc(volume_tonnes)) %>%
  slice_head(n = N_TOP_ARETES_CRITIQUES) %>%
  pull(arete_idx)

cat("  Arêtes candidates :", length(aretes_candidates),
    "(top", N_TOP_ARETES_CRITIQUES, "par volume de trafic)\n")

# ── Fonction de calcul du surcoût total pour une suppression individuelle ──────
# calculer_surcout_total() :
#   - Prend un vecteur d'indices d'arêtes physiques à supprimer
#   - Construit un graphe temporaire avec ces arêtes bloquées (poids = Inf)
#   - Recalcule les distances OD pour les paires les plus importantes
#   - Retourne le surcoût total agrégé (en USD)
#
# Pour accélérer, on ne recalcule que les paires OD avec un volume de fret
# supérieur à un seuil (SEUIL_PAIRES_CRITICITE), ce qui exclut les paires
# marginales qui ne changent pas le classement de criticité.

SEUIL_PAIRES_CRITICITE <- 100   # tonnes — ignorer les paires < 100t pour la criticité

calculer_surcout_total <- function(indices_a_supprimer) {
  
  # Construction du graphe temporaire
  graphe_temp <- graphe_multimodal
  
  # Indices multi-modaux à bloquer (toutes couches véhicule)
  idx_mm_temp <- which(
    lookup_type     == "route" &
      lookup_physique %in% indices_a_supprimer
  )
  igraph::E(graphe_temp)$weight[idx_mm_temp] <- Inf
  
  # Paires OD à tester (uniquement les paires avec fort volume de fret)
  # flux_tonnes_total a été construit en Partie VIII
  paires_importantes <- which(flux_tonnes_total > SEUIL_PAIRES_CRITICITE,
                              arr.ind = TRUE)
  
  surcout_cumule <- 0
  
  for (k in seq_len(nrow(paires_importantes))) {
    i_k <- paires_importantes[k, 1]
    j_k <- paires_importantes[k, 2]
    if (i_k == j_k) next
    
    sources_k <- sapply(seq_len(n_vehicules),
                        function(v) node_multi(v, warehouse_nodes_base[i_k]))
    cols_k    <- j_k + (seq_len(n_vehicules) - 1) * length(warehouse_nodes_base)
    targets_k <- as.vector(sapply(seq_len(n_vehicules),
                                  function(v) node_multi(v, warehouse_nodes_base)))
    
    dists_k <- igraph::distances(
      graphe_temp,
      v       = sources_k,
      to      = targets_k[cols_k],
      weights = igraph::E(graphe_temp)$weight
    )
    
    cout_degrade_k <- min(dists_k, na.rm = TRUE)
    
    # Coût de référence pour cette paire (depuis od_long)
    ref_k <- od_long %>%
      filter(id_origine == i_k, id_destination == j_k) %>%
      pull(cout_usd)
    if (length(ref_k) == 0 || is.na(ref_k)) next
    
    delta_k <- if (is.infinite(cout_degrade_k)) ref_k * 10 else
      max(0, cout_degrade_k - ref_k)
    # Pondération par le volume de fret : une arête qui détourne 10 000 tonnes
    # est plus critique qu'une arête qui détourne 10 tonnes au même surcoût.
    surcout_cumule <- surcout_cumule +
      delta_k * flux_tonnes_total[i_k, j_k]
  }
  
  surcout_cumule
}

# ── Calcul de la criticité pour chaque arête candidate ────────────────────────
cat("  Calcul de la criticité (", length(aretes_candidates),
    "arêtes × Dijkstra) — peut prendre quelques minutes...\n")

criticite_df <- tibble(
  arete_idx         = aretes_candidates,
  surcout_pondere   = NA_real_
)

pb_crit <- progress_bar$new(
  format = "  Criticité [:bar] :percent | ETA: :eta",
  total  = length(aretes_candidates),
  clear  = FALSE,
  width  = 60
)

for (k in seq_along(aretes_candidates)) {
  criticite_df$surcout_pondere[k] <- calculer_surcout_total(aretes_candidates[k])
  pb_crit$tick()
}

# ── Enrichissement avec les attributs de chaque arête ─────────────────────────
# On récupère les attributs (road_type, longueur, etc.) pour interpréter
# les résultats de criticité.
criticite_df <- criticite_df %>%
  left_join(
    aretes_reseau_sf %>%
      st_drop_geometry() %>%
      select(arete_idx, osm_id, name, road_type, surface,
             longueur_m, volume_tonnes, cost_per_tkm),
    by = "arete_idx"
  ) %>%
  arrange(desc(surcout_pondere)) %>%
  mutate(
    rang              = row_number(),
    longueur_km       = round(longueur_m / 1000, 2),
    surcout_pondere_k = round(surcout_pondere / 1000, 1)   # En milliers USD×tonnes
  )

# ── Sauvegarde de la table de criticité dans DuckDB ───────────────────────────
duck_write(criticite_df, paste0("criticite_aretes_", NOM_SCENARIO))

cat("\n✓ Top 10 des arêtes les plus critiques :\n")
print(
  criticite_df %>%
    slice_head(n = 10) %>%
    select(rang, osm_id, name, road_type, longueur_km,
           volume_tonnes, surcout_pondere_k) %>%
    rename(
      Rang        = rang,
      OSM_ID      = osm_id,
      Nom         = name,
      Type        = road_type,
      Long_km     = longueur_km,
      Vol_t       = volume_tonnes,
      Criticite_k = surcout_pondere_k
    )
)
cat("\n")


################################################################################
# PARTIE IX.6 — CARTES ET EXPORTS
#
# Génère quatre sorties visuelles :
#   Carte A — Réseau dégradé : arêtes perturbées + impact sur les OD
#   Carte B — Arêtes critiques : classement des segments les plus sensibles
#   Carte C — Surcoûts par zone : gradient de vulnérabilité économique
#   Graphique — Distribution des surcoûts relatifs par type de route
################################################################################

cat("── Génération des cartes et exports ──────────────────────────────────\n\n")

# Palette spécifique aux types d'impact (cohérente avec la mind map)
PALETTE_IMPACT <- c(
  "inchange"   = "#CCCCCC",   # Gris — pas d'impact
  "faible"     = "#FFFFB2",   # Jaune pâle — détour < 10%
  "modere"     = "#FECC5C",   # Jaune-orange — détour 10-50%
  "fort"       = "#FD8D3C",   # Orange — détour 50-100%
  "tres_fort"  = "#E31A1C",   # Rouge vif — doublement du coût
  "deconnecte" = "#800026"    # Rouge foncé — zone coupée du réseau
)

# ── Préparation des couches spatiales ─────────────────────────────────────────

# Arêtes perturbées (pour les surligner sur la carte)
aretes_perturbees_sf <- aretes_reseau_sf %>%
  filter(arete_idx %in% indices_aretes_perturbees)

# Arêtes critiques (top N pour la Carte B)
N_ARETES_AFFICHEES <- min(20, nrow(criticite_df))
aretes_critiques_sf <- aretes_reseau_sf %>%
  filter(arete_idx %in% criticite_df$arete_idx[1:N_ARETES_AFFICHEES]) %>%
  left_join(
    criticite_df %>% select(arete_idx, rang, surcout_pondere_k),
    by = "arete_idx"
  )

# Points des zones colorés par impact (surcoût moyen relatif)
impact_par_zone_sf <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  st_as_sf() %>%
  left_join(
    surcouts_par_zone %>%
      select(Zone, pct_surcout_moyen, n_deconnexions, surcout_total_usd),
    by = c("warehouse_name" = "Zone")
  ) %>%
  mutate(
    pct_surcout_moyen = replace_na(pct_surcout_moyen, 0),
    surcout_total_usd = replace_na(surcout_total_usd, 0)
  )

# ── CARTE A : Réseau dégradé et zones d'impact ────────────────────────────────
cat("  Génération Carte A — réseau dégradé...\n")

# Zone tampon visible autour des arêtes perturbées (pour la localiser sur la carte)
# st_buffer() + st_union() : crée une zone en surbrillance autour des routes coupées
zone_impact_visible <- aretes_perturbees_sf %>%
  st_buffer(dist = 2000) %>%   # 2km de buffer pour être visible sur la carte
  st_union()

carte_reseau_degrade <- fond_carte() +
  
  # Réseau de base en gris clair
  tm_shape(aretes_reseau_sf) +
  tm_lines(col = "#DDDDDD", lwd = 0.4) +
  
  # Zone d'impact en surbrillance semi-transparente
  tm_shape(zone_impact_visible %>% st_as_sf()) +
  tm_polygons(
    fill       = "#FF6B6B",
    col        = "#CC0000",
    fill_alpha = 0.25,
    lwd        = 1.5,
    fill.legend = tm_legend(show = FALSE)
  ) +
  
  # Arêtes perturbées en rouge épais
  tm_shape(aretes_perturbees_sf) +
  tm_lines(col = "#CC0000", lwd = 3.5,
           col.legend = tm_legend(show = FALSE)) +
  
  # Points des zones avec couleur selon le surcoût moyen
  tm_shape(impact_par_zone_sf) +
  tm_dots(
    fill       = "pct_surcout_moyen",
    fill.scale = tm_scale_intervals(
      style  = "fixed",
      breaks = c(0, 5, 20, 50, 100, Inf),
      values = c("#CCCCCC", "#FFFFB2", "#FD8D3C", "#E31A1C", "#800026")
    ),
    fill.legend = tm_legend(title = "Surcoût moyen\n(% hausse)"),
    size = 0.8
  ) +
  
  tm_title(paste0("Réseau dégradé — ", NOM_SCENARIO,
                  "\n", DESCRIPTION_SCENARIO)) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_reseau_degrade,
  file.path(DIR_OUTPUT, paste0("carte_reseau_degrade_", NOM_SCENARIO, ".png")),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ Carte A sauvegardée\n")

# ── CARTE B : Arêtes critiques (top N classées par criticité) ─────────────────
cat("  Génération Carte B — arêtes critiques...\n")

carte_criticite <- fond_carte() +
  
  # Réseau de base en gris très clair
  tm_shape(aretes_reseau_sf) +
  tm_lines(col = "#EEEEEE", lwd = 0.3) +
  
  # Arêtes avec trafic, colorées par leur rang de criticité
  # (plus rouge = plus critique = suppression la plus coûteuse)
  tm_shape(aretes_critiques_sf) +
  tm_lines(
    col        = "rang",
    col.scale  = tm_scale_intervals(
      style  = "fixed",
      breaks = c(0, 5, 10, 15, 20, Inf),
      values = rev(c("#FFF5F0", "#FCBBA1", "#FC7050", "#EF3B2C", "#99000D"))
    ),
    col.legend = tm_legend(title = paste0("Rang de criticité\n(top ",
                                          N_ARETES_AFFICHEES, ")")),
    lwd        = 3
  ) +
  
  # Arêtes perturbées du scénario actuel
  tm_shape(aretes_perturbees_sf) +
  tm_lines(col = "#0000CC", lwd = 2,
           col.legend = tm_legend(show = FALSE)) +
  
  tm_title(paste0("Arêtes critiques du réseau — ",
                  "Top ", N_ARETES_AFFICHEES, " par surcoût pondéré")) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_criticite,
  file.path(DIR_OUTPUT, paste0("carte_criticite_aretes_", NOM_SCENARIO, ".png")),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ Carte B sauvegardée\n")

# ── CARTE C : Vulnérabilité économique des zones ──────────────────────────────
cat("  Génération Carte C — vulnérabilité des zones...\n")

carte_vulnerabilite <- fond_carte() +
  
  tm_shape(aretes_reseau_sf) +
  tm_lines(col = "#DDDDDD", lwd = 0.3) +
  
  # Taille des points proportionnelle au surcoût total (exposition économique)
  # Couleur selon la présence de déconnexions (rouge = zone coupée du réseau)
  tm_shape(impact_par_zone_sf) +
  tm_dots(
    fill       = "n_deconnexions",
    fill.scale = tm_scale_intervals(
      breaks = c(-Inf, 0, 1, 5, Inf),
      values = c("#2166AC", "#FEE08B", "#F46D43", "#A50026")
    ),
    fill.legend = tm_legend(title = "Nb de destinations\ncoupées"),
    size        = "surcout_total_usd",
    size.scale  = tm_scale(values.range = c(0.3, 2.5)),
    size.legend = tm_legend(title = "Surcoût total\n(USD)")
  ) +
  
  # Arêtes perturbées pour référence
  tm_shape(aretes_perturbees_sf) +
  tm_lines(col = "#CC0000", lwd = 3) +
  
  tm_title(paste0("Vulnérabilité économique des zones\n",
                  NOM_SCENARIO, " — Durée estimée : ",
                  DUREE_JOURS, " jours")) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_vulnerabilite,
  file.path(DIR_OUTPUT, paste0("carte_vulnerabilite_zones_", NOM_SCENARIO, ".png")),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ Carte C sauvegardée\n")

# ── GRAPHIQUE : Distribution des surcoûts relatifs ────────────────────────────
cat("  Génération du graphique de distribution...\n")

g_surcouts <- od_compare %>%
  filter(!is.na(surcout_relatif_pct), surcout_relatif_pct > 0) %>%
  ggplot(aes(x = surcout_relatif_pct, fill = type_impact)) +
  geom_histogram(bins = 40, color = "white", linewidth = 0.2) +
  scale_fill_manual(
    values = PALETTE_IMPACT,
    name   = "Type d'impact"
  ) +
  scale_x_continuous(
    labels = scales::percent_format(scale = 1),
    breaks = c(0, 10, 25, 50, 75, 100, 150, 200)
  ) +
  labs(
    title    = paste0("Distribution des surcoûts de transport — ", NOM_SCENARIO),
    subtitle = paste0(DESCRIPTION_SCENARIO,
                      "\nDurée estimée : ", DUREE_JOURS, " jours"),
    x        = "Hausse du coût de transport (%)",
    y        = "Nombre de paires OD affectées"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#555555")
  )

ggsave(
  file.path(DIR_OUTPUT, paste0("graphique_surcouts_", NOM_SCENARIO, ".png")),
  g_surcouts, width = 11, height = 6, dpi = 300
)
cat("  ✓ Graphique sauvegardé\n\n")

# ── EXPORTS CSV et Parquet ─────────────────────────────────────────────────────
# Export de la table de comparaison OD (avant / après)
dbExecute(con, paste0(
  "COPY (SELECT * FROM ", nom_table_impact, ") TO '",
  file.path(DIR_OUTPUT, paste0("impact_od_", NOM_SCENARIO, ".csv")),
  "' (FORMAT CSV, HEADER)"
))

# Export de la table de criticité des arêtes
dbExecute(con, paste0(
  "COPY (SELECT * FROM criticite_aretes_", NOM_SCENARIO, ") TO '",
  file.path(DIR_OUTPUT, paste0("criticite_aretes_", NOM_SCENARIO, ".csv")),
  "' (FORMAT CSV, HEADER)"
))

cat("✓ Exports CSV terminés\n\n")

# ── RAPPORT FINAL DE LA PARTIE IX ────────────────────────────────────────────

cat("==========================================================\n")
cat("  RAPPORT — ANALYSE DE VULNÉRABILITÉ\n")
cat("==========================================================\n\n")
cat("Scénario        :", NOM_SCENARIO, "\n")
cat("Description     :", DESCRIPTION_SCENARIO, "\n")
cat("Durée estimée   :", DUREE_JOURS, "jours\n")
cat("Type d'événement:", TYPE_EVENEMENT, "\n\n")

cat("RÉSEAU PERTURBÉ :\n")
cat("  Arêtes coupées            :", n_perturb, "\n")
cat("  Longueur hors service     :", resume_perturb$longueur_km, "km\n")
cat("  Part du réseau total      :", resume_perturb$pct_du_reseau, "%\n\n")

cat("IMPACT SUR LES FLUX OD :\n")
cat("  Paires inchangées         :",
    sum(od_compare$type_impact == "inchange", na.rm = TRUE), "\n")
cat("  Paires avec détour faible :",
    sum(od_compare$type_impact == "faible", na.rm = TRUE), "\n")
cat("  Paires avec détour modéré :",
    sum(od_compare$type_impact == "modere", na.rm = TRUE), "\n")
cat("  Paires avec détour fort   :",
    sum(od_compare$type_impact == "fort", na.rm = TRUE), "\n")
cat("  Paires fortement impactées:",
    sum(od_compare$type_impact == "tres_fort", na.rm = TRUE), "\n")
cat("  Paires déconnectées       :",
    sum(od_compare$type_impact == "deconnecte", na.rm = TRUE), "\n\n")

cat("SURCOÛT MOYEN (paires affectées) :",
    round(mean(od_compare$surcout_absolu_usd, na.rm = TRUE), 2), "USD\n")
cat("SURCOÛT RELATIF MOYEN            :",
    round(mean(od_compare$surcout_relatif_pct, na.rm = TRUE), 1), "%\n\n")

cat("ARÊTES LES PLUS CRITIQUES (top 5) :\n")
print(
  criticite_df %>%
    slice_head(n = 5) %>%
    select(rang, road_type, longueur_km, volume_tonnes, surcout_pondere_k) %>%
    rename(Rang = rang, Type = road_type, Long_km = longueur_km,
           Vol_t = volume_tonnes, Criticite = surcout_pondere_k)
)

cat("\nFICHIERS GÉNÉRÉS (Partie IX) :\n")
cat("  • carte_reseau_degrade_",   NOM_SCENARIO, ".png\n", sep = "")
cat("  • carte_criticite_aretes_", NOM_SCENARIO, ".png\n", sep = "")
cat("  • carte_vulnerabilite_zones_", NOM_SCENARIO, ".png\n", sep = "")
cat("  • graphique_surcouts_",     NOM_SCENARIO, ".png\n", sep = "")
cat("  • impact_od_",              NOM_SCENARIO, ".csv\n", sep = "")
cat("  • criticite_aretes_",       NOM_SCENARIO, ".csv\n", sep = "")

cat("\nPROCHAINES ÉTAPES :\n")
cat("  → Partie X  : Propagation sectorielle via Leontief\n")
cat("  → Partie XI : Dynamique temporelle et effets de second tour\n")
cat("  Pour tester un autre scénario : modifier NOM_SCENARIO et les\n")
cat("  paramètres de IX.1 puis relancer uniquement la Partie IX.\n")
cat("==========================================================\n")


# ==============================================================================
# RAPPORT FINAL COMPLET
# ==============================================================================

cat("===============================================================\n")
cat("     MODÈLE DE FRET RWANDA - TERMINÉ AVEC SUCCÈS\n")
cat("===============================================================\n\n")

cat("RÉSUMÉ DU MODÈLE:\n")
cat("  Secteurs économiques   :", N_SECTEURS, "\n")
cat("  Zones modélisées       :", n_warehouses, "\n")
cat("  PIB modélisé           :", round(sum(production_totale)/1000, 1), "Md USD\n")
cat("  Flux total (modèle grav.):", round(sum(flux_total), 0), "M USD\n")
cat("  Tonnage total modélisé :",
    format(round(tonnage_total), big.mark = " "), "tonnes\n")
cat("  Arêtes avec trafic fret:", stats_trafic$n_aretes_actives, "\n\n")

cat("FICHIERS GÉNÉRÉS (Parties 16-20):\n")
cat("  Données:\n")
cat("    • table_io_coefficients.csv          - Matrice A des coef. techniques\n")
cat("    • table_io_recap.csv                 - Résumé sectoriel IO\n")
cat("    • offre_demande_zones.csv            - Offre/demande par zone\n")
cat("    • matrice_flux_gravitaire_musd.csv   - Flux OD en M USD\n")
cat("    • matrice_flux_fret_tonnes.csv       - Flux OD en tonnes\n")
cat("    • reseau_rwanda_avec_fret.gpkg       - Réseau + volumes fret\n")
cat("\n  Cartes:\n")
cat("    • carte_trafic_fret.png              - Intensité fret sur réseau\n")
cat("    • carte_flux_od.png                  - Desire lines OD\n")
cat("\n  Graphiques:\n")
cat("    • graphique_flux_secteurs.png        - Flux par secteur\n")
cat("    • graphique_offre_demande.png        - Offre vs Demande par zone\n")
cat("    • heatmap_flux_od.png                - Matrice OD (heatmap)\n")
cat("    • graphique_composition_sectorielle  - Composition par zone\n\n")

cat("NOTE: Pour utiliser des données IO réelles du Rwanda:\n")
cat("  1. Télécharger la table IO sur www.statistics.gov.rw (NISR)\n")
cat("  2. Remplacer les matrices A et production_totale en Partie 16\n")
cat("  3. Les Parties 17-20 se recalculent automatiquement\n\n")

cat("Script terminé le:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("===============================================================\n")

################################################################################
# FIN DU SCRIPT
################################################################################
