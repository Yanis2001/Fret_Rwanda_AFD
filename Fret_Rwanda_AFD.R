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
#  system("git clone https://github.com/GEMMES-AFD/Transport.git")
#  Pour activer l'onglet Git dans RStudio : File -> Open Project
################################################################################

# Authentification Git via le Personal Access Token stocké en variable d'env.
# Sys.getenv() lit la variable d'environnement GITHUB_PAT sans l'exposer
# dans le code source (bonne pratique de sécurité).
token <- Sys.getenv("GITHUB_PAT")
system("git config --global credential.helper '!f() { echo \"username=token\"; echo \"password=$GITHUB_PAT\"; }; f'")

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

# Liste exhaustive des packages nécessaires avec leur rôle :
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
  "scales",         # Mise à l'échelle et formatage pour ggplot2 (rescale, percent…)
  "progress"       # Barre de progression
)

# Fonction d'installation conditionnelle : n'installe que les packages absents
# pour ne pas réinstaller inutilement à chaque exécution du script.
installer_si_necessaire <- function(packages) {
  # installed.packages()[,"Package"] retourne le vecteur des packages installés
  nouveaux <- packages[!(packages %in% installed.packages()[, "Package"])]
  if (length(nouveaux)) install.packages(nouveaux, dependencies = TRUE)
}

installer_si_necessaire(packages_requis)

# Chargement silencieux (invisible supprime l'affichage des messages de chargement)
invisible(lapply(packages_requis, library, character.only = TRUE))

# Augmenter le timeout global pour les téléchargements de gros fichiers (DEM, PBF)
options(timeout = 600)

# Graine aléatoire pour la reproductibilité des données fictives générées 
set.seed(123)

cat("✓ Tous les packages sont chargés\n\n")

# ==============================================================================
# I.2 : Connexion DuckDB et fonctions utilitaires
# Ouvre la base analytique persistante et définit les raccourcis duck_write()
# et duck_query() utilisés dans toutes les parties suivantes.
# ==============================================================================

# Fermeture propre la connexion à DuckDB afin de la rouvrir ensuite proprement
if (exists("con")) {
  tryCatch(
    DBI::dbDisconnect(con, shutdown = TRUE),
    error = function(e) NULL
  )
}

cat("=== Connexion DuckDB ===\n")

# Chemin vers le fichier de base de données (créé s'il n'existe pas)
DB_PATH <- "reseau_rwanda.duckdb"

# dbConnect() ouvre la connexion. duckdb() est le pilote DBI pour DuckDB.
# dbdir = ":memory:" créerait une base en RAM uniquement (non persistante).
con <- dbConnect(duckdb(), dbdir = DB_PATH)

# Note : l'extension spatiale DuckDB existe mais n'est pas utilisée ici
# car sfnetworks ne sait pas lire depuis DuckDB spatial.
# Pour l'activer si besoin : dbExecute(con, "INSTALL spatial; LOAD spatial;")

cat("✓ DuckDB connecté :", DB_PATH, "\n\n")

# Répertoire de sortie local (évite les erreurs de permissions sur /mnt/)
DIR_OUTPUT <- "outputs"
dir.create(DIR_OUTPUT, showWarnings = FALSE, recursive = TRUE)

# ── Fonctions utilitaires DuckDB ──────────────────────────────────────────────

# Écrit un data.frame R dans DuckDB (crée ou remplace la table).
# overwrite = TRUE évite les erreurs si la table existe déjà (utile pour relancer).
duck_write <- function(df, table_name) {
  dbWriteTable(con, table_name, df, overwrite = TRUE)
  invisible(df)  # Retourne df invisiblement pour permettre le chaînage (%>%)
}

# Exécute une requête SQL et retourne le résultat sous forme de data.frame R.
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

# ── Table 1 : paramètres scalaires par véhicule ──────────────────────────────
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

# Véhicule de référence pour la matrice OD et le modèle gravitaire
VEHICULE_REFERENCE   <- "camion_moyen"
CONSO_PAR_METRE_D_PLUS <- 0.03

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
routes_rwanda_raw <- st_read(
  chemin_pbf,
  layer = "lines",
  query = "SELECT * FROM lines
           WHERE highway IN
           ('motorway','trunk','primary','secondary','tertiary','unclassified')",
  quiet = FALSE  # Afficher les informations de chargement
)

cat("✓ Données chargées :", nrow(routes_rwanda_raw), "segments\n\n")

# ==============================================================================
# II.2 : Couches administratives et fond de carte
# Extrait frontières, provinces, lacs et parcs depuis le PBF.
# Définit fond_carte(), la fonction réutilisée dans toutes les cartes du script.
# ==============================================================================

# Vérification des landuse disponibles
landuse_test <- st_read(
  chemin_pbf, layer = "multipolygons",
  query = "SELECT landuse FROM multipolygons
           WHERE landuse IN ('residential','commercial','industrial','retail')",
  quiet = TRUE
)
cat("Zones par landuse :\n")
print(table(landuse_test$landuse))

# Vérification des place disponibles
place_test <- st_read(
  chemin_pbf, layer = "multipolygons",
  query = "SELECT place FROM multipolygons
           WHERE place IN ('city','town','village','suburb','neighbourhood')",
  quiet = TRUE
)
cat("\nZones par place :\n")
print(table(place_test$place))

# Vérifier le nom de la colonne géométrie dans la couche points
villes_raw <- st_read(
  chemin_pbf, layer = "points",
  query = "SELECT name, place FROM points
           WHERE place IN ('city','town')",
  quiet = TRUE
)
cat("Colonnes disponibles :\n")
print(names(villes_raw))

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
attrs_df <- routes_attrs_raw %>% st_drop_geometry()
duck_write(attrs_df, "routes_attrs_raw")

# La requête CASE WHEN harmonise les valeurs OSM hétérogènes de "surface"
# (ex : "asphalt", "concrete", "paved" → tous ramenés à "paved")
# puis impute les valeurs manquantes selon le type de route :
#   - Les routes nationales (trunk, primary) sont supposées bitumées au Rwanda
#   - Les routes secondaires : gravier (fréquent hors Kigali)
#   - Les routes tertiaires et non classées : piste en terre par défaut
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
routes_rwanda <- routes_attrs_raw %>%
  select(osm_id, geometry) %>%
  left_join(attrs_clean, by = "osm_id") %>%
  st_as_sf() %>%
  # CRS 32735 = WGS 84 / UTM Zone 35S : projection métrique adaptée au Rwanda
  # Nécessaire pour calculer des longueurs en mètres et des pentes en %
  st_transform(crs = 32735)

cat("✓ Nettoyage terminé :", nrow(routes_rwanda), "segments — surface harmonisée via DuckDB\n\n")


# ── Frontière nationale (admin_level = 2) ─────────────────────────────────────
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

# 1. Calcul du centroïde du Rwanda (point central)
centre_rwanda <- rwanda_national %>% st_centroid() %>% st_coordinates()
centre_x <- centre_rwanda[1, "X"]  # Coordonnée X (Est-Ouest) du centroïde
centre_y <- centre_rwanda[1, "Y"]  # Coordonnée Y (Nord-Sud) du centroïde

# 2. Définition du buffer de 125 km 
buffer_km <- 125000 

# 3. Construction manuelle d'un polygone carré (bbox) autour du centroïde
#    Les coordonnées sont calculées en ajoutant/soustrayant buffer_km aux coordonnées du centroïde
#    Format : liste de points dans l'ordre (coin bas-gauche → coin bas-droit → coin haut-droit → coin haut-gauche → retour au coin bas-gauche)
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
# Crée les couches de base (provinces, frontière, lacs) communes à toutes les cartes.
# Retourne un objet tmap auquel on ajoute des couches thématiques avec +.

fond_carte <- function() {
  
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

tmap_save(carte_verif_routes,
          file.path(DIR_OUTPUT, "carte_verif_routes_partie3.png"),
          width = 3000, height = 2400, dpi = 300)

# Ouvre une carte zoomable dans l'onglet Viewer de RStudio
if (FALSE) {
  tmap_mode("view")
  print(carte_verif_routes)
  tmap_mode("plot")   # Remettre en mode statique pour la suite du script
}

cat("✓ Carte de vérification générée\n\n")


# ==============================================================================
# II.3 : Modèle Numérique de Terrain (DEM)
# Télécharge le DEM SRTM depuis AWS via elevatr. En cas d'échec, génère
# un DEM fictif calibré sur la topographie réelle du Rwanda.
# Utilisé uniquement en Partie IV.2 pour le calcul des pentes.
# ==============================================================================


# Le DEM (Digital Elevation Model) est une grille de pixels où chaque valeur
# représente l'altitude en mètres au-dessus du niveau de la mer.
# Il sera utilisé pour calculer la pente de chaque segment routier
# (ratio dénivelé/longueur × 100 = pourcentage de pente).


# Créer l'emprise géographique à partir de la bbox des routes
# pour ne télécharger que la zone d'intérêt (Rwanda uniquement)
bbox_routes <- st_bbox(routes_rwanda)
emprise_points <- data.frame(
  x = c(bbox_routes["xmin"], bbox_routes["xmax"]),
  y = c(bbox_routes["ymin"], bbox_routes["ymax"])
)

# Reconvertir en objet sf en WGS84 (elevatr attend des coordonnées géographiques)
emprise_sf <- st_as_sf(emprise_points, coords = c("x","y"), crs = 32735) %>%
  st_transform(crs = 4326)

tryCatch({
  # z = 9 correspond à un zoom de ~300m/pixel, suffisant pour des routes
  # clip = "locations" : on ne récupère les données que dans l'emprise fournie
  dem_rwanda <- get_elev_raster(emprise_sf, z = 9, clip = "locations")
  dem_rwanda <- rast(dem_rwanda)   # Conversion raster R → terra SpatRaster
  # Reprojection en UTM 35S pour cohérence avec les routes
  # method = "bilinear" : interpolation bilinéaire (meilleure qualité que "nearest")
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
  dem_rwanda <<- rast(ext_utm, resolution = 90, crs = "EPSG:32735")
  
  set.seed(123)   # Graine pour reproductibilité du bruit aléatoire
  n_cells    <- ncell(dem_rwanda)
  
  # xFromCell() retourne la coordonnée X (longitude UTM) du centre de chaque cellule
  x_coords <- xFromCell(dem_rwanda, 1:n_cells)
  
  # Gradient d'élévation : 1 500m à l'Est → 2 300m à l'Ouest
  # La formule normalise x_coords entre 0 (Est) et 1 (Ouest) puis multiplie par 800m
  base_elevation <- 1500 + (max(x_coords) - x_coords) /
    (max(x_coords) - min(x_coords)) * 800
  
  # Ajout d'un bruit gaussien (sd=150m) pour simuler collines et vallées
  # pmax/pmin bornent les valeurs entre 950m et 2 500m
  values(dem_rwanda) <<- pmax(950, pmin(2500, base_elevation + rnorm(n_cells, 0, 150)))
  
  cat("✓ DEM fictif créé\n")
})


# Découpe le raster dem_rwanda pour ne garder que la zone qui chevauche le 
# polygone rwanda_boundary pour éviter de traiter des données hors de la zone d'intérêt
dem_rwanda <- crop(dem_rwanda, vect(rwanda_boundary)) 

# Masque les pixels du raster qui ne sont pas à l'intérieur du polygone rwanda_boundary
# définis comme NA
dem_rwanda <- mask(dem_rwanda, vect(rwanda_boundary))

# Limite les valeurs du raster à un intervalle donné et remplace les valeurs hors seuil par NA
dem_rwanda <- clamp(dem_rwanda, lower = 800, upper = 4600, values = NA)

cat("  Élévation min :", round(global(dem_rwanda, "min", na.rm = TRUE)[,1]), "m\n")
cat("  Élévation max :", round(global(dem_rwanda, "max", na.rm = TRUE)[,1]), "m\n\n")

################################################################################
# PARTIE III — CONSTRUCTION ET CORRECTION DU RÉSEAU ROUTIER
# Transforme les segments OSM bruts en graphe sfnetworks topologiquement
# cohérent, puis extrait la composante géante (réseau principal connecté).
# Toute modification ici invalide le cache des pentes.
################################################################################

# ==============================================================================
# III.1 : Nettoyage et harmonisation des attributs
# Extrait les tags OSM (surface, vitesse, sens unique) via regex, puis
# harmonise les valeurs hétérogènes en SQL via DuckDB (CASE WHEN).
# ==============================================================================


# Cartographier rapidement pour identifier visuellement les anomalies
plot(dem_rwanda, main = "DEM Rwanda — vérification")
plot(st_geometry(rwanda_boundary), add = TRUE, border = "red")

# sfnetworks représente le réseau routier comme un graphe topologique où :
#   - les NŒUDS sont les intersections et extrémités de routes
#   - les ARÊTES sont les segments de route entre deux nœuds
# Ce graphe servira ensuite à igraph pour le calcul de plus courts chemins.

# ── Homogénéisation des types de géométrie ───────────────────────────────────
# Le fichier PBF peut contenir des MULTILINESTRING (plusieurs lignes groupées)
# que sfnetworks ne sait pas gérer. st_cast() les éclate en LINESTRING simples.
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
# PARTIE 7 : CORRECTIONS TOPOLOGIQUES DU RÉSEAU
# ==============================================================================
# Les données OSM contiennent fréquemment des erreurs topologiques :
#   1. Routes qui se croisent sans nœud d'intersection (pont raté, erreur de saisie)
#   2. Nœuds intermédiaires inutiles (points de degré 2 sur une ligne droite)
# Ces erreurs créent des composantes connexes multiples (le réseau est "fragmenté")
# et empêchent les algorithmes de plus court chemin de trouver des itinéraires.

cat("=== PARTIE 7 : Corrections topologiques ===\n")

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

cat("  Étape 4/4 : extraction de la composante géante...\n")

# - `as_tbl_graph()` : Convertit le réseau sfnetwork en un graphe tidygraph/igraph.
#   Cela permet d'utiliser les fonctions d'analyse de graphe d'igraph.
# - `igraph::components()` : Identifie toutes les composantes connexes du graphe.
# - Résultat : `composantes_finales` est une liste avec deux éléments :
#    - $membership : Vecteur indiquant à quelle composante appartient chaque nœud.
#    - $csize : Vecteur indiquant la taille (nombre de nœuds) de chaque composante.
composantes_finales <- igraph::components(reseau_lisse %>% as_tbl_graph())   

# Identification de la composante géante
id_geante           <- which.max(composantes_finales$csize)

# Extraction des nœuds appartenant à la composante géante
noeuds_geante       <- which(composantes_finales$membership == id_geante)

# Calcul du pourcentage de nœuds dans la composante géante
pct_noeuds          <- round(length(noeuds_geante) / igraph::vcount(reseau_lisse) * 100, 1)

cat("  Composante géante :", length(noeuds_geante), "nœuds (", pct_noeuds, "% du réseau)\n")

# ==============================================================================
# PARTIE 8 : DIAGNOSTIC DES ARÊTES PERDUES
# ==============================================================================

cat("=== PARTIE 8 : Diagnostic des arêtes hors composante géante ===\n")

# Vérifier les colonnes disponibles dans rwanda_provinces
cat("Colonnes de rwanda_provinces :\n")
print(names(rwanda_provinces))

# ── Récupérer les arêtes du réseau AVANT filtrage (reseau_lisse) ─────────────
aretes_lisse <- reseau_lisse %>% activate("edges") %>% st_as_sf() %>%
  mutate(longueur_m = as.numeric(st_length(geometry)))  
noeuds_lisse <- reseau_lisse %>% activate("nodes") %>% st_as_sf()

# Appartenance de chaque nœud à une composante (calculée sur reseau_lisse)
comp_lisse <- igraph::components(reseau_lisse %>% as_tbl_graph())
noeuds_lisse$composante  <- comp_lisse$membership
noeuds_lisse$taille_comp <- comp_lisse$csize[comp_lisse$membership]

# Identifier les nœuds hors composante géante
id_geante_lisse    <- which.max(comp_lisse$csize)
noeuds_hors_geante <- noeuds_lisse %>%
  filter(composante != id_geante_lisse)

# ── Joindre l'info composante aux arêtes via leurs nœuds extrémité ───────────
# Une arête est "hors géante" si au moins un de ses nœuds l'est
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
  
  tm_title("Arêtes exclues de la composante géante\n(~8% du réseau — Partie 7)") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_aretes_perdues,
  file.path(DIR_OUTPUT, "carte_aretes_perdues_partie7.png"),
  width = 3000, height = 2400, dpi = 300
)
if (FALSE) {
  tmap_mode("view")
  print(carte_aretes_perdues)
  tmap_mode("plot")
}

cat("✓ Carte des arêtes perdues sauvegardée\n\n")

# ==============================================================================
# PARTIE 9 : EXTRACTION DE LA COMPOSANTE GÉANTE
# ==============================================================================

pb_geante <- progress_bar$new(
  format = "  Filtrage   [:bar] :percent | durée : :elapsed",
  total  = length(noeuds_geante),
  clear  = FALSE,
  width  = 60
)

# Création du réseau avec uniquement la composante géante
reseau_rwanda <- reseau_lisse %>%
  activate("nodes") %>%
  filter({
    pb_geante$tick()
    row_number() %in% noeuds_geante
  }) %>%
  mutate(node_id = row_number())

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
# quand deux nœuds sont géométriquement confondus. On les élimine ici,
# avant le calcul des pentes (Partie 11) et des coûts (Partie 12),
# pour éviter toute propagation de NA en aval.
n_avant_filtre <- igraph::ecount(reseau_rwanda)

reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(longueur_m_brute = as.numeric(st_length(geometry))) %>%
  filter(longueur_m_brute > 0.5) %>%        # Seuil 0.5m
  select(-longueur_m_brute)                  # Colonne temporaire, on la retire

n_apres_filtre <- igraph::ecount(reseau_rwanda)
cat("Arêtes dégénérées supprimées :", n_avant_filtre - n_apres_filtre,
    "(", round((n_avant_filtre - n_apres_filtre)/n_avant_filtre*100, 1), "% du réseau)\n")
cat("Arêtes conservées            :", n_apres_filtre, "\n\n")

cat("✓ Réseau corrigé —",
    igraph::vcount(reseau_rwanda), "nœuds,",
    igraph::ecount(reseau_rwanda), "arêtes\n\n")


# ── Diagnostic complet de la fragmentation ───────────────────────────────────

composantes_finales <- igraph::components(reseau_rwanda %>% as_tbl_graph())
sizes <- sort(composantes_finales$csize, decreasing = TRUE) # trie les tailles des composantes connexes du réseau par ordre décroissant

cat("=== Diagnostic de fragmentation ===\n\n")

cat("Distribution des composantes :\n")
cat("  >= 1000 noeuds :", sum(sizes >= 1000), "composantes\n")
cat("  100–999 noeuds :", sum(sizes >= 100 & sizes < 1000), "composantes\n")
cat("  10–99  noeuds  :", sum(sizes >= 10  & sizes < 100),  "composantes\n")
cat("  2–9    noeuds  :", sum(sizes >= 2   & sizes < 10),   "composantes\n")
cat("  1      noeud   :", sum(sizes == 1),                  "composantes\n")

cat("\nTop 5 composantes (nb noeuds) :\n")
print(head(sizes, 5))

cat("Nombre de nœuds dans reseau_rwanda :", igraph::vcount(reseau_rwanda), "\n")
cat("Nombre d'arêtes dans reseau_rwanda :", igraph::ecount(reseau_rwanda), "\n")

# Répartition géographique : les fragments sont-ils concentrés dans une zone ?
# On récupère le centroïde de chaque composante pour cartographier la fragmentation
noeuds_sf <- reseau_rwanda %>% activate("nodes") %>% st_as_sf()
noeuds_sf$composante <- composantes_finales$membership

cat("Nombre de nœuds dans noeuds_sf :", nrow(noeuds_sf), "\n")
if (nrow(noeuds_sf) == 0) {
  cat("⚠ noeuds_sf est vide. Vérifie la construction de reseau_rwanda.\n")
} else {
  cat("✓ noeuds_sf est valide.\n")
}

# ==============================================================================
# PARTIE 9 BIS : CHARGEMENT DES ZONES D'USAGE DU SOL ET TAGUAGE DES ARÊTES
# ==============================================================================
# On charge les zones résidentielles, commerciales, industrielles et retail
# depuis le PBF, puis on détermine pour chaque arête du réseau si elle traverse
# une zone urbaine dense (résidentielle ou commerciale).
# Cette information sera utilisée en Partie 13 pour appliquer la pénalité
# urbaine aux poids lourds.

cat("=== PARTIE 9 BIS : Zones d'usage du sol ===\n")

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
zones_retail <- zones_urbaines %>%
  filter(landuse == "retail") %>%
  mutate(aire_km2 = as.numeric(st_area(geometry)) / 1e6) %>%
  filter(aire_km2 > 0.005)
    
cat("  Zones retail :", nrow(zones_retail), "\n\n")

# ── Taguage des arêtes : zone_urbaine = TRUE si l'arête traverse une zone dense ──
# On utilise le centroïde de chaque arête pour l'intersection (plus rapide
# qu'une intersection complète ligne × polygone sur 29 000 arêtes)
cat("  Taguage des arêtes du réseau...\n")

aretes_centroides <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  st_centroid(of_largest_polygon = FALSE) %>%
  mutate(arete_idx = row_number())

# Union des zones urbaines pour une seule opération d'intersection
zones_urbaines_union <- zones_urbaines %>%
  st_union() %>%
  st_make_valid()

# st_intersects retourne une liste de vecteurs d'indices — lengths() > 0 = intersection
in_urbain <- lengths(st_intersects(aretes_centroides, zones_urbaines_union)) > 0

# Intégration dans le réseau
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(zone_urbaine = in_urbain)

n_urbain <- sum(in_urbain)
cat("  Arêtes en zone urbaine :", n_urbain,
    "(", round(n_urbain / igraph::ecount(reseau_rwanda) * 100, 1), "% du réseau)\n\n")

# Stocker dans DuckDB pour usage dans la table des coûts (Partie 13)
duck_write(
  tibble(
    zone_urbaine    = c(TRUE, FALSE),
    label_zone      = c("urbaine", "rurale")
  ),
  "ref_zones"
)

cat("✓ Zones d'usage du sol chargées et arêtes taguées\n\n")

# ==============================================================================
# PARTIE 11 (DÉPLACÉE) : CALCUL DES PENTES POUR CHAQUE ARÊTE
# ==============================================================================
# Seul un changement sur le réseau (Parties 6-9) ou le DEM (Partie 5)
# nécessite de relancer cette partie.
#
# CACHE : les pentes sont sauvegardées dans outputs/pentes_cache.rds
# Le cache est invalidé automatiquement si le nombre d'arêtes change.
# Pour forcer un recalcul complet : supprimer outputs/pentes_cache.rds
# ==============================================================================

cat("=== PARTIE 11 : Calcul des pentes ===\n")

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
  
  calculer_pente_arete <- function(ligne_geom, dem, espacement = 100) {
    
    longueur <- as.numeric(st_length(ligne_geom))
    n_points <- max(2, floor(longueur / espacement))
    points   <- if (longueur < espacement * 2)
      st_line_sample(ligne_geom, n = 2, type = "regular")
    else
      st_line_sample(ligne_geom, n = n_points, type = "regular")
    
    points_sf   <- st_cast(points, "POINT")
    elevations  <- terra::extract(dem, vect(points_sf), method = "bilinear")
    elev_values <- elevations[, 2]
    
    if (any(is.na(elev_values)) || length(elev_values) < 2)
      return(list(slope_mean=0, elevation_gain=0, elevation_loss=0, rugosity=0))
    
    denivele_net   <- elev_values[length(elev_values)] - elev_values[1]
    slope_mean_pct <- (denivele_net / longueur) * 100
    differences    <- diff(elev_values)
    elevation_gain <- sum(differences[differences > 0], na.rm = TRUE)
    elevation_loss <- abs(sum(differences[differences < 0], na.rm = TRUE))
    rugosity       <- (elevation_gain + elevation_loss) / longueur
    
    list(slope_mean     = slope_mean_pct,
         elevation_gain = elevation_gain,
         elevation_loss = elevation_loss,
         rugosity       = rugosity)
  }
  
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
  
  pentes_df <- bind_rows(resultats_pentes)
  
  # ── Sauvegarde du cache ──────────────────────────────────────────────────────
  # On sauvegarde pentes_df + le nombre d'arêtes pour validation future
  saveRDS(
    list(pentes_df = pentes_df, n_aretes = n_aretes),
    CACHE_PENTES
  )
  cat("  ✓ Cache sauvegardé :", CACHE_PENTES, "\n\n")
}

# ── Intégration des pentes dans le réseau (toujours exécuté) ──────────────────
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
# PARTIE 10 : DÉFINITION DES NŒUDS D'ENTREPOSAGE
# ==============================================================================
# Les nœuds d'entreposage sont les origines/destinations du modèle de fret.
# Ils représentent des zones économiques importantes (hub, SEZ, frontières…).
# ATTENTION : ces données sont fictives mais réalistes ; les coordonnées sont
# approximatives. Remplacer par les vraies localisations si disponibles.

cat("=== PARTIE 10 : Création des nœuds d'entreposage ===\n")

# ── Entrepôts manuels  ────────────────────────────────────────────────────────
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

# ── Entrepôts depuis city/town OSM ───────────────────────────────────────────
# On exclut les villes déjà présentes dans les entrepôts manuels
# via une jointure spatiale avec buffer de 3km (même ville = même localité)
villes_osm_sf <- villes_osm %>%
  mutate(
    lon = st_coordinates(geometry)[,1],
    lat = st_coordinates(geometry)[,2]
  ) %>%
  st_drop_geometry()

# Conversion des entrepôts manuels en sf pour la comparaison spatiale
manuels_sf <- entreposages_manuels %>%
  st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
  st_transform(crs = 32735)

villes_osm_sf2 <- villes_osm %>%
  mutate(lon = st_coordinates(geometry)[,1],
         lat = st_coordinates(geometry)[,2])

# Filtrer uniquement les villes dans le territoire rwandais
# Évite que les villes des pays voisins se snappent toutes sur les mêmes nœuds frontières
villes_osm <- villes_osm %>%
  st_filter(rwanda_national %>% st_buffer(dist = 5000))
# Buffer de 5km pour garder les villes très proches de la frontière
cat("  Villes OSM dans ou proches du Rwanda :", nrow(villes_osm), "\n")

# Identifier les villes OSM non dupliquées avec les entrepôts manuels
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
  centroides_indus_sf <- centroides_indus %>%
    st_as_sf(coords = c("lon","lat"), crs = 32735)
  
  tous_existants <- bind_rows(
    entreposages_manuels %>% st_as_sf(coords = c("lon","lat"), crs = 32735),
    villes_nouvelles    %>% st_as_sf(coords = c("lon","lat"), crs = 32735)
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
    st_as_sf(coords = c("lon","lat"), crs = 32735)
  
  tous_existants2 <- bind_rows(
    entreposages_manuels      %>% st_as_sf(coords = c("lon","lat"), crs = 32735),
    villes_nouvelles          %>% st_as_sf(coords = c("lon","lat"), crs = 32735),
    zones_indus_nouvelles     %>% st_as_sf(coords = c("lon","lat"), crs = 32735)
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
entreposages_sf <- entreposages_fictifs %>%
  st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
  st_transform(crs = 32735)

# ── Accrochage (snapping) des entrepôts au réseau ────────────────────────────
# Les coordonnées des entrepôts ne tombent pas exactement sur le réseau routier.
# st_nearest_feature() trouve pour chaque entrepôt le nœud du réseau le plus proche.
noeuds_reseau <- reseau_rwanda %>% activate("nodes") %>% st_as_sf()

entreposages_avec_snap <- entreposages_sf %>%
  mutate(
    noeud_proche_id = st_nearest_feature(geometry, noeuds_reseau),
    # Calcul de la distance d'accrochage pour contrôle qualité
    # (une distance > 2km indiquerait un entrepôt mal positionné)
    distance_snap   = as.numeric(
      st_distance(geometry, noeuds_reseau[noeud_proche_id,], by_element = TRUE)
    )
  )

# Associe le noeud le plus proche à chaque entrepot ainsi que son type
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
    warehouse_type = if_else(                                              # Type de l'entrepôt (ex: "port", "aéroport", "centre logistique"), sinon NA
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

cat("✓", nrow(entreposages_sf), "entreposages intégrés au réseau\n\n")


# ==============================================================================
# PARTIE 12 : COÛTS GÉNÉRALISÉS — REQUÊTE SQL UNIQUE SUR TOUTE LA FLOTTE
# ==============================================================================
# Formules appliquées :
#   speed_kmh     = vitesse_base × facteur_pente
#   conso (L/100km) = conso_base × facteur_surface × (1 + slope × FACTEUR / 100)
#   cost_fuel     = (length_km × conso/100) × prix_carburant
#   cost_wear     = length_km × usure_usd_km
#   cost_time     = (length_km / speed_kmh) × valeur_temps
#   cost_total    = cost_fuel + cost_wear + cost_time  [coût généralisé]

cat("=== PARTIE 12 : Coûts généralisés ===\n")

aretes_df <- reseau_rwanda %>%
  activate("edges") %>% st_as_sf() %>% st_drop_geometry() %>%
  mutate(arete_id = row_number())
duck_write(aretes_df, "aretes_base")

duck_query("
  CREATE OR REPLACE TABLE aretes_couts_tous AS

  WITH

  -- Étape 1 : combinaison de chaque arête avec chaque véhicule de la flotte
  -- CROSS JOIN : N_arêtes × N_véhicules lignes (ex : 15 000 × 3 = 45 000 lignes)
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
  avec_couts AS (
    SELECT
      *,
      NULLIF(longueur_m, 0) / 1000.0                                    AS length_km,
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
    (length_km / speed_kmh) * valeur_temps          AS cost_time_usd,
    travel_time_h,
    -- Coût par tkm avec pénalité urbaine sur le temps et l'usure
    (fuel_consumption_L * prix_carburant
      + length_km * usure_usd_km * facteur_urbain_applique
      + (length_km / speed_kmh) * valeur_temps * facteur_urbain_applique)
      / NULLIF(length_km, 0)
      / NULLIF(capacite_tonnes, 0)                            AS cost_per_tkm
  FROM avec_couts
")

# Stats récapitulatives par véhicule depuis DuckDB
stats_flotte <- duck_query("
  SELECT
    vehicule_id,
    vehicule_nom,
    ROUND(AVG(cost_per_tkm), 3)                                AS cout_par_tkm_moyen,
    ROUND(AVG(cost_fuel_usd / NULLIF(cost_per_tkm * length_km, 0)) * 100, 1) AS part_carburant_pct,
    ROUND(AVG(cost_time_usd / NULLIF(cost_per_tkm * length_km, 0)) * 100, 1) AS part_temps_pct,
    ROUND(AVG(cost_wear_usd / NULLIF(cost_per_tkm * length_km, 0)) * 100, 1) AS part_usure_pct
  FROM aretes_couts_tous
  GROUP BY vehicule_id, vehicule_nom
  ORDER BY cout_par_tkm_moyen
")
print(stats_flotte)

# Export Parquet de la table consolidée
dbExecute(con, paste0(
  "COPY (SELECT * FROM aretes_couts_tous) TO '",
  file.path(DIR_OUTPUT, "aretes_couts_tous_vehicules.parquet"),
  "' (FORMAT PARQUET)"
))
# Le chiffre qu'il y a à la suite de la commande précédente correspond au nombre de lignes bien exportées

# ── Réintégration dans sfnetworks pour le véhicule de référence ───────────────
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
arete_ids_duckdb <- duck_query(glue::glue("
  SELECT arete_id FROM aretes_couts_tous
  WHERE vehicule_id = '{VEHICULE_REFERENCE}'
  ORDER BY arete_id
"))
cat("arete_id max dans DuckDB :", max(arete_ids_duckdb$arete_id), "\n")
cat("Nb arêtes réseau         :", n_reseau, "\n")
cat("Correspondance parfaite  :", max(arete_ids_duckdb$arete_id) == n_reseau, "\n")

# ==============================================================================
# PARTIE 13 : GRAPHE MULTI-MODAL AVEC TRANSBORDEMENTS AUX ENTREPÔTS
# ==============================================================================
# Structure du graphe en couches :
#
#   Couche camionnette  : nœuds  1        →  N_noeuds
#   Couche camion_moyen : nœuds  N+1      →  2×N_noeuds
#   Couche camion_lourd : nœuds  2×N+1    →  3×N_noeuds
#
#   Arêtes intra-couche : routes normales avec coûts propres à chaque véhicule
#   Arêtes inter-couches: transbordements aux entrepôts uniquement (coût fixe)
#
# Le Dijkstra sur ce graphe étendu trouve automatiquement la combinaison
# optimale de véhicules pour chaque paire OD.

cat("=== PARTIE 13 : Construction du graphe multi-modal ===\n")

# ── Paramètres de base ────────────────────────────────────────────────────────
n_vehicules <- nrow(VEHICULES_IDS)
graphe_base <- reseau_rwanda %>% as_tbl_graph()
n_noeuds    <- igraph::vcount(graphe_base)

# Fonction de remappage : nœud n dans la couche du véhicule v_idx
# Ex : v_idx=2 (camion_moyen), n=150 → nœud 150 + N_noeuds dans le graphe étendu
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
couts_transb       <- duck_query("SELECT * FROM couts_transbordement")
warehouse_nodes_base <- which(igraph::V(graphe_base)$is_warehouse)

cat("\n  Entrepôts disponibles pour transbordement :",
    length(warehouse_nodes_base), "\n")

edges_transb <- list()
k <- 0

for (wh_node in warehouse_nodes_base) {
  for (r in seq_len(nrow(couts_transb))) {
    
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
all_edges_mm <- bind_rows(c(edges_intra, edges_transb))

# ── Table de mapping : arête multi-modale → arête physique + véhicule ─────────
# Nécessaire pour l'affectation All-or-Nothing en Partie 20
# Chaque arête intra-couche du graphe multi-modal est associée à :
#   - son indice dans le réseau physique (arete_physique_idx)
#   - son véhicule (vehicule_id)
#   - son type (route ou transbordement)

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

# ── Vecteurs d'accès direct pour le remappage (Partie 20) ─────────────────────
# Indexés par idx_mm → accès en O(1) au lieu de O(n) par recherche dans le tibble
# Taille = n_vehicules × n_aretes_physiques + n_transbordements
max_idx_mm <- max(mapping_aretes_mm$idx_mm)

lookup_type     <- character(max_idx_mm)
lookup_physique <- integer(max_idx_mm)
lookup_vehicule <- character(max_idx_mm)

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
vertices_mm <- tibble(
  name      = seq_len(n_noeuds * n_vehicules),
  node_base = rep(seq_len(n_noeuds), n_vehicules),
  vehicule  = rep(VEHICULES_IDS$vehicule_id, each = n_noeuds)
)

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
# PARTIE 14 : CARTES PAR VÉHICULE — PILOTÉES PAR LA TABLE params_flotte
# ==============================================================================

cat("=== PARTIE 14 : Cartes de coûts par véhicule ===\n")

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

# ==============================================================================
# PARTIE 15 : MATRICE ORIGINE-DESTINATION STOCKÉE DANS DUCKDB
# ==============================================================================
# La matrice OD donne le coût, la distance et le temps de trajet optimal entre
# chaque paire d'entrepôts. Elle est calculée par l'algorithme de Dijkstra
# (igraph) et stockée dans DuckDB en FORMAT LONG plutôt qu'en matrices carrées R.
#
# Format long (avantages sur les matrices carrées) :
#   - Requêtable : SELECT ... WHERE nom_origine = 'Kigali' ORDER BY cout_usd
#   - Filtrable : ignorer les paires non connectées sans "trous" dans la matrice
#   - Joinable : directement utilisable dans le modèle gravitaire (Partie 18)
#   - Compact : ne stocke que les paires connectées (pas de zéros inutiles)

cat("=== PARTIE 15 : Matrice OD dans DuckDB ===\n")

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

# Indices igraph des nœuds d'entreposage (igraph indexe de 1 à N)
warehouse_node_ids <- which(igraph::V(graphe_igraph)$is_warehouse)

# ── Calcul des plus courts chemins multi-modaux par Dijkstra ──────────────────
# Pour chaque paire d'entrepôts (i,j) :
#   1. Tester tous les véhicules de départ possibles en i
#   2. Tester tous les véhicules d'arrivée possibles en j
#   3. Le chemin optimal dans le graphe multi-modal donne automatiquement
#      la meilleure combinaison, y compris avec transbordements intermédiaires

od_rows <- list()
idx     <- 0

for (i in seq_along(warehouse_nodes_base)) {
  
  sources_i <- sapply(seq_len(n_vehicules),
                      function(v) node_multi(v, warehouse_nodes_base[i]))
  
  # Tous les nœuds destination (toutes couches × tous entrepôts) en une seule passe
  targets_all <- as.vector(sapply(
    seq_len(n_vehicules),
    function(v) node_multi(v, warehouse_nodes_base)
  ))
  
  # Une seule passe depuis i vers tous les j × toutes les couches
  dists_all <- igraph::distances(
    graphe_multimodal,
    v       = sources_i,
    to      = targets_all,
    weights = igraph::E(graphe_multimodal)$weight
  )
  
  for (j in seq_along(warehouse_nodes_base)) {
    if (i == j) next
    
    # Colonnes correspondant à l'entrepôt j dans toutes les couches véhicules
    cols_j   <- j + (seq_len(n_vehicules) - 1) * length(warehouse_nodes_base)
    min_cout <- min(dists_all[, cols_j], na.rm = TRUE)
    if (is.infinite(min_cout)) next
    
    # Identifier la meilleure combinaison de véhicules
    best_idx  <- which(dists_all[, cols_j] == min_cout, arr.ind = TRUE)[1, ]
    best_from <- sources_i[best_idx[1]]
    best_to   <- targets_all[cols_j[best_idx[2]]]
    
    # Récupérer le chemin réel pour distance et temps
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
cat("  Paires connectées            :", od_stats$n_paires, "\n")
cat("  Coût moyen                   :", od_stats$cout_moyen_usd, "USD\n")
cat("  Paires avec transbordement   :", od_stats$paires_avec_transbordement, "\n")
cat("  Transbordements moyens/trajet:", od_stats$transbordements_moyens, "\n\n")


# ==============================================================================
# PARTIE 16 : EXPORT VIA DUCKDB (PARQUET + CSV + GEOPACKAGE)
# ==============================================================================
# COPY TO est la commande DuckDB pour exporter des tables vers des fichiers.
# Avantages sur write.csv() :
#   - Parquet : format colonnaire compressé (~10× plus compact que CSV)
#   - Vitesse : écriture multithread native de DuckDB
#   - SQL : filtrer/transformer les données à l'export sans créer de df R intermédiaire

cat("=== PARTIE 16 : Export via DuckDB ===\n")

# ── Récupération des coûts de tous les véhicules depuis DuckDB ────────────────
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


# ==============================================================================
# PARTIE 17 : TABLE INPUT-OUTPUT DU RWANDA
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
# NOTE : La table IO officielle du Rwanda (NISR) n'est pas disponible librement.
# Ces données sont fictives mais calibrées sur la structure économique connue.
# Pour utiliser les données réelles :
#   1. Télécharger sur https://www.statistics.gov.rw (NISR)
#   2. Charger avec readxl::read_excel("io_rwanda.xlsx")
#   3. Remplacer les matrices A et production_totale ci-dessous

cat("=== PARTIE 17 : Table Input-Output dans DuckDB ===\n")

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
# A %*% X = produit matriciel : vecteur des intrants totaux par secteur
conso_interm   <- as.vector(A %*% production_totale)
# Valeur ajoutée = production - consommations intermédiaires (travail + capital)
valeur_ajoutee <- production_totale - conso_interm
# Demande finale ≈ 85% de la valeur ajoutée (ménages + investissement + exports nets)
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
A_long <- as.data.frame(A) %>%
  rownames_to_column("secteur_input") %>%
  pivot_longer(-secteur_input, names_to="secteur_output", values_to="coef_a")
duck_write(A_long, "matrice_a_long")

# ── Multiplicateurs de Leontief ───────────────────────────────────────────────
# L = (I - A)^(-1) : la matrice inverse de Leontief
# L[i,j] = augmentation de production du secteur i nécessaire pour fournir 1 USD
# de demande finale supplémentaire dans le secteur j (effets directs + indirects)
# solve() calcule l'inverse matricielle
leontief <- solve(diag(N_SECTEURS) - A)
leontief_long <- as.data.frame(leontief) %>%
  setNames(SECTEURS) %>%
  mutate(secteur_demande = SECTEURS) %>%
  pivot_longer(-secteur_demande, names_to="secteur_production", values_to="multiplicateur")
duck_write(leontief_long, "multiplicateurs_leontief")

cat("✓ Table IO + multiplicateurs de Leontief chargés dans DuckDB\n\n")


# ==============================================================================
# PARTIE 18 : GÉNÉRATION DES OFFRES ET DEMANDES PAR ZONE
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

cat("=== PARTIE 18 : Offres et demandes par zone ===\n")

# Profils d'offre (ce que chaque type de zone produit et offre au marché)
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

# Part du PIB qui "voyage" entre zones (le reste est consommé localement)
# 35% est une hypothèse conservatrice pour un pays enclavé comme le Rwanda
PART_ECHANGEABLE <- 0.35
echelle_offre    <- sum(production_totale) * PART_ECHANGEABLE
echelle_demande  <- sum(demande_finale)    * PART_ECHANGEABLE

# Graine différente de la Partie 4 pour des bruits indépendants
set.seed(456)

# Génération des matrices offre et demande (lignes = zones, colonnes = secteurs)
offre_zones   <- matrix(0, n_warehouses, N_SECTEURS,
                        dimnames=list(noeuds_entreposage$warehouse_name, SECTEURS))
demande_zones <- matrix(0, n_warehouses, N_SECTEURS,
                        dimnames=list(noeuds_entreposage$warehouse_name, SECTEURS))

# ── Calcul de la composition d'usage du sol autour de chaque entrepôt ─────────
# Pour chaque entrepôt, on calcule la part de chaque landuse dans un buffer
# de 2km. Cette composition module les profils d'offre/demande.

cat("  Calcul de la composition landuse par zone...\n")

# Union de toutes les zones par type pour les intersections
zones_all <- bind_rows(
  zones_urbaines    %>% select(geometry),
  zones_industrielles %>% select(geometry)
) %>% st_make_valid()

# Buffer de 2km autour de chaque entrepôt
entreposages_buffer <- entreposages_sf %>%
  st_buffer(dist = 2000) %>%
  mutate(zone_idx = row_number())

# Calcul des aires de chaque type de landuse dans chaque buffer
calc_part_landuse <- function(buffer_geom, zones_type) {
  inter <- tryCatch(
    st_intersection(buffer_geom, zones_type %>% st_union()),
    error = function(e) NULL
  )
  if (is.null(inter) || length(inter) == 0) return(0)
  as.numeric(st_area(inter)) / as.numeric(st_area(buffer_geom))
}

part_urbain    <- numeric(n_warehouses)
part_industriel <- numeric(n_warehouses)

for (i in seq_len(n_warehouses)) {
  buf <- entreposages_buffer[i, ]$geometry
  part_urbain[i]     <- calc_part_landuse(buf, zones_urbaines)
  part_industriel[i] <- calc_part_landuse(buf, zones_industrielles)
  if (i %% 5 == 0) cat("  Landuse par zone :", round(i/n_warehouses*100), "%\n")
}

cat("✓ Composition landuse calculée\n\n")

# ── Modification des profils selon la composition landuse ─────────────────────
# Principe : plus une zone est industrielle, plus son profil d'offre favorise
# l'Industrie et la Construction ; plus elle est urbaine, plus elle favorise
# le Commerce et les Services.

for (i in 1:n_warehouses) {
  nom_zone  <- noeuds_entreposage$warehouse_name[i]
  type_zone <- noeuds_entreposage$warehouse_type[i]
  taille    <- TAILLE_ZONE[nom_zone]
  if (is.na(taille)) taille <- 0.10
  
  bruit_o <- runif(N_SECTEURS, 0.80, 1.20)
  bruit_d <- runif(N_SECTEURS, 0.80, 1.20)
  
  # Profil de base selon le type de zone
  profil_o <- PROFILS_OFFRE[[type_zone]]
  profil_d <- PROFILS_DEMANDE[[type_zone]]
  
  # ── Ajustement selon la part industrielle ──────────────────────────────────
  # Chaque point de part industrielle renforce Industrie et Construction
  # et réduit proportionnellement Commerce et Services
  p_ind <- min(part_industriel[i], 0.5)   # Plafond à 50% pour éviter les extrêmes
  if (p_ind > 0.05) {
    bonus_ind <- p_ind * 0.3   # Amplitude max du bonus
    profil_o["Industrie"]    <- profil_o["Industrie"]    + bonus_ind
    profil_o["Construction"] <- profil_o["Construction"] + bonus_ind * 0.5
    profil_o["Commerce"]     <- profil_o["Commerce"]     - bonus_ind * 0.8
    profil_o["Services"]     <- profil_o["Services"]     - bonus_ind * 0.7
    # Renormalisation pour que la somme reste à 1
    profil_o <- pmax(profil_o, 0.01)
    profil_o <- profil_o / sum(profil_o)
  }
  
  # ── Ajustement selon la part urbaine ──────────────────────────────────────
  p_urb <- min(part_urbain[i], 0.8)
  if (p_urb > 0.1) {
    bonus_urb <- p_urb * 0.2
    profil_d["Commerce"]  <- profil_d["Commerce"]  + bonus_urb
    profil_d["Services"]  <- profil_d["Services"]  + bonus_urb * 0.8
    profil_d["Industrie"] <- profil_d["Industrie"] - bonus_urb * 0.9
    profil_d["Construction"] <- profil_d["Construction"] - bonus_urb * 0.9
    profil_d <- pmax(profil_d, 0.01)
    profil_d <- profil_d / sum(profil_d)
  }
  
  offre_zones[i,]   <- profil_o * taille * echelle_offre   / sum(TAILLE_ZONE) * bruit_o
  demande_zones[i,] <- profil_d * taille * echelle_demande / sum(TAILLE_ZONE) * bruit_d
}

for (i in 1:n_warehouses) {
  nom_zone  <- noeuds_entreposage$warehouse_name[i]
  type_zone <- noeuds_entreposage$warehouse_type[i]
  taille    <- TAILLE_ZONE[nom_zone]
  if (is.na(taille)) taille <- 0.10   # Taille par défaut pour les zones inconnues
  
  # Bruit multiplicatif ±20% pour simuler l'hétérogénéité réelle entre zones similaires
  # runif(N, 0.8, 1.2) génère N valeurs uniformément distribuées entre 0.8 et 1.2
  bruit_o <- runif(N_SECTEURS, 0.80, 1.20)
  bruit_d <- runif(N_SECTEURS, 0.80, 1.20)
  
  # Volume final = profil sectoriel × taille relative × échelle nationale × bruit
  # sum(TAILLE_ZONE) normalise pour que la somme des parts soit cohérente avec l'échelle
  offre_zones[i,]   <- PROFILS_OFFRE[[type_zone]]   * taille * echelle_offre   / sum(TAILLE_ZONE) * bruit_o
  demande_zones[i,] <- PROFILS_DEMANDE[[type_zone]] * taille * echelle_demande / sum(TAILLE_ZONE) * bruit_d
}

# ── Stockage dans DuckDB en format long ───────────────────────────────────────
# Format long (1 ligne = 1 zone × 1 secteur) plus adapté aux jointures SQL
# que les matrices R carrées (1 ligne = 1 zone, 1 colonne = 1 secteur)
offre_long_df <- as.data.frame(offre_zones) %>%
  rownames_to_column("zone") %>%
  pivot_longer(-zone, names_to="secteur", values_to="offre_musd")
duck_write(offre_long_df, "offre_zones")

demande_long_df <- as.data.frame(demande_zones) %>%
  rownames_to_column("zone") %>%
  pivot_longer(-zone, names_to="secteur", values_to="demande_musd")
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
# PARTIE 19 : MODÈLE GRAVITAIRE DES ÉCHANGES
# ==============================================================================

cat("=== PARTIE 19 : Modèle gravitaire des échanges ===\n\n")

# Le modèle gravitaire estime les flux commerciaux bilatéraux :
#
#   T_ij^s = K^s * O_i^s * D_j^s * F_ij
#
# où :
#   T_ij^s  = flux du secteur s de la zone i vers la zone j (M USD)
#   O_i^s   = offre du secteur s en zone i
#   D_j^s   = demande du secteur s en zone j
#   F_ij    = facteur de friction = C_ij^(-beta)
#   C_ij    = coût généralisé de transport (USD) entre i et j
#   beta    = paramètre de friction (sensibilité au coût)
#   K^s     = constante de calibration par secteur

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

# ── Reconstruction de la matrice coputs en R carrées ──────────────────────────
matrice_couts     <- matrix(0, n_warehouses, n_warehouses,
                            dimnames = list(noeuds_entreposage$warehouse_name, noeuds_entreposage$warehouse_name))

for (r in seq_len(nrow(od_long))) {
  i <- od_long$id_origine[r]; j <- od_long$id_destination[r]
  matrice_couts[i, j]     <- od_long$cout_usd[r]
}

# --- Préparation de la matrice de coûts ---
C_ij <- matrice_couts
diag(C_ij) <- NA          # Pas d'échange intrazone
C_ij[C_ij == 0] <- NA     # Zones non connectées → pas de flux

# ── Récupération des coûts pré-frontière depuis DuckDB ────────────────────────
couts_prebordure <- duck_query("SELECT * FROM couts_prebordure")

# ── Identification des entrepôts frontière et de leur pays ────────────────────
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
  
  if (type_zone != "frontiere") next
  
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
flux_total      <- matrix(0, nrow = n_warehouses, ncol = n_warehouses,
                          dimnames = list(noeuds_entreposage$warehouse_name,
                                          noeuds_entreposage$warehouse_name))

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
  flux_brut <- outer(O_s, D_s) * friction
  diag(flux_brut) <- 0
  
  # Calibration : normalisation pour respecter les totaux
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

# Top 10 des paires OD
flux_total_long <- as.data.frame(flux_total) %>%
  rownames_to_column("Origine") %>%
  pivot_longer(-Origine, names_to = "Destination", values_to = "flux_musd") %>%
  filter(flux_musd > 0.01) %>%
  arrange(desc(flux_musd))

cat("\nTop 10 des flux commerciaux bilatéraux (M USD):\n")
print(head(flux_total_long, 10))
cat("\n")
cat("✓ Flux total modélisé:", round(sum(flux_total), 1), "M USD\n")
cat("  Nombre de paires actives:", nrow(flux_total_long), "\n\n")


# ==============================================================================
# PARTIE 20 : MODÉLISATION DU FRET ET AFFECTATION AU RÉSEAU
# ==============================================================================

cat("=== PARTIE 20 : Modélisation du fret et affectation au réseau ===\n")

# --- Étape 1 : Conversion des flux monétaires en tonnes ---
cat("Conversion des flux en tonnes...\n")

flux_tonnes_total <- matrix(0, nrow = n_warehouses, ncol = n_warehouses,
                            dimnames = list(noeuds_entreposage$warehouse_name,
                                            noeuds_entreposage$warehouse_name))

# Pour chaque secteur, convertir M USD → tonnes
for (s in SECTEURS) {
  flux_tonnes_total <- flux_tonnes_total + flux_gravitaire[[s]] * TONNES_PAR_musd[s]
}

tonnage_total <- sum(flux_tonnes_total)
cat("  Tonnage total modélisé:",
    format(round(tonnage_total), big.mark = " "), "tonnes\n\n")

# --- Étape 2 : Affectation All-or-Nothing multi-modale au réseau ---
# Pour chaque paire OD, le chemin optimal dans le graphe multi-modal
# est décomposé en arêtes physiques avec leur véhicule associé.
# Le volume est affecté par arête ET par véhicule.

cat("Affectation du fret au réseau (All-or-Nothing multi-modal)...\n")

SEUIL_FLUX_TONNES <- 50

# Matrice de volumes par arête physique ET par véhicule
# Dimensions : N_aretes_physiques × N_vehicules
volume_trafic_mm <- matrix(
  0,
  nrow = n_aretes_physiques,
  ncol = n_vehicules,
  dimnames = list(NULL, VEHICULES_IDS$vehicule_id)
)

paires_traitees       <- 0
paires_non_connectees <- 0

for (i in seq_len(n_warehouses)) {
  
  sources_i <- sapply(seq_len(n_vehicules),
                      function(v) node_multi(v, warehouse_nodes_base[i]))
  
  targets_all <- as.vector(sapply(
    seq_len(n_vehicules),
    function(v) node_multi(v, warehouse_nodes_base)
  ))
  
  dists_all <- igraph::distances(
    graphe_multimodal,
    v       = sources_i,
    to      = targets_all,
    weights = igraph::E(graphe_multimodal)$weight
  )
  
  for (j in seq_len(n_warehouses)) {
    if (i == j) next
    
    flux_ij <- flux_tonnes_total[i, j]
    if (flux_ij <= SEUIL_FLUX_TONNES) next
    
    cols_j   <- j + (seq_len(n_vehicules) - 1) * n_warehouses
    min_cout <- min(dists_all[, cols_j], na.rm = TRUE)
    if (is.infinite(min_cout)) {
      paires_non_connectees <- paires_non_connectees + 1
      next
    }
    
    best_idx  <- which(dists_all[, cols_j] == min_cout, arr.ind = TRUE)[1, ]
    best_from <- sources_i[best_idx[1]]
    best_to   <- targets_all[cols_j[best_idx[2]]]
    
    # Récupérer le chemin détaillé (indices d'arêtes dans le graphe multi-modal)
    path_obj   <- igraph::shortest_paths(
      graphe_multimodal,
      from    = best_from,
      to      = best_to,
      weights = igraph::E(graphe_multimodal)$weight,
      output  = "epath"
    )
    edges_path_mm <- as.integer(path_obj$epath[[1]])
    
    if (length(edges_path_mm) == 0) {
      paires_non_connectees <- paires_non_connectees + 1
      next
    }
    
    # ── Remappage vectorisé : arête multi-modale → arête physique + véhicule ──
    for (edge_mm in edges_path_mm) {
      if (edge_mm > max_idx_mm) next
      if (lookup_type[edge_mm] == "transbordement" || lookup_type[edge_mm] == "") next
      idx_phys <- lookup_physique[edge_mm]
      veh_id   <- lookup_vehicule[edge_mm]
      if (idx_phys < 1 || idx_phys > n_aretes_physiques || veh_id == "") next
      col_veh  <- which(VEHICULES_IDS$vehicule_id == veh_id)
      if (length(col_veh) == 0) next
      volume_trafic_mm[idx_phys, col_veh] <-
        volume_trafic_mm[idx_phys, col_veh] + flux_ij
    }
    
    paires_traitees <- paires_traitees + 1
  }
  
  if (i %% 3 == 0) cat("  Zone", i, "/", n_warehouses, "traitée\n")
}

# Volume total toutes couches confondues (pour la cartographie)
volume_trafic <- rowSums(volume_trafic_mm)

cat("✓ Affectation multi-modale terminée\n")
cat("  Paires traitées      :", paires_traitees, "\n")
cat("  Paires non connectées:", paires_non_connectees, "\n\n")

# ── Statistiques de répartition modale ────────────────────────────────────────
cat("Répartition modale du trafic (tonnes × km) :\n")
for (v in seq_len(n_vehicules)) {
  veh_id  <- VEHICULES_IDS$vehicule_id[v]
  veh_nom <- VEHICULES_IDS$nom[v]
  
  # Récupérer les longueurs des arêtes physiques
  longueurs_km <- reseau_rwanda %>%
    activate("edges") %>%
    as_tibble() %>%
    pull(length_km)
  
  tkm_veh <- sum(volume_trafic_mm[, v] * longueurs_km, na.rm = TRUE)
  pct      <- round(tkm_veh / sum(volume_trafic * longueurs_km, na.rm = TRUE) * 100, 1)
  cat("  ", veh_nom, ":", format(round(tkm_veh), big.mark=" "), "t×km (", pct, "%)\n")
}
cat("\n")

# --- Étape 3 : Intégration des volumes au réseau ---
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    volume_tonnes          = volume_trafic,
    # Volume par type de véhicule (utile pour analyses modales)
    volume_camionnette     = volume_trafic_mm[, "camionnette"],
    volume_camion_moyen    = volume_trafic_mm[, "camion_moyen"],
    volume_camion_lourd    = volume_trafic_mm[, "camion_lourd"],
    # Part de chaque véhicule sur chaque arête
    part_camion_lourd      = if_else(
      volume_tonnes > 0,
      round(volume_camion_lourd / volume_tonnes * 100, 1),
      0
    ),
    classe_trafic = case_when(
      volume_tonnes == 0       ~ "Aucun",
      volume_tonnes < 500      ~ "Très faible",
      volume_tonnes < 5000     ~ "Faible",
      volume_tonnes < 25000    ~ "Moyen",
      volume_tonnes < 100000   ~ "Élevé",
      TRUE                     ~ "Très élevé"
    ),
    classe_trafic = factor(classe_trafic,
                           levels = c("Aucun", "Très faible", "Faible",
                                      "Moyen", "Élevé", "Très élevé"))
  )

# Statistiques de trafic
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

# Zones les plus actives
cat("Activité fret par zone (origines + destinations):\n")
volumes_par_zone <- tibble(
  Zone          = noeuds_entreposage$warehouse_name,
  Type          = noeuds_entreposage$warehouse_type,
  Offre_kt      = round(rowSums(flux_tonnes_total) / 1000, 1),
  Demande_kt    = round(colSums(flux_tonnes_total) / 1000, 1)
) %>%
  mutate(Total_kt = Offre_kt + Demande_kt) %>%
  arrange(desc(Total_kt))

print(volumes_par_zone)
cat("\n")

# === DIAGNOSTIC PARTIE 20 ===

cat("=== Diagnostic de connectivité des entrepôts ===\n\n")

# 1. Vérifier les composantes connexes du graphe
composantes <- igraph::components(graphe_igraph)
cat("Nombre de composantes connexes:", composantes$no, "\n")
cat("Taille de la plus grande composante:", max(composantes$csize), "nœuds\n\n")

# 2. Pour chaque entrepôt, vérifier dans quelle composante il se trouve
cat("Composante de chaque entrepôt:\n")
for (i in seq_along(warehouse_node_ids)) {
  node_id <- warehouse_node_ids[i]
  comp    <- composantes$membership[node_id]
  taille  <- composantes$csize[comp]
  cat("  [", i, "]", noeuds_entreposage$warehouse_name[i],
      "→ composante", comp, "(", taille, "nœuds)\n")
}

# 3. Vérifier les distances entre toutes les paires
cat("\nTest de connectivité paire par paire:\n")
for (i in 1:min(5, length(warehouse_node_ids))) {
  for (j in 1:min(5, length(warehouse_node_ids))) {
    if (i == j) next
    dist_ij <- igraph::distances(
      graphe_igraph,
      v    = warehouse_node_ids[i],
      to   = warehouse_node_ids[j],
      weights = igraph::E(graphe_igraph)$weight
    )
    cat("  ", i, "→", j, ":",
        ifelse(is.infinite(dist_ij), "NON CONNECTÉ", paste(round(dist_ij, 2), "USD")), "\n")
  }
}

# ==============================================================================
# PARTIE 21 : VISUALISATIONS DES ÉCHANGES MODÉLISÉS
# ==============================================================================

cat("=== PARTIE 21 : Visualisations des échanges modélisés ===\n\n")

# --- Préparation des couches spatiales ---

# Forcer les colonnes numériques explicitement
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

coords_zones_sf <- coords_zones_sf %>%
  mutate(match_idx = match(warehouse_name, noeuds_entreposage$warehouse_name)) %>%
  filter(!is.na(match_idx)) %>%
  arrange(match_idx)

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

# Construire les lignes OD
desire_list <- list()
k <- 0

for (i in 1:n_warehouses) {
  for (j in 1:n_warehouses) {
    if (i == j) next
    flux_ij <- flux_total[i, j]
    if (is.na(flux_ij) || flux_ij < SEUIL_DESIRE) next
    
    idx_i <- which(coords_zones_sf$warehouse_name ==
                     noeuds_entreposage$warehouse_name[i])
    idx_j <- which(coords_zones_sf$warehouse_name ==
                     noeuds_entreposage$warehouse_name[j])
    
    if (length(idx_i) == 0 || length(idx_j) == 0) next
    
    pt_i <- st_coordinates(coords_zones_sf[idx_i, ])
    pt_j <- st_coordinates(coords_zones_sf[idx_j, ])
    
    if (any(is.na(pt_i)) || any(is.na(pt_j))) next
    
    k <- k + 1
    desire_list[[k]] <- list(
      Origine     = noeuds_entreposage$warehouse_name[i],
      Destination = noeuds_entreposage$warehouse_name[j],
      flux_musd   = as.numeric(flux_ij),                            # 
      flux_kt     = as.numeric(flux_tonnes_total[i, j] / 1000),    # 
      geom        = st_linestring(rbind(
        c(pt_i[1, "X"], pt_i[1, "Y"]),
        c(pt_j[1, "X"], pt_j[1, "Y"])
      ))
    )
  }
}

if (k > 0) {
  
  # Création de desire_sf AVANT carte_od
  desire_sf <- st_sf(
    Origine     = sapply(desire_list, `[[`, "Origine"),
    Destination = sapply(desire_list, `[[`, "Destination"),
    flux_musd   = as.numeric(sapply(desire_list, `[[`, "flux_musd")),
    flux_kt     = as.numeric(sapply(desire_list, `[[`, "flux_kt")),
    geometry    = st_sfc(lapply(desire_list, `[[`, "geom"), crs = 32735)
  ) %>%
    mutate(flux_log = as.numeric(log10(flux_musd + 0.01)))
  
  cat("  Desire lines créées:", nrow(desire_sf), "\n")
  cat("  flux_musd range:", round(min(desire_sf$flux_musd), 3),
      "-", round(max(desire_sf$flux_musd), 3), "\n")
  
  carte_od <- fond_carte() +
    
    tm_shape(desire_sf) +
    tm_lines(
      col = "flux_musd",
      col.scale = tm_scale_intervals(style="quantile", n=5, values=PALETTE_FLUX_OD),
      col.legend = tm_legend(title = "Flux commercial\n(M USD)"),
      lwd = "flux_log",
      lwd.scale = tm_scale(values.range = c(0.5, 5)),
      lwd.legend = tm_legend(show = FALSE),
      col_alpha = 0.65
    ) +
    
    tm_shape(coords_zones_sf) +
    tm_dots(
      fill = "warehouse_type",
      fill.scale = tm_scale(values = PALETTE_ZONE_TYPE),
      fill.legend = tm_legend(title = "Type de zone"),
      size = 0.45
    ) +
    
    tm_title("Flux Commerciaux Interzonaux\nModèle gravitaire - Rwanda") +
    tm_layout(legend.outside = TRUE, frame = TRUE) +
    tm_scalebar(position = c("left", "bottom")) +
    tm_compass(position = c("right", "top"))
  
  tmap_save(carte_od,
            file.path(DIR_OUTPUT,"carte_flux_od.png"),
            width = 3000, height = 2400, dpi = 300)
  cat("✓ Carte flux OD sauvegardée (", k, "flux représentés)\n")
  
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
# EXPORT DES DONNÉES DE FRET
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