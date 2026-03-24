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
#  Pour activer l'onglet Git dans RStudio : File -> Open Project
################################################################################

# Authentification Git via le Personal Access Token stocké en variable d'env.
# Sys.getenv() lit la variable d'environnement GITHUB_PAT sans l'exposer
# dans le code source (bonne pratique de sécurité).
token <- Sys.getenv("GITHUB_PAT")
system("git config --global credential.helper '!f() { echo \"username=token\"; echo \"password=$GITHUB_PAT\"; }; f'")


# ==============================================================================
# PARTIE 0 : INSTALLATION ET CHARGEMENT DES PACKAGES
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
  "scales"         # Mise à l'échelle et formatage pour ggplot2 (rescale, percent…)
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

# Graine aléatoire pour la reproductibilité des données fictives générées en Partie 17
set.seed(123)

cat("✓ Tous les packages sont chargés\n\n")


# ==============================================================================
# PARTIE 0 BIS : CONNEXION DUCKDB
# ==============================================================================
# DuckDB crée un fichier .duckdb sur disque (comme SQLite) qui persiste entre
# les sessions. Avantages par rapport à des data.frames R :
#   1. Reprendre l'analyse sans tout recalculer (les tables sont déjà là)
#   2. Interroger les résultats directement en SQL depuis R ou un client externe
#   3. Exporter en Parquet/CSV via COPY TO sans charger toute la donnée en RAM
#   4. Exécuter des jointures et agrégations sur de gros volumes hors-mémoire

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
# PARTIE 1 : DÉFINITION DES PARAMÈTRES DU MODÈLE
# ==============================================================================

cat("=== PARTIE 1 : Définition des paramètres ===\n")

# ── Paramètres du véhicule de référence ───────────────────────────────────────
# On modélise un camion moyen (5-10 tonnes) représentatif du fret routier rwandais.
# Les facteurs d'ajustement selon la surface traduisent la surconsommation
# due aux vibrations, aux freinages fréquents et à la résistance au roulement.
PARAMS_VEHICULE <- list(
  conso_base      = 20,       # Consommation de base (L/100km) sur route plane bitumée
  type            = "Camion moyen (5-10 tonnes)",
  facteur_paved   = 1.0,      # Pas de surconsommation sur bitume (référence)
  facteur_gravel  = 1.15,     # +15% sur gravier (moins bonne adhérence)
  facteur_unpaved = 1.3       # +30% sur piste en terre (ornières, boue, poussière)
)

# ── Paramètres économiques ────────────────────────────────────────────────────
# Ces valeurs sont calibrées sur des données de terrain rwandaises (2022-2023).
# La valeur du temps inclut le coût du chauffeur ET l'immobilisation du véhicule.
PARAMS_ECONOMIQUES <- list(
  prix_carburant = 1.40,   # Prix du diesel au Rwanda en USD/litre (source : GIZ 2023)
  valeur_temps   = 7.5,    # Coût total par heure d'immobilisation en USD/h
  usure_paved    = 0.05,   # Coût d'usure mécanique sur bitume en USD/km
  usure_gravel   = 0.08,   # Coût d'usure sur gravier (plus de chocs, pièces d'usure)
  usure_unpaved  = 0.12    # Coût d'usure sur piste (amortisseurs, filtres, pneus)
)

# ── Table des vitesses de référence ──────────────────────────────────────────
# Vitesses en km/h pour un camion moyen selon la combinaison type de route × surface.
# Ces valeurs sont conservatrices (inférieures aux vitesses des véhicules légers)
# pour refléter les contraintes de sécurité et de chargement des poids lourds.
# Cette table sera chargée dans DuckDB pour être utilisée dans les jointures SQL.
VITESSES_REFERENCE <- tibble(
  road_type   = c("motorway","trunk","trunk","primary","primary",
                  "secondary","secondary","tertiary","tertiary",
                  "unclassified","unclassified"),
  surface     = c("paved","paved","gravel","paved","gravel",
                  "paved","gravel","paved","unpaved",
                  "gravel","unpaved"),
  vitesse_kmh = c(100, 60, 40, 60, 40, 50, 35, 45, 25, 30, 20)
  # Lecture : un camion sur "trunk" + "paved" roule à 60 km/h,
  #           mais sur "trunk" + "gravel", il ne fait plus que 40 km/h.
)

# Chargement dans DuckDB pour utilisation dans la requête SQL de la Partie 8-10
duck_write(VITESSES_REFERENCE, "vitesses_reference")

# ── Facteurs de réduction de vitesse selon la pente ──────────────────────────
# En montée, les camions chargés perdent de la vitesse (moteur très sollicité).
# Ces facteurs multiplicatifs s'appliquent sur la vitesse de base.
FACTEURS_PENTE <- list(
  plat    = 1.0,   # Pente < 2% : pas d'impact sur la vitesse
  legere  = 0.9,   # Pente 2-5% : -10% de vitesse
  moderee = 0.75,  # Pente 5-8% : -25% de vitesse (montées longues difficiles)
  forte   = 0.6    # Pente > 8% : -40% de vitesse (cols montagneux)
)

# ── Paramètre de surconsommation en montée ────────────────────────────────────
# Pour chaque % de pente en montée, la consommation augmente de FACTEUR_CONSO_PENTE%.
# En descente, on ne modélise pas de récupération d'énergie (pas de moteur hybride).
FACTEUR_CONSO_PENTE    <- 1.5    # % d'augmentation de conso par % de pente montante
CONSO_PAR_METRE_D_PLUS <- 0.03  # Litres supplémentaires par mètre de dénivelé positif

cat("✓ Paramètres définis et table vitesses_reference chargée dans DuckDB\n\n")


# ==============================================================================
# PARTIE 2 : ACQUISITION DES DONNÉES ROUTIÈRES (FICHIER GEOFABRIK)
# ==============================================================================

cat("=== PARTIE 2 : Chargement des données routières ===\n")

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
# PARTIE 3 : NETTOYAGE ET PRÉPARATION DES DONNÉES ROUTIÈRES
# ==============================================================================
# Stratégie DuckDB : les attributs textuels (surface, vitesse…) sont extraits
# d'abord en R (opération non vectorisable en SQL pur), puis l'harmonisation
# des valeurs est réalisée en SQL via CASE WHEN — plus lisible et plus rapide
# que des chaînes de case_when() imbriqués dans mutate().

cat("=== PARTIE 3 : Nettoyage des données routières ===\n")

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

# ==============================================================================
# PARTIE 3 BIS : VÉRIFICATION VISUELLE + EXTRACTION DES COUCHES ADMINISTRATIVES
# ==============================================================================
# On extrait ici toutes les couches géographiques de référence depuis le PBF
# (frontière, provinces, lacs) car chemin_pbf est déjà disponible.
# Ces objets seront réutilisés en Partie 4 (masquage DEM) et Partie 12 (cartes).

cat("=== PARTIE 3 BIS : Couches administratives + vérification visuelle ===\n")

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

# ── Lacs depuis le PBF ────────────────────────────────────────────────────────
# Filtrage sur > 1 km² pour ne conserver que les lacs significatifs
# (lac Kivu, lac Rweru, lac Muhazi…). tryCatch gère l'absence de données.

lacs_ok <- FALSE
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
    mutate(aire_km2 = as.numeric(st_area(geometry)) / 1e6) %>%
    filter(aire_km2 > 1)
  if (nrow(lacs_raw) > 0) lacs_ok <- TRUE
  cat("  Lacs chargés :", nrow(lacs_raw), "\n")
}, error = function(e) cat("  ⚠ Lacs non disponibles dans le PBF\n"))

cat("✓ Couches administratives extraites\n")

# ── Zone d'affichage (bbox 250km × 250km centrée sur le Rwanda) ───────────────
# Buffer de 125km de chaque côté du centroïde pour afficher les frontières voisines

centre_rwanda <- rwanda_national %>% st_centroid() %>% st_coordinates()
centre_x <- centre_rwanda[1, "X"]; centre_y <- centre_rwanda[1, "Y"]
buffer_km <- 125000
bbox_poly <- st_sfc(st_polygon(list(rbind(
  c(centre_x - buffer_km, centre_y - buffer_km),
  c(centre_x + buffer_km, centre_y - buffer_km),
  c(centre_x + buffer_km, centre_y + buffer_km),
  c(centre_x - buffer_km, centre_y + buffer_km),
  c(centre_x - buffer_km, centre_y - buffer_km)
))), crs = 32735) %>% st_as_sf()
bbox_carto <- st_bbox(bbox_poly)


# ── Fonction de fond de carte réutilisable ────────────────────────────────────
# Crée les couches de base (provinces, frontière, lacs) communes à toutes les cartes.
# Retourne un objet tmap auquel on ajoute des couches thématiques avec +.

creer_fond_carte <- function() {
  carte <- tm_shape(rwanda_provinces, bbox = bbox_carto) +
    tm_polygons(fill = "#F5F5F0", col = "#AAAAAA", lwd = 0.8,
                fill.legend = tm_legend(show = FALSE)) +
    tm_shape(rwanda_national) +
    tm_borders(col = "#222222", lwd = 2.5)
  if (lacs_ok) carte <- carte +
      tm_shape(lacs_raw) +
      tm_polygons(fill = "#A8C8E8", col = "#7AAAC8", lwd = 0.5,
                  fill.legend = tm_legend(show = FALSE))
  carte
}

# ── Carte 1 : vérification post-nettoyage ──────────────────────────────────────
carte_verif_routes <- creer_fond_carte() +
  tm_shape(routes_rwanda) +
  tm_lines(
    col       = "road_type",
    col.scale = tm_scale(values = c(
      motorway     = "#E41A1C",
      trunk        = "#FF4400",
      primary      = "#FF7F00",
      secondary    = "#E8A000",
      tertiary     = "#999999",
      unclassified = "#CCCCCC"
    )),
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

cat("✓ Carte de vérification générée\n\n")

# ==============================================================================
# PARTIE 4 : ACQUISITION DES DONNÉES D'ÉLÉVATION
# ==============================================================================
# Le DEM (Digital Elevation Model) est une grille de pixels où chaque valeur
# représente l'altitude en mètres au-dessus du niveau de la mer.
# Il sera utilisé pour calculer la pente de chaque segment routier
# (ratio dénivelé/longueur × 100 = pourcentage de pente).
# elevatr et terra ne s'interfacent pas avec DuckDB → cette partie reste en R pur.

cat("=== PARTIE 4 : Téléchargement des données d'élévation ===\n")

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

options(timeout = 600)  # Téléchargement DEM peut prendre plusieurs minutes

# tryCatch() permet de continuer le script si le téléchargement échoue
# (ex : pas d'accès internet, quota AWS dépassé) en créant un DEM fictif
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


# Clip + mask sur les frontières réelles + suppression des valeurs aberrantes SRTM
dem_rwanda <- crop(dem_rwanda, vect(rwanda_boundary))
dem_rwanda <- mask(dem_rwanda, vect(rwanda_boundary))
dem_rwanda <- clamp(dem_rwanda, lower = 800, upper = 4600, values = NA)

cat("  Élévation min :", round(global(dem_rwanda, "min", na.rm = TRUE)[,1]), "m\n")
cat("  Élévation max :", round(global(dem_rwanda, "max", na.rm = TRUE)[,1]), "m\n\n")

# Cartographier rapidement pour identifier visuellement les anomalies
plot(dem_rwanda, main = "DEM Rwanda — vérification")
plot(st_geometry(rwanda_boundary), add = TRUE, border = "red")

# ==============================================================================
# PARTIE 5 : CRÉATION DU GRAPHE ROUTIER AVEC SFNETWORKS
# ==============================================================================
# sfnetworks représente le réseau routier comme un graphe topologique où :
#   - les NŒUDS sont les intersections et extrémités de routes
#   - les ARÊTES sont les segments de route entre deux nœuds
# Ce graphe servira ensuite à igraph pour le calcul de plus courts chemins.

cat("=== PARTIE 5 : Création du graphe routier ===\n")

# ── Homogénéisation des types de géométrie ───────────────────────────────────
# Le fichier PBF peut contenir des MULTILINESTRING (plusieurs lignes groupées)
# que sfnetworks ne sait pas gérer. st_cast() les éclate en LINESTRING simples.
routes_rwanda_clean <- routes_rwanda %>%
  st_cast("LINESTRING", warn = FALSE) %>%
  filter(st_geometry_type(.) == "LINESTRING") %>%  # Supprimer les types non conformes
  st_make_valid()

# Création du réseau non orienté (directed = FALSE) :
# un segment peut être parcouru dans les deux sens (routes bidirectionnelles).
# Les routes à sens unique seraient gérées avec directed = TRUE + attribut oneway.
reseau_rwanda <- as_sfnetwork(routes_rwanda_clean, directed = FALSE) %>%
  activate("edges") %>%
  # st_length() calcule la longueur en mètres (grâce à la projection UTM 35S)
  # as.numeric() supprime l'unité "m" pour faciliter les calculs arithmétiques
  mutate(longueur_m = as.numeric(st_length(geometry)))

# ── Subdivision des arêtes longues ────────────────────────────────────────────
# Les arêtes très longues (>5km) peuvent masquer des variations de pente importantes.
# On les découpe en segments de 2km maximum pour un calcul de pente plus précis.
# Cette subdivision augmente le nombre d'arêtes mais améliore la qualité du modèle.
subdiviser_ligne <- function(ligne, attributs, crs, max_length = 2000) {
  longueur_totale <- as.numeric(st_length(st_sfc(ligne, crs = crs)))
  
  # Si le segment est déjà court : le retourner tel quel sans subdivision
  if (longueur_totale <= max_length) {
    return(st_sf(attributs, geometry = st_sfc(ligne, crs = crs)))
  }
  
  # Nombre de sous-segments nécessaires (arrondi supérieur)
  n_segments  <- ceiling(longueur_totale / max_length)
  # proportions : vecteur de 0 à 1 en n_segments+1 étapes équidistantes
  proportions <- seq(0, 1, length.out = n_segments + 1)
  segments    <- list()
  
  for (i in 1:(length(proportions) - 1)) {
    debut <- proportions[i]; fin <- proportions[i + 1]
    if (debut == 0 && fin == 1) {
      segment <- ligne  # Segment unique : pas besoin de découper
    } else {
      # st_line_sample() échantillonne des points à des proportions données de la ligne
      points <- st_line_sample(st_sfc(ligne, crs = crs), sample = c(debut, fin))
      coords <- st_coordinates(points)
      # Création d'une nouvelle LINESTRING entre les deux points échantillonnés
      segment <- if (nrow(coords) >= 2) st_linestring(coords[1:2, 1:2]) else ligne
    }
    segments[[i]] <- st_sf(attributs, geometry = st_sfc(segment, crs = crs))
  }
  bind_rows(segments)
}

aretes_avant   <- reseau_rwanda %>% activate("edges") %>% st_as_sf()
aretes_courtes <- aretes_avant %>% filter(longueur_m <= 5000)  # Conservées telles quelles
aretes_longues <- aretes_avant %>% filter(longueur_m >  5000)  # À subdiviser
crs_reseau     <- st_crs(aretes_longues)

if (nrow(aretes_longues) > 0) {
  aretes_subdiv_list <- vector("list", nrow(aretes_longues))
  for (i in seq_len(nrow(aretes_longues))) {
    aretes_subdiv_list[[i]] <- subdiviser_ligne(
      st_geometry(aretes_longues[i,])[[1]],
      aretes_longues[i,] %>% st_drop_geometry(),
      crs = crs_reseau, max_length = 2000   # 2km maximum par sous-segment
    )
  }
  # Recombinaison : arêtes courtes inchangées + nouvelles arêtes subdivisions
  routes_final  <- bind_rows(aretes_courtes, bind_rows(aretes_subdiv_list))
  reseau_rwanda <- as_sfnetwork(routes_final, directed = FALSE) %>%
    activate("edges") %>%
    mutate(longueur_m = as.numeric(st_length(geometry)))
}

cat("✓ Réseau créé — nœuds :", igraph::vcount(reseau_rwanda),
    "— arêtes :", igraph::ecount(reseau_rwanda), "\n\n")


# ==============================================================================
# PARTIE 5 BIS : CORRECTIONS TOPOLOGIQUES DU RÉSEAU
# ==============================================================================
# Les données OSM contiennent fréquemment des erreurs topologiques :
#   1. Routes qui se croisent sans nœud d'intersection (pont raté, erreur de saisie)
#   2. Extrémités de routes proches mais pas connectées (gap de quelques mètres)
#   3. Nœuds intermédiaires inutiles (points de degré 2 sur une ligne droite)
# Ces erreurs créent des composantes connexes multiples (le réseau est "fragmenté")
# et empêchent les algorithmes de plus court chemin de trouver des itinéraires.

cat("=== PARTIE 5 BIS : Corrections topologiques ===\n")

# ── Étape 1 : Subdivision aux intersections ───────────────────────────────────
# to_spatial_subdivision() détecte les croisements de routes sans nœud commun
# et crée des nœuds aux points d'intersection. C'est l'opération fondamentale
# pour connecter des routes qui se croisent physiquement.
reseau_subdivise <- reseau_rwanda %>% convert(to_spatial_subdivision)
cat("  Après subdivision :", igraph::count_components(reseau_subdivise), "composantes\n")

# ── Étape 2 : Snapping des extrémités proches ─────────────────────────────────
# st_snap() "aimante" les géométries proches en deçà d'un seuil de tolérance.
# Appliqué progressivement (du plus fin au plus large) pour éviter de connecter
# des routes parallèles proches (ex : voies séparées d'une autoroute).
snapper_reseau <- function(reseau, tolerance_m) {
  aretes_sf <- reseau %>% activate("edges") %>% st_as_sf()
  aretes_snapped <- aretes_sf %>%
    mutate(
      # Déplace les extrémités de chaque géométrie vers les géométries
      # voisines dans un rayon de tolerance_m
      geometry = st_snap(geometry, geometry, tolerance = tolerance_m)
    ) %>%
    st_make_valid() %>%
    filter(st_geometry_type(geometry) == "LINESTRING") %>%
    filter(!st_is_empty(geometry))
  # Reconstruction du réseau depuis les arêtes corrigées
  as_sfnetwork(aretes_snapped, directed = FALSE) %>%
    activate("edges") %>%
    mutate(longueur_m = as.numeric(st_length(geometry)))
}

# Trois passes de snapping avec des tolérances croissantes :
#   10m  → corrige les micro-gaps (erreurs de numérisation fine)
#   20m → corrige les gaps de précision GPS
#   30m → corrige les gaps de saisie manuelle grossière
reseau_snap <- reseau_subdivise
for (seuil in c(10, 20, 30)) {
  tryCatch({
    reseau_snap <- snapper_reseau(reseau_snap, seuil) %>%
      convert(to_spatial_subdivision)  # Subdivision après chaque snap (nouveaux nœuds)
    cat("  Seuil", seuil, "m →", igraph::count_components(reseau_snap), "composantes\n")
  }, error = function(e) cat("  ⚠ Seuil", seuil, "m échoué\n"))
}

# ── Étape 3 : Suppression des pseudo-nœuds ────────────────────────────────────
# Un pseudo-nœud (degré 2) est un nœud connecté à exactement 2 arêtes.
# Il n'est pas topologiquement nécessaire (pas une vraie intersection) et alourdit
# le graphe. to_spatial_smooth() les supprime et fusionne les arêtes adjacentes.
reseau_lisse <- reseau_snap %>% convert(to_spatial_smooth)
cat("  Après lissage :", igraph::count_components(reseau_lisse), "composantes\n")

# ── Étape 4 : Extraction de la composante géante ──────────────────────────────
# Même après corrections, le réseau peut rester fragmenté (routes isolées).
# On conserve uniquement la plus grande composante connexe (composante géante),
# qui couvre la quasi-totalité du territoire national.
composantes_finales <- igraph::components(reseau_lisse %>% as_tbl_graph())
id_geante     <- which.max(composantes_finales$csize)
noeuds_geante <- which(composantes_finales$membership == id_geante)
pct_noeuds    <- round(length(noeuds_geante) / igraph::vcount(reseau_lisse) * 100, 1)
cat("  Composante géante :", length(noeuds_geante), "nœuds (", pct_noeuds, "% du réseau)\n")

# Filtrage du réseau sur la composante géante
reseau_rwanda <- reseau_lisse %>%
  activate("nodes") %>%
  filter(row_number() %in% noeuds_geante) %>%
  mutate(node_id = row_number())   # Réindexation des nœuds après filtrage

cat("✓ Réseau corrigé\n\n")


# ==============================================================================
# PARTIE 6 : DÉFINITION DES NŒUDS D'ENTREPOSAGE
# ==============================================================================
# Les nœuds d'entreposage sont les origines/destinations du modèle de fret.
# Ils représentent des zones économiques importantes (hub, SEZ, frontières…).
# ATTENTION : ces données sont fictives mais réalistes ; les coordonnées sont
# approximatives. Remplacer par les vraies localisations si disponibles.

cat("=== PARTIE 6 : Création des nœuds d'entreposage ===\n")

entreposages_fictifs <- tibble(
  nom  = c(
    # Capitale : hub logistique et industriel principal du pays
    "Kigali - Hub Central", "Kigali - SEZ Masoro", "Kigali - Marché Kimisagara",
    # Frontières : points d'entrée/sortie des marchandises importées/exportées
    "Frontière Gatuna (Ouganda)", "Frontière Rusumo (Tanzanie)",
    "Frontière Rubavu/Goma (RDC)", "Frontière Kagitumba (Ouganda)",
    # Capitales de province : centres de redistribution régionale
    "Huye (Butare) - Centre Sud", "Musanze - Centre Nord",
    "Rubavu - Centre Ouest", "Rusizi - Centre Sud-Ouest",
    # Zone économique spéciale agro-industrielle
    "Bugesera SEZ (Agro-industrie)",
    # Centres urbains secondaires
    "Muhanga", "Nyanza", "Rwamagana"
  ),
  type = c(
    "hub","sez","marche",                              # Kigali
    "frontiere","frontiere","frontiere","frontiere",   # Frontières
    "ville","ville","ville","ville",                   # Villes de province
    "sez",                                             # Bugesera
    "ville","ville","ville"                            # Centres secondaires
  ),
  # Coordonnées approximatives en WGS84 (longitude, latitude en degrés décimaux)
  lon = c(30.0619,30.1300,30.0588,30.0890,30.7850,29.2600,30.7500,
          29.7388,29.6333,29.2650,29.0100,30.1500,29.7400,29.7550,30.4300),
  lat = c(-1.9536,-1.9000,-1.9700,-1.3800,-2.3800,-1.6667,-1.3100,
          -2.5965,-1.4992,-1.6750,-2.4900,-2.1000,-2.0850,-2.3500,-1.8700)
)

# Stocker dans DuckDB pour les jointures avec les flux gravitaires (Parties 17-18)
duck_write(entreposages_fictifs, "zones_entreposage")

# Conversion en objet sf et reprojection en UTM 35S (même CRS que le réseau)
entreposages_sf <- entreposages_fictifs %>%
  st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
  st_transform(crs = 32735)

# ── Accrochage (snapping) des entrepôts au réseau ────────────────────────────
# Les coordonnées des entrepôts ne tombent pas exactement sur le réseau routier.
# st_nearest_feature() trouve pour chaque entrepôt le nœud du réseau le plus proche.
# Opération O(n×m) — acceptable ici car on a peu d'entrepôts (~15).
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

# Marquage des nœuds d'entreposage dans le réseau (attributs booléens + textuels)
# match() retourne l'indice de la première occurrence de node_id dans les IDs snappés
reseau_rwanda <- reseau_rwanda %>%
  activate("nodes") %>%
  mutate(
    node_id        = row_number(),
    is_warehouse   = node_id %in% entreposages_avec_snap$noeud_proche_id,
    warehouse_name = if_else(
      is_warehouse,
      entreposages_avec_snap$nom[match(node_id, entreposages_avec_snap$noeud_proche_id)],
      NA_character_
    ),
    warehouse_type = if_else(
      is_warehouse,
      entreposages_avec_snap$type[match(node_id, entreposages_avec_snap$noeud_proche_id)],
      NA_character_
    )
  )

cat("✓", nrow(entreposages_sf), "entreposages intégrés au réseau\n\n")


# ==============================================================================
# PARTIE 7 : CALCUL DES PENTES POUR CHAQUE ARÊTE
# ==============================================================================
# La pente d'un segment routier influence à la fois la vitesse des véhicules
# et leur consommation de carburant. On la calcule en échantillonnant des points
# le long de chaque arête et en extrayant leur altitude depuis le DEM.
# terra et raster ne s'interfacent pas avec DuckDB → cette partie reste en R pur.

cat("=== PARTIE 7 : Calcul des pentes ===\n")

calculer_pente_arete <- function(ligne_geom, dem, espacement = 100) {
  
  # Calcul de la longueur du segment en mètres (CRS métrique requis)
  longueur <- as.numeric(st_length(ligne_geom))
  
  # Nombre de points d'échantillonnage (au moins 2 : début et fin)
  n_points <- max(2, floor(longueur / espacement))
  points   <- if (longueur < espacement * 2)
    st_line_sample(ligne_geom, n = 2)         # Segment très court : 2 points seulement
  else
    st_line_sample(ligne_geom, n = n_points)  # Un point tous les 100m
  
  # st_cast() éclate le MULTIPOINT en POINTs individuels pour terra::extract()
  points_sf <- st_cast(points, "POINT")
  
  # terra::extract() récupère la valeur du pixel DEM sous chaque point
  # method = "bilinear" : interpolation entre les 4 pixels voisins (plus précis)
  # vect() convertit l'objet sf en format terra::SpatVector (requis par terra)
  elevations  <- terra::extract(dem, vect(points_sf), method = "bilinear")
  elev_values <- elevations[, 2]  # Colonne 1 = ID du point, colonne 2 = altitude
  
  # Gestion des NA (points hors de l'emprise du DEM) : retour à zéro plutôt qu'erreur
  if (any(is.na(elev_values)) || length(elev_values) < 2)
    return(list(slope_mean=0, elevation_gain=0, elevation_loss=0, rugosity=0))
  
  # Pente nette (début → fin) en pourcentage : dénivelé / longueur × 100
  # Positive = montée ; négative = descente
  denivele_net   <- elev_values[length(elev_values)] - elev_values[1]
  slope_mean_pct <- (denivele_net / longueur) * 100
  
  # diff() calcule les différences entre valeurs consécutives du vecteur d'élévation
  # Dénivelé cumulé positif (D+) = somme des montées entre points successifs
  # Dénivelé cumulé négatif (D-) = somme des descentes (valeur absolue)
  differences    <- diff(elev_values)
  elevation_gain <- sum(differences[differences > 0], na.rm = TRUE)
  elevation_loss <- abs(sum(differences[differences < 0], na.rm = TRUE))
  
  # Rugosité altimétrique : (D+ + D-) / longueur
  # Mesure l'ondulation du profil — élevée sur une route très accidentée
  rugosity <- (elevation_gain + elevation_loss) / longueur
  
  list(slope_mean=slope_mean_pct, elevation_gain=elevation_gain,
       elevation_loss=elevation_loss, rugosity=rugosity)
}

# Application sur toutes les arêtes du réseau (boucle nécessaire car terra ne
# vectorise pas l'extraction sur des géométries sf disparates)
aretes_avec_geom <- reseau_rwanda %>% activate("edges") %>% st_as_sf()
n_aretes         <- nrow(aretes_avec_geom)
resultats_pentes <- vector("list", n_aretes)

for (i in seq_len(n_aretes)) {
  if (i %% 500 == 0 || i == n_aretes) cat("  Pentes :", round(i/n_aretes*100,1), "%\n")
  resultats_pentes[[i]] <- calculer_pente_arete(
    aretes_avec_geom$geometry[i],
    dem_rwanda,
    espacement = 100   # Un point d'élévation tous les 100 mètres
  )
}

# bind_rows() convertit la liste de listes en data.frame (une ligne = une arête)
pentes_df <- bind_rows(resultats_pentes)

# Intégration des pentes dans le réseau sfnetworks
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    slope_mean      = pentes_df$slope_mean,
    elevation_gain  = pentes_df$elevation_gain,
    elevation_loss  = pentes_df$elevation_loss,
    rugosity        = pentes_df$rugosity,
    # Catégorisation de la pente pour les CASE WHEN SQL et la cartographie
    slope_category  = case_when(
      abs(slope_mean) < 2 ~ "plat",     # Plaine : quasi-aucun impact
      abs(slope_mean) < 5 ~ "legere",   # Légère : impact modéré
      abs(slope_mean) < 8 ~ "moderee",  # Modérée : impact significatif
      TRUE                ~ "forte"     # Forte : impact majeur (cols, falaises)
    )
  )

cat("✓ Pentes calculées pour toutes les arêtes\n\n")


# ==============================================================================
# PARTIES 8-10 : VITESSES, CONSOMMATION ET COÛTS GÉNÉRALISÉS VIA DUCKDB SQL
# ==============================================================================
# Ces trois parties sont fusionnées en une seule requête SQL à 4 CTEs (Common
# Table Expressions). L'approche SQL présente plusieurs avantages :
#   - Lisibilité : chaque CTE a un rôle clairement nommé et isolé
#   - Performance : DuckDB optimise l'ensemble de la requête en une seule passe
#   - Maintenabilité : changer un paramètre économique = modifier une valeur
#   - Traçabilité : toutes les étapes intermédiaires sont visibles en SQL
#
# Formules appliquées :
#   speed_kmh     = vitesse_base × facteur_pente
#   conso (L/100) = conso_base × facteur_surface × (1 + slope × FACTEUR / 100)
#   cost_fuel     = (length_km × conso/100) × prix_carburant
#   cost_wear     = length_km × usure_usd_km
#   cost_time     = (length_km / speed_kmh) × valeur_temps
#   cost_total    = cost_fuel + cost_wear + cost_time  [coût généralisé]

cat("=== PARTIES 8-10 : Vitesses, consommation et coûts via DuckDB ===\n")

# ── Chargement des attributs tabulaires des arêtes dans DuckDB ────────────────
# st_drop_geometry() supprime la colonne géométrie (non stockable dans DuckDB standard)
aretes_df <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  st_drop_geometry() %>%
  mutate(arete_id = row_number())   # Identifiant pour réaligner les résultats après la requête

duck_write(aretes_df, "aretes_base")

# ── Tables de paramètres dans DuckDB ─────────────────────────────────────────
# Stocker les paramètres sous forme de tables DuckDB permet de les modifier
# facilement pour des scénarios alternatifs sans toucher à la requête principale.

# Facteurs de consommation et coûts d'usure par type de surface
duck_write(
  tibble(
    surface         = c("paved","gravel","unpaved"),
    facteur_surface = c(PARAMS_VEHICULE$facteur_paved,
                        PARAMS_VEHICULE$facteur_gravel,
                        PARAMS_VEHICULE$facteur_unpaved),
    usure_usd_km    = c(PARAMS_ECONOMIQUES$usure_paved,
                        PARAMS_ECONOMIQUES$usure_gravel,
                        PARAMS_ECONOMIQUES$usure_unpaved)
  ),
  "params_surface"
)

# Facteurs multiplicatifs de vitesse par catégorie de pente
duck_write(
  tibble(
    slope_category  = c("plat","legere","moderee","forte"),
    facteur_pente   = c(FACTEURS_PENTE$plat, FACTEURS_PENTE$legere,
                        FACTEURS_PENTE$moderee, FACTEURS_PENTE$forte)
  ),
  "params_pente"
)

# ── Requête SQL principale : 4 CTEs chaînées ──────────────────────────────────
# glue::glue() injecte les constantes R dans la chaîne SQL avant exécution
# (ex : {PARAMS_VEHICULE$conso_base} est remplacé par 20)
aretes_couts <- duck_query(glue::glue("
  WITH

  -- CTE 1 : Jointure avec les vitesses de référence
  -- LEFT JOIN pour conserver les arêtes sans correspondance exacte (vitesse = 30 par défaut)
  avec_vitesse AS (
    SELECT
      a.*,
      COALESCE(v.vitesse_kmh, 30) AS vitesse_base
      -- COALESCE : retourne vitesse_kmh si non NULL, sinon 30 km/h (valeur de secours)
    FROM aretes_base a
    LEFT JOIN vitesses_reference v
      ON a.road_type = v.road_type AND a.surface = v.surface
  ),

  -- CTE 2 : Application du facteur de pente sur la vitesse
  -- speed_kmh = vitesse_base × facteur_pente
  -- Ex : 60 km/h × 0.9 (pente légère) = 54 km/h effectif
  avec_vitesse_pente AS (
    SELECT
      av.*,
      av.vitesse_base * pp.facteur_pente AS speed_kmh,
      pp.facteur_pente
    FROM avec_vitesse av
    LEFT JOIN params_pente pp ON av.slope_category = pp.slope_category
  ),

  -- CTE 3 : Calcul de la consommation de carburant
  -- En montée (slope_mean > 0) : surconsommation proportionnelle à la pente
  -- En descente (slope_mean <= 0) : pas de récupération d'énergie modélisée
  avec_conso AS (
    SELECT
      avp.*,
      ps.facteur_surface,
      ps.usure_usd_km,
      {PARAMS_VEHICULE$conso_base}       -- Base : 20 L/100km sur bitume plat
        * ps.facteur_surface             -- Ajustement surface (ex : ×1.15 sur gravier)
        * CASE
            WHEN slope_mean > 0
            THEN 1.0 + (slope_mean * {FACTEUR_CONSO_PENTE} / 100.0)
            -- Ex : pente 5% → facteur 1 + (5×1.5/100) = 1.075 → +7.5% de conso
            ELSE 1.0  -- En descente : pas de bonus de consommation
          END AS conso_L_per_100km
    FROM avec_vitesse_pente avp
    LEFT JOIN params_surface ps ON avp.surface = ps.surface
  ),

  -- CTE 4 : Conversion des unités et calcul des grandeurs intermédiaires
  avec_couts AS (
    SELECT
      *,
      longueur_m / 1000.0                               AS length_km,
      (longueur_m / 1000.0) / speed_kmh                AS travel_time_h,
      -- Litres consommés sur ce segment = km × (L/100km ÷ 100)
      (longueur_m / 1000.0) * (conso_L_per_100km / 100.0) AS fuel_consumption_L
    FROM avec_conso
  )

  -- Sélection finale : tous les composantes de coût + colonnes identifiantes
  SELECT
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
    -- Coût carburant (USD) = litres consommés × prix du litre de diesel
    fuel_consumption_L * {PARAMS_ECONOMIQUES$prix_carburant}    AS cost_fuel_usd,
    -- Coût d'usure (USD) = km parcourus × taux d'usure par km (variable selon surface)
    length_km * usure_usd_km                                    AS cost_wear_usd,
    -- Coût du temps (USD) = heures de trajet × valeur économique du temps
    (length_km / speed_kmh) * {PARAMS_ECONOMIQUES$valeur_temps} AS cost_time_usd,
    length_km / speed_kmh                                       AS travel_time_h
  FROM avec_couts
"))

# Coût généralisé = somme des trois composantes (carburant + usure + temps)
# Calculé en R car DuckDB ne peut pas référencer des alias de la même clause SELECT
aretes_couts <- aretes_couts %>%
  mutate(
    cost_generalized_usd = cost_fuel_usd + cost_wear_usd + cost_time_usd,
    # Coût au km : indicateur de comparaison entre segments de longueurs différentes
    cost_per_km          = if_else(length_km > 0, cost_generalized_usd / length_km, 0)
  )

# Persister les coûts dans DuckDB (réutilisés en Parties 18-19)
duck_write(aretes_couts, "aretes_couts")

# Statistiques de contrôle directement depuis DuckDB
stats_couts_sql <- duck_query("
  SELECT
    ROUND(AVG(cost_per_km), 3)                                  AS cout_par_km_moyen,
    ROUND(AVG(cost_fuel_usd / cost_generalized_usd) * 100, 1)  AS part_carburant,
    ROUND(AVG(cost_time_usd / cost_generalized_usd) * 100, 1)  AS part_temps,
    ROUND(AVG(cost_wear_usd / cost_generalized_usd) * 100, 1)  AS part_usure
  FROM aretes_couts
")

cat("✓ Coûts calculés via DuckDB SQL (4 CTEs chaînées)\n")
cat("  Coût moyen/km :", stats_couts_sql$cout_par_km_moyen, "USD/km\n")
cat("  Part carburant:", stats_couts_sql$part_carburant, "%\n")
cat("  Part temps    :", stats_couts_sql$part_temps, "%\n")
cat("  Part usure    :", stats_couts_sql$part_usure, "%\n\n")

# ── Réintégration des coûts dans sfnetworks ───────────────────────────────────
# Tri par arete_id pour garantir l'alignement ligne à ligne avec l'ordre des
# arêtes dans le réseau. Sans ce tri, les coûts seraient affectés aux mauvaises arêtes.
aretes_couts_ord <- aretes_couts %>% arrange(arete_id)

reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    length_km            = aretes_couts_ord$length_km,
    speed_kmh            = aretes_couts_ord$speed_kmh,
    travel_time_h        = aretes_couts_ord$travel_time_h,
    conso_L_per_100km    = aretes_couts_ord$conso_L_per_100km,
    fuel_consumption_L   = aretes_couts_ord$fuel_consumption_L,
    cost_fuel_usd        = aretes_couts_ord$cost_fuel_usd,
    cost_wear_usd        = aretes_couts_ord$cost_wear_usd,
    cost_time_usd        = aretes_couts_ord$cost_time_usd,
    cost_generalized_usd = aretes_couts_ord$cost_generalized_usd,
    cost_per_km          = aretes_couts_ord$cost_per_km
  )

# Recalcul des attributs manquants après les corrections topologiques de la Partie 5 BIS
# (certaines arêtes créées lors du snapping/subdivision peuvent manquer de vitesse ou de coût)
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    # Si la vitesse est manquante ou nulle, imputer selon le type de route
    speed_kmh = case_when(
      !is.na(speed_kmh) & speed_kmh > 0 ~ speed_kmh,
      road_type == "trunk"               ~ 60,
      road_type == "primary"             ~ 60,
      road_type == "secondary"           ~ 50,
      road_type == "tertiary"            ~ 45,
      TRUE                               ~ 30   # Valeur par défaut conservative
    ),
    # Si le coût est manquant, utiliser une formule simplifiée de secours
    cost_generalized_usd = case_when(
      !is.na(cost_generalized_usd) & cost_generalized_usd > 0 ~ cost_generalized_usd,
      # Approximation : temps × valeur_temps + km × taux_usure_gravier
      TRUE ~ travel_time_h * 7.5 + length_km * 0.08
    )
  )


# ==============================================================================
# PARTIE 11 : MATRICE ORIGINE-DESTINATION STOCKÉE DANS DUCKDB
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

cat("=== PARTIE 11 : Matrice OD dans DuckDB ===\n")

# Extraction des nœuds identifiés comme entrepôts dans le réseau
noeuds_entreposage <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  as_tibble() %>%
  mutate(warehouse_id = row_number())

n_warehouses <- nrow(noeuds_entreposage)
cat("Entreposages :", n_warehouses, "\n")

# ── Préparation du graphe igraph pour Dijkstra ────────────────────────────────
# as_tbl_graph() convertit sfnetworks en tidygraph/igraph tout en conservant
# les attributs des nœuds et des arêtes (notamment cost_generalized_usd).
graphe_igraph      <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(weight = cost_generalized_usd) %>%   # weight = métrique de coût pour Dijkstra
  as_tbl_graph()

# Indices igraph des nœuds d'entreposage (igraph indexe de 1 à N)
warehouse_node_ids <- which(igraph::V(graphe_igraph)$is_warehouse)

# ── Calcul des plus courts chemins par Dijkstra ────────────────────────────────
# Pour chaque entrepôt i, calcul en batch vers tous les autres entrepôts j.
# On accumule les résultats dans od_rows (liste → data.frame final).
od_rows <- list()
idx     <- 0

for (i in seq_along(warehouse_node_ids)) {
  # Calcul depuis i vers TOUS les entrepôts en une seule passe (plus efficace
  # que d'appeler shortest_paths() pour chaque paire i→j individuellement)
  # output = "both" : retourne les nœuds traversés (vpath) ET les arêtes (epath)
  chemins   <- igraph::shortest_paths(
    graphe_igraph,
    from    = warehouse_node_ids[i],
    to      = warehouse_node_ids,
    weights = igraph::E(graphe_igraph)$cost_generalized_usd,
    output  = "both"
  )
  edge_data <- igraph::edge_attr(graphe_igraph)  # Attributs de toutes les arêtes du graphe
  
  for (j in seq_along(warehouse_node_ids)) {
    if (i == j) next   # Pas d'auto-trajet
    
    edges_path <- chemins$epath[[j]]   # Indices igraph des arêtes sur le chemin i→j
    
    if (length(edges_path) > 0) {   # Chemin trouvé (zones connectées)
      idx <- idx + 1
      od_rows[[idx]] <- list(
        id_origine      = i,
        id_destination  = j,
        nom_origine     = noeuds_entreposage$warehouse_name[i],
        nom_destination = noeuds_entreposage$warehouse_name[j],
        # Somme des attributs sur toutes les arêtes du chemin optimal
        cout_usd    = sum(edge_data$cost_generalized_usd[edges_path]),
        distance_km = sum(edge_data$length_km[edges_path]),
        temps_h     = sum(edge_data$travel_time_h[edges_path])
      )
    }
    # Si edges_path est vide (length == 0) : zones non connectées → pas de ligne
  }
  if (i %% 3 == 0) cat("  OD :", round(i/n_warehouses*100,1), "%\n")
}

# Stockage de la matrice OD en format long dans DuckDB
od_long <- bind_rows(od_rows)
duck_write(od_long, "matrice_od")

# Statistiques OD agrégées en SQL
od_stats <- duck_query("
  SELECT
    COUNT(*)                    AS n_paires,
    ROUND(AVG(cout_usd), 2)     AS cout_moyen_usd,
    ROUND(AVG(distance_km), 1)  AS dist_moyenne_km,
    ROUND(MAX(distance_km), 1)  AS dist_max_km
  FROM matrice_od
")

cat("✓ Matrice OD stockée dans DuckDB (format long)\n")
cat("  Paires connectées :", od_stats$n_paires, "\n")
cat("  Coût moyen        :", od_stats$cout_moyen_usd, "USD\n")
cat("  Distance moyenne  :", od_stats$dist_moyenne_km, "km\n\n")

# ── Reconstruction des matrices R carrées ─────────────────────────────────────
# Nécessaires pour la Partie 14 (trajet exemple) et la Partie 19 (affectation igraph).
# On reconstitue les 3 matrices à partir du format long DuckDB.
matrice_couts     <- matrix(0, n_warehouses, n_warehouses,
                            dimnames = list(noeuds_entreposage$warehouse_name, noeuds_entreposage$warehouse_name))
matrice_distances <- matrix(0, n_warehouses, n_warehouses,
                            dimnames = list(noeuds_entreposage$warehouse_name, noeuds_entreposage$warehouse_name))
matrice_temps     <- matrix(0, n_warehouses, n_warehouses,
                            dimnames = list(noeuds_entreposage$warehouse_name, noeuds_entreposage$warehouse_name))

for (r in seq_len(nrow(od_long))) {
  i <- od_long$id_origine[r]; j <- od_long$id_destination[r]
  matrice_couts[i, j]     <- od_long$cout_usd[r]
  matrice_distances[i, j] <- od_long$distance_km[r]
  matrice_temps[i, j]     <- od_long$temps_h[r]
}


# ==============================================================================
# PARTIE 12 : VISUALISATIONS CARTOGRAPHIQUES (tmap)
# ==============================================================================
# tmap travaille avec des objets sf et ne s'interface pas avec DuckDB.
# Cette partie reste entièrement en R/sf. Les couches géographiques
# (frontières, lacs, provinces) sont extraites depuis le même fichier PBF
# que les routes, via la couche "multipolygons".

cat("=== PARTIE 12 : Visualisations ===\n")


# ── Carte 2 : Coûts généralisés par km ───────────────────────────────────────
# Les tronçons les plus chers (rouge foncé) combinent pente forte + surface dégradée.
# Utile pour identifier les goulets d'étranglement logistiques.
carte_couts <- creer_fond_carte() +
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col="cost_per_km",
           col.scale=tm_scale_intervals(style="quantile", n=5, values="brewer.yl_or_rd"),
           col.legend=tm_legend(title="Coût (USD/km)"), lwd=1.5) +
  tm_shape(entreposages_sf) + tm_dots(fill="black", size=0.2) +
  tm_title("Coûts de Transport Généralisés par km") +
  tm_layout(legend.outside=TRUE, frame=TRUE) +
  tm_scalebar(position=c("left","bottom")) + tm_compass(position=c("right","top"))
tmap_save(carte_couts, file.path(DIR_OUTPUT,"carte_couts_rwanda.png"),
          width=3000, height=2400, dpi=300)

# ── Carte 3 : Catégories de pente ─────────────────────────────────────────────
# Le Rwanda est surnommé "le pays des mille collines" : les pentes fortes y sont très
# fréquentes, notamment dans les préfectures du nord et de l'ouest.
carte_pentes <- creer_fond_carte() +
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col="slope_category",
           col.scale=tm_scale(values=c(plat="#00AA00", legere="#AACC00",
                                       moderee="#FF9900", forte="#FF0000")),
           col.legend=tm_legend(title="Catégorie de pente"), lwd=1.5) +
  tm_title("Pentes du Réseau Routier") +
  tm_layout(legend.outside=TRUE, frame=TRUE) +
  tm_scalebar(position=c("left","bottom")) + tm_compass(position=c("right","top"))
tmap_save(carte_pentes, file.path(DIR_OUTPUT,"carte_pentes_rwanda.png"),
          width=3000, height=2400, dpi=300)

cat("✓ 2 cartes générées\n\n")


# ==============================================================================
# PARTIE 13 : EXPORT VIA DUCKDB (PARQUET + CSV + GEOPACKAGE)
# ==============================================================================
# COPY TO est la commande DuckDB pour exporter des tables vers des fichiers.
# Avantages sur write.csv() :
#   - Parquet : format colonnaire compressé (~10× plus compact que CSV)
#   - Vitesse : écriture multithread native de DuckDB
#   - SQL : filtrer/transformer les données à l'export sans créer de df R intermédiaire

cat("=== PARTIE 13 : Export via DuckDB ===\n")

# Chargement de la table des arêtes finales dans DuckDB (sans géométrie)
aretes_finales <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  select(osm_id, name, road_type, surface, length_km, slope_mean,
         elevation_gain, elevation_loss, speed_kmh, fuel_consumption_L,
         cost_fuel_usd, cost_wear_usd, cost_time_usd, cost_generalized_usd, cost_per_km)

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
dbExecute(con, "
  COPY (SELECT * FROM matrice_od)
  TO file.path(DIR_OUTPUT,'matrice_od_long.csv') (FORMAT CSV, HEADER)
")
dbExecute(con, "
  COPY (SELECT * FROM aretes_finales)
  TO file.path(DIR_OUTPUT,'aretes_finales.csv') (FORMAT CSV, HEADER)
")

# Exports Parquet depuis DuckDB
# Lisible directement avec : Python → pd.read_parquet() ; R → arrow::read_parquet()
dbExecute(con, "
  COPY (SELECT * FROM aretes_finales)
  TO file.path(DIR_OUTPUT, 'aretes_finales.parquet') (FORMAT PARQUET)
")
dbExecute(con, "
  COPY (SELECT * FROM matrice_od)
  TO file.path(DIR_OUTPUT, 'matrice_od.parquet') (FORMAT PARQUET)
")

cat("✓ Exports CSV + Parquet via DuckDB COPY TO\n\n")


# ==============================================================================
# PARTIE 16 : TABLE INPUT-OUTPUT DU RWANDA
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

cat("=== PARTIE 16 : Table Input-Output dans DuckDB ===\n")

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
  #  Agri  Mines AgroI Indus Const Comm  Trans Serv
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
# PARTIE 17 : GÉNÉRATION DES OFFRES ET DEMANDES PAR ZONE
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

cat("=== PARTIE 17 : Offres et demandes par zone ===\n")

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
               Construction=0.02, Commerce=0.20, Transport=0.05, Services=0.04)
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
               Construction=0.04, Commerce=0.20, Transport=0.05, Services=0.04)
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
  "Rwamagana"                     = 0.09   # Capitale de la Province de l'Est
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
# PARTIE 18 : MODÈLE GRAVITAIRE DES ÉCHANGES
# ==============================================================================

cat("=== PARTIE 18 : Modèle gravitaire des échanges ===\n\n")

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

# --- Préparation de la matrice de coûts ---
C_ij <- matrice_couts
diag(C_ij) <- NA          # Pas d'échange intrazone
C_ij[C_ij == 0] <- NA     # Zones non connectées → pas de flux

# --- Calcul des flux gravitaires par secteur ---
cat("Calcul des flux gravitaires...\n")

flux_gravitaire <- list()   # Matrice de flux par secteur (M USD)
flux_total      <- matrix(0, nrow = n_warehouses, ncol = n_warehouses,
                          dimnames = list(noeuds_entreposage$warehouse_name,
                                          noeuds_entreposage$warehouse_name))

for (s in SECTEURS) {
  beta_s <- BETA_SECTEUR[s]
  
  # Friction : zones proches ont plus d'échanges
  friction <- C_ij^(-beta_s)
  friction[is.na(friction)] <- 0
  
  # Offres et demandes sectorielles
  O_s <- offre_zones[, s]
  D_s <- demande_zones[, s]
  
  # Flux gravitaire brut : T_ij = O_i * D_j * F_ij
  flux_brut <- outer(O_s, D_s) * friction
  diag(flux_brut) <- 0
  
  # Calibration : normalisation pour respecter les totaux
  if (sum(flux_brut) > 0) {
    # Calibration proportionnelle (méthode IPF simplifiée, 1 itération)
    facteur_k <- (sum(O_s) * sum(D_s)) / (sum(flux_brut)^2 / sum(flux_brut))
    flux_calibre <- flux_brut * sqrt(facteur_k)
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
# PARTIE 19 : MODÉLISATION DU FRET ET AFFECTATION AU RÉSEAU
# ==============================================================================

cat("=== PARTIE 19 : Modélisation du fret et affectation au réseau ===\n")

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

# --- Étape 2 : Affectation All-or-Nothing au réseau ---
# Pour chaque paire OD avec flux significatif, tout le fret
# emprunte le chemin de coût généralisé minimal (Dijkstra)

cat("Affectation du fret au réseau (algorithme All-or-Nothing)...\n")

SEUIL_FLUX_TONNES <- 50  # Ignorer les flux < 50 tonnes

# Initialiser le vecteur de charge sur chaque arête
n_edges <- igraph::ecount(graphe_igraph)
volume_trafic <- numeric(n_edges)  # En tonnes

# Compteurs
paires_traitees <- 0
paires_non_connectees <- 0

for (i in seq_len(n_warehouses)) {
  # Calcul en batch depuis la zone i vers toutes les autres
  from_node <- warehouse_node_ids[i]
  
  # Sélectionner destinations avec flux suffisant
  to_indices <- which(flux_tonnes_total[i, ] > SEUIL_FLUX_TONNES & 
                        seq_len(n_warehouses) != i)
  
  if (length(to_indices) == 0) next
  
  to_nodes <- warehouse_node_ids[to_indices]
  
  # Plus courts chemins depuis i vers tous les to_nodes en une seule passe
  chemins_batch <- igraph::shortest_paths(
    graphe_igraph,
    from   = from_node,
    to     = to_nodes,
    weights= igraph::E(graphe_igraph)$cost_generalized_usd,
    output = "epath"
  )
  
  for (k in seq_along(to_indices)) {
    j <- to_indices[k]
    flux_ij <- flux_tonnes_total[i, j]
    edges_path <- chemins_batch$epath[[k]]
    
    if (length(edges_path) > 0) {
      volume_trafic[edges_path] <- volume_trafic[edges_path] + flux_ij
      paires_traitees <- paires_traitees + 1
    } else {
      paires_non_connectees <- paires_non_connectees + 1
    }
  }
  
  if (i %% 3 == 0) cat("  Zone", i, "/", n_warehouses, "traitée\n")
}

cat("✓ Affectation terminée\n")
cat("  Paires traitées     :", paires_traitees, "\n")
cat("  Paires non connectées:", paires_non_connectees, "\n\n")

# --- Étape 3 : Intégration des volumes au réseau ---
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    volume_tonnes = volume_trafic,
    # Catégories de trafic fret
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

# === DIAGNOSTIC PARTIE 19 ===

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
      weights = igraph::E(graphe_igraph)$cost_generalized_usd
    )
    cat("  ", i, "→", j, ":",
        ifelse(is.infinite(dist_ij), "NON CONNECTÉ", paste(round(dist_ij, 2), "USD")), "\n")
  }
}

# ==============================================================================
# PARTIE 20 : VISUALISATIONS DES ÉCHANGES MODÉLISÉS
# ==============================================================================

cat("=== PARTIE 20 : Visualisations des échanges modélisés ===\n\n")

# --- Préparation des couches spatiales ---

# ✅ Forcer les colonnes numériques explicitement
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
# ✅ tm_scale_continuous() au lieu de tm_scale_intervals()
# ============================================================

cat("Génération de la carte du trafic fret...\n")

carte_fret <- creer_fond_carte() +
  
  # Réseau de base en gris très clair
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.3) +
  
  # Arêtes avec trafic
  tm_shape(aretes_fret) +
  tm_lines(
    col = "volume_tonnes",
    col.scale = tm_scale_continuous(  # ✅ continuous au lieu de intervals
      values = "brewer.yl_or_rd"
    ),
    col.legend = tm_legend(title = "Volume fret\n(tonnes)"),
    lwd = "lwd_val",
    lwd.scale = tm_scale(values.range = c(0.4, 5)),
    lwd.legend = tm_legend(show = FALSE)
  ) +
  
  # Points des zones
  tm_shape(coords_zones_sf) +
  tm_dots(
    fill = "warehouse_type",
    fill.scale = tm_scale(
      values = c(
        "hub"       = "#000000",
        "sez"       = "#0055FF",
        "marche"    = "#00AA00",
        "frontiere" = "#FF0000",
        "ville"     = "#880088"
      )
    ),
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
      flux_musd   = as.numeric(flux_ij),                            # ✅
      flux_kt     = as.numeric(flux_tonnes_total[i, j] / 1000),    # ✅
      geom        = st_linestring(rbind(
        c(pt_i[1, "X"], pt_i[1, "Y"]),
        c(pt_j[1, "X"], pt_j[1, "Y"])
      ))
    )
  }
}

if (k > 0) {
  
  # ✅ Création de desire_sf AVANT carte_od
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
  
  carte_od <- creer_fond_carte() +
    
    tm_shape(desire_sf) +
    tm_lines(
      col = "flux_musd",
      col.scale = tm_scale_continuous(  # ✅ continuous au lieu de intervals
        values = "brewer.blues"
      ),
      col.legend = tm_legend(title = "Flux commercial\n(M USD)"),
      lwd = "flux_log",
      lwd.scale = tm_scale(values.range = c(0.5, 5)),
      lwd.legend = tm_legend(show = FALSE),
      col_alpha = 0.65
    ) +
    
    tm_shape(coords_zones_sf) +
    tm_dots(
      fill = "warehouse_type",
      fill.scale = tm_scale(
        values = c(
          "hub"       = "#000000",
          "sez"       = "#0055FF",
          "marche"    = "#00AA00",
          "frontiere" = "#FF0000",
          "ville"     = "#880088"
        )
      ),
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
    cols      = c(Offre_totale_musd, Demande_totale_musd),
    names_to  = "Type_flux",
    values_to = "Valeur"
  ) %>%
  mutate(
    Zone_court = str_trunc(Zone, 28),
    Type_flux  = recode(Type_flux,
                        "Offre_totale_musd"   = "Offre",
                        "Demande_totale_musd" = "Demande")
  ) %>%
  ggplot(aes(x = reorder(Zone_court, Valeur),
             y = Valeur,
             fill = Type_flux)) +
  geom_col(position = "dodge", width = 0.7) +
  coord_flip() +
  scale_fill_manual(values = c("Offre" = "#1976D2", "Demande" = "#D32F2F")) +
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
# ✅ Noms courts uniques via make.unique()
# ============================================================

noms_courts_raw <- noeuds_entreposage$warehouse_name %>%
  str_remove(" - .*") %>%
  str_remove(" \\(.*") %>%
  str_trunc(18)

noms_courts <- make.unique(noms_courts_raw, sep = "_")  # ✅ Kigali, Kigali_1, Kigali_2

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

aretes_fret_export <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  select(osm_id, name, road_type, surface,
         length_km, speed_kmh,
         cost_generalized_usd, cost_per_km,
         volume_tonnes, classe_trafic)

st_write(aretes_fret_export,
         file.path(DIR_OUTPUT,"reseau_rwanda_avec_fret.gpkg"),
         delete_dsn = TRUE, quiet = TRUE)

cat("✓ Données exportées\n\n")


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