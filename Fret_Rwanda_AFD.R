################################################################################
# PROJET : Réseau Routier pour Modélisation du Commerce de Fret - Rwanda
# OBJECTIF : Créer un graphe routier avec coûts de transport généralisés
# AUTEUR : Yanis
# DATE : Mars 2024
################################################################################

token <- Sys.getenv("GITHUB_PAT")
system("git config --global credential.helper '!f() { echo \"username=token\"; echo \"password=$GITHUB_PAT\"; }; f'")

# ==============================================================================
# PARTIE 0 : INSTALLATION ET CHARGEMENT DES PACKAGES
# ==============================================================================

# Liste des packages nécessaires
packages_requis <- c(
  "sf",           # Manipulation de données géospatiales
  "osmdata",      # Extraction de données OpenStreetMap
  "elevatr",      # Téléchargement de données d'élévation
  "terra",        # Manipulation de rasters (élévation)
  "sfnetworks",   # Création et analyse de réseaux spatiaux
  "tidyverse",    # Manipulation de données (dplyr, ggplot2, etc.)
  "igraph",       # Analyse de graphes
  "tmap",         # Cartographie thématique
  "units",        # Gestion des unités
  "lwgeom",       # Opérations géométriques avancées
  "tidygraph",    # Manipuler et d’analyser des graphes
  "geodata",      # Frontières administratives GADM
  "rnaturalearth"
)

# Fonction pour installer les packages manquants
installer_si_necessaire <- function(packages) {
  nouveaux_packages <- packages[!(packages %in% installed.packages()[,"Package"])]
  if(length(nouveaux_packages)) {
    install.packages(nouveaux_packages, dependencies = TRUE)
  }
}

# Installation des packages manquants
installer_si_necessaire(packages_requis)

# Chargement de tous les packages
invisible(lapply(packages_requis, library, character.only = TRUE))

# Configuration générale
options(timeout = 300)  # Augmenter le timeout pour téléchargements
set.seed(123)           # Pour reproductibilité des données fictives


cat("✓ Tous les packages sont chargés\n\n")


# ==============================================================================
# PARTIE 1 : DÉFINITION DES PARAMÈTRES DU MODÈLE
# ==============================================================================

cat("=== PARTIE 1 : Définition des paramètres ===\n")

# --- Paramètres du véhicule de référence (camion moyen) ---
PARAMS_VEHICULE <- list(
  # Consommation de base (litres/100km) sur terrain plat, route bitumée
  conso_base = 20,
  
  # Type de véhicule
  type = "Camion moyen (5-10 tonnes)",
  
  # Facteurs d'ajustement de consommation selon surface
  facteur_paved = 1.0,      # Route bitumée
  facteur_gravel = 1.15,    # Route gravier
  facteur_unpaved = 1.3     # Route terre
)

# --- Paramètres économiques ---
PARAMS_ECONOMIQUES <- list(
  # Prix du carburant (diesel) au Rwanda en USD/litre
  prix_carburant = 1.40,
  
  # Valeur du temps pour transport de fret (USD/heure)
  # Inclut: coût chauffeur + immobilisation véhicule
  valeur_temps = 7.5,
  
  # Coût d'usure par km selon type de surface (USD/km)
  usure_paved = 0.05,
  usure_gravel = 0.08,
  usure_unpaved = 0.12
)

# --- Vitesses de référence (km/h) pour camions selon type de route ---
VITESSES_REFERENCE <- tibble(
  road_type = c("motorway", "trunk", "trunk", "primary", "primary", 
                "secondary", "secondary", "tertiary", "tertiary",
                "unclassified", "unclassified"),
  surface = c("paved", "paved", "gravel", "paved", "gravel",
              "paved", "gravel", "paved", "unpaved",
              "gravel", "unpaved"),
  vitesse_kmh = c(100, 60, 40, 60, 40, 50, 35, 45, 25, 30, 20)
)

# --- Facteurs d'ajustement de vitesse selon pente ---
FACTEURS_PENTE <- list(
  plat = 1.0,         # -2% à +2%
  legere = 0.9,       # 2% à 5%
  moderee = 0.75,     # 5% à 8%
  forte = 0.6         # > 8%
)

# --- Paramètres de consommation selon pente ---
# Pour chaque % de pente en montée : augmentation de consommation
FACTEUR_CONSO_PENTE <- 1.5  # % d'augmentation par % de pente

# Coût supplémentaire par mètre de dénivelé positif (litres/m)
CONSO_PAR_METRE_D_PLUS <- 0.03

cat("✓ Paramètres du modèle définis\n\n")


# ==============================================================================
# PARTIE 2 : ACQUISITION DES DONNÉES ROUTIÈRES (FICHIER GEOFABRIK)
# ==============================================================================

cat("=== PARTIE 2 : Chargement des données routières ===\n")

# --- Chargement du fichier OSM PBF depuis Geofabrik ---
# IMPORTANT: Placer le fichier rwanda-260315.osm.pbf dans le dossier data/raw/

cat("Chargement du fichier rwanda-latest.osm.pbf...\n")
cat("(Cela peut prendre 1-3 minutes)\n")

# Chemin vers le fichier (à adapter si nécessaire)
chemin_pbf <- "data/raw/rwanda-260315.osm.pbf"

# Vérifier que le fichier existe
if (!file.exists(chemin_pbf)) {
  stop("ERREUR: Le fichier rwanda-260315.osm.pbf n'a pas été trouvé dans data/raw/\n",
       "Veuillez placer le fichier téléchargé depuis Geofabrik dans ce dossier.")
}

# Lecture de la couche "lines" (routes) depuis le fichier PBF
routes_rwanda_raw <- st_read(
  chemin_pbf,
  layer = "lines",
  query = "SELECT * FROM lines WHERE highway IN ('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified')",
  quiet = FALSE
)

cat("✓ Données chargées:", nrow(routes_rwanda_raw), "segments de route\n\n")

# Définition de la bbox pour référence (calculée depuis les données)
bbox_rwanda <- st_bbox(routes_rwanda_raw)
cat("Emprise des données:\n")
cat("  Longitude:", bbox_rwanda[c("xmin", "xmax")], "\n")
cat("  Latitude:", bbox_rwanda[c("ymin", "ymax")], "\n\n")


# ==============================================================================
# PARTIE 3 : NETTOYAGE ET PRÉPARATION DES DONNÉES ROUTIÈRES
# ==============================================================================

cat("=== PARTIE 3 : Nettoyage des données routières ===\n")

#Vérification des colonnes présentes dans routes_rwanda_raw
glimpse(routes_rwanda_raw)  

# --- Fonction pour extraire une valeur depuis other_tags ---
extraire_tag <- function(other_tags, cle) {
  if (is.na(other_tags)) return(NA_character_)
  
  # Chercher le pattern "cle"=>"valeur"
  pattern <- paste0('"', cle, '"=>"([^"]*)"')
  match <- regmatches(other_tags, regexec(pattern, other_tags))
  
  if (length(match[[1]]) > 1) {
    return(match[[1]][2])
  } else {
    return(NA_character_)
  }
}


# --- Extraction des attributs depuis other_tags ---
cat("Extraction des attributs depuis other_tags...\n")

routes_rwanda <- routes_rwanda_raw %>%
  # Renommer la colonne géométrie
  rename(geometry = `_ogr_geometry_`) %>%
  
  # Extraire les informations depuis other_tags
  mutate(
    surface = sapply(other_tags, extraire_tag, cle = "surface"),
    maxspeed = sapply(other_tags, extraire_tag, cle = "maxspeed"),
    lanes = sapply(other_tags, extraire_tag, cle = "lanes"),
    oneway = sapply(other_tags, extraire_tag, cle = "oneway")
  ) %>%
  
  # Sélection des colonnes pertinentes
  select(osm_id, name, highway, surface, maxspeed, lanes, oneway, geometry) %>%
  
  # Renommage de highway en road_type pour cohérence avec le modèle
  rename(road_type = highway) %>%
  
  # Conversion en objet sf (si nécessaire)
  st_as_sf() %>%
  
  # Suppression des géométries invalides
  filter(st_is_valid(geometry)) %>%
  
  # Correction des géométries invalides restantes :
  # Détecte les géométries invalides et les répare automatiquement en appliquant des règles géométriques standard.
  # Découpe les polygones auto-intersectés en polygones simples valides.
  # Supprime les doublons ou réorganise les points pour que les lignes/polygones soient cohérents.
  # Conserve autant que possible la forme originale, mais garantit que le résultat respecte les standards géométriques (comme ceux de l’OGC).
  st_make_valid()

cat("✓ Attributs extraits\n")

#Vérification des colonnes présentes dans routes_rwanda
glimpse(routes_rwanda)

# --- Harmonisation du type de surface ---
# Beaucoup de valeurs manquantes dans OSM pour la surface
# On impute selon le type de route

routes_rwanda <- routes_rwanda %>%
  mutate(
    # Harmonisation des valeurs de surface existantes
    surface_clean = case_when(
      surface %in% c("paved", "asphalt", "concrete") ~ "paved",
      surface %in% c("gravel", "compacted", "fine_gravel") ~ "gravel",
      surface %in% c("unpaved", "dirt", "earth", "ground") ~ "unpaved",
      TRUE ~ NA_character_
    ),
    
    # Imputation des valeurs manquantes selon type de route
    surface_final = case_when(
      !is.na(surface_clean) ~ surface_clean,
      road_type %in% c("motorway", "trunk", "primary") ~ "paved",
      road_type == "secondary" ~ "gravel",
      road_type %in% c("tertiary", "unclassified") ~ "unpaved",
      TRUE ~ "unpaved"  # Par défaut
    )
  ) %>%
  select(-surface, -surface_clean) %>%
  rename(surface = surface_final)


# --- Projection vers un CRS métrique (UTM Zone 35S pour Rwanda) ---
# EPSG:32735 = WGS 84 / UTM zone 35S
# Nécessaire pour calculs de distances et pentes en mètres

routes_rwanda <- st_transform(routes_rwanda, crs = 32735)

cat("✓ Nettoyage terminé:", nrow(routes_rwanda), "segments conservés\n")
cat("✓ Projection:", st_crs(routes_rwanda)$input, "\n\n")


# ==============================================================================
# PARTIE 4 : ACQUISITION DES DONNÉES D'ÉLÉVATION
# ==============================================================================

cat("=== PARTIE 4 : Téléchargement des données d'élévation ===\n")

# --- Téléchargement du DEM (Digital Elevation Model) ---
cat("Téléchargement du raster d'élévation SRTM...\n")
cat("(Cela peut prendre 1-3 minutes)\n")

# Créer une emprise simple à partir des routes
bbox_routes <- st_bbox(routes_rwanda)

# Créer un dataframe avec les coins de la bbox
emprise_points <- data.frame(
  x = c(bbox_routes["xmin"], bbox_routes["xmax"]),
  y = c(bbox_routes["ymin"], bbox_routes["ymax"])
)

# Convertir en sf et transformer en WGS84 : données en degré longitude-latitude
emprise_sf <- st_as_sf(emprise_points, coords = c("x", "y"), crs = 32735) %>%
  st_transform(crs = 4326)

# Augmenter le timeout
options(timeout = 600)

# Tentative de téléchargement avec gestion d'erreur
tryCatch({
  dem_rwanda <- get_elev_raster(emprise_sf, z = 9, clip = "locations")
  dem_rwanda <- rast(dem_rwanda)
  dem_rwanda <- project(dem_rwanda, "EPSG:32735", method = "bilinear")
  # z = 9 : niveau de zoom : <=> environ 1 pixel = 2,4-3m^2 
  # clip = "location" signifie que je ne récupère les données d'altitude que pour le bbox_rwanda
  # Raster = format de données sous forme de grille de pixels
  
  cat("✓ DEM téléchargé et reprojeté\n")
  
}, error = function(e) {
  cat("\n⚠ ERREUR: Impossible de télécharger le DEM\n")
  cat("→ Création d'un DEM FICTIF mais réaliste pour le Rwanda\n\n")
  
  # Création d'un DEM fictif réaliste
  # Rwanda : altitude 950m (min) à 4507m (max), très montagneux
  
  # Définir l'étendue en UTM 35S
  ext_utm <- ext(bbox_routes["xmin"], bbox_routes["xmax"],
                 bbox_routes["ymin"], bbox_routes["ymax"])
  
  # Créer un raster vide avec résolution ~90m
  dem_rwanda <<- rast(ext_utm, resolution = 90, crs = "EPSG:32735")
  
  # Générer des valeurs d'élévation réalistes
  set.seed(123) # Permet de générer toujours les mêmes valeurs aléatoires
  
  n_cells <- ncell(dem_rwanda)
  
  # Élévations de base (gradient ouest-est, Rwanda plus élevé à l'ouest)
  x_coords <- xFromCell(dem_rwanda, 1:n_cells)
  base_elevation <- 1500 + (max(x_coords) - x_coords) / 
    (max(x_coords) - min(x_coords)) * 800
  # x_coords est un vecteur contenant le chiffre de la longitude de chaque cellule
  # Plus on est à l'Ouest, plus x_coords est petit (et inversement à l'est)
  
  # Ajouter de la variabilité (collines et vallées)
  variability <- rnorm(n_cells, mean = 0, sd = 150)
  
  # Valeurs finales
  values(dem_rwanda) <<- pmax(950, pmin(2500, base_elevation + variability))
  
  cat("✓ DEM fictif créé\n")
})

cat("  Résolution:", paste(round(res(dem_rwanda)), collapse = " x "), "mètres\n")
cat("  Élévation min:", round(global(dem_rwanda, "min", na.rm=TRUE)[,1]), "m\n")
cat("  Élévation max:", round(global(dem_rwanda, "max", na.rm=TRUE)[,1]), "m\n\n")


# ==============================================================================
# PARTIE 5 : CRÉATION DU GRAPHE ROUTIER AVEC SFNETWORKS
# ==============================================================================

cat("=== PARTIE 5 : Création du graphe routier ===\n")

# --- Nettoyage des types de géométries ---
cat("Vérification des types de géométries...\n")

types_geom <- st_geometry_type(routes_rwanda)
cat("  Types trouvés:", paste(unique(types_geom), collapse = ", "), "\n")

routes_rwanda_clean <- routes_rwanda %>%
  st_cast("LINESTRING", warn = FALSE) %>%
  filter(st_geometry_type(.) == "LINESTRING") %>%
  st_make_valid()

cat("✓ Géométries nettoyées:", nrow(routes_rwanda_clean), "LINESTRING\n\n")

# --- Conversion en réseau sfnetworks ---
cat("Création du réseau...\n")
reseau_rwanda <- as_sfnetwork(routes_rwanda_clean, directed = FALSE)

cat("✓ Réseau créé\n")
cat("  Nœuds:", igraph::vcount(reseau_rwanda), "\n")
cat("  Arêtes:", igraph::ecount(reseau_rwanda), "\n\n")

# --- Calcul de la longueur des arêtes ---
cat("Calcul des longueurs des arêtes...\n")

reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(longueur_m = as.numeric(st_length(geometry)))

# --- Fonction pour subdiviser une ligne en segments de longueur max ---
subdiviser_ligne <- function(ligne, attributs, crs, max_length = 2000) {
  longueur_totale <- as.numeric(st_length(st_sfc(ligne, crs = crs)))
  
  if (longueur_totale <= max_length) {
    return(st_sf(attributs, geometry = st_sfc(ligne, crs = crs)))
  }
  
  n_segments <- ceiling(longueur_totale / max_length)
  proportions <- seq(0, 1, length.out = n_segments + 1)
  segments <- list()
  
  for (i in 1:(length(proportions) - 1)) {
    debut <- proportions[i]
    fin <- proportions[i + 1]
    
    if (debut == 0 && fin == 1) {
      segment <- ligne
    } else {
      points <- st_line_sample(st_sfc(ligne, crs = crs), sample = c(debut, fin))
      coords <- st_coordinates(points)
      if (nrow(coords) >= 2) {
        segment <- st_linestring(coords[1:2, 1:2])
      } else {
        segment <- ligne
      }
    }
    
    segments[[i]] <- st_sf(attributs, geometry = st_sfc(segment, crs = crs))
  }
  
  return(bind_rows(segments))
}

# --- Subdivision RÉELLE des arêtes longues ---
cat("Subdivision des arêtes longues (>5km)...\n")

# Extraire les arêtes
aretes_avant <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf()

n_aretes_avant <- nrow(aretes_avant)

# Identifier arêtes courtes et longues
aretes_courtes <- aretes_avant %>% filter(longueur_m <= 5000)
aretes_longues <- aretes_avant %>% filter(longueur_m > 5000)

cat("  Arêtes à subdiviser:", nrow(aretes_longues), "\n")

# Récupérer le CRS depuis le dataframe (pas depuis la géométrie individuelle)
crs_reseau <- st_crs(aretes_longues)

if (nrow(aretes_longues) > 0) {
  # Subdiviser chaque arête longue
  aretes_subdiv_list <- list()
  
  for (i in 1:nrow(aretes_longues)) {
    ligne <- st_geometry(aretes_longues[i,])[[1]]
    attributs <- aretes_longues[i,] %>% st_drop_geometry()
    
    segments <- subdiviser_ligne(ligne, attributs, crs = crs_reseau, max_length = 2000)
    aretes_subdiv_list[[i]] <- segments
  }
  
  # Combiner tous les segments
  aretes_subdiv <- bind_rows(aretes_subdiv_list)
  
  # Recombiner avec arêtes courtes
  routes_final <- bind_rows(aretes_courtes, aretes_subdiv)
  
  # Recréer le réseau
  reseau_rwanda <- as_sfnetwork(routes_final, directed = FALSE)
  
  # Recalculer les longueurs
  reseau_rwanda <- reseau_rwanda %>%
    activate("edges") %>%
    mutate(longueur_m = as.numeric(st_length(geometry)))
}

n_aretes_apres <- igraph::ecount(reseau_rwanda)

cat("✓ Subdivision terminée\n")
cat("  Arêtes avant:", n_aretes_avant, "\n")
cat("  Arêtes après:", n_aretes_apres, "\n")
cat("  Arêtes ajoutées:", n_aretes_apres - n_aretes_avant, "\n")
cat("  Nœuds finaux:", igraph::vcount(reseau_rwanda), "\n\n")

# ==============================================================================
# PARTIE 5 BIS : CORRECTIONS TOPOLOGIQUES DU RÉSEAU (méthode snapping + subdivision)
# ==============================================================================

cat("=== PARTIE 5 BIS : Corrections topologiques du réseau ===\n\n")

cat("État initial:\n")
cat("  Nœuds:", igraph::vcount(reseau_rwanda), "\n")
cat("  Arêtes:", igraph::ecount(reseau_rwanda), "\n")
cat("  Composantes:", igraph::count_components(reseau_rwanda), "\n\n")

# =========================================================
# ÉTAPE 1 : SUBDIVISION DES ARÊTES AUX INTERSECTIONS
# Crée des nœuds là où des routes se croisent sans nœud commun
# Équivalent de la Figure 4 dans la méthode décrite
# =========================================================

cat("Étape 1 : Subdivision aux intersections...\n")

reseau_subdivise <- reseau_rwanda %>%
  convert(to_spatial_subdivision)

cat("  Nœuds après subdivision:", igraph::vcount(reseau_subdivise), "\n")
cat("  Arêtes après subdivision:", igraph::ecount(reseau_subdivise), "\n")
cat("  Composantes après subdivision:",
    igraph::count_components(reseau_subdivise), "\n\n")

# =========================================================
# ÉTAPE 2 : SNAPPING DES EXTRÉMITÉS PROCHES
# Connecte les bouts de routes séparés par une faible distance
# Équivalent des "~328 snaps" décrits dans la méthode
# On applique plusieurs seuils successifs
# =========================================================

cat("Étape 2 : Snapping des extrémités proches...\n")

# Fonction de snapping : connecte les nœuds du réseau
# qui sont dans un rayon `tolerance` mais pas encore reliés
snapper_reseau <- function(reseau, tolerance_m) {
  
  # Extraire les arêtes
  aretes_sf <- reseau %>%
    activate("edges") %>%
    st_as_sf()
  
  # Appliquer st_snap sur les géométries des arêtes
  # Connecte les extrémités proches entre segments
  aretes_snapped <- aretes_sf %>%
    mutate(
      geometry = st_snap(geometry, geometry, tolerance = tolerance_m)
    ) %>%
    st_make_valid() %>%
    filter(st_geometry_type(geometry) == "LINESTRING") %>%
    filter(!st_is_empty(geometry))
  
  # Reconstruire le réseau
  reseau_new <- as_sfnetwork(aretes_snapped, directed = FALSE) %>%
    activate("edges") %>%
    mutate(longueur_m = as.numeric(st_length(geometry)))
  
  return(reseau_new)
}

# Snapping progressif avec 3 seuils
# (du plus fin au plus large, comme recommandé en pratique)
seuils <- c(5, 15, 30)  # en mètres

reseau_snap <- reseau_subdivise

for (seuil in seuils) {
  n_comp_avant <- igraph::count_components(reseau_snap)
  
  tryCatch({
    reseau_snap <- snapper_reseau(reseau_snap, seuil)
    
    # Subdivision après chaque snap pour intégrer les nouveaux nœuds
    reseau_snap <- reseau_snap %>%
      convert(to_spatial_subdivision)
    
    n_comp_apres <- igraph::count_components(reseau_snap)
    
    cat("  Seuil", seuil, "m →",
        n_comp_avant, "composantes →",
        n_comp_apres, "composantes\n")
    
  }, error = function(e) {
    cat("  ⚠ Seuil", seuil, "m échoué:", conditionMessage(e), "\n")
  })
}

cat("\n")

# =========================================================
# ÉTAPE 3 : SUPPRESSION DES PSEUDO-NŒUDS (lissage)
# Retire les nœuds de degré 2 qui ne sont que des points
# intermédiaires sur une ligne droite
# =========================================================

cat("Étape 3 : Lissage du réseau (suppression pseudo-nœuds)...\n")

reseau_lisse <- reseau_snap %>%
  convert(to_spatial_smooth)

cat("  Nœuds après lissage:", igraph::vcount(reseau_lisse), "\n")
cat("  Arêtes après lissage:", igraph::ecount(reseau_lisse), "\n")
cat("  Composantes après lissage:",
    igraph::count_components(reseau_lisse), "\n\n")

# =========================================================
# ÉTAPE 4 : EXTRACTION DE LA COMPOSANTE GÉANTE
# Après corrections topologiques, la composante géante
# devrait couvrir la grande majorité du réseau
# =========================================================

cat("Étape 4 : Extraction de la composante géante...\n")

composantes_finales <- igraph::components(reseau_lisse %>% as_tbl_graph())
id_geante <- which.max(composantes_finales$csize)
noeuds_geante <- which(composantes_finales$membership == id_geante)

# Part du réseau dans la composante géante
pct_noeuds <- round(length(noeuds_geante) / igraph::vcount(reseau_lisse) * 100, 1)
pct_aretes <- round(composantes_finales$csize[id_geante] / igraph::vcount(reseau_lisse) * 100, 1)

cat("  Composante géante:", composantes_finales$csize[id_geante], "nœuds\n")
cat("  Soit", pct_noeuds, "% du réseau total\n\n")

reseau_rwanda <- reseau_lisse %>%
  activate("nodes") %>%
  filter(row_number() %in% noeuds_geante) %>%
  activate("nodes") %>%
  mutate(node_id = row_number())

# =========================================================
# ÉTAPE 5 : RECALCUL DES ATTRIBUTS MANQUANTS
# Après les opérations topologiques, certains attributs
# peuvent avoir été perdus → recalcul propre
# =========================================================

cat("Étape 5 : Recalcul des attributs...\n")

reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    longueur_m = as.numeric(st_length(geometry)),
    length_km  = longueur_m / 1000,
    # Recalcul vitesse si manquante
    speed_kmh = case_when(
      !is.na(speed_kmh) & speed_kmh > 0 ~ speed_kmh,
      road_type == "trunk"               ~ 60,
      road_type == "primary"             ~ 60,
      road_type == "secondary"           ~ 50,
      road_type == "tertiary"            ~ 45,
      TRUE                               ~ 30
    ),
    # Recalcul temps de parcours
    travel_time_h = length_km / speed_kmh,
    # Recalcul coût généralisé si manquant
    cost_generalized_usd = case_when(
      !is.na(cost_generalized_usd) & cost_generalized_usd > 0 ~ cost_generalized_usd,
      TRUE ~ travel_time_h * 7.5 + length_km * 0.08
    ),
    cost_per_km = cost_generalized_usd / length_km
  )

# =========================================================
# BILAN FINAL
# =========================================================

cat("\n=== Bilan des corrections topologiques ===\n")
cat("  Nœuds initiaux    :", igraph::vcount(reseau_rwanda %>%
                                              activate("nodes")), "\n")
cat("  Arêtes finales    :", igraph::ecount(reseau_rwanda), "\n")
cat("  Composantes finales:", igraph::count_components(reseau_rwanda), "\n")

# Vérification : longueur totale du réseau
longueur_totale <- reseau_rwanda %>%
  activate("edges") %>%
  as_tibble() %>%
  summarise(total = sum(length_km, na.rm = TRUE)) %>%
  pull(total)

cat("  Longueur totale   :", round(longueur_totale), "km\n")
cat("  (attendu ~14 000 km pour le Rwanda)\n\n")

# Alerte si réseau trop réduit
if (longueur_totale < 5000) {
  cat("⚠ ATTENTION: réseau trop réduit —",
      "vérifier les seuils de snapping\n\n")
} else {
  cat("✓ Réseau topologiquement corrigé et prêt\n\n")
}


# ==============================================================================
# PARTIE 6 : DÉFINITION DES NŒUDS D'ENTREPOSAGE
# ==============================================================================

cat("=== PARTIE 6 : Création des nœuds d'entreposage ===\n")

# NOTE : Les positions exactes des entrepôts, SEZ, et marchés ne sont pas
# toujours disponibles dans OSM de manière fiable. Nous créons donc une
# base de données fictive mais réaliste pour le Rwanda.

cat("ATTENTION: Données d'entreposage non disponibles publiquement\n")
cat("→ Création d'une base de données FICTIVE mais réaliste\n\n")

# --- Création de nœuds d'entreposage fictifs ---
# Basés sur les principales zones économiques connues du Rwanda

entreposages_fictifs <- tibble(
  nom = c(
    # Capitale et hub principal
    "Kigali - Hub Central",
    "Kigali - SEZ Masoro",
    "Kigali - Marché Kimisagara",
    
    # Frontières (points d'entrée/sortie critiques)
    "Frontière Gatuna (Ouganda)",
    "Frontière Rusumo (Tanzanie)",
    "Frontière Rubavu/Goma (RDC)",
    "Frontière Kagitumba (Ouganda)",
    
    # Capitales de province
    "Huye (Butare) - Centre Sud",
    "Musanze - Centre Nord",
    "Rubavu - Centre Ouest",
    "Rusizi - Centre Sud-Ouest",
    
    # Zones économiques spéciales
    "Bugesera SEZ (Agro-industrie)",
    
    # Centres urbains secondaires
    "Muhanga",
    "Nyanza",
    "Rwamagana"
  ),
  
  type = c(
    "hub", "sez", "marche",
    "frontiere", "frontiere", "frontiere", "frontiere",
    "ville", "ville", "ville", "ville",
    "sez",
    "ville", "ville", "ville"
  ),
  
  # Coordonnées approximatives (lon, lat en WGS84)
  # Ces coordonnées sont fictives mais réalistes pour les vraies localisations
  lon = c(
    30.0619, 30.1300, 30.0588,           # Kigali
    30.0890, 30.7850, 29.2600, 30.7500,  # Frontières
    29.7388, 29.6333, 29.2650, 29.0100,  # Villes
    30.1500,                              # Bugesera
    29.7400, 29.7550, 30.4300            # Centres secondaires
  ),
  
  lat = c(
    -1.9536, -1.9000, -1.9700,           # Kigali
    -1.3800, -2.3800, -1.6667, -1.3100,  # Frontières
    -2.5965, -1.4992, -1.6750, -2.4900,  # Villes
    -2.1000,                              # Bugesera
    -2.0850, -2.3500, -1.8700            # Centres secondaires
  )
)

# --- Conversion en objet sf et projection ---
entreposages_sf <- entreposages_fictifs %>%
  st_as_sf(coords = c("lon", "lat"), crs = 4326) %>%
  st_transform(crs = 32735)  # Même CRS que le réseau

cat("✓ Nœuds d'entreposage créés:", nrow(entreposages_sf), "\n")
cat("  Types:", paste(unique(entreposages_sf$type), collapse = ", "), "\n\n")

# --- Accrochage (snapping) des entrepôts au réseau ---
# On trouve le nœud du réseau le plus proche de chaque entrepôt

cat("Accrochage des entrepôts au réseau routier...\n")

# Extraction des nœuds du réseau
noeuds_reseau <- reseau_rwanda %>%
  activate("nodes") %>%
  st_as_sf()

# Pour chaque entrepôt, trouver le nœud le plus proche
entreposages_avec_snap <- entreposages_sf %>%
  mutate(
    # Index du nœud le plus proche
    noeud_proche_id = st_nearest_feature(geometry, noeuds_reseau),
    
    # Distance au nœud (pour vérification qualité)
    distance_snap = as.numeric(
      st_distance(geometry, noeuds_reseau[noeud_proche_id,], by_element = TRUE)
    )
  )

# Vérification des distances de snap
cat("  Distance de snap moyenne:", round(mean(entreposages_avec_snap$distance_snap)), "m\n")
cat("  Distance de snap maximale:", round(max(entreposages_avec_snap$distance_snap)), "m\n")

# Ajouter un attribut aux nœuds du réseau pour identifier les entreposages
reseau_rwanda <- reseau_rwanda %>%
  activate("nodes") %>%
  mutate(
    node_id = row_number(),
    is_warehouse = node_id %in% entreposages_avec_snap$noeud_proche_id,
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

cat("✓ Entreposages intégrés au réseau\n\n")


# ==============================================================================
# PARTIE 7 : CALCUL DES PENTES
# ==============================================================================

cat("=== PARTIE 7 : Calcul des pentes pour chaque arête ===\n")

# --- Fonction pour calculer la pente d'une arête ---
calculer_pente_arete <- function(ligne_geom, dem, espacement = 100) {
  # Échantillonnage de points le long de la ligne
  # espacement en mètres
  longueur <- as.numeric(st_length(ligne_geom))  # Convertir en numérique
  
  # Si la ligne est très courte, utiliser seulement début et fin
  if (longueur < espacement * 2) {
    points <- st_line_sample(ligne_geom, n = 2)
  } else {
    # Nombre de points à échantillonner
    n_points <- max(2, floor(longueur / espacement))
    points <- st_line_sample(ligne_geom, n = n_points)
  }
  
  # Conversion en points individuels
  points_sf <- st_cast(points, "POINT")
  
  # Extraction des élévations
  elevations <- terra::extract(dem, vect(points_sf), method = "bilinear")
  elev_values <- elevations[, 2]  # 2ème colonne = valeur d'élévation
  
  # Gestion des NA (points hors du DEM)
  if (any(is.na(elev_values)) || length(elev_values) < 2) {
    return(list(
      slope_mean = 0,
      elevation_gain = 0,
      elevation_loss = 0,
      rugosity = 0
    ))
  }
  
  # Calcul de la pente moyenne (début → fin)
  elev_debut <- elev_values[1]
  elev_fin <- elev_values[length(elev_values)]
  denivele_net <- elev_fin - elev_debut
  slope_mean_pct <- (denivele_net / longueur) * 100
  
  # Calcul du dénivelé positif (D+) et négatif (D-)
  differences <- diff(elev_values)
  elevation_gain <- sum(differences[differences > 0], na.rm = TRUE)
  elevation_loss <- abs(sum(differences[differences < 0], na.rm = TRUE))
  
  # Coefficient de rugosité altimétrique
  rugosity <- (elevation_gain + elevation_loss) / longueur
  
  return(list(
    slope_mean = slope_mean_pct,
    elevation_gain = elevation_gain,
    elevation_loss = elevation_loss,
    rugosity = rugosity
  ))
}

# --- Application du calcul à toutes les arêtes ---
cat("Calcul des pentes en cours...\n")
cat("(Cela peut prendre 3-10 minutes selon le nombre d'arêtes)\n")

# Extraction des arêtes
aretes_avec_geom <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf()

# Calcul des pentes pour chaque arête
n_aretes <- nrow(aretes_avec_geom)
resultats_pentes <- vector("list", n_aretes)

# Boucle avec indication de progression
for (i in seq_len(n_aretes)) {
  if (i %% 500 == 0 || i == n_aretes) {
    cat("  Progression:", round(i/n_aretes*100, 1), "%\n")
  }
  
  resultats_pentes[[i]] <- calculer_pente_arete(
    aretes_avec_geom$geometry[i],
    dem_rwanda,
    espacement = 100  # Point tous les 100m
  )
}

cat("  Calcul terminé\n")

# Conversion des résultats en dataframe
pentes_df <- bind_rows(resultats_pentes)

# Ajout au réseau
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    slope_mean = pentes_df$slope_mean,
    elevation_gain = pentes_df$elevation_gain,
    elevation_loss = pentes_df$elevation_loss,
    rugosity = pentes_df$rugosity,
    
    # Catégorie de pente (pour faciliter analyse)
    slope_category = case_when(
      abs(slope_mean) < 2 ~ "plat",
      abs(slope_mean) < 5 ~ "legere",
      abs(slope_mean) < 8 ~ "moderee",
      TRUE ~ "forte"
    )
  )

cat("✓ Pentes calculées pour toutes les arêtes\n")

# Statistiques sur les pentes
stats_pentes <- reseau_rwanda %>%
  activate("edges") %>%
  as_tibble() %>%
  summarise(
    pente_moyenne = mean(abs(slope_mean), na.rm = TRUE),
    pente_mediane = median(abs(slope_mean), na.rm = TRUE),
    d_plus_moyen = mean(elevation_gain, na.rm = TRUE),
    d_moins_moyen = mean(elevation_loss, na.rm = TRUE)
  )

cat("  Pente moyenne:", round(stats_pentes$pente_moyenne, 2), "%\n")
cat("  D+ moyen par arête:", round(stats_pentes$d_plus_moyen, 1), "m\n\n")


# ==============================================================================
# PARTIE 8 : ATTRIBUTION DES VITESSES
# ==============================================================================

cat("=== PARTIE 8 : Attribution des vitesses ===\n")

# --- Jointure avec les vitesses de référence ---
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  left_join(VITESSES_REFERENCE, by = c("road_type", "surface")) %>%
  mutate(
    # Si vitesse manquante (cas non prévu), utiliser 30 km/h par défaut
    vitesse_base = if_else(is.na(vitesse_kmh), 30, vitesse_kmh)
  )

# --- Ajustement selon la pente ---
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    # Facteur de réduction selon pente
    facteur_pente = case_when(
      slope_category == "plat" ~ FACTEURS_PENTE$plat,
      slope_category == "legere" ~ FACTEURS_PENTE$legere,
      slope_category == "moderee" ~ FACTEURS_PENTE$moderee,
      slope_category == "forte" ~ FACTEURS_PENTE$forte,
      TRUE ~ 1.0
    ),
    
    # Vitesse effective après ajustement pente
    speed_kmh = vitesse_base * facteur_pente
  )

cat("✓ Vitesses attribuées\n")

# Statistiques
stats_vitesses <- reseau_rwanda %>%
  activate("edges") %>%
  as_tibble() %>%
  summarise(
    vitesse_min = min(speed_kmh, na.rm = TRUE),
    vitesse_moyenne = mean(speed_kmh, na.rm = TRUE),
    vitesse_max = max(speed_kmh, na.rm = TRUE)
  )

cat("  Vitesse moyenne:", round(stats_vitesses$vitesse_moyenne, 1), "km/h\n")
cat("  Plage:", round(stats_vitesses$vitesse_min, 1), "-", 
    round(stats_vitesses$vitesse_max, 1), "km/h\n\n")


# ==============================================================================
# PARTIE 9 : CALCUL DE LA CONSOMMATION DE CARBURANT
# ==============================================================================

cat("=== PARTIE 9 : Calcul de la consommation de carburant ===\n")

# --- Consommation ajustée selon surface et pente ---
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    # Facteur d'ajustement selon surface
    facteur_surface = case_when(
      surface == "paved" ~ PARAMS_VEHICULE$facteur_paved,
      surface == "gravel" ~ PARAMS_VEHICULE$facteur_gravel,
      surface == "unpaved" ~ PARAMS_VEHICULE$facteur_unpaved,
      TRUE ~ 1.0
    ),
    
    # Ajustement selon pente (méthode simplifiée)
    # Pour chaque % de pente en montée : +1.5% de consommation
    facteur_pente_conso = if_else(
      slope_mean > 0,
      1 + (slope_mean * FACTEUR_CONSO_PENTE / 100),
      1.0  # Pas de bonus en descente (freinage moteur compensé par sécurité)
    ),
    
    # Consommation finale (L/100km)
    conso_L_per_100km = PARAMS_VEHICULE$conso_base * facteur_surface * facteur_pente_conso,
    
    # Consommation totale pour cette arête (litres)
    fuel_consumption_L = (longueur_m / 1000) * (conso_L_per_100km / 100)
  )

cat("✓ Consommation calculée\n")

# Statistiques
stats_conso <- reseau_rwanda %>%
  activate("edges") %>%
  as_tibble() %>%
  summarise(
    conso_moyenne = mean(conso_L_per_100km, na.rm = TRUE),
    conso_min = min(conso_L_per_100km, na.rm = TRUE),
    conso_max = max(conso_L_per_100km, na.rm = TRUE)
  )

cat("  Consommation moyenne:", round(stats_conso$conso_moyenne, 1), "L/100km\n")
cat("  Plage:", round(stats_conso$conso_min, 1), "-", 
    round(stats_conso$conso_max, 1), "L/100km\n\n")


# ==============================================================================
# PARTIE 10 : CALCUL DES COÛTS GÉNÉRALISÉS
# ==============================================================================

cat("=== PARTIE 10 : Calcul des coûts de transport généralisés ===\n")

# --- Calcul de tous les coûts ---
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    # Recalcul de la longueur en km (pour clarté)
    length_km = longueur_m / 1000,
    
    # Temps de parcours (heures)
    travel_time_h = length_km / speed_kmh,
    
    # Coût carburant (USD)
    cost_fuel_usd = fuel_consumption_L * PARAMS_ECONOMIQUES$prix_carburant,
    
    # Coût usure véhicule (USD)
    cost_wear_usd = case_when(
      surface == "paved" ~ length_km * PARAMS_ECONOMIQUES$usure_paved,
      surface == "gravel" ~ length_km * PARAMS_ECONOMIQUES$usure_gravel,
      surface == "unpaved" ~ length_km * PARAMS_ECONOMIQUES$usure_unpaved,
      TRUE ~ length_km * PARAMS_ECONOMIQUES$usure_gravel
    ),
    
    # Coût du temps (USD)
    cost_time_usd = travel_time_h * PARAMS_ECONOMIQUES$valeur_temps,
    
    # COÛT GÉNÉRALISÉ TOTAL (métrique principale)
    cost_generalized_usd = cost_fuel_usd + cost_wear_usd + cost_time_usd,
    
    # Coût par kilomètre (pour comparaisons)
    cost_per_km = cost_generalized_usd / length_km
  )

cat("✓ Coûts généralisés calculés\n")

# Statistiques finales
stats_couts <- reseau_rwanda %>%
  activate("edges") %>%
  as_tibble() %>%
  summarise(
    cout_total_moyen = mean(cost_generalized_usd, na.rm = TRUE),
    cout_par_km_moyen = mean(cost_per_km, na.rm = TRUE),
    part_carburant = mean(cost_fuel_usd / cost_generalized_usd, na.rm = TRUE) * 100,
    part_temps = mean(cost_time_usd / cost_generalized_usd, na.rm = TRUE) * 100,
    part_usure = mean(cost_wear_usd / cost_generalized_usd, na.rm = TRUE) * 100
  )

cat("  Coût moyen par km:", round(stats_couts$cout_par_km_moyen, 3), "USD/km\n")
cat("  Répartition des coûts:\n")
cat("    - Carburant:", round(stats_couts$part_carburant, 1), "%\n")
cat("    - Temps:", round(stats_couts$part_temps, 1), "%\n")
cat("    - Usure:", round(stats_couts$part_usure, 1), "%\n\n")


# ==============================================================================
# PARTIE 11 : CRÉATION DE LA MATRICE ORIGINE-DESTINATION
# ==============================================================================

cat("=== PARTIE 11 : Calcul de la matrice origine-destination ===\n")

# --- Extraction des nœuds d'entreposage ---
noeuds_entreposage <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  as_tibble() %>%
  mutate(warehouse_id = row_number())

n_warehouses <- nrow(noeuds_entreposage)
cat("Nombre de nœuds d'entreposage:", n_warehouses, "\n")

# --- Calcul des plus courts chemins (algorithme de Dijkstra) ---
# On utilise le coût généralisé comme poids

cat("Calcul des plus courts chemins entre tous les entreposages...\n")
cat("(Cela peut prendre 1-5 minutes)\n")

# Extraction du graphe igraph sous-jacent
graphe_igraph <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(weight = cost_generalized_usd) %>%
  as_tbl_graph()

# Identifiants des nœuds d'entreposage dans le graphe
warehouse_node_ids <- which(igraph::V(graphe_igraph)$is_warehouse)

# Initialisation de la matrice
matrice_couts <- matrix(0, nrow = n_warehouses, ncol = n_warehouses)
matrice_distances <- matrix(0, nrow = n_warehouses, ncol = n_warehouses)
matrice_temps <- matrix(0, nrow = n_warehouses, ncol = n_warehouses)

# Calcul pour chaque paire
for (i in seq_along(warehouse_node_ids)) {
  # Plus courts chemins depuis ce nœud vers tous les autres
  chemins <- igraph::shortest_paths(
    graphe_igraph,
    from = warehouse_node_ids[i],
    to = warehouse_node_ids,
    weights = igraph::E(graphe_igraph)$cost_generalized_usd,
    output = "both"
  )
  
  # Pour chaque destination
  for (j in seq_along(warehouse_node_ids)) {
    if (i != j) {
      # Extraire les arêtes du chemin
      edges_path <- chemins$epath[[j]]
      
      if (length(edges_path) > 0) {
        # Récupérer les attributs des arêtes du chemin
        edge_data <- igraph::edge_attr(graphe_igraph)
        
        matrice_couts[i, j] <- sum(edge_data$cost_generalized_usd[edges_path])
        matrice_distances[i, j] <- sum(edge_data$length_km[edges_path])
        matrice_temps[i, j] <- sum(edge_data$travel_time_h[edges_path])
      }
    }
  }
  
  if (i %% 3 == 0) {
    cat("  Progression:", round(i/n_warehouses*100, 1), "%\n")
  }
}

cat("  Progression: 100.0%\n")
cat("✓ Matrice O-D calculée\n\n")

# --- Conversion en dataframes pour export ---
rownames(matrice_couts) <- noeuds_entreposage$warehouse_name
colnames(matrice_couts) <- noeuds_entreposage$warehouse_name

rownames(matrice_distances) <- noeuds_entreposage$warehouse_name
colnames(matrice_distances) <- noeuds_entreposage$warehouse_name

rownames(matrice_temps) <- noeuds_entreposage$warehouse_name
colnames(matrice_temps) <- noeuds_entreposage$warehouse_name


# ==============================================================================
# PARTIE 12 : VISUALISATIONS
# ==============================================================================

cat("=== PARTIE 12 : Création des visualisations ===\n")

cat("Chargement des couches depuis le fichier PBF...\n")

# =========================================================
# FRONTIÈRES ADMINISTRATIVES DEPUIS LE FICHIER PBF
# =========================================================

cat("Extraction des frontières administratives...\n")

limites_raw <- st_read(
  chemin_pbf,
  layer = "multipolygons",
  query = "SELECT * FROM multipolygons WHERE admin_level IN ('2', '4')",
  quiet = TRUE
) %>%
  rename(geometry = `_ogr_geometry_`) %>%
  st_as_sf() %>%
  st_make_valid() %>%
  filter(st_geometry_type(geometry) %in% c("POLYGON", "MULTIPOLYGON"))

# Frontière nationale du Rwanda (admin_level = 2)
rwanda_national <- limites_raw %>%
  filter(admin_level == "2") %>%
  st_union() %>%
  st_as_sf() %>%
  st_transform(crs = 32735) %>%
  st_make_valid()

cat("✓ Frontière nationale extraite\n")

# Provinces du Rwanda (admin_level = 4)
rwanda_provinces <- limites_raw %>%
  filter(admin_level == "4") %>%
  st_transform(crs = 32735) %>%
  st_make_valid()

if (nrow(rwanda_provinces) > 0) {
  cat("✓ Provinces extraites:", nrow(rwanda_provinces), "provinces\n")
} else {
  cat("⚠ Pas de provinces - utilisation frontière nationale\n")
  rwanda_provinces <- rwanda_national
}

# =========================================================
# LACS DEPUIS LE FICHIER PBF
# =========================================================

cat("Extraction des étendues d'eau...\n")

lacs_ok <- FALSE
lacs_raw <- NULL

tryCatch({
  lacs_raw <- st_read(
    chemin_pbf,
    layer = "multipolygons",
    query = "SELECT * FROM multipolygons WHERE 
             natural = 'water' OR 
             other_tags LIKE '%\"natural\"=>\"water\"%' OR 
             other_tags LIKE '%\"water\"=>\"lake\"%' OR
             other_tags LIKE '%\"water\"=>\"reservoir\"%'",
    quiet = TRUE
  ) %>%
    rename(geometry = `_ogr_geometry_`) %>%
    st_as_sf() %>%
    st_make_valid() %>%
    filter(st_geometry_type(geometry) %in% c("POLYGON", "MULTIPOLYGON")) %>%
    st_transform(crs = 32735) %>%
    mutate(aire_km2 = as.numeric(st_area(geometry)) / 1e6) %>%
    filter(aire_km2 > 1)
  
  if (nrow(lacs_raw) > 0) {
    lacs_ok <- TRUE
    cat("✓ Étendues d'eau extraites:", nrow(lacs_raw), "objets (>1km²)\n")
  } else {
    cat("⚠ Aucune étendue d'eau significative trouvée\n")
  }
  
}, error = function(e) {
  cat("⚠ Extraction lacs échouée:", conditionMessage(e), "\n")
  lacs_ok <<- FALSE
  lacs_raw <<- NULL
})

# =========================================================
# BBOX 250KM x 250KM CENTRÉE SUR LE RWANDA
# =========================================================

centre_rwanda <- rwanda_national %>%
  st_centroid() %>%
  st_coordinates()

centre_x <- centre_rwanda[1, "X"]
centre_y <- centre_rwanda[1, "Y"]

cat("Centre Rwanda (UTM 35S): X =", round(centre_x), "/ Y =", round(centre_y), "\n")

buffer_km <- 125000

bbox_poly <- st_sfc(
  st_polygon(list(rbind(
    c(centre_x - buffer_km, centre_y - buffer_km),
    c(centre_x + buffer_km, centre_y - buffer_km),
    c(centre_x + buffer_km, centre_y + buffer_km),
    c(centre_x - buffer_km, centre_y + buffer_km),
    c(centre_x - buffer_km, centre_y - buffer_km)
  ))),
  crs = 32735
) %>% st_as_sf()

bbox_carto <- st_bbox(bbox_poly)
cat("✓ bbox 250km x 250km créée\n\n")

cat("Résumé des couches chargées:\n")
cat("  Frontière nationale :", "✓\n")
cat("  Provinces           :", nrow(rwanda_provinces), "\n")
if (lacs_ok) {
  cat("  Lacs (>1km²)        :", nrow(lacs_raw), "\n\n")
} else {
  cat("  Lacs                : aucun\n\n")
}


# ==============================================================================
# FONCTION UTILITAIRE : fond de carte commun
# ==============================================================================

creer_fond_carte <- function() {
  
  carte <- tm_shape(rwanda_provinces, bbox = bbox_carto) +
    tm_polygons(
      fill = "#F5F5F0",
      col = "#AAAAAA",
      lwd = 0.8,
      fill.legend = tm_legend(show = FALSE)
    ) +
    
    tm_shape(rwanda_national) +
    tm_borders(
      col = "#222222",
      lwd = 2.5
    )
  
  if (lacs_ok && !is.null(lacs_raw) && nrow(lacs_raw) > 0) {
    carte <- carte +
      tm_shape(lacs_raw) +
      tm_polygons(
        fill = "#A8C8E8",
        col = "#7AAAC8",
        lwd = 0.5,
        fill.legend = tm_legend(show = FALSE)
      )
  }
  
  return(carte)
}


# --- Carte 1 : Hiérarchie du réseau routier ---
cat("Génération de la carte de hiérarchie routière...\n")

carte_hierarchie <- creer_fond_carte() +
  
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(
    col = "road_type",
    col.scale = tm_scale(
      values = c(
        "trunk"        = "#E41A1C",
        "primary"      = "#FF7F00",
        "secondary"    = "#E8A000",  # ✅ Ancien : #FFD700
        "tertiary"     = "#999999",
        "unclassified" = "#CCCCCC"
      )
    ),
    col.legend = tm_legend(title = "Type de route"),
    lwd = 1.5
  ) +
  
  tm_shape(entreposages_sf) +
  tm_dots(
    fill = "type",
    fill.scale = tm_scale(
      values = c(
        "hub"       = "#000000",
        "sez"       = "#0000FF",
        "marche"    = "#00CC00",
        "frontiere" = "#FF0000",
        "ville"     = "#800080"
      )
    ),
    fill.legend = tm_legend(title = "Entreposages"),
    size = 0.3
  ) +
  
  tm_title("Réseau Routier du Rwanda\nHiérarchie des routes") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position = c("right", "top"))

tmap_save(carte_hierarchie, "/mnt/user-data/outputs/carte_hierarchie_rwanda.png",
          width = 3000, height = 2400, dpi = 300)
cat("✓ Carte de hiérarchie sauvegardée\n")


# --- Carte 2 : Coûts de transport par kilomètre ---
cat("Génération de la carte des coûts...\n")

carte_couts <- creer_fond_carte() +
  
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(
    col = "cost_per_km",
    col.scale = tm_scale_intervals(
      style = "quantile",
      n = 5,
      values = "brewer.yl_or_rd"
    ),
    col.legend = tm_legend(title = "Coût (USD/km)"),
    lwd = 1.5
  ) +
  
  tm_shape(entreposages_sf) +
  tm_dots(fill = "black", size = 0.2) +
  
  tm_title("Coûts de Transport Généralisés\npar kilomètre") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position = c("right", "top"))

tmap_save(carte_couts, "/mnt/user-data/outputs/carte_couts_rwanda.png",
          width = 3000, height = 2400, dpi = 300)
cat("✓ Carte des coûts sauvegardée\n")


# --- Carte 3 : Pentes ---
cat("Génération de la carte des pentes...\n")

carte_pentes <- creer_fond_carte() +
  
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(
    col = "slope_category",
    col.scale = tm_scale(
      values = c(
        "plat"    = "#00AA00",  # ✅ Ancien : #00FF00
        "legere"  = "#AACC00",  # ✅ Ancien : #FFFF00
        "moderee" = "#FF9900",
        "forte"   = "#FF0000"
      )
    ),
    col.legend = tm_legend(title = "Catégorie de pente"),
    lwd = 1.5
  ) +
  
  tm_title("Pentes du Réseau Routier") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position = c("right", "top"))

tmap_save(carte_pentes, "/mnt/user-data/outputs/carte_pentes_rwanda.png",
          width = 3000, height = 2400, dpi = 300)
cat("✓ Carte des pentes sauvegardée\n\n")




# ==============================================================================
# PARTIE 13 : EXPORT DES DONNÉES
# ==============================================================================

cat("=== PARTIE 13 : Export des données ===\n")

# --- Export du réseau complet ---
cat("Export du réseau complet (GeoPackage)...\n")

# Extraction des arêtes avec tous les attributs
aretes_finales <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  select(
    osm_id, name, road_type, surface,
    length_km, slope_mean, elevation_gain, elevation_loss,
    speed_kmh, fuel_consumption_L,
    cost_fuel_usd, cost_wear_usd, cost_time_usd,
    cost_generalized_usd, cost_per_km
  )

# Export en GeoPackage
st_write(aretes_finales, 
         "/mnt/user-data/outputs/reseau_rwanda_aretes.gpkg",
         delete_dsn = TRUE, quiet = TRUE)

cat("✓ Arêtes exportées: reseau_rwanda_aretes.gpkg\n")

# Extraction des nœuds
noeuds_finaux <- reseau_rwanda %>%
  activate("nodes") %>%
  st_as_sf() %>%
  select(node_id, is_warehouse, warehouse_name, warehouse_type)

st_write(noeuds_finaux,
         "/mnt/user-data/outputs/reseau_rwanda_noeuds.gpkg",
         delete_dsn = TRUE, quiet = TRUE)

cat("✓ Nœuds exportés: reseau_rwanda_noeuds.gpkg\n")

# --- Export des matrices O-D en CSV ---
cat("Export des matrices origine-destination...\n")

write.csv(matrice_couts, 
          "/mnt/user-data/outputs/matrice_couts_usd.csv",
          row.names = TRUE)

write.csv(matrice_distances,
          "/mnt/user-data/outputs/matrice_distances_km.csv",
          row.names = TRUE)

write.csv(matrice_temps,
          "/mnt/user-data/outputs/matrice_temps_heures.csv",
          row.names = TRUE)

cat("✓ Matrices O-D exportées (CSV)\n")

# --- Export d'un tableau récapitulatif ---
cat("Export du tableau récapitulatif...\n")

recap_reseau <- tibble(
  Métrique = c(
    "Nombre d'arêtes",
    "Nombre de nœuds",
    "Nombre d'entreposages",
    "Longueur totale (km)",
    "Pente moyenne (%)",
    "Vitesse moyenne (km/h)",
    "Consommation moyenne (L/100km)",
    "Coût moyen (USD/km)",
    "Part carburant (%)",
    "Part temps (%)",
    "Part usure (%)"
  ),
  Valeur = c(
    igraph::ecount(reseau_rwanda),
    igraph::vcount(reseau_rwanda),
    n_warehouses,
    round(sum(aretes_finales$length_km, na.rm = TRUE), 1),
    round(mean(abs(aretes_finales$slope_mean), na.rm = TRUE), 2),
    round(mean(aretes_finales$speed_kmh, na.rm = TRUE), 1),
    round(mean(aretes_finales$fuel_consumption_L / aretes_finales$length_km * 100, na.rm = TRUE), 1),
    round(mean(aretes_finales$cost_per_km, na.rm = TRUE), 3),
    round(stats_couts$part_carburant, 1),
    round(stats_couts$part_temps, 1),
    round(stats_couts$part_usure, 1)
  )
)

write.csv(recap_reseau,
          "/mnt/user-data/outputs/recapitulatif_reseau.csv",
          row.names = FALSE)

cat("✓ Tableau récapitulatif exporté\n\n")


# ==============================================================================
# PARTIE 14 : ANALYSE D'EXEMPLE - TRAJET OPTIMAL
# ==============================================================================

cat("=== PARTIE 14 : Exemple d'analyse - Trajet optimal ===\n")

# Exemple : Calculer le trajet optimal entre Kigali et la frontière de Rusumo

# Identifiants des nœuds
kigali_id <- warehouse_node_ids[which(noeuds_entreposage$warehouse_name == "Kigali - Hub Central")]
rusumo_id <- warehouse_node_ids[which(noeuds_entreposage$warehouse_name == "Frontière Rusumo (Tanzanie)")]

cat("Calcul du trajet Kigali → Rusumo (Tanzanie)...\n")

# Plus court chemin
chemin_optimal <- igraph::shortest_paths(
  graphe_igraph,
  from = kigali_id,
  to = rusumo_id,
  weights = igraph::E(graphe_igraph)$cost_generalized_usd,
  output = "both"
)

# Extraction des arêtes du chemin
edges_chemin <- chemin_optimal$epath[[1]]
edges_data <- igraph::edge_attr(graphe_igraph)

# Statistiques du trajet
stats_trajet <- tibble(
  distance_km = sum(edges_data$length_km[edges_chemin]),
  temps_h = sum(edges_data$travel_time_h[edges_chemin]),
  denivele_plus_m = sum(edges_data$elevation_gain[edges_chemin]),
  cout_total_usd = sum(edges_data$cost_generalized_usd[edges_chemin]),
  cout_carburant_usd = sum(edges_data$cost_fuel_usd[edges_chemin]),
  cout_temps_usd = sum(edges_data$cost_time_usd[edges_chemin])
)

cat("\nRÉSULTATS DU TRAJET OPTIMAL:\n")
cat("  Distance:", round(stats_trajet$distance_km, 1), "km\n")
cat("  Temps de parcours:", round(stats_trajet$temps_h, 2), "heures\n")
cat("  Dénivelé positif:", round(stats_trajet$denivele_plus_m, 0), "m\n")
cat("  Coût total:", round(stats_trajet$cout_total_usd, 2), "USD\n")
cat("    - dont carburant:", round(stats_trajet$cout_carburant_usd, 2), "USD\n")
cat("    - dont temps:", round(stats_trajet$cout_temps_usd, 2), "USD\n\n")


# ==============================================================================
# PARTIE 15 : RAPPORT FINAL
# ==============================================================================

cat("===============================================================\n")
cat("         PROJET TERMINÉ AVEC SUCCÈS\n")
cat("===============================================================\n\n")

cat("FICHIERS GÉNÉRÉS:\n")
cat("  Données:\n")
cat("    • reseau_rwanda_aretes.gpkg    - Réseau routier complet\n")
cat("    • reseau_rwanda_noeuds.gpkg    - Nœuds du réseau\n")
cat("    • matrice_couts_usd.csv        - Matrice O-D des coûts\n")
cat("    • matrice_distances_km.csv     - Matrice O-D des distances\n")
cat("    • matrice_temps_heures.csv     - Matrice O-D des temps\n")
cat("    • recapitulatif_reseau.csv     - Statistiques globales\n")
cat("\n  Cartes:\n")
cat("    • carte_hierarchie_rwanda.png  - Hiérarchie routière\n")
cat("    • carte_couts_rwanda.png       - Coûts par kilomètre\n")
cat("    • carte_pentes_rwanda.png      - Catégories de pente\n")
cat("\n")

cat("UTILISATION DES RÉSULTATS:\n")
cat("  1. Charger les données dans QGIS/ArcGIS pour visualisation\n")
cat("  2. Importer les matrices CSV dans Excel/Python pour modélisation\n")
cat("  3. Utiliser le graphe pour optimisation de tournées\n")
cat("  4. Analyser l'impact d'investissements routiers\n")
cat("\n")

cat("PROCHAINES ÉTAPES POSSIBLES:\n")
cat("  • Valider avec des données réelles de transporteurs\n")
cat("  • Créer des scénarios (saison sèche vs pluies)\n")
cat("  • Intégrer des données de trafic\n")
cat("  • Développer une application Shiny interactive\n")
cat("\n")

cat("===============================================================\n")
cat("Script terminé le:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")
cat("===============================================================\n")


# ==============================================================================
# PACKAGE SUPPLÉMENTAIRE (ajouter "scales" dans packages_requis en Partie 0)
# ==============================================================================

if (!requireNamespace("scales", quietly = TRUE)) install.packages("scales")
library(scales)


# ==============================================================================
# PARTIE 16 : TABLE INPUT-OUTPUT DU RWANDA (DONNÉES FICTIVES RÉALISTES)
# ==============================================================================
# NOTE: La table IO officielle du Rwanda (NISR) n'est pas disponible
# gratuitement via API. Ces données sont fictives mais calibrées sur la
# structure économique connue du Rwanda (économie agricole en transition).
#
# POUR REMPLACER PAR LES DONNÉES RÉELLES :
#   1. Télécharger la table IO sur : https://www.statistics.gov.rw
#   2. Charger le fichier Excel : io_table <- readxl::read_excel("io_rwanda.xlsx")
#   3. Extraire la matrice A et les vecteurs de production/demande
# ==============================================================================

cat("=== PARTIE 16 : Table Input-Output du Rwanda ===\n")
cat("NOTE: Données fictives réalistes (structure économique Rwanda ~2022)\n\n")

# --- Définition des secteurs économiques (8 secteurs) ---
SECTEURS <- c(
  "Agriculture",     # Café, thé, pyrèthre, cultures vivrières
  "Mines",           # Coltan, cassitérite, wolfram
  "Agro_industrie",  # Transformation alimentaire et boissons
  "Industrie",       # Textiles, matériaux de construction
  "Construction",    # BTP, infrastructures
  "Commerce",        # Commerce de gros et de détail
  "Transport",       # Transport et logistique
  "Services"         # Finance, tourisme, services publics
)
N_SECTEURS <- length(SECTEURS)

# --- Matrice des coefficients techniques A ---
# a_ij = part de la production du secteur j consommée par le secteur i (input)
# Source de calibration : structures économiques d'Afrique subsaharienne agricole
# Lignes = secteur qui reçoit (input), Colonnes = secteur qui produit (output)
A <- matrix(c(
  #  Agri   Mines  AgroI  Indus  Const  Comm   Trans  Serv
  0.08,  0.00,  0.45,  0.05,  0.01,  0.05,  0.02,  0.03,  # Agriculture
  0.00,  0.05,  0.01,  0.08,  0.05,  0.00,  0.01,  0.00,  # Mines
  0.05,  0.00,  0.08,  0.02,  0.00,  0.06,  0.03,  0.04,  # Agro_industrie
  0.02,  0.03,  0.03,  0.06,  0.15,  0.03,  0.04,  0.02,  # Industrie
  0.01,  0.02,  0.01,  0.02,  0.08,  0.02,  0.03,  0.05,  # Construction
  0.04,  0.05,  0.06,  0.08,  0.05,  0.06,  0.05,  0.06,  # Commerce
  0.03,  0.06,  0.04,  0.05,  0.06,  0.07,  0.06,  0.04,  # Transport
  0.02,  0.02,  0.02,  0.03,  0.04,  0.06,  0.05,  0.08   # Services
), nrow = N_SECTEURS, ncol = N_SECTEURS, byrow = TRUE,
dimnames = list(SECTEURS, SECTEURS))

# --- Production totale par secteur (millions USD) ---
# Calibrées sur PIB Rwanda ~13 milliards USD (Banque Mondiale 2022)
production_totale <- c(
  Agriculture    = 2100,
  Mines          = 280,
  Agro_industrie = 520,
  Industrie      = 380,
  Construction   = 750,
  Commerce       = 1100,
  Transport      = 480,
  Services       = 2200
)

# --- Calcul des grandeurs dérivées ---
# Consommations intermédiaires totales par secteur
conso_interm <- as.vector(A %*% production_totale)
names(conso_interm) <- SECTEURS

# Valeur ajoutée (= production - consommations intermédiaires)
valeur_ajoutee <- production_totale - conso_interm

# Demande finale (consommation finale ménages + GFCF + exports nets)
demande_finale <- valeur_ajoutee * 0.85  # Simplification réaliste

# --- Facteurs de conversion valeur → masse (tonnes par million USD) ---
# Selon les caractéristiques physiques de chaque secteur
TONNES_PAR_MUSD <- c(
  Agriculture    = 8000,   # Produits agricoles bruts, faible valeur/tonne
  Mines          = 3000,   # Minerais semi-bruts
  Agro_industrie = 4000,   # Produits transformés (céréales, huile...)
  Industrie      = 2000,   # Produits manufacturés (emballages, ciment...)
  Construction   = 10000,  # Matériaux très lourds (gravier, sable, béton)
  Commerce       = 1500,   # Mix de produits distribués
  Transport      = 300,    # Services peu matériels
  Services       = 100     # Services très peu matériels
)

# --- Affichage du résumé ---
recap_io <- tibble(
  Secteur            = SECTEURS,
  Production_MUSD    = round(production_totale, 0),
  Conso_interm_MUSD  = round(conso_interm, 0),
  Valeur_ajoutee_MUSD= round(valeur_ajoutee, 0),
  Part_PIB_pct       = round(production_totale / sum(production_totale) * 100, 1),
  Tonnes_par_MUSD    = TONNES_PAR_MUSD
)

cat("Structure économique modélisée:\n")
print(recap_io)
cat("\n")
cat("✓ Table IO définie:", N_SECTEURS, "secteurs\n")
cat("  PIB total modélisé:", round(sum(production_totale)/1000, 1), "milliards USD\n")
cat("  Multiplicateur moyen:", round(mean(solve(diag(N_SECTEURS) - A) %*% 
                                            rep(1, N_SECTEURS)), 2), "\n\n")

# Affichage de la matrice A
cat("Matrice des coefficients techniques A:\n")
print(round(A, 3))
cat("\n")


# ==============================================================================
# PARTIE 17 : GÉNÉRATION DES OFFRES ET DEMANDES PAR ZONE
# ==============================================================================

cat("=== PARTIE 17 : Génération des offres et demandes par zone ===\n")

# --- Profils sectoriels par type de zone ---
# Chaque profil représente la distribution de l'activité économique
# selon le type de zone. Somme des poids = 1 pour chaque type.

PROFILS_OFFRE <- list(
  # Hub (Kigali) : très diversifié, dominant sur services et commerce
  hub = c(Agriculture=0.02, Mines=0.01, Agro_industrie=0.15,
          Industrie=0.12, Construction=0.08, Commerce=0.25,
          Transport=0.20, Services=0.17),
  # Zone économique spéciale : forte industrie et agro-industrie
  sez = c(Agriculture=0.05, Mines=0.05, Agro_industrie=0.25,
          Industrie=0.30, Construction=0.10, Commerce=0.10,
          Transport=0.10, Services=0.05),
  # Frontière : transit, commerce transfrontalier, mines (export)
  frontiere = c(Agriculture=0.15, Mines=0.15, Agro_industrie=0.10,
                Industrie=0.08, Construction=0.05, Commerce=0.30,
                Transport=0.12, Services=0.05),
  # Ville secondaire : commerce local, construction, services
  ville = c(Agriculture=0.12, Mines=0.02, Agro_industrie=0.08,
            Industrie=0.05, Construction=0.15, Commerce=0.30,
            Transport=0.10, Services=0.18),
  # Marché : très agricole, agro-industrie locale
  marche = c(Agriculture=0.45, Mines=0.01, Agro_industrie=0.20,
             Industrie=0.03, Construction=0.02, Commerce=0.20,
             Transport=0.05, Services=0.04)
)

PROFILS_DEMANDE <- list(
  hub = c(Agriculture=0.05, Mines=0.02, Agro_industrie=0.20,
          Industrie=0.15, Construction=0.10, Commerce=0.20,
          Transport=0.15, Services=0.13),
  sez = c(Agriculture=0.10, Mines=0.08, Agro_industrie=0.15,
          Industrie=0.25, Construction=0.15, Commerce=0.10,
          Transport=0.12, Services=0.05),
  frontiere = c(Agriculture=0.12, Mines=0.06, Agro_industrie=0.12,
                Industrie=0.12, Construction=0.06, Commerce=0.28,
                Transport=0.18, Services=0.06),
  ville = c(Agriculture=0.15, Mines=0.02, Agro_industrie=0.18,
            Industrie=0.10, Construction=0.12, Commerce=0.22,
            Transport=0.08, Services=0.13),
  marche = c(Agriculture=0.38, Mines=0.01, Agro_industrie=0.22,
             Industrie=0.06, Construction=0.04, Commerce=0.20,
             Transport=0.05, Services=0.04)
)

# --- Taille économique relative de chaque zone (Kigali Hub = 1.0) ---
TAILLE_ZONE <- c(
  "Kigali - Hub Central"          = 1.00,
  "Kigali - SEZ Masoro"           = 0.35,
  "Kigali - Marché Kimisagara"    = 0.18,
  "Frontière Gatuna (Ouganda)"    = 0.25,
  "Frontière Rusumo (Tanzanie)"   = 0.20,
  "Frontière Rubavu/Goma (RDC)"   = 0.30,
  "Frontière Kagitumba (Ouganda)" = 0.15,
  "Huye (Butare) - Centre Sud"    = 0.18,
  "Musanze - Centre Nord"         = 0.15,
  "Rubavu - Centre Ouest"         = 0.14,
  "Rusizi - Centre Sud-Ouest"     = 0.12,
  "Bugesera SEZ (Agro-industrie)" = 0.22,
  "Muhanga"                       = 0.08,
  "Nyanza"                        = 0.07,
  "Rwamagana"                     = 0.09
)

# --- Facteur d'échelle : part du PIB qui circule entre zones ---
# (le reste est consommé localement dans chaque zone)
PART_ECHANGEABLE <- 0.35

echelle_offre   <- sum(production_totale) * PART_ECHANGEABLE
echelle_demande <- sum(demande_finale)    * PART_ECHANGEABLE

# --- Génération des matrices offre/demande ---
set.seed(456)  # Reproductibilité

offre_zones <- matrix(0, nrow = n_warehouses, ncol = N_SECTEURS,
                      dimnames = list(noeuds_entreposage$warehouse_name, SECTEURS))
demande_zones <- matrix(0, nrow = n_warehouses, ncol = N_SECTEURS,
                        dimnames = list(noeuds_entreposage$warehouse_name, SECTEURS))

for (i in 1:n_warehouses) {
  nom_zone  <- noeuds_entreposage$warehouse_name[i]
  type_zone <- noeuds_entreposage$warehouse_type[i]
  taille    <- TAILLE_ZONE[nom_zone]
  if (is.na(taille)) taille <- 0.10
  
  profil_o <- PROFILS_OFFRE[[type_zone]]
  profil_d <- PROFILS_DEMANDE[[type_zone]]
  
  # Bruit multiplicatif réaliste ±20%
  bruit_o <- runif(N_SECTEURS, 0.80, 1.20)
  bruit_d <- runif(N_SECTEURS, 0.80, 1.20)
  
  # Offre et demande en millions USD
  offre_zones[i, ]   <- profil_o * taille * echelle_offre   / sum(TAILLE_ZONE) * bruit_o
  demande_zones[i, ] <- profil_d * taille * echelle_demande / sum(TAILLE_ZONE) * bruit_d
}

# --- Tableau récapitulatif ---
recap_zones <- tibble(
  Zone               = noeuds_entreposage$warehouse_name,
  Type               = noeuds_entreposage$warehouse_type,
  Offre_totale_MUSD  = round(rowSums(offre_zones), 2),
  Demande_totale_MUSD= round(rowSums(demande_zones), 2),
  Solde_MUSD         = round(rowSums(offre_zones) - rowSums(demande_zones), 2)
)

cat("Offres et demandes par zone (millions USD):\n")
print(recap_zones)
cat("\n")
cat("✓ Offres et demandes générées\n")
cat("  Offre totale :", round(sum(offre_zones), 1), "M USD\n")
cat("  Demande totale:", round(sum(demande_zones), 1), "M USD\n\n")


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
  Flux_total_MUSD  = sapply(SECTEURS, function(s) round(sum(flux_gravitaire[[s]]), 1)),
  Flux_moyen_MUSD  = sapply(SECTEURS, function(s) {
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
  flux_tonnes_total <- flux_tonnes_total + flux_gravitaire[[s]] * TONNES_PAR_MUSD[s]
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
          "/mnt/user-data/outputs/carte_trafic_fret.png",
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
            "/mnt/user-data/outputs/carte_flux_od.png",
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
  ggplot(aes(x = reorder(Secteur, Flux_total_MUSD),
             y = Flux_total_MUSD,
             fill = Secteur)) +
  geom_col(show.legend = FALSE, width = 0.75) +
  geom_text(aes(label = paste0(Flux_total_MUSD, " M$")),
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

ggsave("/mnt/user-data/outputs/graphique_flux_secteurs.png",
       g1, width = 11, height = 6, dpi = 300)
cat("✓ Graphique flux secteurs sauvegardé\n")


# ============================================================
# GRAPHIQUE 2 : Offre vs Demande par zone
# ============================================================

g2 <- recap_zones %>%
  pivot_longer(
    cols      = c(Offre_totale_MUSD, Demande_totale_MUSD),
    names_to  = "Type_flux",
    values_to = "Valeur"
  ) %>%
  mutate(
    Zone_court = str_trunc(Zone, 28),
    Type_flux  = recode(Type_flux,
                        "Offre_totale_MUSD"   = "Offre",
                        "Demande_totale_MUSD" = "Demande")
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

ggsave("/mnt/user-data/outputs/graphique_offre_demande.png",
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

ggsave("/mnt/user-data/outputs/heatmap_flux_od.png",
       g3, width = 13, height = 11, dpi = 300)
cat("✓ Heatmap flux OD sauvegardée\n")


# ============================================================
# GRAPHIQUE 4 : Composition sectorielle des flux par zone
# ============================================================

offre_long <- as.data.frame(offre_zones) %>%
  rownames_to_column("Zone") %>%
  pivot_longer(-Zone, names_to = "Secteur", values_to = "Offre_MUSD") %>%
  mutate(Zone_court = str_trunc(str_remove(Zone, " - .*"), 22))

g4 <- offre_long %>%
  group_by(Zone_court) %>%
  mutate(Part_pct = Offre_MUSD / sum(Offre_MUSD) * 100) %>%
  ungroup() %>%
  ggplot(aes(x = reorder(Zone_court, -Offre_MUSD),
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

ggsave("/mnt/user-data/outputs/graphique_composition_sectorielle.png",
       g4, width = 14, height = 8, dpi = 300)
cat("✓ Graphique composition sectorielle sauvegardé\n\n")


# ==============================================================================
# EXPORT DES DONNÉES DE FRET
# ==============================================================================

cat("Export des données du modèle de fret...\n")

write.csv(as.data.frame(A),
          "/mnt/user-data/outputs/table_io_coefficients.csv",
          row.names = TRUE)
write.csv(recap_io,
          "/mnt/user-data/outputs/table_io_recap.csv",
          row.names = FALSE)
write.csv(as.data.frame(flux_total) %>% rownames_to_column("Zone"),
          "/mnt/user-data/outputs/matrice_flux_gravitaire_musd.csv",
          row.names = FALSE)
write.csv(as.data.frame(flux_tonnes_total) %>% rownames_to_column("Zone"),
          "/mnt/user-data/outputs/matrice_flux_fret_tonnes.csv",
          row.names = FALSE)
write.csv(recap_zones,
          "/mnt/user-data/outputs/offre_demande_zones.csv",
          row.names = FALSE)

aretes_fret_export <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  select(osm_id, name, road_type, surface,
         length_km, speed_kmh,
         cost_generalized_usd, cost_per_km,
         volume_tonnes, classe_trafic)

st_write(aretes_fret_export,
         "/mnt/user-data/outputs/reseau_rwanda_avec_fret.gpkg",
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