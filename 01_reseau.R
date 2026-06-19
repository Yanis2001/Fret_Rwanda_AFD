################################################################################
# 01_reseau.R
# RÔLE : Acquisition des données géographiques, construction du réseau
#        sfnetworks et enrichissement (pentes, entrepôts, démographie, RWI,
#        emploi RPHC5, tailles composites).
# ENTRÉES  : Fichier PBF MinIO, DEM SRTM, WorldPop, NISR, RPHC5
# SORTIES  : persist_geodata.rds, persist_reseau_base.rds,
#            persist_entreposages.rds + tables DuckDB
# DÉPEND DE : 00_parametres.R
################################################################################
source("00_parametres.R")

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

# Téléchargement du PBF depuis Geofabrik si absent en local.
# Source publique et pérenne — indépendante du compte SSPCloud.
# L'URL datée garantit la reproductibilité (données OSM à date fixe).
if (!file.exists(chemin_pbf)) {
  cat("  Téléchargement du PBF depuis Geofabrik...\n")
  download.file(
    url      = GEOFABRIK_PBF_URL,
    destfile = chemin_pbf,
    mode     = "wb",
    quiet    = FALSE
  )
  cat("✓ PBF téléchargé :", chemin_pbf, "\n\n")
} else {
  cat("✓ PBF déjà présent en local :", chemin_pbf, "\n\n")
}

# Vérification de l'existence du fichier avant de continuer.
# stop() interrompt le script avec un message d'erreur explicite.
if (!file.exists(chemin_pbf)) stop("Fichier PBF introuvable.")

# ── Lecture sélective du fichier PBF ──────────────────────────────────────────
# st_read() peut lire directement un fichier PBF via le driver GDAL/OGR.
# On ne charge que la couche "lines" (routes linéaires) et uniquement
# les types de routes utiles pour le fret (pas les chemins piétons, pistes cyclables…).
# La clause WHERE est exécutée au niveau du driver GDAL : seuls les segments
# pertinents sont chargés en mémoire (gain mémoire important sur un pays entier).
# "highway IN ('motorway', ...)" : dans OSM, l'attribut "highway" classe le type
# de route. On ne garde que les routes sur lesquelles un camion peut circuler.
routes_raw <- st_read(
  chemin_pbf,
  layer = "lines",
  query = "SELECT * FROM lines
           WHERE highway IN
           ('motorway','trunk','primary','secondary','tertiary','unclassified')",
  quiet = FALSE  # Afficher les informations de chargement
)

cat("✓ Données chargées :", nrow(routes_raw), "segments\n\n")

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

################################################################################
# DIAGNOSTIC — EXPLORATION DES TAGS OSM DISPONIBLES DANS LE PBF
# Objectif : inventaire complet des tags directs ET des clés cachées dans
# other_tags, sur toutes les couches (lines, points, multipolygons)
################################################################################

cat("==========================================================\n")
cat("  DIAGNOSTIC — TAGS OSM DISPONIBLES DANS LE PBF\n")
cat("==========================================================\n\n")

if (FALSE) {

# ==============================================================================
# 1. COLONNES DIRECTES PAR COUCHE
# Ces colonnes sont promues au rang de champs SQL par le driver GDAL/OGR
# car elles apparaissent très fréquemment dans le PBF (highway, name, etc.)
# ==============================================================================

cat("── 1. Colonnes directes disponibles par couche ───────────────────────\n\n")

couches_a_explorer <- c("lines", "points", "multipolygons")

for (couche in couches_a_explorer) {
  
  tryCatch({
    # On charge juste 1 ligne pour obtenir la structure sans charger le fichier entier
    echantillon <- st_read(chemin_pbf, layer = couche, query = paste0(
      "SELECT * FROM ", couche, " LIMIT 1"
    ), quiet = TRUE)
    
    cat("  Couche [", couche, "] — ", ncol(echantillon), "colonnes :\n")
    cat("  ", paste(names(echantillon), collapse = ", "), "\n\n")
    
  }, error = function(e) {
    cat("  ⚠ Couche [", couche, "] inaccessible :", conditionMessage(e), "\n\n")
  })
}

# ==============================================================================
# 2. CLÉS CACHÉES DANS other_tags PAR COUCHE
# other_tags stocke tous les attributs secondaires au format :
#   "clé1"=>"valeur1","clé2"=>"valeur2",...
# On extrait toutes les clés uniques présentes dans le fichier.
# ==============================================================================

cat("── 2. Clés cachées dans other_tags par couche ────────────────────────\n\n")

# Taille de l'échantillon pour l'analyse de other_tags
# Augmenter N_LIGNES_SAMPLE pour une analyse plus exhaustive (mais plus lente)
N_LIGNES_SAMPLE <- 5000

# Fonction d'extraction de TOUTES les clés depuis une colonne other_tags
extraire_toutes_cles <- function(vecteur_other_tags) {
  # gregexpr() : trouve toutes les occurrences d'un pattern dans chaque chaîne
  # Pattern : capture le texte entre guillemets AVANT "=>"
  # Exemple : '"surface"=>"asphalt"' → extrait "surface"
  matches <- regmatches(
    vecteur_other_tags,
    gregexpr('"([^"]+)"=>', vecteur_other_tags)
  )
  # unlist() aplatit la liste de résultats en un vecteur
  cles_brutes <- unlist(matches)
  # Nettoyage : supprime les guillemets et "=>"
  cles <- gsub('"([^"]+)"=>', "\\1", cles_brutes)
  sort(unique(cles[cles != ""]))
}

for (couche in couches_a_explorer) {
  
  tryCatch({
    # Chargement d'un échantillon avec uniquement other_tags
    sample_df <- st_read(
      chemin_pbf, layer = couche,
      query = paste0("SELECT other_tags FROM ", couche,
                     " LIMIT ", N_LIGNES_SAMPLE),
      quiet = TRUE
    ) %>% st_drop_geometry()
    
    if (!"other_tags" %in% names(sample_df)) {
      cat("  Couche [", couche, "] : pas de colonne other_tags\n\n")
      next
    }
    
    # Suppression des lignes sans other_tags
    other_tags_valides <- sample_df$other_tags[!is.na(sample_df$other_tags)]
    
    cles_trouvees <- extraire_toutes_cles(other_tags_valides)
    
    cat("  Couche [", couche, "] — ", length(cles_trouvees),
        "clés uniques trouvées sur", length(other_tags_valides),
        "lignes non-NA (sample =", N_LIGNES_SAMPLE, ") :\n")
    
    # Affichage par blocs de 8 clés par ligne pour lisibilité
    chunks <- split(cles_trouvees, ceiling(seq_along(cles_trouvees) / 8))
    for (chunk in chunks) {
      cat("    ", paste(chunk, collapse = " | "), "\n")
    }
    cat("\n")
    
  }, error = function(e) {
    cat("  ⚠ Couche [", couche, "] other_tags inaccessible :",
        conditionMessage(e), "\n\n")
  })
}

# ==============================================================================
# 3. FOCUS : clés les plus fréquentes dans other_tags (top 30)
# Avec leur nombre d'occurrences — utile pour prioriser ce qu'il vaut la
# peine d'extraire dans la suite du script.
# ==============================================================================

cat("── 3. Top 30 des clés les plus fréquentes (couche lines) ────────────\n\n")

tryCatch({
  
  sample_lines <- st_read(
    chemin_pbf, layer = "lines",
    query = paste0("SELECT other_tags FROM lines LIMIT ", N_LIGNES_SAMPLE),
    quiet = TRUE
  ) %>%
    st_drop_geometry() %>%
    filter(!is.na(other_tags))
  
  # Pour chaque ligne, extraire toutes les clés individuellement
  toutes_cles_lines <- unlist(lapply(
    sample_lines$other_tags,
    function(ot) {
      m <- regmatches(ot, gregexpr('"([^"]+)"=>', ot))[[1]]
      gsub('"([^"]+)"=>', "\\1", m)
    }
  ))
  
  freq_cles <- sort(table(toutes_cles_lines), decreasing = TRUE)
  
  top30 <- head(freq_cles, 30)
  
  freq_df <- tibble(
    Cle         = names(top30),
    Occurrences = as.integer(top30),
    Pct_lignes  = round(as.integer(top30) / nrow(sample_lines) * 100, 1)
  )
  
  print(freq_df, n = 30)
  cat("\n  (sur un sample de", nrow(sample_lines), "lignes avec other_tags)\n\n")
  
}, error = function(e) {
  cat("  ⚠ Analyse fréquences échouée :", conditionMessage(e), "\n\n")
})

# ==============================================================================
# 4. APERÇU DES VALEURS pour les clés routières importantes
# Pour chaque clé fréquente, on liste les valeurs distinctes trouvées
# dans le sample — utile pour concevoir les CASE WHEN de nettoyage.
# ==============================================================================

cat("── 4. Valeurs distinctes pour les clés fréquentes (lines) ──────────\n\n")

CLES_A_INSPECTER <- c(
  "surface", "maxspeed", "lanes", "oneway", "lit",
  "bridge", "tunnel", "layer", "access", "vehicle",
  "smoothness", "tracktype", "width", "toll", "junction"
)

tryCatch({
  
  sample_inspect <- st_read(
    chemin_pbf, layer = "lines",
    query = paste0("SELECT other_tags FROM lines LIMIT ", N_LIGNES_SAMPLE),
    quiet = TRUE
  ) %>%
    st_drop_geometry() %>%
    filter(!is.na(other_tags))
  
  for (cle in CLES_A_INSPECTER) {
    
    # Extraction de la valeur de cette clé pour chaque ligne
    valeurs <- sapply(sample_inspect$other_tags, function(ot) {
      pattern <- paste0('"', cle, '"=>"([^"]*)"')
      m <- regmatches(ot, regexec(pattern, ot))[[1]]
      if (length(m) > 1) m[2] else NA_character_
    })
    
    valeurs_non_na <- valeurs[!is.na(valeurs)]
    
    if (length(valeurs_non_na) == 0) {
      cat("  [", cle, "] : absent du sample\n")
      next
    }
    
    freq_val <- sort(table(valeurs_non_na), decreasing = TRUE)
    top_vals <- head(freq_val, 10)
    
    cat("  [", cle, "] —", length(valeurs_non_na),
        "occurrences — valeurs distinctes (top 10) :\n")
    cat("    ",
        paste(paste0(names(top_vals), " (", top_vals, ")"), collapse = ", "),
        "\n")
  }
  
}, error = function(e) {
  cat("  ⚠ Inspection valeurs échouée :", conditionMessage(e), "\n\n")
})

cat("\n✓ Diagnostic des tags OSM terminé\n")
cat("  → Modifier CLES_A_INSPECTER pour explorer d'autres clés\n")
cat("  → Augmenter N_LIGNES_SAMPLE pour un inventaire plus complet\n\n")

}

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
# adaptées au pays, permettant de mesurer des distances en mètres).
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
routes_attrs_raw <- routes_raw %>%
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
#   - Les routes nationales (trunk, primary) sont supposées bitumées dans le pays
#   - Les routes secondaires : gravier (fréquent hors des grandes villes)
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
routes <- routes_attrs_raw %>%
  select(osm_id, geometry) %>%
  left_join(attrs_clean, by = "osm_id") %>%
  st_as_sf() %>%
  # CRS 32735 = WGS 84 / UTM Zone 35S : projection métrique adaptée à l'Afrique de l'Est
  # Nécessaire pour calculer des longueurs en mètres et des pentes en %
  st_transform(crs = 32735)

cat("✓ Nettoyage terminé :", nrow(routes), "segments — surface harmonisée via DuckDB\n\n")

# ==============================================================================
# II.3 : Couches administratives et fond de carte
# Extrait frontières, provinces, lacs et parcs depuis le PBF.
# Définit fond_carte(), la fonction réutilisée dans toutes les cartes du script.
# ==============================================================================

# ── Frontière nationale (admin_level = 2) ─────────────────────────────────────
# Dans OSM, admin_level = 2 désigne les frontières nationales.
# st_union() fusionne tous les polygones de la couche en un seul polygone,
# ce qui est utile pour tracer la frontière nationale d'un seul tenant.
pays_boundary <- st_read(
  chemin_pbf, layer = "multipolygons",
  query = "SELECT * FROM multipolygons WHERE admin_level = '2'",
  quiet = TRUE
) %>%
  rename(geometry = `_ogr_geometry_`) %>%
  st_as_sf() %>%
  st_make_valid() %>%
  st_transform(crs = 32735)

pays_national <- pays_boundary %>%
  st_union() %>%
  st_as_sf() %>%
  st_make_valid()

# ── Provinces (admin_level = 4) ───────────────────────────────────────────────
# Dans OSM, admin_level = 4 correspond aux subdivisions de premier niveau
# (régions de premier niveau). On filtre ensuite pour ne garder que les géométries
# de type POLYGON ou MULTIPOLYGON (et non des lignes ou des points).
pays_provinces <- st_read(
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
if (nrow(pays_provinces) == 0) pays_provinces <- pays_national

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

# ── Zone d'affichage (bbox 250km × 250km centrée sur le pays) ────────────────
# Buffer de 125km de chaque côté du centroïde pour afficher les frontières voisines
# Cette zone d'affichage légèrement plus grande que le pays permet de voir
# les pays voisins sur les cartes.

# 1. Calcul du centroïde du pays (point central)
centre_pays <- pays_national %>% st_centroid() %>% st_coordinates()
centre_x      <- centre_pays[1, "X"]  # Coordonnée X (Est-Ouest) du centroïde
centre_y      <- centre_pays[1, "Y"]  # Coordonnée Y (Nord-Sud) du centroïde

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
  carte <- tm_shape(pays_provinces, bbox = bbox_carto) +
    tm_polygons(
      fill = "#F5F5F0",
      col  = "#AAAAAA",
      lwd  = 0.8,
      fill.legend = tm_legend(show = FALSE)
    ) +
    tm_shape(pays_national) +
    tm_borders(col = "#222222", lwd = 2.5)
  
  # ── Parcs naturels (sous les lacs pour ne pas les masquer) ──────────────────
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
  
  # ── Lacs ────────────────────────────────────────────────────────────────────
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

# ==============================================================================
# II.4 : Modèle Numérique de Terrain (DEM) 
# Télécharge le DEM SRTM depuis AWS via elevatr. En cas d'échec, génère 
# un DEM fictif de substitution.
# Utilisé uniquement en Partie IV.2 pour le calcul des pentes.
# ==============================================================================

# Le DEM (Digital Elevation Model) est une grille de pixels où chaque valeur
# représente l'altitude en mètres au-dessus du niveau de la mer.
# Il sera utilisé pour calculer la pente de chaque segment routier
# (ratio dénivelé/longueur × 100 = pourcentage de pente).

# Créer l'emprise géographique à partir de la bbox des routes
# pour ne télécharger que la zone d'intérêt
bbox_routes <- st_bbox(routes)
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
  # clip = "locations" : on ne récupère les données que dans l'emprise fournie
  dem <- get_elev_raster(emprise_sf, z = DEM_ZOOM, clip = "locations")
  dem <- rast(dem)   # Conversion raster R → terra SpatRaster
  # Reprojection en UTM 35S pour cohérence avec les routes
  # method = "bilinear" : interpolation bilinéaire (meilleure qualité que "nearest")
  # L'interpolation bilinéaire calcule la valeur d'un pixel en faisant une
  # moyenne pondérée de ses 4 voisins les plus proches, ce qui donne des
  # transitions d'altitude plus douces que le simple voisin le plus proche.
  dem <- project(dem, "EPSG:32735", method = "bilinear")
  cat("✓ DEM téléchargé et reprojeté\n")
  
}, error = function(e) {
  cat("⚠ Téléchargement DEM échoué — création d'un DEM fictif réaliste\n")
  
  # DEM fictif de substitution (gradient Ouest→Est + bruit gaussien) :
  ext_utm <- ext(bbox_routes["xmin"], bbox_routes["xmax"],
                 bbox_routes["ymin"], bbox_routes["ymax"])
  
  # Raster vide avec résolution ~90m (comparable au SRTM niveau 3)
  dem <<- rast(ext_utm, resolution = DEM_FICTIF_RESOLUTION_M, crs = "EPSG:32735")
  
  set.seed(123)   # Graine pour reproductibilité du bruit aléatoire
  n_cells    <- ncell(dem)  # Nombre total de pixels dans le raster
  
  # xFromCell() retourne la coordonnée X (longitude UTM) du centre de chaque cellule
  x_coords <- xFromCell(dem, 1:n_cells)
  
  # Gradient d'élévation : 1 500m à l'Est → 2 300m à l'Ouest
  # La formule normalise x_coords entre 0 (Est) et 1 (Ouest) puis multiplie par 800m
  # Cette formule produit un gradient d'altitude Ouest→Est.
  base_elevation <- DEM_FICTIF_ALT_EST + (max(x_coords) - x_coords) /
    (max(x_coords) - min(x_coords)) * (DEM_FICTIF_ALT_OUEST - DEM_FICTIF_ALT_EST)
  
  # Ajout d'un bruit gaussien (sd=150m) pour simuler collines et vallées
  # rnorm(n, 0, 150) génère n valeurs aléatoires suivant une loi normale
  # de moyenne 0 et d'écart-type 150m.
  # pmax/pmin bornent les valeurs entre 950m et 2 500m
  values(dem) <<- pmax(950, pmin(2500, base_elevation + rnorm(n_cells, 0, 150)))
  
  cat("✓ DEM fictif créé\n")
})


# Découpe le raster dem pour ne garder que la zone qui chevauche le 
# polygone pays_boundary pour éviter de traiter des données hors de la zone d'intérêt
# crop() : réduit le raster à l'emprise rectangulaire d'un polygone.
dem <- crop(dem, vect(pays_boundary)) 

# Masque les pixels du raster qui ne sont pas à l'intérieur du polygone pays_boundary
# définis comme NA
# mask() : met à NA tous les pixels hors du polygone. Ainsi les pixels
# des pays voisins (Ouganda, RDC…) sont exclus du calcul des pentes.
dem <- mask(dem, vect(pays_boundary))

# Limite les valeurs du raster à un intervalle donné et remplace les valeurs hors seuil par NA
# Valeurs hors de [DEM_ALTITUDE_MIN, DEM_ALTITUDE_MAX] sont considérées irréalistes : on les supprime.
dem <- clamp(dem, lower = 800, upper = 4600, values = NA)

cat("  Élévation min :", round(global(dem, "min", na.rm = TRUE)[,1]), "m\n")
cat("  Élévation max :", round(global(dem, "max", na.rm = TRUE)[,1]), "m\n\n")

# Cartographier rapidement pour identifier visuellement les anomalies
# plot() (de terra) affiche le raster en nuances de couleur dans la fenêtre R.
# add = TRUE superpose la frontière en rouge par-dessus le raster.
plot(dem, main = paste("DEM", NOM_PAYS, "— vérification"))
plot(st_geometry(pays_boundary), add = TRUE, border = "red")


################################################################################
# PARTIE III — CONSTRUCTION ET CORRECTION DU RÉSEAU ROUTIER
# Transforme les segments OSM bruts en graphe sfnetworks topologiquement
# cohérent, puis extrait la composante géante (réseau principal connecté).
# Toute modification ici invalide le cache des pentes.
#
# ── MISE EN CACHE ─────────────────────────────────────────────────────────────
# Les corrections topologiques (subdivision aux intersections, suppression
# des pseudo-nœuds, extraction de la composante géante) prennent ~3-5 min.
# Le résultat est mis en cache dans outputs/reseau_corrige_cache.rds.
# Le cache est invalidé automatiquement si :
#   - le fichier PBF a changé (détection par taille de fichier)
#   - le nombre de segments d'entrée a changé
# Pour forcer un recalcul : supprimer le fichier .rds.
################################################################################

CACHE_RESEAU     <- file.path(DIR_CACHE, "reseau_corrige_cache.rds")
cache_reseau_valide <- FALSE

# Empreinte du PBF : la taille du fichier est un proxy simple et rapide
# (quelques ms) pour détecter une modification de la source.
# Pour une validation plus stricte, on pourrait utiliser digest::digest(file = ...)
# mais ça prendrait ~1s pour un PBF de 50 Mo, ce qui n'apporte rien en pratique.
pbf_size_actuelle     <- file.size(chemin_pbf)
n_segments_entree_act <- nrow(routes)

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
    
    reseau       <- cache_reseau$reseau
    cache_reseau_valide <- TRUE
    
    cat("  ✓ Cache réseau valide\n")
    cat("    Nœuds  :", igraph::vcount(reseau), "\n")
    cat("    Arêtes :", igraph::ecount(reseau), "\n")
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
  
  # ── Homogénéisation des types de géométrie ────────────────────────────────────
  # Le fichier PBF peut contenir des MULTILINESTRING (plusieurs lignes groupées)
  # que sfnetworks ne sait pas gérer. st_cast() les éclate en LINESTRING simples.
  # Un LINESTRING est une séquence de points formant une ligne.
  # Un MULTILINESTRING est un groupe de plusieurs lignes, comme si une route
  # était découpée en morceaux non contigus — sfnetworks ne peut pas en faire
  # un segment de graphe cohérent.
  routes_clean <- routes %>%
    st_cast("LINESTRING", warn = FALSE) %>%
    filter(st_geometry_type(.) == "LINESTRING") %>%  # Supprimer les types non conformes
    st_make_valid()
  
  # as_sfnetwork() convertit le sf en réseau non orienté (directed = FALSE):
  # un segment peut être parcouru dans les deux sens (routes bidirectionnelles).
  # Les routes à sens unique seraient gérées avec directed = TRUE + attribut oneway.
  reseau <- as_sfnetwork(routes_clean, directed = FALSE) 
  
  cat("✓ Réseau initial — nœuds :", igraph::vcount(reseau),
      "— arêtes :", igraph::ecount(reseau), "\n\n")
  
  
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
  
  cat("  Étape 1/3 : subdivision aux intersections...\n")

  reseau_lisse <- reseau %>%
    convert(to_spatial_subdivision)

  cat("  → ", igraph::count_components(reseau_lisse), "composantes après subdivision\n")

  # Remplacer FALSE par TRUE si on veut activer cette partie du code : ⚠ ~1 jour de calcul
  if(FALSE) {
    # ── Étape 2 : snapping ciblé post-topologie ─────────────────────────────────
    # Maintenant que la topologie est propre, un snapping léger (5m seulement)
    # connecte les extrémités quasi-jointives.
    # Les gaps < 5m sont rarissimes dans les PBF OSM bien maintenus.
    # La subdivision (étape 1) règle déjà l'essentiel des problèmes de connectivité.
    # À réactiver uniquement sur un sous-réseau local si des composantes isolées
    # persistent après l'étape 4.
    # Le "snapping" consiste à "aimanter" les extrémités de routes qui sont très
    # proches mais pas exactement connectées (écart de quelques mètres dû à
    # des imprécisions de saisie dans OSM).
    
    cat("  Étape 2/3 : snapping léger (5m)...\n")
    
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
  
  cat("  Étape 3/3 : extraction de la composante géante...\n")
  
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
  
  # Vérifier les colonnes disponibles dans pays_provinces
  cat("Colonnes de pays_provinces :\n")
  print(names(pays_provinces))
  
  # ── Récupérer les arêtes du réseau AVANT filtrage (reseau_lisse) ──────────────
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
  
  # ── Joindre l'info composante aux arêtes via leurs nœuds extrémité ────────────
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
  if (nrow(aretes_perdues) > 0 && nrow(pays_provinces) > 0) {
    
    # Renommage AVANT la jointure pour éviter le conflit avec la colonne
    # "name" des arêtes (nom de route OSM)
    provinces_join <- pays_provinces %>%
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
  reseau <- reseau_lisse %>%
    activate("nodes") %>%
    filter({
      pb_geante$tick()
      row_number() %in% noeuds_geante
    }) %>%
    mutate(node_id = row_number())
  
  # st_length() : calcule la longueur de chaque arête en mètres à partir de sa géométrie.
  # as.numeric() : convertit le résultat (objet "units") en nombre ordinaire.
  reseau <- reseau %>%
    activate("edges") %>%
    mutate(longueur_m = as.numeric(st_length(geometry)))
  
  # Vérification immédiate
  n_na_longueur <- reseau %>%
    activate("edges") %>% st_as_sf() %>%
    pull(longueur_m) %>%
    { sum(is.na(.) | . == 0) }
  
  cat("✓ longueur_m recalculée sur toutes les arêtes\n")
  cat("  Arêtes avec longueur_m = 0 ou NA :", n_na_longueur, "(doit être 0)\n\n")
  
  # to_spatial_subdivision() crée des fragments de longueur nulle aux intersections
  # quand deux nœuds sont géométriquement confondus. On les élimine ici pour éviter 
  # toute propagation de NA en aval.
  n_avant_filtre <- igraph::ecount(reseau)
  
  reseau <- reseau %>%
    activate("edges") %>%
    mutate(longueur_m_brute = as.numeric(st_length(geometry))) %>%
    filter(longueur_m_brute > SEUIL_LONGUEUR_ARETE_M) %>%         # Seuil 0.5m
    select(-longueur_m_brute)                  # Colonne temporaire, on la retire
  
  n_apres_filtre <- igraph::ecount(reseau)
  cat("Arêtes dégénérées supprimées :", n_avant_filtre - n_apres_filtre,
      "(", round((n_avant_filtre - n_apres_filtre)/n_avant_filtre*100, 1), "% du réseau)\n")
  cat("Arêtes conservées            :", n_apres_filtre, "\n\n")
  
  cat("✓ Réseau corrigé —",
      igraph::vcount(reseau), "nœuds,",
      igraph::ecount(reseau), "arêtes\n\n")
  
  
  # ── Diagnostic complet de la fragmentation ────────────────────────────────────
  # On recalcule les composantes connexes sur le réseau final pour vérifier
  # qu'il est bien dominé par une seule grande composante.
  
  composantes_finales <- igraph::components(reseau %>% as_tbl_graph())
  sizes <- sort(composantes_finales$csize, decreasing = TRUE) # trie les tailles des composantes connexes du réseau par ordre décroissant
  
  cat("=== Diagnostic de fragmentation ===\n\n")
  
  cat("Distribution des composantes :\n")
  cat("  >= 1000 noeuds :", sum(sizes >= 1000), "composantes\n")
  cat("  100–999 noeuds :", sum(sizes >= 100 & sizes < 1000), "composantes\n")
  cat("  10–99  noeuds  :", sum(sizes >= 10  & sizes < 100),  "composantes\n")
  cat("  2–9    noeuds  :", sum(sizes >= 2   & sizes < 10),   "composantes\n")
  cat("  1      noeud   :", sum(sizes == 1),                  "composantes\n")
  
  cat("Nombre de nœuds dans reseau :", igraph::vcount(reseau), "\n")
  cat("Nombre d'arêtes dans reseau :", igraph::ecount(reseau), "\n")
  
  rm(composantes_finales)
  
  # ════════════════════════════════════════════════════════════════════════════
  # SAUVEGARDE DU CACHE
  # ════════════════════════════════════════════════════════════════════════════
  
  cat("=== Sauvegarde du cache réseau ===\n")
  
  saveRDS(
    list(
      reseau      = reseau,
      pbf_size           = pbf_size_actuelle,
      n_segments_entree  = n_segments_entree_act,
      n_noeuds           = igraph::vcount(reseau),
      n_aretes           = igraph::ecount(reseau),
      date_creation      = Sys.time()
    ),
    CACHE_RESEAU
  )
  
  saveRDS(
    list(aretes_perdues    = aretes_perdues,
         noeuds_hors_geante = noeuds_hors_geante,
         n_aretes_avant      = nrow(aretes_lisse)
         ),
    PERSIST_DIAG_RES
  )
  
  cat("  ✓ Cache sauvegardé :", CACHE_RESEAU, "\n")
  cat("  → Au prochain lancement, la Partie III s'exécutera en <1s\n\n")
  
}  # fin du if (!cache_reseau_valide)


# ── Vérifications communes (toujours exécutées, qu'on ait un cache ou non) ────
# Ces vérifications sont rapides et permettent de détecter tôt un problème
# de cohérence avec le reste du script (ex : composante non connectée).
cat("=== Vérifications post-Partie III ===\n")
cat("  Nœuds dans reseau  :", igraph::vcount(reseau), "\n")
cat("  Arêtes dans reseau :", igraph::ecount(reseau), "\n")

n_composantes <- igraph::count_components(reseau %>% as_tbl_graph())
if (n_composantes != 1) {
  warning("  ⚠ Le réseau a ", n_composantes, " composantes (attendu : 1)\n")
} else {
  cat("  ✓ Réseau entièrement connecté (1 composante)\n")
}

n_na_longueur <- reseau %>%
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
  filter(aire_km2 > AIRE_MIN_INDUSTRIEL_KM2)

# ── Extraction des zones retail depuis zones_urbaines (déjà chargées) ─────────
# On extrait les zones retail (commerces de détail) qui étaient incluses dans
# les zones_urbaines mais méritent une colonne distincte pour les analyses.
zones_retail <- zones_urbaines %>%
  filter(landuse == "retail") %>%
  mutate(aire_km2 = as.numeric(st_area(geometry)) / 1e6) %>%
  filter(aire_km2 > AIRE_MIN_RETAIL_KM2)

cat("  Zones retail :", nrow(zones_retail), "\n\n")

# ── Taguage des arêtes : zone_urbaine = TRUE si l'arête traverse une zone dense ──
# On utilise le centroïde de chaque arête pour l'intersection (plus rapide
# qu'une intersection complète ligne × polygone sur 29 000 arêtes)
cat("  Taguage des arêtes du réseau...\n")

# st_centroid() : calcule le point central de chaque arête (un point par ligne).
# Tester si UN POINT est dans un polygone est bien plus rapide que tester si
# UNE LIGNE croise un polygone — gain de temps significatif sur ~30 000 arêtes.
aretes_centroides <- reseau %>%
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
reseau <- reseau %>%
  activate("edges") %>%
  mutate(zone_urbaine = in_urbain)

n_urbain <- sum(in_urbain)
cat("  Arêtes en zone urbaine :", n_urbain,
    "(", round(n_urbain / igraph::ecount(reseau) * 100, 1), "% du réseau)\n\n")

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

CACHE_PENTES <- file.path(DIR_CACHE, "pentes_cache.rds")

aretes_avec_geom <- reseau %>% activate("edges") %>% st_as_sf()
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
  calculer_pente_arete <- function(ligne_geom, dem, espacement = DEM_ESPACEMENT_M) {
    
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
      dem,
      espacement = 100
    )
  }
  
  # bind_rows() : transforme une liste de listes en un data.frame R.
  # Chaque élément de la liste devient une ligne du tableau.
  pentes_df <- bind_rows(resultats_pentes)
  
  # ── Sauvegarde du cache ─────────────────────────────────────────────────────
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
reseau <- reseau %>%
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
# Postes frontières, villes importantes, zones industrielles.
# Chaque entrepôt sera "accroché" au nœud du réseau routier le plus proche
# (snapping), ce qui permettra de calculer des itinéraires entre eux.

# Les nœuds d'entreposage sont les origines/destinations du modèle de fret.
# Ils représentent des zones économiques importantes (hub, SEZ, frontières…).

# Conversion des entrepôts manuels en sf pour la comparaison spatiale
manuels_sf <- entreposages_manuels %>%
  st_as_sf(coords = c("lon","lat"), crs = 4326) %>%
  st_transform(crs = 32735)

# ── Entrepôts depuis city/town OSM ────────────────────────────────────────────
# Filtrer uniquement les villes dans le territoire du pays étudié
# Évite que les villes des pays voisins se snappent toutes sur les mêmes nœuds frontières
# st_filter() : ne garde que les géométries qui intersectent le polygone donné.
# st_buffer(dist = BUFFER_FRONTIERE_VILLES_M) : élargit la frontière de 5km pour inclure les villes
# situées exactement sur la frontière.
villes_osm <- villes_osm %>%
  st_filter(pays_national %>% st_buffer(dist = BUFFER_FRONTIERE_VILLES_M))
# Buffer de 5km pour garder les villes très proches de la frontière
cat("  Villes OSM dans ou proches du pays :", nrow(villes_osm), "\n")

# Identifier les villes OSM non dupliquées avec les entrepôts manuels
# lengths(idx_proches) == 0 : sélectionne les villes OSM qui ne sont proches
# d'AUCUN entrepôt manuel (distances > 3km pour tous).
idx_proches <- st_is_within_distance(villes_osm, manuels_sf, dist = DISTANCE_DEDUP_VILLES_M)
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
  select(nom, type, lon, lat, source) %>%
  distinct(nom, .keep_all = TRUE)  # garde le premier point par nom

cat("  Villes OSM city/town nouvelles (non dupliquées) :",
    nrow(villes_nouvelles), "\n")

# ── Entrepôts depuis zones industrielles (origines de fret) ───────────────────
# Les grandes zones industrielles sont d'importants générateurs de fret.
# On calcule leur centroïde et on les ajoute comme entrepôts potentiels,
# en excluant celles qui sont déjà trop proches des entrepôts existants.
if (nrow(zones_industrielles) > 0) {
  
  # Centroïdes des zones industrielles significatives (> 0.05 km²)
  centroides_indus <- zones_industrielles %>%
    filter(aire_km2 > AIRE_MIN_ENTREPOT_INDUSTRIEL_KM2) %>%
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
                                             tous_existants, dist = DISTANCE_DEDUP_INDUSTRIEL_M)
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
    filter(aire_km2 > AIRE_MIN_ENTREPOT_RETAIL_KM2) %>%
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
                                              tous_existants2, dist = DISTANCE_DEDUP_RETAIL_M)
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
# (ils sont tous internes au pays étudié).
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

# ── Buffer d'agglomération (rayon RAYON_AGGLO_ENTREPOT_M) ──────────────────────
# Cercle de RAYON_AGGLO_ENTREPOT_M (4 km) autour de chaque point candidat. Il sert
# au calcul de la population TEMPORAIRE de chaque candidat (Partie IV.4). Cette
# population ne sert QU'À classer les points lors de la fusion à 4 km
# (Partie IV.3-bis) : la population définitive sera recalculée, sans
# chevauchement, par cellule de Voronoï (Partie IV.6).
entreposages_buffer <- entreposages_sf %>%
  st_buffer(dist = RAYON_AGGLO_ENTREPOT_M)

# ── Nœuds du réseau routier (couche réutilisée pour l'accrochage ultérieur) ────
# st_nearest_feature() accrochera chaque entrepôt CONSERVÉ au nœud routier le plus
# proche — mais seulement APRÈS la fusion (Partie IV.3-bis), une fois connus les
# points survivants. On extrait ici la couche de nœuds une fois pour toutes.
noeuds_reseau <- reseau %>% activate("nodes") %>% st_as_sf()


################################################################################
# PARTIE IV.4 — POPULATION TEMPORAIRE DE CHAQUE POINT CANDIDAT
#
# OBJECTIF : Estimer une population approximative pour CHACUN des points candidats
#            (avant toute fusion), dans le seul but de classer les points lors de
#            la fusion à RAYON_AGGLO_ENTREPOT_M de distance (Partie IV.3-bis) : 
#.           dans un groupe de points proches, on conserve celui de plus forte population.
#
#            ⚠ Cette population est TEMPORAIRE. La population définitive de chaque
#            nœud sera recalculée, sans chevauchement, par cellule de Voronoï
#            (Partie IV.6) en sommant les pixels WorldPop de la cellule.
#
# TROIS SOURCES, FUSIONNÉES PAR PRIORITÉ B (WorldPop) > A (OSM) > C (NISR) > min :
#   A — Tags OSM du fichier PBF (rapide, intégré, mais couverture partielle)
#   B — Raster WorldPop (haute résolution spatiale ; somme dans le buffer 4 km)
#   C — Données de recensement NISR via CSV (source officielle, par district)
#   Le résultat est le vecteur pop_temp (longueur = nombre de candidats N).
#
# EFFET DE BORD UTILE : ce bloc charge aussi des objets réutilisés plus tard —
#   raster_worldpop        (IV.4.B) → somme par cellule de Voronoï (IV.6) + poids RWI (IV.7)
#   pays_districts_gadm    (IV.4.C) → emploi par district (IV.8) + repli population
#
# DÉPEND DE :
#     - entreposages_sf      (Partie IV.3) — géométries des N candidats
#     - entreposages_buffer  (Partie IV.3) — buffers 4 km des candidats
#     - chemin_pbf           (Partie I)    — fichier PBF OSM
################################################################################

cat("==========================================================\n")
cat("  PARTIE IV.4 — POPULATION TEMPORAIRE PAR CANDIDAT (fusion)\n")
cat("==========================================================\n\n")

# ==============================================================================
# IV.4.A : Extraction des tags de population depuis le fichier PBF
#
# Dans OpenStreetMap, certains noeuds de type "place" possèdent un tag
# "population" indiquant le nombre d'habitants. Ce tag est maintenu par la
# communauté OSM et couvre les grandes villes mais rarement les petites zones.
#
# AVANTAGES  : Aucun fichier externe, déjà dans le PBF téléchargé.
# INCONVÉNIENTS : Couverture très partielle (< 30% des zones),
#                 données souvent obsolètes ou approximatives.
# ==============================================================================

cat("── Approche A : tags OSM de population ──────────────────────────────\n")

# Extraction des noeuds de type place avec un tag population depuis le PBF.
# On réutilise la même syntaxe que pour les villes en Partie II.2.
# other_tags contient les attributs secondaires au format "clé"=>"valeur".
# La fonction extraire_tag() est définie en Partie II.2 et réutilisée ici.
population_osm_raw <- tryCatch({
  
  result <- st_read(
    chemin_pbf,
    layer = "points",
    # On charge tous les lieux habités (city, town, village) qui ont potentiellement
    # un tag population dans other_tags.
    query = "SELECT name, place, other_tags FROM points
             WHERE place IN ('city', 'town', 'village', 'suburb')",
    quiet = TRUE
  ) 
  
  # Normalisation robuste du nom de la colonne géométrie.
  # Quand query= est spécifié, st_read retourne un sf dont la colonne
  # géométrie peut s'appeler "geom", "geometry", "wkb_geometry", etc.
  # st_geometry(x) <- "geometry" renomme la colonne active quelle que soit
  # son appellation d'origine, sans erreur si elle s'appelle déjà "geometry".
  st_geometry(result) <- "geometry"
  
  result %>%
    st_as_sf() %>%
    st_transform(crs = 32735) %>%
    filter(!is.na(name)) %>%
    mutate(
      # Extraction du tag "population" depuis la chaîne other_tags.
      # extraire_tag() utilise une regex pour trouver "population"=>"<valeur>".
      # as.numeric() convertit la chaîne "45000" en entier 45000.
      # suppressWarnings() évite les avertissements sur les valeurs non convertibles
      # (ex : "45,000" avec virgule → NA, ce qui est le comportement voulu).
      pop_osm_brute = suppressWarnings(
        as.numeric(sapply(other_tags, extraire_tag, cle = "population"))
      )
    ) %>%
    filter(!is.na(pop_osm_brute), pop_osm_brute > 0)
  
}, error = function(e) {
  cat("  ⚠ Extraction PBF population échouée :", conditionMessage(e), "\n")
  # Retourner un sf vide avec la même structure pour ne pas bloquer la suite
  st_sf(name          = character(0),
        pop_osm_brute = numeric(0),
        geometry      = st_sfc(crs = 32735))
})

cat("  Lieux OSM avec tag population :", nrow(population_osm_raw), "\n")

# ── Association à chaque entrepôt : somme des points OSM dans le buffer ────────
# Pour chaque entrepôt, on cherche tous les points OSM peuplés dans un rayon de
# RAYON_AGGLO_ENTREPOT_M mètres et on somme leurs populations.
# S'il n'y en a aucun, la population OSM reste NA (sera complétée par B ou C).

if (nrow(population_osm_raw) > 0) {

  # st_is_within_distance() : matrice booléenne entrepôts × lieux OSM peuplés.
  # Retourne pour chaque entrepôt la liste des indices des points OSM dans le buffer.
  within_buffer_A <- st_is_within_distance(
    entreposages_sf,
    population_osm_raw,
    dist = RAYON_AGGLO_ENTREPOT_M
  )

  # Pour chaque entrepôt, on somme la population de tous les points OSM dans le buffer.
  # sum() avec na.rm = TRUE ignore les tags mal renseignés sans planter le calcul.
  pop_osm_par_entrepot <- sapply(seq_len(nrow(entreposages_sf)), function(i) {

    candidats <- within_buffer_A[[i]]  # Indices des lieux OSM dans le buffer

    if (length(candidats) == 0) return(NA_real_)  # Aucun lieu OSM à proximité

    sum(population_osm_raw$pop_osm_brute[candidats], na.rm = TRUE)
  })
  
} else {
  # Aucun tag population dans le PBF : vecteur de NA de la bonne taille
  pop_osm_par_entrepot <- rep(NA_real_, nrow(entreposages_sf))
}

cat("  Entrepôts avec pop. OSM :",
    sum(!is.na(pop_osm_par_entrepot)), "/", nrow(entreposages_sf), "\n")
cat("  Population OSM min :", round(min(pop_osm_par_entrepot, na.rm = TRUE)),
    "| max :", round(max(pop_osm_par_entrepot, na.rm = TRUE)), "\n\n")


# ==============================================================================
# IV.4.B : Population depuis un raster WorldPop (~100m de résolution)
#
# WorldPop produit des rasters de densité de population à haute résolution
# à partir de recensements, d'images satellites et de modèles statistiques.
# Les données sont disponibles pour 2020 (100m par pixel).
#
# AVANTAGES  : Haute résolution spatiale (100m), couvre tout le territoire.
# INCONVÉNIENTS : Fichier lourd (~150 Mo), nécessite un téléchargement externe,
#                 données 2020 (pas 2022).
# ==============================================================================

cat("── Approche B : raster WorldPop (population ~100m) ──────────────────\n")

#Création d'un vecteur avec que des NA
pop_worldpop_par_entrepot <- rep(NA_real_, nrow(entreposages_sf))

# On tente d'abord de charger le raster depuis le disque (cache local).
# Si le fichier n'existe pas, on le télécharge depuis le site WorldPop

raster_worldpop <- NULL
worldpop_ok     <- FALSE

if (!is.null(WORLDPOP_LOCAL_PATH) && file.exists(WORLDPOP_LOCAL_PATH)) {
  
  cat("  Chargement du raster WorldPop depuis le cache local...\n")
  
  tryCatch({
    raster_worldpop <- rast(WORLDPOP_LOCAL_PATH)
    # Vérification : le raster doit avoir au moins un pixel non-NA sur la zone
    n_valeurs_valides <- global(raster_worldpop, "notNA")[,1]
    if (n_valeurs_valides > 0) {
      worldpop_ok <- TRUE
      cat("  ✓ Raster WorldPop chargé (", n_valeurs_valides, "pixels)\n")
    } else {
      cat("  ⚠ Raster WorldPop vide, retéléchargement nécessaire\n")
    }
  }, error = function(e) {
    cat("  ⚠ Chargement raster échoué :", conditionMessage(e), "\n")
  })
}

# Si le raster n'est pas disponible localement, on tente le téléchargement.
# URL directe WorldPop 2020 (non constrained, 100m) — à adapter selon le pays.
# Pour d'autres années ou résolutions, consulter :
# https://hub.worldpop.org/geodata/listing?id=29
# ── REMPLACER le bloc de téléchargement WorldPop ──────────────────────────────
if (!worldpop_ok) {
  
  for (url_tentative in WORLDPOP_URLS_CANDIDATES) {
    
    cat("  Tentative :", url_tentative, "\n")
    
    tryCatch({
      
      dir.create(dirname(WORLDPOP_LOCAL_PATH),
                 showWarnings = FALSE, recursive = TRUE)
      
      download.file(url_tentative, WORLDPOP_LOCAL_PATH,
                    mode = "wb", method = "auto", quiet = FALSE)
      
      # Vérification que le fichier téléchargé est un raster valide
      # (un fichier HTML d'erreur serait téléchargé sans exception)
      test_rast <- rast(WORLDPOP_LOCAL_PATH)
      if (ncell(test_rast) > 0) {
        raster_worldpop <- project(test_rast, "EPSG:32735", method = "bilinear")
        worldpop_ok     <- TRUE
        cat("  ✓ WorldPop téléchargé depuis :", url_tentative, "\n")
        break   # Sortir de la boucle dès qu'une URL fonctionne
      }
      
    }, error = function(e) {
      cat("  ✗ Échec :", conditionMessage(e), "\n")
      # Supprimer le fichier partiel éventuel avant de tenter la prochaine URL
      if (file.exists(WORLDPOP_LOCAL_PATH)) file.remove(WORLDPOP_LOCAL_PATH)
    })
  }
  
  if (!worldpop_ok) {
    cat("  ⚠ Toutes les URLs WorldPop ont échoué\n")
    cat("    → Téléchargement manuel sur https://hub.worldpop.org\n")
    cat("    → Chercher : pays > Population > 2020 > 100m\n")
    cat("    → Sauvegarder sous :", WORLDPOP_LOCAL_PATH, "\n\n")
  }
}

# ── Agrégation du raster dans un buffer autour de chaque entrepôt ─────────────
# Pour chaque entrepôt, on somme les pixels WorldPop dans un cercle de
# RAYON_AGGLO_ENTREPOT_M mètres. Chaque pixel représente le nombre d'habitants
# vivant dans cette cellule de 100m × 100m.
if (worldpop_ok) {

  cat("  Agrégation WorldPop sur les buffers de",
      RAYON_AGGLO_ENTREPOT_M / 1000, "km...\n")
  
  # Création des buffers de chaque entrepôt
  # entreposages_buffer est déjà défini en Partie IV.3, on le réutilise.
  
  tryCatch({
    
    # exactextractr::exact_extract() : pour chaque polygone (buffer d'entrepôt),
    # calcule la somme des valeurs du raster WorldPop à l'intérieur.
    # fun = "sum" : on veut le nombre TOTAL d'habitants dans le buffer.
    # Pour obtenir une densité moyenne, utiliser fun = "mean".
    # progress = FALSE : supprime la barre de progression interne d'exactextractr
    # (on gère notre propre affichage ci-dessous).
    resultats_wp <- exactextractr::exact_extract(
      raster_worldpop,
      entreposages_buffer %>% st_transform(st_crs(raster_worldpop)),
      fun      = "sum",
      progress = FALSE
    )
    
    # exact_extract avec fun="sum" retourne un vecteur numérique.
    # Quand tous les pixels d'un buffer sont NA, R calcule sum(NA, na.rm=TRUE) = 0,
    # ce qui masquerait le vrai "sans données" et empêcherait coalesce() de
    # basculer sur NISR ou OSM. On reconvertit donc ces 0 en NA.
    pop_worldpop_par_entrepot <- as.numeric(resultats_wp)
    pop_worldpop_par_entrepot[
      !is.na(pop_worldpop_par_entrepot) & pop_worldpop_par_entrepot == 0
    ] <- NA_real_

    cat("  ✓ Population WorldPop calculée pour",
        sum(!is.na(pop_worldpop_par_entrepot)), "/", nrow(entreposages_sf),
        "entrepôts\n")
    cat("  Pop. WorldPop min :",
        round(min(pop_worldpop_par_entrepot, na.rm = TRUE)),
        "| max :", round(max(pop_worldpop_par_entrepot, na.rm = TRUE)), "\n\n")
    
  }, error = function(e) {
    cat("  ⚠ Agrégation WorldPop échouée :", conditionMessage(e), "\n")
    cat("  → Approche B ignorée\n\n")
    pop_worldpop_par_entrepot <<- rep(NA_real_, nrow(entreposages_sf))
  })
}


# ==============================================================================
# IV.4.C : Données de recensement NISR (source officielle, recommandée)
#
# L'Institut national de statistiques (NISR pour le Rwanda) publie les résultats
# du recensement RPHC-5 (2022) par district. Les données sont disponibles sur HDX 
# (Humanitarian Data Exchange)
#
# PROCÉDURE DE TÉLÉCHARGEMENT :
#   1. Aller sur  https://data.humdata.org/dataset/cod-ps-rwa
#   2. Chercher "rwa_admpop_adm2_2023.csv" (niveau district)
#   3. Télécharger le CSV (bouton "Download")
#   4. Placer le fichier dans data/raw/rwa_admpop_adm2_2023.csv
#
# Le fichier contient les districts du pays avec population par sexe.
# On fait une jointure spatiale : chaque entrepôt est associé au district
# dans lequel il se trouve, puis on récupère la population de ce district.
#
# INCONVÉNIENTS : Résolution district uniquement (pas de granularité plus fine),
#                 nécessite un téléchargement manuel.
# ==============================================================================

cat("── Approche C : recensement NISR 2022 (par district) ────────────────\n")

pop_nisr_par_entrepot <- rep(NA_real_, nrow(entreposages_sf))

if (file.exists(NISR_CSV_PATH)) {
  
  tryCatch({
    
    # ── Chargement du CSV NISR ────────────────────────────────────────────────
    # read_csv() est plus robuste que read.csv() pour les fichiers avec encodage
    # UTF-8 (noms de districts avec accents) et les colonnes numériques avec
    # des espaces ou virgules comme séparateurs de milliers.
    nisr_pop_raw <- read_csv(NISR_CSV_PATH, show_col_types = FALSE)
    
    cat("  CSV NISR chargé :", nrow(nisr_pop_raw), "lignes\n")
    cat("  Colonnes disponibles :", paste(names(nisr_pop_raw), collapse = ", "), "\n")
    
    # ── Nettoyage du tableau NISR ─────────────────────────────────────────────
    # On extrait uniquement les colonnes dont on a besoin et on normalise
    # les noms de districts (suppression des accents, minuscules)
    # pour faciliter la jointure avec les données OSM/GADM.
    nisr_pop <- nisr_pop_raw %>%
      # Renommage des colonnes selon les paramètres définis en IV.4.0
      rename(
        district  = any_of(NISR_COL_DISTRICT),
        province  = any_of(NISR_COL_PROVINCE),
        pop_total = any_of(NISR_COL_POP_TOTAL)
      ) %>%
      # Nettoyage des noms de districts pour les jointures textuelles.
      # str_to_lower() + str_trim() : minuscules + suppression espaces.
      # iconv() : translittération des caractères accentués vers ASCII
      # (ex : "Gasabò" → "Gasabo") pour éviter les problèmes d'encodage.
      mutate(
        district_clean = iconv(str_to_lower(str_trim(district)),
                               from = "UTF-8", to = "ASCII//TRANSLIT"),
        pop_total      = as.numeric(str_remove_all(
          as.character(pop_total), "[,\\s]"))  # Supprime "," et espaces dans les nombres
      ) %>%
      filter(!is.na(pop_total), pop_total > 0) %>%
      select(district, district_clean, province, pop_total)
    
    cat("  Districts NISR après nettoyage :", nrow(nisr_pop), "\n")
    
    # ── Téléchargement des frontières de districts (GADM) ─────────────────────
    # GADM (Global Administrative Areas) fournit les polygones des limites
    # administratives pour tous les pays du monde.
    # geodata::gadm() télécharge le niveau 2 (districts) pour le pays.
    # level = 2 : provinces = 1, districts = 2, secteurs = 3.
    cat("  Téléchargement des frontières de districts GADM...\n")
    
    pays_districts_gadm <- tryCatch({
      
      geodata::gadm(country = "RWA", level = 2, path = tempdir()) %>%
        st_as_sf() %>%
        st_transform(crs = 32735) %>%
        # NAME_2 est le champ GADM contenant le nom du district en anglais.
        # On applique la même normalisation que pour nisr_pop$district_clean.
        mutate(
          district_clean = iconv(str_to_lower(str_trim(NAME_2)),
                                 from = "UTF-8", to = "ASCII//TRANSLIT")
        ) %>%
        select(district_gadm = NAME_2, district_clean, geometry)
      
    }, error = function(e) {
      cat("  ⚠ Téléchargement GADM échoué :", conditionMessage(e), "\n")
      NULL
    })
    
    if (!is.null(pays_districts_gadm)) {
      
      # ── Jointure GADM × NISR ────────────────────────────────────────────────
      # On fusionne le tableau de population NISR avec les polygones GADM
      # via le nom de district normalisé.
      # left_join() conserve tous les polygones GADM même sans correspondance NISR.
      districts_avec_pop <- pays_districts_gadm %>%
        left_join(
          nisr_pop %>% select(district_clean, pop_total),
          by = "district_clean"
        )
      
      # Vérification du taux de couverture de la jointure
      n_sans_pop <- sum(is.na(districts_avec_pop$pop_total))
      cat("  Jointure GADM × NISR :", nrow(districts_avec_pop) - n_sans_pop,
          "/", nrow(districts_avec_pop), "districts appariés\n")
      
      # Si des districts restent sans population après la jointure textuelle,
      # c'est souvent dû à de légères différences d'orthographe
      # (ex : "Nyarugenge" vs "Nyarugege"). On les signale pour correction manuelle.
      if (n_sans_pop > 0) {
        cat("  ⚠ Districts GADM sans correspondance NISR :\n")
        manquants <- districts_avec_pop %>%
          filter(is.na(pop_total)) %>%
          pull(district_gadm)
        cat("   ", paste(manquants, collapse = ", "), "\n")
        cat("    → Vérifier l'orthographe dans", NISR_CSV_PATH, "\n")
      }
      
      # ── Jointure spatiale entrepôts × districts ─────────────────────────────
      # Pour chaque entrepôt, on identifie dans quel district il se trouve
      # (st_within) et on récupère la population du district correspondant.
      # st_join() avec join = st_within : chaque entrepôt hérite des attributs
      # du district qui le contient géographiquement.
      # Une frontière peut poser problème : un entrepôt exactement sur la
      # limite de deux districts. Dans ce cas, st_within peut retourner
      # plusieurs résultats ou aucun. On utilise st_nearest_feature comme
      # fallback pour les entrepôts non couverts.
      entrepots_join_nisr <- entreposages_sf %>%
        st_join(
          districts_avec_pop %>% select(district_gadm, pop_total),
          join    = st_within,
          largest = TRUE   # Si plusieurs correspondances, prendre le plus grand polygone
        )
      
      pop_nisr_par_entrepot <- entrepots_join_nisr$pop_total

      # Fallback pour les entrepôts hors district (frontières, problèmes topo)
      # st_nearest_feature() : pour les entrepôts sans district (NA), on leur
      # associe le district le plus proche géographiquement.
      manquants_idx <- which(is.na(pop_nisr_par_entrepot))
      if (length(manquants_idx) > 0) {

        idx_district_proche <- st_nearest_feature(
          entreposages_sf[manquants_idx, ],
          districts_avec_pop
        )
        pop_nisr_par_entrepot[manquants_idx] <-
          districts_avec_pop$pop_total[idx_district_proche]

        cat("  Entrepôts hors district (fallback nearest) :", length(manquants_idx), "\n")
      }

      # ── Partage égal de la population entre entrepôts du même district ─────────
      # On divise la population du district par le nombre d'entrepôts qu'il contient,
      # en supposant que chaque entrepôt dessert une part égale de la population.
      # NOTE : cette hypothèse sera affinée lorsque les données NISR au niveau
      # administratif 3 (secteurs) seront disponibles.
      district_par_entrepot <- entrepots_join_nisr$district_gadm

      # Pour les entrepôts du fallback nearest, on leur attribue le nom du district
      # le plus proche afin qu'ils participent au partage.
      if (length(manquants_idx) > 0) {
        district_par_entrepot[manquants_idx] <-
          districts_avec_pop$district_gadm[idx_district_proche]
      }

      # Compte le nombre d'entrepôts par district puis divise la population.
      n_entrepots_par_district <- table(district_par_entrepot)
      pop_nisr_par_entrepot <- pop_nisr_par_entrepot /
        as.numeric(n_entrepots_par_district[district_par_entrepot])

      n_districts_partages <- sum(n_entrepots_par_district > 1)
      if (n_districts_partages > 0) {
        cat("  Districts avec plusieurs entrepôts (population partagée) :",
            n_districts_partages, "\n")
      }

      cat("  ✓ Population NISR associée :", sum(!is.na(pop_nisr_par_entrepot)),
          "/", nrow(entreposages_sf), "entrepôts\n")
      cat("  Pop. NISR min :", round(min(pop_nisr_par_entrepot, na.rm = TRUE)),
          "| max :", round(max(pop_nisr_par_entrepot, na.rm = TRUE)), "\n\n")
      
    } else {
      cat("  ⚠ GADM non disponible — approche C abandonnée\n\n")
    }
    
  }, error = function(e) {
    cat("  ⚠ Approche C échouée :", conditionMessage(e), "\n\n")
  })
  
} else {
  cat("  Fichier NISR non trouvé :", NISR_CSV_PATH, "\n")
  cat("  → Télécharger sur https://data.humdata.org/dataset/cod-ps-rwa\n")
  cat("  → Approche C ignorée\n\n")
}


# ==============================================================================
# IV.4.D : Fusion des trois sources → population TEMPORAIRE (pop_temp)
#
# On assemble les trois vecteurs (A, B, C) en une population par CANDIDAT selon
# la hiérarchie B (WorldPop) > A (OSM) > C (NISR) > POP_FALLBACK_MIN.
# Rappel : pop_temp ne sert qu'à classer les points lors de la fusion (IV.3-bis).
# ==============================================================================

cat("── Fusion des sources → population temporaire par candidat ──────────\n")

# Cohérence : les trois vecteurs doivent couvrir tous les candidats.
stopifnot(
  length(pop_osm_par_entrepot)      == nrow(entreposages_sf),
  length(pop_worldpop_par_entrepot) == nrow(entreposages_sf),
  length(pop_nisr_par_entrepot)     == nrow(entreposages_sf)
)

# coalesce() : premier argument non-NA, de gauche à droite (hiérarchie de sources).
pop_temp <- coalesce(
  replace_na(pop_worldpop_par_entrepot, NA_real_),  # Source B : WorldPop
  replace_na(pop_osm_par_entrepot,      NA_real_),  # Source A : OSM
  replace_na(pop_nisr_par_entrepot,     NA_real_),  # Source C : NISR
  rep(POP_FALLBACK_MIN, nrow(entreposages_sf))      # Fallback
) %>%
  round()

cat("  Population temporaire : min =", round(min(pop_temp)),
    "| médiane =", round(median(pop_temp)),
    "| max =", round(max(pop_temp)), "\n\n")


################################################################################
# PARTIE IV.3-bis — FUSION DES ENTREPÔTS À 4 km (ancrage glouton par population)
#
# OBJECTIF : Réduire le bruit du jeu de candidats (nombreux points OSM proches)
#            en agglomérant les points distants de moins de RAYON_AGGLO_ENTREPOT_M.
#
# RÈGLES :
#   • Les postes FRONTIÈRES sont protégés : toujours conservés comme nœuds
#     distincts, jamais fusionnés ni absorbants (indispensables au modèle de
#     commerce international, 03_transport.R).
#   • Les autres points sont traités par population DÉCROISSANTE : le premier
#     point libre devient une ANCRE et absorbe tous les points libres situés à
#     ≤ 4 km. On répète jusqu'à épuisement. Chaque cluster est représenté par son
#     ancre = le point de plus FORTE population (garanti par l'ordre décroissant).
#
#   Conséquence « un seul intermédiaire » : deux points d'un même cluster sont à
#   au plus 2 sauts l'un de l'autre VIA l'ancre. Un point accessible seulement
#   par 2 intermédiaires (> 4 km de l'ancre) n'est pas rattaché et devient une
#   ancre distincte.
################################################################################

cat("==========================================================\n")
cat("  PARTIE IV.3-bis — FUSION DES ENTREPÔTS À",
    RAYON_AGGLO_ENTREPOT_M / 1000, "km\n")
cat("==========================================================\n\n")

n_cand        <- nrow(entreposages_sf)
est_frontiere <- entreposages_sf$type == "frontiere"   # points à protéger

# Liste des voisins à ≤ 4 km pour chaque candidat (indices dans entreposages_sf).
# st_is_within_distance(x, x) renvoie, pour chaque point, les indices des points
# situés à ≤ dist (lui-même inclus).
voisins <- st_is_within_distance(
  entreposages_sf, entreposages_sf, dist = RAYON_AGGLO_ENTREPOT_M
)

# ancre_de[i] = indice de l'ancre qui représente le candidat i (0 = non affecté).
ancre_de <- integer(n_cand)

# Les frontières sont leurs propres ancres (protégées).
ancre_de[est_frontiere] <- which(est_frontiere)

# Parcours des NON-frontières par population temporaire décroissante.
ordre <- order(pop_temp, decreasing = TRUE)
ordre <- ordre[!est_frontiere[ordre]]

for (i in ordre) {
  if (ancre_de[i] != 0L) next          # déjà absorbé par une ancre plus peuplée
  ancre_de[i] <- i                     # i devient une ancre
  # Voisins encore libres et non-frontières → rattachés à l'ancre i.
  vois <- voisins[[i]]
  vois <- vois[ancre_de[vois] == 0L & !est_frontiere[vois]]
  ancre_de[vois] <- i
}

# Indices des points conservés (ceux qui sont leur propre ancre).
idx_kept       <- which(ancre_de == seq_len(n_cand))
# Taille de chaque cluster (nb de candidats rattachés à l'ancre) — diagnostic.
taille_cluster <- tabulate(ancre_de, nbins = n_cand)[idx_kept]

# Jeu d'entrepôts conservés (sf), avec leur population temporaire et la taille
# du cluster qu'ils représentent.
entreposages_kept <- entreposages_sf[idx_kept, ] %>%
  mutate(
    pop_temp       = pop_temp[idx_kept],
    n_absorbes     = taille_cluster,
    is_frontiere   = est_frontiere[idx_kept]
  )

cat("  Candidats initiaux :", n_cand, "→ conservés après fusion :",
    nrow(entreposages_kept), "\n")
cat("  dont frontières (protégées) :", sum(entreposages_kept$is_frontiere), "\n")
cat("  Clusters de plus d'un point :", sum(taille_cluster > 1),
    "| taille max :", max(taille_cluster), "\n\n")

# ── Accrochage (snapping) des entrepôts conservés au réseau ───────────────────
# st_nearest_feature() accroche chaque entrepôt conservé au nœud routier le plus
# proche (indispensable pour que Dijkstra parte d'un nœud du graphe).
entreposages_avec_snap <- entreposages_kept %>%
  mutate(
    noeud_proche_id = st_nearest_feature(geometry, noeuds_reseau),
    distance_snap   = as.numeric(
      st_distance(geometry, noeuds_reseau[noeud_proche_id, ], by_element = TRUE)
    )
  ) %>%
  # Garde-fou : si deux entrepôts conservés tombent sur le même nœud réseau (très
  # rare, car distants d'au moins 4 km), on garde la frontière sinon le plus peuplé.
  arrange(desc(is_frontiere), desc(pop_temp)) %>%
  distinct(noeud_proche_id, .keep_all = TRUE)

cat("  Entrepôts accrochés à un nœud distinct :",
    nrow(entreposages_avec_snap), "\n\n")

# ── Marquage des nœuds-entrepôts dans reseau ──────────────────────────────────
# match(node_id, noeud_proche_id) retrouve le nom/type/pays de l'entrepôt accroché.
reseau <- reseau %>%
  activate("nodes") %>%
  mutate(
    node_id        = row_number(),
    is_warehouse   = node_id %in% entreposages_avec_snap$noeud_proche_id,
    warehouse_name = if_else(is_warehouse,
      entreposages_avec_snap$nom[match(node_id, entreposages_avec_snap$noeud_proche_id)],
      NA_character_),
    warehouse_type = if_else(is_warehouse,
      entreposages_avec_snap$type[match(node_id, entreposages_avec_snap$noeud_proche_id)],
      NA_character_),
    warehouse_pays = if_else(is_warehouse,
      entreposages_avec_snap$pays[match(node_id, entreposages_avec_snap$noeud_proche_id)],
      NA_character_)
  )

# ── noeuds_entreposage : table des nœuds-entrepôts (1 ligne par nœud) ─────────
# warehouse_id = numéro de ligne (1..n_warehouses) dans l'ordre des nœuds du réseau.
noeuds_entreposage <- reseau %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  as_tibble() %>%
  mutate(warehouse_id = row_number())

n_warehouses <- nrow(noeuds_entreposage)

# seeds_sf : mêmes nœuds en objet sf POINT (même ordre → même warehouse_id),
# utilisés comme germes du pavage de Voronoï (IV.6).
seeds_sf <- reseau %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  st_as_sf() %>%
  mutate(warehouse_id = row_number())

cat("✓ noeuds_entreposage défini :", n_warehouses, "nœuds-entrepôts\n")
cat("  dont frontières :",
    sum(noeuds_entreposage$warehouse_type == "frontiere"), "\n\n")


################################################################################
# PARTIE IV.6 — PAVAGE DE VORONOÏ ET POPULATION DÉFINITIVE PAR CELLULE
#
# OBJECTIF : Découper le territoire en polygones de Voronoï (un par nœud-entrepôt).
#            Chaque point du territoire est ainsi rattaché à l'entrepôt le plus
#            proche, et l'entrepôt hérite des caractéristiques de l'espace qu'il
#            représente — à commencer par sa POPULATION définitive, somme des
#            pixels WorldPop de sa cellule (couverture exhaustive, sans
#            chevauchement → le total national est conservé).
################################################################################

cat("==========================================================\n")
cat("  PARTIE IV.6 — PAVAGE DE VORONOÏ + POPULATION PAR CELLULE\n")
cat("==========================================================\n\n")

# ── Construction du pavage ────────────────────────────────────────────────────
# st_voronoi() tesselle le plan à partir d'un MULTIPOINT (union des germes).
# L'enveloppe (bbox du pays) borne la tessellation ; on rogne ensuite sur la
# frontière nationale pour que les cellules ne débordent pas du pays.
pays_utm <- pays_boundary %>%
  st_transform(32735) %>%
  st_union() %>%
  st_make_valid()

zones_voronoi <- st_voronoi(
    st_union(seeds_sf),
    envelope = st_as_sfc(st_bbox(pays_utm))
  ) %>%
  st_collection_extract("POLYGON") %>%       # une polygone par germe
  st_sf(geometry = .) %>%
  st_set_crs(st_crs(seeds_sf)) %>%           # st_voronoi peut perdre le CRS
  st_make_valid() %>%
  st_intersection(pays_utm) %>%              # rognage sur le pays
  st_make_valid()

# Rattachement de chaque cellule à son germe : st_nearest_feature() renvoie le
# germe contenu dans la cellule (distance nulle), donc son warehouse_id.
zones_voronoi <- zones_voronoi %>%
  mutate(warehouse_id = seeds_sf$warehouse_id[
    st_nearest_feature(geometry, seeds_sf)
  ])

# Contrôle : une cellule par nœud-entrepôt, identifiants tous distincts.
stopifnot(
  nrow(zones_voronoi)               == n_warehouses,
  n_distinct(zones_voronoi$warehouse_id) == n_warehouses
)

# ── Population définitive = somme des pixels WorldPop dans chaque cellule ──────
if (worldpop_ok) {
  pop_cellule <- as.numeric(exactextractr::exact_extract(
    raster_worldpop,
    zones_voronoi %>% st_transform(st_crs(raster_worldpop)),
    fun = "sum", progress = FALSE
  ))
  source_pop_cellule <- "WorldPop_cellule_Voronoi"

} else if (exists("districts_avec_pop") && !is.null(districts_avec_pop)) {
  # Repli : à défaut de WorldPop, on répartit la population NISR de chaque
  # district au prorata de l'aire de cellule qui le recouvre.
  cat("  ⚠ WorldPop indisponible → population par cellule via aire × districts NISR\n")
  inter <- suppressWarnings(st_intersection(
    zones_voronoi %>% select(warehouse_id),
    districts_avec_pop %>% select(district_clean, pop_total)
  )) %>%
    mutate(aire = as.numeric(st_area(geometry))) %>%
    st_drop_geometry() %>%
    group_by(district_clean) %>%
    mutate(part_aire = aire / sum(aire)) %>%   # part de chaque cellule dans le district
    ungroup() %>%
    mutate(pop_part = pop_total * part_aire) %>%
    group_by(warehouse_id) %>%
    summarise(pop = sum(pop_part, na.rm = TRUE), .groups = "drop")
  pop_cellule <- inter$pop[match(zones_voronoi$warehouse_id, inter$warehouse_id)]
  source_pop_cellule <- "NISR_aire_cellule"

} else {
  cat("  ⚠ Aucune source de population disponible → fallback minimal\n")
  pop_cellule <- rep(POP_FALLBACK_MIN, nrow(zones_voronoi))
  source_pop_cellule <- paste0("Fallback_", POP_FALLBACK_MIN)
}

# Plancher à POP_FALLBACK_MIN (évite population nulle → division par zéro MRIO).
zones_voronoi$population_zone <- round(pmax(pop_cellule, POP_FALLBACK_MIN,
                                            na.rm = TRUE))

# Vecteur population aligné sur l'ordre warehouse_id (1..n_warehouses).
pop_par_wid <- zones_voronoi$population_zone[
  match(seq_len(n_warehouses), zones_voronoi$warehouse_id)
]

# ── Diagnostic et stockage ────────────────────────────────────────────────────
diag_population <- tibble(
  nom_zone        = noeuds_entreposage$warehouse_name,
  type_zone       = noeuds_entreposage$warehouse_type,
  population_zone = pop_par_wid,
  source          = source_pop_cellule
)

# ── Classification urbain/rural des cellules (pour la demande finale par groupe) ──
# OBJECTIF : calculer pour chaque cellule de Voronoï sa PART URBAINE = part de sa
# population vivant en zone de landuse urbain (zones_urbaines_union), puis un statut
# is_urbain (part ≥ SEUIL_PART_URBAINE). Sert en 03_transport.R à rattacher chaque 
# zone à un groupe de ménages SAM (strate urbain/rural × quintile).
# On mesure une part de POPULATION (et non d'aire).
part_urbaine_par_wid <- setNames(rep(0, n_warehouses), seq_len(n_warehouses))

if (worldpop_ok && exists("zones_urbaines_union") &&
    !is.null(zones_urbaines_union) && length(zones_urbaines_union) > 0) {
  # Intersection de chaque cellule avec l'union des zones urbaines, puis somme des
  # pixels WorldPop tombant dans la partie urbaine de la cellule.
  inter_urb <- suppressWarnings(st_intersection(
    zones_voronoi %>% select(warehouse_id),
    st_make_valid(zones_urbaines_union) %>% st_transform(st_crs(zones_voronoi))
  ))
  if (nrow(inter_urb) > 0) {
    pop_urb <- as.numeric(exactextractr::exact_extract(
      raster_worldpop,
      inter_urb %>% st_transform(st_crs(raster_worldpop)),
      fun = "sum", progress = FALSE
    ))
    # tapply : agrège par warehouse_id (une cellule peut donner plusieurs morceaux).
    pop_urb_agg <- tapply(pop_urb, inter_urb$warehouse_id, sum, na.rm = TRUE)
    part_urbaine_par_wid[as.integer(names(pop_urb_agg))] <- as.numeric(pop_urb_agg)
    # Part urbaine = pop urbaine / pop totale de la cellule, bornée à [0,1].
    part_urbaine_par_wid <- pmin(1, part_urbaine_par_wid / pmax(pop_par_wid, 1))
  }
} else {
  cat("  ⚠ WorldPop ou zones urbaines indisponibles → toutes les zones",
      "classées rurales (part urbaine = 0)\n")
}

# Ajout à diag_population (lignes alignées sur l'ordre warehouse_id = 1..n).
diag_population <- diag_population %>%
  mutate(
    part_urbaine = round(part_urbaine_par_wid, 3),
    is_urbain    = part_urbaine_par_wid >= SEUIL_PART_URBAINE
  )

cat("  Zones urbaines (part pop ≥ ", SEUIL_PART_URBAINE, ") : ",
    sum(diag_population$is_urbain), " / ", n_warehouses,
    " | part pop urbaine nationale : ",
    round(sum(pop_par_wid[diag_population$is_urbain]) / sum(pop_par_wid) * 100, 1),
    " %\n", sep = "")

cat("  Population par cellule : min =", round(min(pop_par_wid)),
    "| max =", round(max(pop_par_wid)), "\n")
cat("  Somme des populations de cellules :",
    format(round(sum(pop_par_wid)), big.mark = " "),
    "(≈ population nationale si WorldPop)\n\n")

cat("Population par zone (top 10) :\n")
print(
  diag_population %>%
    arrange(desc(population_zone)) %>%
    slice_head(n = 10) %>%
    rename(Zone = nom_zone, Type = type_zone,
           Population = population_zone, Source = source)
)

# Stockage DuckDB (table interrogeable par les Parties V à IX).
duck_write(diag_population, "population_entrepots")

# Intégration comme attribut de nœud (NA pour les nœuds non-entrepôt).
reseau <- reseau %>%
  activate("nodes") %>%
  mutate(
    population_zone = diag_population$population_zone[
      match(warehouse_name, diag_population$nom_zone)
    ]
  )

# ── Reconstruction de entreposages_fictifs = table de référence des nœuds ──────
# Après la fusion, entreposages_fictifs ne décrit plus N candidats mais les
# n_warehouses nœuds conservés (clé = nom = warehouse_name). On y remet les
# coordonnées (depuis la géométrie du nœud) et la population définitive.
coords_wgs <- seeds_sf %>% st_transform(4326) %>% st_coordinates()
entreposages_fictifs <- tibble(
  nom               = noeuds_entreposage$warehouse_name,
  type              = noeuds_entreposage$warehouse_type,
  pays              = noeuds_entreposage$warehouse_pays,
  lon               = coords_wgs[, 1],
  lat               = coords_wgs[, 2],
  source            = "voronoi",
  population_zone   = pop_par_wid,
  source_population = source_pop_cellule
)
duck_write(entreposages_fictifs, "zones_entreposage")

cat("\n✓ Partie IV.6 terminée — zones_voronoi + population_zone disponibles dans :\n")
cat("  • reseau  (attribut de nœud population_zone)\n")
cat("  • DuckDB         (table population_entrepots)\n")
cat("  • zones_voronoi  (sf : un polygone par nœud-entrepôt)\n\n")

################################################################################
# PARTIE IV.5 — ENRICHISSEMENT PAR L'INDICE DE RICHESSE RELATIVE (RWI)
#
# OBJECTIF : Associer à chaque zone d'entrepôt un score de richesse relative
#            (Relative Wealth Index, Meta / CIESIN) pour moduler la taille
#            économique des entrepôts dans le modèle gravitaire (Partie VII).
#
# MÉTHODE — MÊME LOGIQUE QUE L'USAGE DES SOLS (Partie IV.3) :
#   ┌──────────────────────────────────────────────────────────────────┐
#   │  Usage des sols : proportion de surface couverte par un type     │
#   │   → scalaire p_ind ou p_urb entre 0 et 1                         │
#   │  RWI           : moyenne pondérée par distance inverse (IDW)     │
#   │   des scores des cellules RWI dans le buffer de chaque entrepôt  │
#   │   → scalaire p_rwi entre 0 et 1 (normalisé min-max)              │                
#   └──────────────────────────────────────────────────────────────────┘
#
# SOURCE : Chi, G., Fang, H., Chatterjee, S. & Blumenstock, J.E. (2022).
#          Microestimates of wealth for all low- and middle-income countries.
#          PNAS, 119(3), e2113658119. doi:10.1073/pnas.2113658119
#          Données téléchargeables librement (CC0) sur HDX :
#          https://data.humdata.org/dataset/relative-wealth-index
#
# PLACEMENT DANS LE SCRIPT :
#   Dépend de :
#     - entreposages_sf, entreposages_buffer (Partie IV.3)
#     - reseau          (Partie III)
#     - pays_boundary        (Partie II.3)
#     - duck_write()           (Partie I.2)
#   Alimente :
#     - Transition IV.5 → V : variable p_rwi (pour diagnostics RWI)
#     - reseau (attribut de nœud : rwi_moyen, p_rwi)
#     - DuckDB (table richesse_entrepots)
################################################################################

cat("==========================================================\n")
cat("  PARTIE IV.5 — INDICE DE RICHESSE RELATIVE (RWI)\n")
cat("==========================================================\n\n")

# ==============================================================================
# IV.5.1 : Téléchargement et préparation des données RWI
#
# Le fichier CSV contient une ligne par cellule de ~2,4 km² avec :
#   - latitude  : latitude WGS84 du centroïde de la cellule
#   - longitude : longitude WGS84 du centroïde de la cellule
#   - rwi       : score de richesse relative (centré sur 0, pas d'unité)
#   - error     : incertitude du modèle (écart-type de la prédiction)
#
# On convertit ce tableau en objet sf, on reprojette en UTM 35S, puis on
# met en cache pour éviter le retéléchargement aux prochaines sessions.
# ==============================================================================

cat("── Chargement des données RWI ────────────────────────────────────────\n")

rwi_sf   <- NULL
rwi_ok   <- FALSE

# ── Tentative 1 : chargement depuis le cache local ────────────────────────────
# Si le CSV RWI a déjà été extrait lors d'une session précédente, on
# l'utilise directement sans retélécharger le ZIP.
if (file.exists(RWI_CSV_LOCAL)) {
  
  cat("  CSV RWI trouvé en cache local :", RWI_CSV_LOCAL, "\n")
  
  tryCatch({
    
    rwi_raw <- read_csv(RWI_CSV_LOCAL, show_col_types = FALSE)
    
    # Vérification des colonnes attendues
    cols_attendues <- c("latitude", "longitude", "rwi", "error")
    if (!all(cols_attendues %in% names(rwi_raw))) {
      stop("Colonnes manquantes : ",
           paste(setdiff(cols_attendues, names(rwi_raw)), collapse = ", "))
    }
    
    # Conversion en objet sf : chaque ligne devient un point géospatial.
    # CRS 4326 = WGS84 (système GPS, coordonnées en degrés décimaux).
    # CRS 32735 = UTM Zone 35S (mètres, cohérent avec le réseau routier).
    rwi_sf <- rwi_raw %>%
      filter(!is.na(rwi), !is.na(latitude), !is.na(longitude)) %>%
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
      st_transform(crs = 32735) %>%
      # On conserve uniquement les colonnes utiles pour alléger l'objet
      select(rwi, error)
    
    rwi_ok <- TRUE
    cat("  ✓ RWI chargé depuis cache :", nrow(rwi_sf), "cellules\n")
    
  }, error = function(e) {
    cat("  ⚠ Lecture CSV échouée :", conditionMessage(e), "\n")
    cat("    → Retéléchargement du ZIP\n")
  })
}

# ── Tentative 2 : téléchargement du ZIP et extraction ─────────────────────────
# Le ZIP contient les 93 pays. On le télécharge une fois (~35 Mo), on extrait
# uniquement le fichier du pays, et on supprime le ZIP pour libérer l'espace.
if (!rwi_ok) {
  
  cat("  Téléchargement du ZIP RWI (~35 Mo)...\n")
  cat("  Source :", RWI_ZIP_URL, "\n")
  
  tryCatch({
    
    # download.file() : télécharge un fichier depuis une URL.
    # mode = "wb" (write binary) est indispensable pour les archives ZIP.
    # quiet = FALSE : afficher la progression du téléchargement.
    download.file(RWI_ZIP_URL, destfile = RWI_ZIP_LOCAL,
                  mode = "wb", quiet = FALSE)
    
    # Liste des fichiers dans le ZIP pour vérifier que le pays est présent.
    # unzip(list = TRUE) ne décompresse pas — il liste uniquement le contenu.
    contenu_zip <- unzip(RWI_ZIP_LOCAL, list = TRUE)
    cat("  Fichiers dans le ZIP :", nrow(contenu_zip), "\n")

    # Vérification que le fichier pays est dans le ZIP.
    # La présence de majuscules/minuscules peut varier selon la version du ZIP.
    # grepl() + ignore.case = TRUE gère les deux cas.
    idx_fichier <- grep(
      pattern     = RWI_FICHIER,
      x           = contenu_zip$Name,
      ignore.case = TRUE
    )

    if (length(idx_fichier) == 0) {
      stop("Fichier pays introuvable dans le ZIP (RWI_FICHIER = ", RWI_FICHIER, ").\n",
           "Fichiers disponibles : ",
           paste(head(contenu_zip$Name, 10), collapse = ", "))
    }

    nom_fichier_zip <- contenu_zip$Name[idx_fichier[1]]
    cat("  Fichier pays dans le ZIP :", nom_fichier_zip, "\n")

    # Extraction du seul fichier pays (évite de décompresser 93 pays).
    # exdir = dirname(RWI_CSV_LOCAL) : répertoire de destination.
    unzip(
      zipfile = RWI_ZIP_LOCAL,
      files   = nom_fichier_zip,
      exdir   = dirname(RWI_CSV_LOCAL)
    )
    
    # Renommage si nécessaire (normalisation vers RWI_CSV_LOCAL)
    chemin_extrait <- file.path(dirname(RWI_CSV_LOCAL), nom_fichier_zip)
    if (chemin_extrait != RWI_CSV_LOCAL && file.exists(chemin_extrait)) {
      file.rename(chemin_extrait, RWI_CSV_LOCAL)
    }
    
    # Suppression du ZIP pour libérer l'espace (~35 Mo)
    # (le CSV extrait fait ~500 Ko et est conservé comme cache)
    file.remove(RWI_ZIP_LOCAL)
    cat("  ZIP supprimé après extraction\n")
    
    # ── Chargement du CSV extrait ─────────────────────────────────────────────
    rwi_raw <- read_csv(RWI_CSV_LOCAL, show_col_types = FALSE)
    
    rwi_sf <- rwi_raw %>%
      filter(!is.na(rwi), !is.na(latitude), !is.na(longitude)) %>%
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
      st_transform(crs = 32735) %>%
      select(rwi, error)
    
    rwi_ok <- TRUE
    cat("  ✓ RWI téléchargé et chargé :", nrow(rwi_sf), "cellules\n")
    
  }, error = function(e) {
    cat("  ⚠ Téléchargement RWI échoué :", conditionMessage(e), "\n")
    cat("    → Téléchargement manuel : ", RWI_ZIP_URL, "\n")
    cat("    → Extraire", RWI_FICHIER, "vers", RWI_CSV_LOCAL, "\n")
    cat("    → Partie IV.5 ignorée, le modèle continue sans RWI\n\n")
  })
}

# ── Statistiques descriptives du RWI ──────────────────────────────────────────
if (rwi_ok) {
  
  rwi_stats <- tibble(
    n_cellules   = nrow(rwi_sf),
    rwi_min      = round(min(rwi_sf$rwi),  3),
    rwi_max      = round(max(rwi_sf$rwi),  3),
    rwi_median   = round(median(rwi_sf$rwi), 3),
    rwi_mean     = round(mean(rwi_sf$rwi),  3),
    erreur_moy   = round(mean(rwi_sf$error, na.rm = TRUE), 3)
  )
  
  cat("\n  Distribution du RWI :\n")
  cat("  Cellules     :", rwi_stats$n_cellules, "\n")
  cat("  Min / Max    :", rwi_stats$rwi_min, "/", rwi_stats$rwi_max, "\n")
  cat("  Médiane / Moy:", rwi_stats$rwi_median, "/", rwi_stats$rwi_mean, "\n")
  cat("  Erreur moy.  :", rwi_stats$erreur_moy, "\n\n")
  
  # ── Rognage aux limites du pays ───────────────────────────────────────────────
  # On s'assure que les cellules RWI sont bien dans le territoire étudié
  # (le ZIP peut contenir des cellules légèrement hors frontière).
  # st_filter() avec st_intersects : conserve les points dans le polygone.
  rwi_sf <- rwi_sf %>%
    st_filter(pays_boundary %>%
                st_buffer(dist = 1000) %>%  # 1km de marge pour les frontières
                st_union())
  
  cat("  Cellules après rognage :", nrow(rwi_sf), "\n\n")
}


# ==============================================================================
# IV.5.2 : RWI moyen par cellule de Voronoï, pondéré par la population
#
# MÉTHODE (le RWI est une variable RELATIVE) :
#   Chaque point RWI tombant dans une cellule de Voronoï reçoit un poids égal à
#   la population locale (somme des pixels WorldPop dans un cercle de
#   BUFFER_POIDS_RWI_M autour du point). Le score de la cellule est la moyenne
#   des RWI pondérée par ces poids :
#       rwi_brut[cellule] = Σ(rwi_i · poids_i) / Σ(poids_i)
#   Les portions densément peuplées d'une cellule pèsent ainsi davantage que ses
#   marges désertes. Le score est ensuite normalisé en p_rwi ∈ [0, 1] (IV.5.3).
# ==============================================================================

cat("── RWI par cellule de Voronoï (moyenne pondérée population) ──────────\n")

if (rwi_ok) {

  # ── Poids-population de chaque point RWI (calculé une seule fois) ───────────
  # exact_extract somme les pixels WorldPop dans un cercle BUFFER_POIDS_RWI_M
  # (≈ demi-maille RWI) autour de chaque point RWI → population qu'il représente.
  if (worldpop_ok) {
    poids_rwi <- as.numeric(exactextractr::exact_extract(
      raster_worldpop,
      rwi_sf %>% st_buffer(BUFFER_POIDS_RWI_M) %>%
        st_transform(st_crs(raster_worldpop)),
      fun = "sum", progress = FALSE
    ))
    # Poids plancher = 1 là où WorldPop ne voit personne (évite Σpoids = 0).
    poids_rwi[is.na(poids_rwi) | poids_rwi <= 0] <- 1
  } else {
    # Sans WorldPop, on retombe sur une moyenne arithmétique simple (poids = 1).
    poids_rwi <- rep(1, nrow(rwi_sf))
  }
  rwi_sf$poids_pop <- poids_rwi

  # ── Rattachement de chaque point RWI à sa cellule de Voronoï ────────────────
  # st_join + st_within : chaque point RWI hérite du warehouse_id de la cellule
  # qui le contient. left = FALSE écarte les points hors du pays (marge de 1 km).
  rwi_in_cell <- rwi_sf %>%
    st_join(zones_voronoi %>% select(warehouse_id),
            join = st_within, left = FALSE)

  # ── Moyenne pondérée par cellule ────────────────────────────────────────────
  rwi_cellule <- rwi_in_cell %>%
    st_drop_geometry() %>%
    group_by(warehouse_id) %>%
    summarise(rwi_brut = sum(rwi * poids_pop) / sum(poids_pop),
              .groups = "drop")

  # Vecteur aligné sur l'ordre warehouse_id (1..n_warehouses).
  rwi_brut_par_wid <- rwi_cellule$rwi_brut[
    match(seq_len(n_warehouses), rwi_cellule$warehouse_id)
  ]

  # Cellules sans aucun point RWI (rare) → score du point RWI le plus proche.
  manquants_rwi <- which(is.na(rwi_brut_par_wid))
  if (length(manquants_rwi) > 0) {
    idx_proche <- st_nearest_feature(seeds_sf[manquants_rwi, ], rwi_sf)
    rwi_brut_par_wid[manquants_rwi] <- rwi_sf$rwi[idx_proche]
    cat("  Cellules sans point RWI (plus proche utilisé) :",
        length(manquants_rwi), "\n")
  }

} else {
  # RWI indisponible → score neutre 0 pour tous (le modèle tourne sans RWI).
  cat("  ⚠ RWI indisponible — valeurs neutres (0) pour tous les nœuds\n")
  rwi_brut_par_wid <- rep(0, n_warehouses)
}


# ==============================================================================
# IV.5.3 : Normalisation min-max → p_rwi ∈ [0, 1]
#
# Le score brut est centré sur 0 (échelle nationale, parfois négatif). On
# le normalise sur l'ensemble des nœuds : 0 = nœud le plus pauvre, 1 = le plus
# riche. PRÉCAUTION : score RELATIF au pays, pas une richesse absolue mondiale.
# ==============================================================================

cat("── Normalisation et intégration des scores RWI ───────────────────────\n")

# Imputation des éventuels NA par la médiane (valeur neutre).
n_na_rwi <- sum(is.na(rwi_brut_par_wid))
if (n_na_rwi > 0) {
  rwi_brut_par_wid[is.na(rwi_brut_par_wid)] <-
    median(rwi_brut_par_wid, na.rm = TRUE)
}

# rescale() (package scales) : transformation min-max en une ligne.
rwi_min <- min(rwi_brut_par_wid)
rwi_max <- max(rwi_brut_par_wid)
p_rwi <- if (rwi_max > rwi_min) {
  rescale(rwi_brut_par_wid, to = c(0, 1))
} else {
  rep(0.5, n_warehouses)   # cas dégénéré : tous les nœuds ont le même score
}

cat("  Score p_rwi : min =", round(min(p_rwi), 3),
    "| médiane =", round(median(p_rwi), 3),
    "| max =", round(max(p_rwi), 3), "\n\n")

stopifnot(length(rwi_brut_par_wid) == n_warehouses,
          length(p_rwi)            == n_warehouses)

# ── Tableau de synthèse (aligné sur noeuds_entreposage) ───────────────────────
diag_rwi <- tibble(
  nom_zone   = noeuds_entreposage$warehouse_name,
  type_zone  = noeuds_entreposage$warehouse_type,
  rwi_brut   = round(rwi_brut_par_wid, 3),
  p_rwi      = round(p_rwi, 3),
  classe_rwi = case_when(
    p_rwi >= 0.75 ~ "Très riche",
    p_rwi >= 0.50 ~ "Riche",
    p_rwi >= 0.25 ~ "Pauvre",
    TRUE          ~ "Très pauvre"
  )
)

cat("Scores RWI par zone (top 10) :\n")
print(
  diag_rwi %>%
    arrange(desc(p_rwi)) %>%
    slice_head(n = 10) %>%
    rename(Zone = nom_zone, Type = type_zone,
           RWI_brut = rwi_brut, Classe = classe_rwi)
)
cat("\n")

# Stockage DuckDB (table interrogeable par les Parties V à IX).
duck_write(diag_rwi, "richesse_entrepots")

# Intégration comme attributs de nœud (NA pour les nœuds non-entrepôt).
reseau <- reseau %>%
  activate("nodes") %>%
  mutate(
    rwi_brut = diag_rwi$rwi_brut[match(warehouse_name, diag_rwi$nom_zone)],
    p_rwi    = diag_rwi$p_rwi[match(warehouse_name, diag_rwi$nom_zone)]
  )

# Enrichissement de la table de référence des nœuds.
stopifnot(nrow(entreposages_fictifs) == nrow(diag_rwi))
entreposages_fictifs <- entreposages_fictifs %>%
  select(-any_of(c("rwi_brut", "p_rwi", "classe_rwi"))) %>%
  bind_cols(diag_rwi %>% select(rwi_brut, p_rwi, classe_rwi))
duck_write(entreposages_fictifs, "zones_entreposage")

cat("✓ Partie IV.5 terminée — rwi_brut, p_rwi disponibles dans :\n")
cat("  • reseau  (attributs de nœud : rwi_brut, p_rwi)\n")
cat("  • DuckDB         (table richesse_entrepots)\n")
cat("  • entreposages_fictifs (colonnes rwi_brut, p_rwi, classe_rwi)\n\n")

################################################################################
# PARTIE IV.4.F — EMPLOI SECTORIEL RPHC5 2022 (par nœud-entrepôt / Voronoï)
#
# OBJECTIF : Construire emploi_zone_secteur[i, s] — matrice n_warehouses ×
#   N_SECTEURS d'effectifs absolus par NŒUD-ENTREPÔT et secteur. Elle alimente
#   le modèle MRIO (03_transport.R) via le poids composite :
#     w[i,s] = α × (emploi[i,s] / emploi_national[s])
#            + (1-α) × (p_rwi[i] / Σ_j p_rwi[j])
#     x[i,s] = production_totale[s] × w[i,s]
#   où α = ALPHA_EMPLOI_RWI (00_parametres.R).
#
# MÉTHODE (prorata d'aire des cellules de Voronoï) :
#   L'emploi est connu au niveau DISTRICT (RPHC5/GADM). On intersecte les cellules
#   de Voronoï avec les polygones de district, puis on attribue à chaque cellule
#   la fraction de l'emploi du district proportionnelle à l'aire de l'intersection
#   (même logique que le fallback NISR pour la population). Les cellules de Voronoï
#   couvrant l'intégralité du territoire, chaque district est découpé entre au
#   moins une cellule : aucun emploi n'est perdu.
#
# DÉPENDANCES :
#   - seeds_sf, noeuds_entreposage   (Partie IV.3-bis) — nœuds conservés
#   - pays_districts_gadm          (Partie IV.4.C — rechargé si absent)
#   - SECTEURS, N_SECTEURS, RPHC5_CORRESPONDANCE_SECTEURS (Paramètres)
# ALIMENTE :
#   - Transition IV.5→V : emploi_zone_secteur (= emploi_zone_secteur_all)
#   - DuckDB            : table "diag_emploi"
################################################################################

cat("==========================================================\n")
cat("  PARTIE IV.4.F — EMPLOI SECTORIEL RPHC5 2022 (par nœud)\n")
cat("==========================================================\n\n")

if (!file.exists(RPHC5_EMPLOI_CSV_PATH)) {
  stop("Fichier RPHC5 emploi introuvable : ", RPHC5_EMPLOI_CSV_PATH,
       "\n  → Télécharger sur https://www.statistics.gov.rw/datasource/census-2022")
}

# ── Chargement et nettoyage du CSV d'emploi RPHC5 ─────────────────────────────
rphc5_emploi_raw <- read_csv(RPHC5_EMPLOI_CSV_PATH, show_col_types = FALSE)
cat("  CSV emploi RPHC5 chargé :", nrow(rphc5_emploi_raw), "lignes\n")

cols_attendues  <- names(RPHC5_CORRESPONDANCE_SECTEURS)
cols_manquantes <- cols_attendues[!cols_attendues %in% names(rphc5_emploi_raw)]
if (length(cols_manquantes) > 0) {
  warning("  ⚠ Colonnes manquantes : ", paste(cols_manquantes, collapse = ", "),
          "\n  Adapter RPHC5_CORRESPONDANCE_SECTEURS dans les paramètres.")
}

# Normalisation des noms de district (même translittération que IV.4.C).
rphc5_emploi <- rphc5_emploi_raw %>%
  rename(district = any_of(RPHC5_COL_DISTRICT_EMPLOI)) %>%
  mutate(
    district_clean = iconv(str_to_lower(str_trim(district)),
                           from = "UTF-8", to = "ASCII//TRANSLIT"),
    across(all_of(intersect(cols_attendues, names(.))),
           ~ suppressWarnings(as.numeric(str_remove_all(as.character(.), "[,\\s]"))))
  ) %>%
  {
    cols_presentes <- intersect(cols_attendues, names(.))
    if (!"emploi_total" %in% names(.) && length(cols_presentes) > 0) {
      mutate(., emploi_total = rowSums(select(., all_of(cols_presentes)), na.rm = TRUE))
    } else { . }
  }

cat("  Districts après nettoyage :", nrow(rphc5_emploi), "\n")

# ── Rechargement de GADM si absent (session interrompue depuis IV.4.C) ─────────
if (!exists("pays_districts_gadm") || is.null(pays_districts_gadm)) {
  cat("  Retéléchargement des frontières GADM...\n")
  pays_districts_gadm <- tryCatch({
    geodata::gadm(country = "RWA", level = 2, path = tempdir()) %>%
      st_as_sf() %>%
      st_transform(crs = 32735) %>%
      mutate(district_clean = iconv(str_to_lower(str_trim(NAME_2)),
                                    from = "UTF-8", to = "ASCII//TRANSLIT")) %>%
      select(district_gadm = NAME_2, district_clean, geometry)
  }, error = function(e) {
    cat("  ⚠ GADM indisponible :", conditionMessage(e), "\n"); NULL
  })
}

if (is.null(pays_districts_gadm)) {
  stop("GADM indisponible — impossible de construire emploi_zone_secteur.")
}

# ── Jointure GADM × emploi RPHC5 ──────────────────────────────────────────────
cols_emploi_disponibles <- intersect(c(cols_attendues, "emploi_total"),
                                     names(rphc5_emploi))

districts_avec_emploi <- pays_districts_gadm %>%
  left_join(rphc5_emploi %>% select(district_clean, all_of(cols_emploi_disponibles)),
            by = "district_clean")

n_sans_emploi <- sum(is.na(districts_avec_emploi$emploi_total))
cat("  Jointure GADM × emploi :",
    nrow(districts_avec_emploi) - n_sans_emploi, "/",
    nrow(districts_avec_emploi), "districts appariés\n")

# ── Répartition de l'emploi de chaque district au prorata de l'aire de Voronoï ─
# On intersecte les cellules de Voronoï avec les districts GADM pour calculer
# la fraction d'aire de chaque cellule dans son district, puis on applique ce
# poids à l'emploi sectoriel du district (même logique que le fallback NISR
# pour la population en IV.6).
inter_emploi <- suppressWarnings(st_intersection(
  zones_voronoi %>% select(warehouse_id),
  districts_avec_emploi %>% select(district_clean, all_of(cols_emploi_disponibles))
)) %>%
  mutate(aire = as.numeric(st_area(geometry))) %>%
  st_drop_geometry() %>%
  group_by(district_clean) %>%
  mutate(part_aire = aire / sum(aire)) %>%   # fraction de l'aire du district dans chaque cellule
  ungroup()

emploi_node_cols <- matrix(
  0, nrow = n_warehouses, ncol = length(cols_emploi_disponibles),
  dimnames = list(noeuds_entreposage$warehouse_name, cols_emploi_disponibles)
)

# Pour chaque colonne sectorielle RPHC5 : emploi_district × part_aire, puis
# agrégation par warehouse_id (une cellule peut chevaucher plusieurs districts).
for (cc in cols_emploi_disponibles) {
  inter_cc <- inter_emploi %>%
    mutate(emploi_part = .data[[cc]] * part_aire) %>%
    group_by(warehouse_id) %>%
    summarise(emploi = sum(emploi_part, na.rm = TRUE), .groups = "drop")
  emploi_node_cols[inter_cc$warehouse_id, cc] <- inter_cc$emploi
}

cat("  Cellules Voronoï utilisées :", nrow(inter_emploi), "intersections district × cellule\n")

# ── Ventilation des colonnes RPHC5 vers les 11 secteurs du modèle ──────────────
emploi_zone_secteur <- matrix(
  0, nrow = n_warehouses, ncol = N_SECTEURS,
  dimnames = list(noeuds_entreposage$warehouse_name, SECTEURS)
)
for (col_csv in intersect(cols_attendues, colnames(emploi_node_cols))) {
  corresp <- RPHC5_CORRESPONDANCE_SECTEURS[[col_csv]]
  for (secteur_m in names(corresp)) {
    if (secteur_m %in% SECTEURS) {
      emploi_zone_secteur[, secteur_m] <-
        emploi_zone_secteur[, secteur_m] + emploi_node_cols[, col_csv] * corresp[[secteur_m]]
    }
  }
}

# Plus de distinction N vs n_warehouses : la matrice « all » = matrice par nœud.
emploi_zone_secteur_all <- emploi_zone_secteur

# Alerte si un nœud a un emploi total nul.
zones_nulles <- which(rowSums(emploi_zone_secteur) == 0)
if (length(zones_nulles) > 0) {
  warning(length(zones_nulles), " nœud(s) avec emploi nul après RPHC5 : ",
          paste(noeuds_entreposage$warehouse_name[zones_nulles], collapse = ", "))
}

# ── Diagnostic et stockage DuckDB ─────────────────────────────────────────────
emploi_total_par_entrepot <- rowSums(emploi_zone_secteur)
diag_emploi <- tibble(
  nom_zone     = noeuds_entreposage$warehouse_name,
  type_zone    = noeuds_entreposage$warehouse_type,
  emploi_total = emploi_total_par_entrepot
)
duck_write(diag_emploi, "diag_emploi")

cat("  Emploi total par nœud : min =", round(min(emploi_total_par_entrepot)),
    "| max =", round(max(emploi_total_par_entrepot)), "\n")
cat("✓ Partie IV.4.F terminée — emploi_zone_secteur [",
    n_warehouses, "×", N_SECTEURS, "]\n\n")

################################################################################
# TRANSITION IV.5 → V — EXTRACTION DE POP_I ET EMPLOI_ZONE_SECTEUR
#
# Dans le modèle MRIO (formules complètes dans 03_transport.R VII.2.B) :
#   - pop_i   : population par zone — entre dans la demande finale de façon
#               multiplicative avec le RWI :
#               d_finale[i,s] ∝ pop_i[i] × (p_rwi[i] + EPSILON_RWI)
#   - emploi_zone_secteur : effectifs par zone × secteur — entre dans la
#               production via un poids composite emploi + RWI :
#               x[i,s] = production_totale[s] × (α × emploi[i,s]/emploi_nat[s]
#                        + (1-α) × p_rwi[i]/Σ p_rwi)
################################################################################

# ── Population par zone active (noeuds_entreposage) ───────────────────────────
pop_i <- diag_population$population_zone[
  match(noeuds_entreposage$warehouse_name, diag_population$nom_zone)
]
pop_i <- replace_na(pop_i, median(pop_i, na.rm = TRUE))

stopifnot(length(pop_i) == n_warehouses)

# ── Emploi sectoriel absolu pour noeuds_entreposage ───────────────────────────
# emploi_zone_secteur (n_warehouses × N_SECTEURS) est déjà calculé par nœud
# (Partie IV.4.F). On se contente de garantir l'ordre des lignes = ordre des
# warehouse_id (le match est une simple ré-indexation de sécurité).
emploi_zone_secteur <- emploi_zone_secteur_all[
  match(noeuds_entreposage$warehouse_name, rownames(emploi_zone_secteur_all)),
  , drop = FALSE
]

stopifnot(nrow(emploi_zone_secteur) == n_warehouses)

cat("✓ pop_i et emploi_zone_secteur extraits pour", n_warehouses, "zones\n")
cat("  Emploi total   : min =",
    round(min(rowSums(emploi_zone_secteur))),
    "| max =", round(max(rowSums(emploi_zone_secteur))), "\n")
cat("  Population     : min =",
    round(min(pop_i)), "| max =", round(max(pop_i)), "\n\n")

# ── Indices des nœuds-entrepôts dans le graphe igraph ─────────────────────────
# warehouse_nodes_base est un vecteur d'entiers : chaque valeur est la position
# (l'indice) d'un nœud-entrepôt dans le graphe igraph sous-jacent à reseau.
# Il est distinct de noeuds_entreposage (qui est un tibble avec noms et types) :
# ici on ne stocke que les numéros de lignes, ce dont igraph::distances() a besoin
# pour lancer Dijkstra depuis les bons points de départ.
# Exemple : warehouse_nodes_base = c(42, 187, 503, ...) signifie que les nœuds
# n°42, 187, 503… du graphe sont des entrepôts.
# Ce vecteur est calculé ici car is_warehouse est défini en IV.3 et reseau
# est complet. Il est ensuite transmis à 02_couts.R et 03_transport.R via
# persist_entreposages.rds.
warehouse_nodes_base <- which(
  igraph::V(reseau %>% as_tbl_graph())$is_warehouse
)
cat("✓ warehouse_nodes_base :", length(warehouse_nodes_base),
    "nœuds-entrepôts indexés\n\n")

# ==============================================================================
# SAUVEGARDE INTER-SCRIPTS
# ==============================================================================

cat("=== Sauvegarde des objets persistants (01_reseau) ===\n")

saveRDS(
  list(
    pays_boundary     = pays_boundary,
    pays_national     = pays_national,
    pays_provinces    = pays_provinces,
    lacs_raw            = lacs_raw,
    lacs_ok             = lacs_ok,
    parcs_raw           = if (exists("parcs_raw")) parcs_raw else NULL,
    parcs_ok            = parcs_ok,
    bbox_carto          = bbox_carto,
    villes_osm          = villes_osm,
    zones_urbaines      = zones_urbaines,      
    zones_industrielles = zones_industrielles
  ),
  PERSIST_GEODATA
)

saveRDS(
  list(
    reseau      = reseau,
    routes      = routes,
    n_aretes_physiques = igraph::ecount(reseau %>% as_tbl_graph()),
    date_creation      = Sys.time()
  ),
  PERSIST_RESEAU_BASE
)

saveRDS(
  list(
    entreposages_fictifs          = entreposages_fictifs,
    entreposages_sf               = entreposages_sf,
    # zones_voronoi : pavage du pays (un polygone par nœud-entrepôt). Chaque
    # cellule = l'espace rattaché à l'entrepôt le plus proche, dont il hérite des
    # caractéristiques (population, RWI, emploi). Remplace les anciens buffers.
    zones_voronoi                 = zones_voronoi,
    entreposages_avec_snap        = entreposages_avec_snap,
    noeuds_entreposage            = noeuds_entreposage,
    n_warehouses                  = n_warehouses,
    warehouse_nodes_base          = warehouse_nodes_base,
    # Emploi sectoriel absolu pour le modèle MRIO
    # emploi_zone_secteur[i,s] : effectifs par nœud i × secteur s (n_warehouses × N_SECTEURS)
    # emploi_zone_secteur_all  : identique (plus de distinction N vs n_warehouses)
    # pop_i : population par nœud (vecteur n_warehouses), pour la demande finale MRIO
    emploi_zone_secteur           = emploi_zone_secteur,
    emploi_zone_secteur_all       = emploi_zone_secteur_all,
    pop_i                         = pop_i,
    # Données démographiques
    diag_population               = diag_population,
    diag_rwi                      = diag_rwi,
    diag_emploi                   = if (exists("diag_emploi")) diag_emploi else NULL,
    rwi_ok                        = rwi_ok,
    rwi_sf                        = if (rwi_ok) rwi_sf else NULL,
    zone_to_prov_placeholder      = NULL
  ),
  PERSIST_ENTREPOSAGES
)

# Création de la closure fond_carte (embarque les données géo)
fond_carte <- local({
  .prov    <- pays_provinces
  .nat     <- pays_national
  .bbox    <- bbox_carto
  .lacs_ok <- lacs_ok
  .lacs    <- if (lacs_ok) lacs_raw  else NULL
  .parc_ok <- parcs_ok
  .parcs   <- if (parcs_ok) parcs_raw else NULL
  
  function() {
    carte <- tm_shape(.prov, bbox = .bbox) +
      tm_polygons(fill = "#F5F5F0", col = "#AAAAAA", lwd = 0.8,
                  fill.legend = tm_legend(show = FALSE)) +
      tm_shape(.nat) +
      tm_borders(col = "#222222", lwd = 2.5)
    
    if (.parc_ok && !is.null(.parcs)) carte <- carte +
        tm_shape(.parcs) +
        tm_polygons(fill = "#A8D5A2", col = "#5A9E52", lwd = 1.2,
                    fill_alpha = 0.45, fill.legend = tm_legend(show = FALSE))
    
    if (.lacs_ok && !is.null(.lacs)) carte <- carte +
        tm_shape(.lacs) +
        tm_polygons(fill = "#A8C8E8", col = "#7AAAC8", lwd = 0.5,
                    fill.legend = tm_legend(show = FALSE))
    carte
  }
})

saveRDS(fond_carte, file.path(DIR_CARTES, "persist_fond_carte.rds"))
cat("✓ persist_fond_carte.rds sauvegardé\n")
cat("✓ persist_geodata.rds\n")
cat("✓ persist_reseau_base.rds\n")
cat("✓ persist_entreposages.rds\n\n")

# Libération des gros objets intermédiaires.
# Ces objets peuvent représenter plusieurs centaines de Mo en RAM et ne sont
# plus nécessaires pour les scripts suivants, qui rechargent depuis les .rds.
objets_a_liberer <- c(
  "raster_worldpop",       # Raster WorldPop (~150 Mo)
  "population_osm_raw",    # Points OSM de population
  "routes",         # Arêtes brutes avant nettoyage
  "aretes_reseau_sf",      # Arêtes sf intermédiaires
  "pop_temp", "voisins",  # Population temporaire + voisinages de la fusion
  "pop_worldpop_par_entrepot", "pop_nisr_par_entrepot", "pop_osm_par_entrepot",
  "pays_districts_gadm", "nisr_pop_raw"
)
rm(list = intersect(objets_a_liberer, ls()))
invisible(gc(verbose = FALSE))
invisible(gc(verbose = FALSE))

cat("Lancer 02_couts.R pour la suite.\n")