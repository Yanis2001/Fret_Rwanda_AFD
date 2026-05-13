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
# PROJET : Réseau Routier pour Modélisation du Commerce de Fret - Rwanda
# AUTEUR  : Yanis
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
  object    = MINIO_PBF_PATH,                      # Chemin dans le bucket S3
  bucket    = MINIO_BUCKET,                        # Nom du bucket MinIO
  file      = chemin_pbf,                          # Nom du fichier local après téléchargement
  region    = "",                                  # Région vide pour MinIO (non AWS standard)
  use_https = TRUE,
  base_url  = MINIO_BASE_URL                       # Point d'accès MinIO SSP Cloud
)

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

################################################################################
# DIAGNOSTIC — EXPLORATION DES TAGS OSM DISPONIBLES DANS LE PBF
# À insérer après le chargement du PBF (Section II.1)
# Objectif : inventaire complet des tags directs ET des clés cachées dans
# other_tags, sur toutes les couches (lines, points, multipolygons)
################################################################################

cat("==========================================================\n")
cat("  DIAGNOSTIC — TAGS OSM DISPONIBLES DANS LE PBF\n")
cat("==========================================================\n\n")

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

# ── Carte 1 : vérification post-nettoyage ─────────────────────────────────────
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
  # clip = "locations" : on ne récupère les données que dans l'emprise fournie
  dem_rwanda <- get_elev_raster(emprise_sf, z = DEM_ZOOM, clip = "locations")
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
  dem_rwanda <<- rast(ext_utm, resolution = DEM_FICTIF_RESOLUTION_M, crs = "EPSG:32735")
  
  set.seed(123)   # Graine pour reproductibilité du bruit aléatoire
  n_cells    <- ncell(dem_rwanda)  # Nombre total de pixels dans le raster
  
  # xFromCell() retourne la coordonnée X (longitude UTM) du centre de chaque cellule
  x_coords <- xFromCell(dem_rwanda, 1:n_cells)
  
  # Gradient d'élévation : 1 500m à l'Est → 2 300m à l'Ouest
  # La formule normalise x_coords entre 0 (Est) et 1 (Ouest) puis multiplie par 800m
  # Cette formule simule la dorsale Congo-Nil qui traverse le Rwanda du Nord au Sud.
  base_elevation <- DEM_FICTIF_ALT_EST + (max(x_coords) - x_coords) /
    (max(x_coords) - min(x_coords)) * (DEM_FICTIF_ALT_OUEST - DEM_FICTIF_ALT_EST)
  
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
# ── MISE EN CACHE ─────────────────────────────────────────────────────────────
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
  
  # ── Homogénéisation des types de géométrie ────────────────────────────────────
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
    # ── Étape 3 : snapping ciblé post-topologie ─────────────────────────────────
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
                    round(nrow(aretes_perdues) / nrow(aretes_lisse) * 100, 1),
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
    filter(longueur_m_brute > SEUIL_LONGUEUR_ARETE_M) %>%         # Seuil 0.5m
    select(-longueur_m_brute)                  # Colonne temporaire, on la retire
  
  n_apres_filtre <- igraph::ecount(reseau_rwanda)
  cat("Arêtes dégénérées supprimées :", n_avant_filtre - n_apres_filtre,
      "(", round((n_avant_filtre - n_apres_filtre)/n_avant_filtre*100, 1), "% du réseau)\n")
  cat("Arêtes conservées            :", n_apres_filtre, "\n\n")
  
  cat("✓ Réseau corrigé —",
      igraph::vcount(reseau_rwanda), "nœuds,",
      igraph::ecount(reseau_rwanda), "arêtes\n\n")
  
  
  # ── Diagnostic complet de la fragmentation ────────────────────────────────────
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


# ── Vérifications communes (toujours exécutées, qu'on ait un cache ou non) ────
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
      dem_rwanda,
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

# ── Entrepôts depuis city/town OSM ────────────────────────────────────────────
# Filtrer uniquement les villes dans le territoire rwandais
# Évite que les villes des pays voisins se snappent toutes sur les mêmes nœuds frontières
# st_filter() : ne garde que les géométries qui intersectent le polygone donné.
# st_buffer(dist = BUFFER_FRONTIERE_VILLES_M) : élargit la frontière de 5km pour inclure les villes
# rwandaises situées exactement sur la frontière.
villes_osm <- villes_osm %>%
  st_filter(rwanda_national %>% st_buffer(dist = BUFFER_FRONTIERE_VILLES_M))
# Buffer de 5km pour garder les villes très proches de la frontière
cat("  Villes OSM dans ou proches du Rwanda :", nrow(villes_osm), "\n")

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
  st_buffer(dist = BUFFER_ENTREPOT_M)

# ── Accrochage (snapping) des entrepôts au réseau ─────────────────────────────
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

# ── Définition de noeuds_entreposage ──────────────────────────────────────────
# noeuds_entreposage est la liste des nœuds du réseau identifiés comme entrepôts,
# après le snapping et la déduplication par nœud (cf. entreposages_avec_snap).
#
# IMPORTANT : ne pas confondre les deux entités manipulées dans ce script :
#   • entreposages_fictifs / entreposages_sf : 123 zones économiques modélisées
#   • noeuds_entreposage                     : 120 nœuds du graphe (après dédup.)
# Les enrichissements (population, RWI) portent sur les 123 zones.
# Les calculs sur le graphe (Dijkstra, OD, modèle gravitaire) portent sur les 120 nœuds.
noeuds_entreposage <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse) %>%
  as_tibble() %>%
  mutate(warehouse_id = row_number())

n_warehouses <- nrow(noeuds_entreposage)

cat("✓ noeuds_entreposage défini :", n_warehouses, "nœuds-entrepôts\n")
cat("  (à comparer avec", nrow(entreposages_fictifs), "zones économiques)\n\n")

cat("✓", nrow(entreposages_avec_snap), "entreposages intégrés au réseau\n\n")

# ── Diagnostic : entrepôts snappés sur le même nœud ───────────────────────────
# On identifie les nœuds partagés par plusieurs entrepôts AVANT déduplication
# pour comprendre si les fusions concernent des zones de même type ou non.
# Un nœud partagé par des types différents (ex : "ville" + "industrie") signale
# une fusion potentiellement problématique pour le modèle gravitaire.

# Calcul du nombre d'entrepôts par nœud (avant distinct())
# On repart de entreposages_sf avant la déduplication par noeud_proche_id
doublons_noeuds <- entreposages_sf %>%
  mutate(
    noeud_proche_id = st_nearest_feature(geometry, noeuds_reseau)
  ) %>%
  st_drop_geometry() %>%
  group_by(noeud_proche_id) %>%
  # On ne garde que les nœuds avec au moins 2 entrepôts
  filter(n() > 1) %>%
  summarise(
    n_entrepots      = n(),
    noms             = paste(nom,  collapse = " | "),
    types            = paste(type, collapse = " | "),
    sources          = paste(source, collapse = " | "),
    # TRUE si tous les entrepôts sur ce nœud sont du même type
    meme_type        = n_distinct(type) == 1,
    .groups          = "drop"
  ) %>%
  arrange(desc(n_entrepots))

cat("=== Diagnostic des entrepôts sur le même nœud ===\n\n")
cat("Nœuds partagés :", nrow(doublons_noeuds), "\n")
cat("dont même type :", sum(doublons_noeuds$meme_type), "\n")
cat("dont types mixtes :", sum(!doublons_noeuds$meme_type), "\n\n")

if (nrow(doublons_noeuds) > 0) {
  cat("Détail des nœuds partagés :\n")
  print(
    doublons_noeuds %>%
      select(noeud_proche_id, n_entrepots, types, meme_type, noms) %>%
      rename(
        Noeud       = noeud_proche_id,
        N           = n_entrepots,
        Types       = types,
        MemeType    = meme_type,
        Zones       = noms
      )
  )
}
cat("\n")
# Alerte si des fusions de types différents sont détectées
if (any(!doublons_noeuds$meme_type)) {
  cat("⚠ Fusions de types différents détectées — la population sera\n")
  cat("  recalculée sur l'union des buffers pour ces nœuds (voir IV.4.B)\n\n")
} else {
  cat("✓ Toutes les fusions concernent des zones de même type\n\n")
}

################################################################################
# PARTIE IV.4 — ENRICHISSEMENT DÉMOGRAPHIQUE DES NŒUDS D'ENTREPÔT
#
# OBJECTIF : Associer à chaque zone d'entrepôt un indicateur de population
#            afin d'améliorer le calibrage du modèle gravitaire (Partie VII).
#            Un hub desservant 800 000 habitants génère plus de demande
#            qu'une petite ville de 20 000 habitants, indépendamment de son
#            type de zone (hub, marché, SEZ…).
#
# TROIS APPROCHES SONT PROPOSÉES :
#   A — Tags OSM du fichier PBF (rapide, intégré, mais couverture partielle)
#   B — Raster WorldPop (haute résolution spatiale, sans requête externe)
#   C — Données de recensement NISR via CSV (source officielle, la plus fiable)
#
# STRATÉGIE DE FUSION :
#   On calcule une colonne "population_zone" finale en appliquant une
#   hiérarchie de priorité : B (WorldPop) > C (NISR) > A (OSM) > 0
#   La colonne est ensuite intégrée dans reseau_rwanda (attribut de nœud)
#   et dans DuckDB pour être accessible aux requêtes SQL des Parties V à IX.
#
# PLACEMENT DANS LE SCRIPT :
#   Ce bloc dépend de :
#     - entreposages_avec_snap    (Partie IV.3) — liste des entrepôts snappés
#     - entreposages_sf           (Partie IV.3) — géométries sf des entrepôts
#     - reseau_rwanda             (Partie III)  — réseau sfnetworks
#     - chemin_pbf                (Partie I)    — fichier PBF OSM
#   Les Parties V à IX peuvent utiliser la colonne "population_zone"
#   comme variable de pondération dans le modèle gravitaire.
################################################################################

cat("==========================================================\n")
cat("  PARTIE IV.4 — ENRICHISSEMENT DÉMOGRAPHIQUE\n")
cat("==========================================================\n\n")

# ==============================================================================
# IV.4.A : Extraction des tags de population depuis le fichier PBF
#
# Dans OpenStreetMap, certains noeuds de type "place" possèdent un tag
# "population" indiquant le nombre d'habitants. Ce tag est maintenu par la
# communauté OSM et couvre les grandes villes mais rarement les petites zones.
#
# AVANTAGES  : Aucun fichier externe, déjà dans le PBF téléchargé.
# INCONVÉNIENTS : Couverture très partielle (< 30% des zones au Rwanda),
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

# ── Association à chaque entrepôt : point OSM le plus proche ──────────────────
# Pour chaque entrepôt, on cherche le point OSM peuplé dans un rayon de
# BUFFER_DEMO_M mètres. S'il y en a plusieurs, on prend le plus proche.
# S'il n'y en a aucun, la population OSM reste NA (sera complétée par B ou C).

if (nrow(population_osm_raw) > 0) {
  
  # st_join() avec st_nearest_feature = FALSE, on préfère st_is_within_distance
  # pour contrôler explicitement la distance maximale d'association.
  # st_is_within_distance() : matrice booléenne entrepôts × lieux OSM peuplés.
  # lengths() > 0 : TRUE si au moins un lieu OSM est dans le buffer.
  within_buffer_A <- st_is_within_distance(
    entreposages_sf,
    population_osm_raw,
    dist = BUFFER_DEMO_M
  )
  
  # Pour chaque entrepôt, on identifie le lieu OSM le plus proche dans le buffer.
  # st_nearest_feature() renvoie un indice (le plus proche parmi les candidats).
  pop_osm_par_entrepot <- sapply(seq_len(nrow(entreposages_sf)), function(i) {
    
    candidats <- within_buffer_A[[i]]  # Indices des lieux OSM dans le buffer
    
    if (length(candidats) == 0) return(NA_real_)  # Aucun lieu OSM à proximité
    
    if (length(candidats) == 1) {
      # Un seul candidat : on prend directement sa population
      return(population_osm_raw$pop_osm_brute[candidats])
    }
    
    # Plusieurs candidats : on prend le plus proche géographiquement.
    # distances() calcule la distance entre l'entrepôt i et chacun des candidats.
    dists_candidats <- as.numeric(
      st_distance(entreposages_sf[i,], population_osm_raw[candidats,])
    )
    # which.min() renvoie l'indice du candidat le plus proche.
    idx_plus_proche <- candidats[which.min(dists_candidats)]
    return(population_osm_raw$pop_osm_brute[idx_plus_proche])
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
# Pour le Rwanda, les données sont disponibles pour 2020 (100m par pixel).
#
# AVANTAGES  : Haute résolution spatiale (100m), couvre tout le territoire,
#              bien calibré sur les données NISR rwandaises.
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
    # Vérification : le raster doit avoir au moins un pixel non-NA sur le Rwanda
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
# URL directe WorldPop pour le Rwanda 2020 (non constrainted, 100m).
# Pour d'autres années ou résolutions, consulter :
# https://hub.worldpop.org/geodata/listing?id=29
# ── REMPLACER le bloc de téléchargement WorldPop ──────────────────────────────
if (!worldpop_ok) {
  
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
    cat("    → Chercher : Rwanda > Population > 2020 > 100m\n")
    cat("    → Sauvegarder sous :", WORLDPOP_LOCAL_PATH, "\n\n")
  }
}

# ── Agrégation du raster dans un buffer autour de chaque entrepôt ─────────────
# Pour chaque entrepôt, on somme les pixels WorldPop dans un cercle de
# BUFFER_DEMO_M mètres. Chaque pixel représente le nombre d'habitants vivant
# dans cette cellule de 100m × 100m.
if (worldpop_ok) {
  
  cat("  Agrégation WorldPop sur les buffers de",
      BUFFER_DEMO_M / 1000, "km...\n")
  
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
    
    # exact_extract avec fun="sum" retourne directement un vecteur numérique.
    # On remplace les éventuels NA par 0 (zones sans données WorldPop).
    pop_worldpop_par_entrepot <- as.numeric(resultats_wp)
    
    cat("  ✓ Population WorldPop calculée pour",
        sum(pop_worldpop_par_entrepot > 0), "/", nrow(entreposages_sf),
        "entrepôts\n")
    cat("  Pop. WorldPop min :",
        round(min(pop_worldpop_par_entrepot[pop_worldpop_par_entrepot > 0])),
        "| max :", round(max(pop_worldpop_par_entrepot)), "\n\n")
    
  }, error = function(e) {
    cat("  ⚠ Agrégation WorldPop échouée :", conditionMessage(e), "\n")
    cat("  → Approche B ignorée\n\n")
    pop_worldpop_par_entrepot <<- rep(NA_real_, nrow(entreposages_sf))
  })
}


# ==============================================================================
# IV.4.C : Données de recensement NISR (source officielle, recommandée)
#
# L'Institut National de Statistiques du Rwanda (NISR) publie les résultats
# du recensement RPHC-5 (2022) par district. Les données sont disponibles sur HDX 
# (Humanitarian Data Exchange)
#
# PROCÉDURE DE TÉLÉCHARGEMENT :
#   1. Aller sur  https://data.humdata.org/dataset/cod-ps-rwa
#   2. Chercher "rwa_admpop_adm2_2023.csv" (niveau district)
#   3. Télécharger le CSV (bouton "Download")
#   4. Placer le fichier dans data/raw/rwa_admpop_adm2_2023.csv
#
# Le fichier contient ~30 districts rwandais avec population par sexe.
# On fait une jointure spatiale : chaque entrepôt est associé au district
# dans lequel il se trouve, puis on récupère la population de ce district.
#
# INCONVÉNIENTS : Résolution district uniquement (pas de granularité plus fine),
#                 nécessite un téléchargement manuel.
# ==============================================================================

cat("── Approche C : recensement NISR 2022 (par district) ────────────────\n")

pop_nisr_par_entrepot <- rep(NA_real_, nrow(entreposages_sf))

# ── Téléchargement du CSV NISR depuis MinIO si pas déjà présent ───────────────
if (!file.exists(NISR_CSV_PATH)) {
  dir.create(dirname(NISR_CSV_PATH), showWarnings = FALSE, recursive = TRUE)
  save_object(
    object    = "data/raw/rwa_admpop_adm2_2023.csv",
    bucket    = MINIO_BUCKET,
    file      = NISR_CSV_PATH,
    region    = "",
    use_https = TRUE,
    base_url  = MINIO_BASE_URL
  )
  cat("  ✓ CSV NISR téléchargé depuis MinIO\n")
}

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
    # geodata::gadm() télécharge le niveau 2 (districts) pour le Rwanda.
    # level = 2 : provinces = 1, districts = 2, secteurs = 3.
    cat("  Téléchargement des frontières de districts GADM...\n")
    
    rwanda_districts_gadm <- tryCatch({
      
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
    
    if (!is.null(rwanda_districts_gadm)) {
      
      # ── Jointure GADM × NISR ────────────────────────────────────────────────
      # On fusionne le tableau de population NISR avec les polygones GADM
      # via le nom de district normalisé.
      # left_join() conserve tous les polygones GADM même sans correspondance NISR.
      districts_avec_pop <- rwanda_districts_gadm %>%
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
# IV.4.D : Fusion des trois sources et intégration dans le modèle
#
# On assemble maintenant les trois vecteurs de population (A, B, C)
# en une seule colonne "population_zone" par entrepôt selon la hiérarchie :
#
#   Priorité 1 : WorldPop (B)        — disponible et non-NA  → utiliser
#   Priorité 2 : NISR officiel (C)   — si C absent ou NA     → utiliser
#   Priorité 3 : OSM (A)             — si B absent ou NA     → utiliser
#   Priorité 0 : Population minimale — si tout est NA        → 1 000 hab (évite les divisions par zéro)
#
# La population finale est intégrée :
#   - dans reseau_rwanda (attribut de nœud sfnetworks)
#   - dans DuckDB (table "population_entrepots")
#   - dans entreposages_fictifs (data.frame de référence)
# ==============================================================================

cat("── Fusion et intégration des données de population ──────────────────\n")

# ── Remise à l'état original d'entreposages_fictifs ───────────────────────────
# Si le bloc IV.4 est re-exécuté, entreposages_fictifs peut avoir été gonflé
# par des left_join() d'une exécution précédente. On le remet à ses 123
# colonnes d'origine avant d'y ajouter les nouvelles variables.
entreposages_fictifs <- entreposages_fictifs %>%
  select(nom, type, pays, lon, lat, source) %>%
  distinct(lon, lat, .keep_all = TRUE)

# coalesce() : prend le premier argument non-NA, de gauche à droite.
# C'est l'opérateur de "hiérarchie de sources" en une seule fonction.
population_zone_finale <- coalesce(
  replace_na(pop_worldpop_par_entrepot, NA_real_),  # Source B : WorldPop
  replace_na(pop_nisr_par_entrepot,     NA_real_),  # Source C : NISR 
  replace_na(pop_osm_par_entrepot,      NA_real_),  # Source A : OSM
  rep(1000, nrow(entreposages_sf))                  # Fallback : 1 000 hab. minimum
) %>%
  round()   # Les populations sont des entiers

# ── Tableau de synthèse des sources utilisées ─────────────────────────────────
# Ce diagnostic permet de vérifier la qualité du remplissage et d'identifier
# les zones pour lesquelles on a dû utiliser le fallback.
source_utilisee <- case_when(
  !is.na(pop_worldpop_par_entrepot) ~ "WorldPop_2020",
  !is.na(pop_nisr_par_entrepot)     ~ "NISR_2022",
  !is.na(pop_osm_par_entrepot)      ~ "OSM",
  TRUE                              ~ "Fallback_1000"
)

# Vérification de cohérence avant construction du tableau.
# Les trois vecteurs de population doivent avoir exactement autant de lignes
# que entreposages_sf (la référence des 123 zones économiques).
stopifnot(
  length(pop_osm_par_entrepot)      == nrow(entreposages_sf),
  length(pop_worldpop_par_entrepot) == nrow(entreposages_sf),
  length(pop_nisr_par_entrepot)     == nrow(entreposages_sf)
)

diag_population <- tibble(
  nom_zone        = entreposages_fictifs$nom,
  type_zone       = entreposages_fictifs$type,
  pop_osm         = round(pop_osm_par_entrepot),
  pop_worldpop    = round(pop_worldpop_par_entrepot),
  pop_nisr        = round(pop_nisr_par_entrepot),
  population_zone = population_zone_finale,
  source          = source_utilisee
)

cat("\nDiagnostic des sources de population :\n")
print(
  diag_population %>%
    count(source) %>%
    mutate(pct = round(n / sum(n) * 100, 1)) %>%
    rename(Source = source, N_zones = n, `Part (%)` = pct)
)

cat("\nPopulation par zone (top 10 par population) :\n")
print(
  diag_population %>%
    arrange(desc(population_zone)) %>%
    slice_head(n = 10) %>%
    select(nom_zone, type_zone, population_zone, source) %>%
    rename(Zone = nom_zone, Type = type_zone,
           Population = population_zone, Source = source)
)

# ── Stockage dans DuckDB ──────────────────────────────────────────────────────
# On crée une table dédiée "population_entrepots" dans DuckDB pour pouvoir
# l'utiliser dans toutes les requêtes SQL des Parties V à IX.
# Exemple d'utilisation en SQL :
#   SELECT m.*, p.population_zone
#   FROM matrice_od m
#   JOIN population_entrepots p ON m.nom_origine = p.nom_zone
duck_write(
  diag_population %>%
    select(nom_zone, type_zone, population_zone, source),
  "population_entrepots"
)

# ── Intégration dans reseau_rwanda (attribut de nœud) ─────────────────────────
# On ajoute la population comme attribut des nœuds d'entrepôt dans le réseau sf.
# Les nœuds non-entrepôt reçoivent NA (ils ne sont pas des zones économiques).
# match() : pour chaque nœud, cherche si son warehouse_name est dans notre table.
reseau_rwanda <- reseau_rwanda %>%
  activate("nodes") %>%
  mutate(
    population_zone = diag_population$population_zone[
      match(warehouse_name,
            diag_population$nom_zone)
    ]
    # Pour les nœuds non-entrepôt, match() retourne NA → population_zone = NA.
    # C'est le comportement voulu : seuls les entrepôts ont une population.
  )

# ── Intégration dans entreposages_fictifs ─────────────────────────────────────
# On enrichit aussi le data.frame de référence (utilisé en Partie IV.3 et VII).
stopifnot(nrow(entreposages_fictifs) == nrow(diag_population))

entreposages_fictifs <- entreposages_fictifs %>%
  select(-any_of(c("population_zone", "source_population"))) %>%   # idempotence
  bind_cols(
    diag_population %>% 
      select(population_zone, source_population = source)
  )

# Mise à jour de la table DuckDB zones_entreposage avec la population
duck_write(entreposages_fictifs, "zones_entreposage")

cat("\n✓ Population intégrée dans reseau_rwanda et DuckDB\n")
cat("  Nœuds avec population_zone > 0 :",
    sum(!is.na(igraph::V(reseau_rwanda %>% as_tbl_graph())$population_zone),
        na.rm = TRUE), "\n\n")


# ==============================================================================
# IV.4.E : Visualisation démographique (carte + graphique)
# ==============================================================================

cat("── Visualisation de la distribution démographique ───────────────────\n")

# ── Carte : population par zone sur le réseau ─────────────────────────────────
# Les entrepôts sont affichés comme des cercles dont le diamètre est
# proportionnel à la population (échelle log pour gérer les ordres de grandeur).
# Kigali (~1 M d'habitants) ne doit pas écraser visuellement les petites villes.

entrepots_pop_sf <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse, !is.na(population_zone)) %>%
  st_as_sf() 

carte_population <- fond_carte() +
  
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.4) +
  
  tm_shape(entrepots_pop_sf) +
  tm_dots(
    fill        = "population_zone",
    fill.scale  = tm_scale_intervals(
      style  = "quantile",
      n      = 5,
      values = "brewer.yl_or_rd"
    ),
    fill.legend = tm_legend(title = "Population\n(habitants)"),
    size        = 0.3
  ) +
  
  tm_title("Distribution démographique des zones d'entrepôt\nSources : NISR 2022 / WorldPop 2020 / OSM") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_population,
  file.path(DIR_OUTPUT, "carte_population_zones.png"),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ carte_population_zones.png\n")

# ── Identification de la zone de référence (max population = future réf. demande) ──
zone_ref_pop_approx <- diag_population %>%
  arrange(desc(population_zone)) %>%
  slice(1) %>%
  pull(nom_zone)

# Etiquette courte pour l'annotation (même troncature que Zone_court dans le plot)
zone_ref_pop_label <- str_trunc(str_remove(zone_ref_pop_approx, " - .*"), 25)

# ── Graphique : population par zone et par type ───────────────────────────────
g_pop <- diag_population %>%
  arrange(desc(population_zone)) %>%
  mutate(
    Zone_court = str_trunc(str_remove(nom_zone, " - .*"), 25),
    Zone_court = make.unique(Zone_court, sep = " #"),   # rend les labels uniques
    Zone_court = factor(Zone_court, levels = rev(Zone_court)),
    est_reference   = (nom_zone == zone_ref_pop_approx)
  ) %>%
  ggplot(aes(x = Zone_court, y = population_zone / 1000,
             fill = type_zone, alpha = source)) +
  geom_col(width = 0.75) +
  # Surlignage de la barre de référence par un contour rouge épais
  geom_col(
    data = ~ filter(., est_reference),
    aes(x = Zone_court, y = population_zone / 1000),
    fill  = NA,
    color = "#CC0000",
    linewidth = 1.2,
    width = 0.75,
    inherit.aes = FALSE
  ) +
  # Annotation textuelle positionnée à droite de la barre de référence
  annotate(
    "text",
    x     = zone_ref_pop_label,
    y     = max(diag_population$population_zone / 1000, na.rm = TRUE) * 0.55,
    label = "◄ Référence demande",
    hjust = 0,
    color = "#CC0000",
    size  = 3.2,
    fontface = "bold"
  ) +
  coord_flip() +
  scale_alpha_manual(
    values = c("NISR_2022"     = 1.0,
               "WorldPop_2020" = 0.8,
               "OSM"           = 0.65,
               "Fallback_1000" = 0.35),
    name   = "Source"
  ) +
  scale_fill_manual(values = PALETTE_ZONE_TYPE, name = "Type de zone") +
  scale_y_continuous(labels = scales::label_number(suffix = " k")) +
  labs(
    title    = "Population par zone d'entrepôt",
    subtitle = paste0(
      "Transparence = fiabilité de la source (opaque = NISR officiel)\n",
      "Contour rouge = zone de référence de normalisation (taille_composite_demande = 1)"
    ),
    x = NULL,
    y = "Population (milliers d'habitants)"
  ) +
  theme_minimal(base_size = 11) +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "right"
  )

ggsave(
  file.path(DIR_OUTPUT, "graphique_population_zones.png"),
  g_pop, width = 12, height = 8, dpi = 300
)
cat("  ✓ graphique_population_zones.png\n\n")

cat("✓ Partie IV.4 terminée — population_zone disponible dans :\n")
cat("  • reseau_rwanda  (attribut de nœud)\n")
cat("  • DuckDB         (table population_entrepots)\n")
cat("  • entreposages_fictifs (colonne population_zone)\n\n")

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
#     - reseau_rwanda          (Partie III)
#     - rwanda_boundary        (Partie II.3)
#     - duck_write()           (Partie I.2)
#   Alimente :
#     - Transition IV.5 → V : variable p_rwi dans le calcul de taille_composite
#     - reseau_rwanda (attribut de nœud : rwi_moyen, p_rwi)
#     - DuckDB (table richesse_entrepots)
################################################################################

cat("==========================================================\n")
cat("  PARTIE IV.5 — INDICE DE RICHESSE RELATIVE (RWI)\n")
cat("==========================================================\n\n")

# ==============================================================================
# IV.5.1 : Téléchargement et préparation des données RWI
#
# Le fichier CSV Rwanda contient une ligne par cellule de ~2,4 km² avec :
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
# Si le CSV Rwanda a déjà été extrait lors d'une session précédente, on
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
# uniquement le fichier Rwanda, et on supprime le ZIP pour libérer l'espace.
if (!rwi_ok) {
  
  cat("  Téléchargement du ZIP RWI (~35 Mo)...\n")
  cat("  Source :", RWI_ZIP_URL, "\n")
  
  tryCatch({
    
    # download.file() : télécharge un fichier depuis une URL.
    # mode = "wb" (write binary) est indispensable pour les archives ZIP.
    # quiet = FALSE : afficher la progression du téléchargement.
    download.file(RWI_ZIP_URL, destfile = RWI_ZIP_LOCAL,
                  mode = "wb", quiet = FALSE)
    
    # Liste des fichiers dans le ZIP pour vérifier que Rwanda est présent.
    # unzip(list = TRUE) ne décompresse pas — il liste uniquement le contenu.
    contenu_zip <- unzip(RWI_ZIP_LOCAL, list = TRUE)
    cat("  Fichiers dans le ZIP :", nrow(contenu_zip), "\n")
    
    # Vérification que le fichier Rwanda est dans le ZIP.
    # La présence de majuscules/minuscules peut varier selon la version du ZIP (RWA_ ou rwa_).
    # grepl() + ignore.case = TRUE gère les deux cas.
    idx_rwanda <- grep(
      pattern     = "rwa.*relative.*wealth",
      x           = contenu_zip$Name,
      ignore.case = TRUE
    )
    
    if (length(idx_rwanda) == 0) {
      stop("Fichier Rwanda introuvable dans le ZIP.\n",
           "Fichiers disponibles : ",
           paste(head(contenu_zip$Name, 10), collapse = ", "))
    }
    
    nom_fichier_zip <- contenu_zip$Name[idx_rwanda[1]]
    cat("  Fichier Rwanda dans le ZIP :", nom_fichier_zip, "\n")
    
    # Extraction du seul fichier Rwanda (évite de décompresser 93 pays).
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
    cat("    → Extraire", RWI_FICHIER_RWANDA, "vers", RWI_CSV_LOCAL, "\n")
    cat("    → Partie IV.5 ignorée, le modèle continue sans RWI\n\n")
  })
}

# ── Statistiques descriptives du RWI Rwanda ───────────────────────────────────
if (rwi_ok) {
  
  rwi_stats <- tibble(
    n_cellules   = nrow(rwi_sf),
    rwi_min      = round(min(rwi_sf$rwi),  3),
    rwi_max      = round(max(rwi_sf$rwi),  3),
    rwi_median   = round(median(rwi_sf$rwi), 3),
    rwi_mean     = round(mean(rwi_sf$rwi),  3),
    erreur_moy   = round(mean(rwi_sf$error, na.rm = TRUE), 3)
  )
  
  cat("\n  Distribution du RWI Rwanda :\n")
  cat("  Cellules     :", rwi_stats$n_cellules, "\n")
  cat("  Min / Max    :", rwi_stats$rwi_min, "/", rwi_stats$rwi_max, "\n")
  cat("  Médiane / Moy:", rwi_stats$rwi_median, "/", rwi_stats$rwi_mean, "\n")
  cat("  Erreur moy.  :", rwi_stats$erreur_moy, "\n\n")
  
  # ── Rognage aux limites du Rwanda ───────────────────────────────────────────
  # On s'assure que les cellules RWI sont bien dans le territoire rwandais
  # (le ZIP peut contenir des cellules légèrement hors frontière).
  # st_filter() avec st_intersects : conserve les points dans le polygone.
  rwi_sf <- rwi_sf %>%
    st_filter(rwanda_boundary %>%
                st_buffer(dist = 1000) %>%  # 1km de marge pour les frontières
                st_union())
  
  cat("  Cellules après rognage Rwanda :", nrow(rwi_sf), "\n\n")
}


# ==============================================================================
# IV.5.2 : Calcul du score RWI moyen par entrepôt (IDW dans buffer)
#
# MÉTHODE — ANALOGIE AVEC L'USAGE DES SOLS :
#
#   RWI (IV.5) :
#     Pour chaque buffer d'entrepôt, on calcule la MOYENNE PONDÉRÉE
#     par distance inverse (IDW) des scores RWI des cellules dans le buffer.
#     → scalaire rwi_brut (valeur centrée proche de 0, typiquement [-3, +3])
#     → normalisé en p_rwi ∈ [0, 1] en fin de cette section
#
# POURQUOI IDW ET PAS UNE SIMPLE MOYENNE ?
#   Les cellules RWI les plus proches du centroïde de l'entrepôt sont plus
#   représentatives de son environnement immédiat que celles en périphérie.
#   L'IDW (Inverse Distance Weighting) pondère chaque cellule par 1/d²,
#   ce qui donne plus de poids aux cellules proches sans exclure les autres.
#   C'est la méthode standard en géostatistique pour l'interpolation spatiale.
#   Elle est cohérente avec l'esprit du calcul de landuse (calc_part_landuse),
#   qui tient implicitement compte de la distance via l'intersection des buffers.
# ==============================================================================

cat("── Calcul IDW du RWI par entrepôt ────────────────────────────────────\n")

# ── Mise en cache ─────────────────────────────────────────────────────────────
# Même logique que le cache landuse : invalider si le nombre d'entrepôts change.
CACHE_RWI <- file.path(DIR_OUTPUT, "rwi_cache.rds")
cache_rwi_valide <- FALSE

if (file.exists(CACHE_RWI) && rwi_ok) {
  
  cache_rwi_data <- readRDS(CACHE_RWI)
  n_zones_actuel <- nrow(entreposages_sf)
  
  if (!is.null(cache_rwi_data$n_zones) &&
      cache_rwi_data$n_zones == n_zones_actuel &&
      !is.null(cache_rwi_data$buffer_m) &&
      cache_rwi_data$buffer_m == BUFFER_RWI_M) {
    
    rwi_brut_par_entrepot <- cache_rwi_data$rwi_brut_par_entrepot
    cache_rwi_valide      <- TRUE
    cat("  ✓ Cache RWI valide (", n_zones_actuel, "zones,",
        BUFFER_RWI_M, "m buffer) — calcul IDW ignoré\n\n")
  } else {
    cat("  ⚠ Cache RWI invalide (changement de nombre de zones ou buffer) — recalcul IDW\n")
  }
}

# ── Fonction de calcul IDW : RWI moyen pondéré dans un buffer ─────────────────
#
#   calc_rwi_idw(centroide_geom, rwi_sf, rayon_m, puissance) :
#     → moyenne IDW des scores RWI dans le buffer
#     → scalaire réel (centré sur 0 avant normalisation)
#
# Les deux fonctions :
#   - prennent une géométrie sf représentant la zone d'entrepôt en entrée
#   - retournent un scalaire représentant l'influence de l'environnement
#   - utilisent le même rayon de buffer (BUFFER_ENTREPOT_M)
#
# Paramètres :
#   centroide_geom — géométrie sf du point-centroïde de l'entrepôt (POINT)
#   rwi_sf         — objet sf des cellules RWI (POINT, déjà filtré sur Rwanda)
#   rayon_m        — rayon en mètres du buffer de recherche
#   puissance      — exposant de l'IDW (voir paramètres)

calc_rwi_idw <- function(centroide_geom, rwi_sf, rayon_m, puissance) {
  
  # Encapsulation de la géométrie brute en objet sf complet avec CRS.
  # Sans st_sfc(crs = 32735), st_is_within_distance() ne peut pas comparer
  # les systèmes de coordonnées et lèverait une erreur.
  centre_sf <- st_as_sf(st_sfc(centroide_geom, crs = 32735))
  
  # Identification des cellules RWI dans le buffer circulaire.
  # st_is_within_distance() retourne une liste : l'élément [[1]] donne les
  # indices des cellules de rwi_sf qui se trouvent à ≤ rayon_m du centroïde.
  # C'est l'équivalent de "quelles zones de landuse chevauchent le buffer ?"
  idx_candidats <- st_is_within_distance(
    centre_sf, rwi_sf, dist = rayon_m
  )[[1]]
  
  # Cas sans cellules RWI dans le buffer (zone sans données, frontière…).
  # On retourne NA — ce cas sera géré en IV.5.3 par le fallback médiane.
  if (length(idx_candidats) == 0) return(NA_real_)
  
  # Extraction des cellules candidates et calcul des distances au centroïde.
  cellules_buf <- rwi_sf[idx_candidats, ]
  distances_m  <- as.numeric(
    st_distance(centre_sf, cellules_buf)
  )
  
  # Plafonnement de la distance minimale pour éviter 1/0^2 = Inf.
  # Si une cellule est exactement sur le centroïde (distance = 0), on lui
  # donne une distance fictive de RWI_DISTANCE_MIN_M = 50m.
  # En pratique ce cas est extrêmement rare avec des données grillées à 2,4 km.
  distances_m <- pmax(distances_m, RWI_DISTANCE_MIN_M)
  
  # Calcul des poids IDW : w_i = 1 / d_i^puissance
  # Plus une cellule est proche (d petit), plus son poids est élevé.
  # Avec puissance = 2 :
  #   une cellule à 100m a un poids (1/100²) = 10 000× plus élevé
  #   qu'une cellule à 1 000m (1/1000²).
  poids <- 1 / (distances_m ^ puissance)
  
  # Moyenne pondérée IDW :
  #   rwi_idw = Σ(rwi_i × w_i) / Σ(w_i)
  # C'est la formule standard de Shepard (1968) pour l'interpolation spatiale.
  # Elle garantit que le résultat est dans [min(rwi_i), max(rwi_i)].
  sum(cellules_buf$rwi * poids) / sum(poids)
}

# ── Calcul pour tous les entrepôts ────────────────────────────────────────────
if (!cache_rwi_valide && rwi_ok) {
  
  n_zones <- nrow(entreposages_sf)
  cat("  Calcul IDW pour", n_zones, "zones économiques...\n")
  
  # Initialisation du vecteur de résultats
  rwi_brut_par_entrepot <- numeric(n_zones)
  
  for (i in seq_len(n_zones)) {
    
    # On passe la géométrie brute (pas l'objet sf entier) pour correspondre
    # au contrat de calc_rwi_idw(), comme on le faisait avec calc_part_landuse()
    # dans la boucle de Partie IV.3.
    rwi_brut_par_entrepot[i] <- calc_rwi_idw(
      centroide_geom = entreposages_sf$geometry[i],
      rwi_sf         = rwi_sf,
      rayon_m        = BUFFER_RWI_M,
      puissance      = RWI_IDW_PUISSANCE
    )
    
    if (i %% 10 == 0 || i == n_zones) {
      cat("  IDW RWI :", round(i / n_zones * 100), "%\n")
    }
  }
  
  # ── Sauvegarde du cache ─────────────────────────────────────────────────────
  saveRDS(
    list(
      rwi_brut_par_entrepot = rwi_brut_par_entrepot,
      n_zones               = nrow(entreposages_sf),  
      buffer_m              = BUFFER_RWI_M,
      puissance             = RWI_IDW_PUISSANCE,
      date_creation         = Sys.time()
    ),
    CACHE_RWI
  )
  cat("  ✓ Cache RWI sauvegardé :", CACHE_RWI, "\n\n")
  
} else if (!rwi_ok) {
  
  # Si le téléchargement a échoué, on remplace par la valeur neutre 0
  # (correspond à la richesse médiane du Rwanda — ni riche ni pauvre).
  # Le modèle gravitaire peut tourner sans RWI, juste avec moins de précision.
  cat("  ⚠ RWI indisponible — valeurs neutres (0) utilisées pour tous les entrepôts\n\n")
  rwi_brut_par_entrepot <- rep(0, nrow(entreposages_sf))
}


# ==============================================================================
# IV.5.3 : Normalisation 
#
# Le score IDW brut est centré sur 0 à l'échelle international et peut être négatif (zones pauvres).
# Pour l'utiliser on normalise avec une transformation min-max sur l'ensemble des scores Rwanda.
#
# FORMULE :
#   p_rwi = (rwi_brut - min_entrepôt) / (max_entrepôt - min_entrepôt)
#
# Avec cette normalisation :
#   p_rwi = 0 → zone la plus pauvre de l'échantillon 
#   p_rwi = 1 → zone la plus riche de l'échantillon 
#   p_rwi = 0.5 → niveau médian national
#
# IMPORTANTE PRÉCAUTION — ÉCHELLE RELATIVE :
#   Le RWI est relatif au Rwanda (pas une richesse absolue mondiale).
#   Un p_rwi = 0.9 au Rwanda ne correspond pas à p_rwi = 0.9 en France.
#   Ce score ne dit rien sur la richesse absolue, uniquement sur le
#   positionnement d'une zone dans la distribution nationale.
# ==============================================================================

cat("── Normalisation et intégration des scores RWI ───────────────────────\n")

# ── Imputation des NA (entrepôts sans cellule RWI dans le buffer) ─────────────
# Si un entrepôt n'a aucune cellule RWI dans son buffer (rare : frontière,
# zone très isolée), on lui affecte la médiane nationale comme valeur neutre.
n_na_rwi <- sum(is.na(rwi_brut_par_entrepot))
if (n_na_rwi > 0) {
  mediane_rwi <- median(rwi_brut_par_entrepot, na.rm = TRUE)
  cat("  Entrepôts sans cellule RWI :", n_na_rwi,
      "→ imputation médiane (", round(mediane_rwi, 3), ")\n")
  rwi_brut_par_entrepot[is.na(rwi_brut_par_entrepot)] <- mediane_rwi
}

# ── Normalisation min-max ─────────────────────────────────────────────────────
# rescale() du package scales effectue la transformation min-max en une ligne.
# to = c(0, 1) : borne inférieure et supérieure de l'intervalle cible.
# La normalisation est calculée sur les scores ENTREPÔTS (pas sur le Rwanda
# entier) pour que les extrêmes correspondent aux zones réellement modélisées.
rwi_min_entrepots <- min(rwi_brut_par_entrepot)
rwi_max_entrepots <- max(rwi_brut_par_entrepot)

p_rwi <- if (rwi_max_entrepots > rwi_min_entrepots) {
  # Cas normal : il y a de la variabilité entre entrepôts
  rescale(rwi_brut_par_entrepot, to = c(0, 1))
} else {
  # Cas dégénéré : tous les entrepôts ont le même score (données manquantes…)
  # → valeur neutre 0.5 pour tous (pas d'effet sur les profils)
  rep(0.5, n_warehouses)
}

cat("  Score p_rwi après normalisation :\n")
cat("  Min :", round(min(p_rwi), 3), "| Max :", round(max(p_rwi), 3),
    "| Médiane :", round(median(p_rwi), 3), "\n\n")

# ── Vérification d'alignement avant construction du tableau ───────────────────
# rwi_brut_par_entrepot doit avoir exactement autant d'éléments que entreposages_sf
# (la référence des 123 zones économiques). Un désalignement ici provoquerait
# une attribution erronée des scores RWI à des zones qui ne sont pas les leurs.
# C'est le même contrat que celui utilisé en IV.4.D pour les vecteurs population.
stopifnot(
  length(rwi_brut_par_entrepot) == nrow(entreposages_sf),
  length(p_rwi)                 == nrow(entreposages_sf)
)

# ── Tableau de synthèse ───────────────────────────────────────────────────────
diag_rwi <- tibble(
  nom_zone   = entreposages_fictifs$nom,
  type_zone  = entreposages_fictifs$type,
  rwi_brut   = round(rwi_brut_par_entrepot, 3),
  p_rwi      = round(p_rwi, 3),
  classe_rwi = case_when(
    p_rwi >= 0.75 ~ "Très riche",
    p_rwi >= 0.50 ~ "Riche",
    p_rwi >= 0.25 ~ "Pauvre",
    TRUE          ~ "Très pauvre"
  )
)

cat("Scores RWI par zone (classement décroissant) :\n")
print(
  diag_rwi %>%
    arrange(desc(p_rwi)) %>%
    select(nom_zone, type_zone, rwi_brut, p_rwi, classe_rwi) %>%
    rename(Zone = nom_zone, Type = type_zone,
           RWI_brut = rwi_brut, p_rwi = p_rwi, Classe = classe_rwi)
)
cat("\n")

# ── Stockage dans DuckDB ──────────────────────────────────────────────────────
# La table "richesse_entrepots" est utilisable dans toutes les requêtes
# SQL des Parties V à IX, par exemple pour pondérer la demande par le niveau
# de richesse dans les exports ou analyses de vulnérabilité.
duck_write(
  diag_rwi %>%
    select(nom_zone, type_zone, rwi_brut, p_rwi, classe_rwi),
  "richesse_entrepots"
)
cat("✓ Table richesse_entrepots créée dans DuckDB\n")

# ── Intégration dans reseau_rwanda (attribut de nœud sfnetworks) ──────────────
# On ajoute rwi_brut et p_rwi comme attributs des nœuds d'entrepôt.
# Les nœuds non-entrepôt reçoivent NA (même logique que population_zone).
# match() : pour chaque nœud, trouve si son warehouse_name est dans diag_rwi.
reseau_rwanda <- reseau_rwanda %>%
  activate("nodes") %>%
  mutate(
    rwi_brut = diag_rwi$rwi_brut[
      match(warehouse_name, diag_rwi$nom_zone)
    ],
    p_rwi    = diag_rwi$p_rwi[
      match(warehouse_name, diag_rwi$nom_zone)
    ]
  )

# ── Intégration dans entreposages_fictifs ─────────────────────────────────────
# Mise à jour du data.frame de référence pour le modèle gravitaire (VII.2)
# et les exports CSV finaux (VIII.3).
stopifnot(nrow(entreposages_fictifs) == nrow(diag_rwi))

entreposages_fictifs <- entreposages_fictifs %>%
  select(-any_of(c("rwi_brut", "p_rwi", "classe_rwi"))) %>%
  bind_cols(
    diag_rwi %>% select(rwi_brut, p_rwi, classe_rwi)
  )

# Mise à jour de la table DuckDB
duck_write(entreposages_fictifs, "zones_entreposage")

cat("✓ rwi_brut et p_rwi intégrés dans reseau_rwanda et DuckDB\n\n")


# ==============================================================================
# IV.5.4 : Visualisations
#
# Deux sorties graphiques :
#   Carte  — score p_rwi par zone d'entrepôt sur le réseau routier
#   Graphique — corrélation RWI × population (les deux enrichissements)
# ==============================================================================

cat("── Visualisations RWI ────────────────────────────────────────────────\n")

# ── Préparation de la couche sf pour les entrepôts enrichis RWI ───────────────
entrepots_rwi_sf <- reseau_rwanda %>%
  activate("nodes") %>%
  filter(is_warehouse, !is.na(p_rwi)) %>%
  st_as_sf() %>%
  mutate(
    taille_cercle = rescale(p_rwi, to = c(0.3, 2.5)),
    classe_rwi    = case_when(
      p_rwi >= 0.75 ~ "Très riche",
      p_rwi >= 0.50 ~ "Riche",
      p_rwi >= 0.25 ~ "Pauvre",
      TRUE          ~ "Très pauvre"
    ),
    classe_rwi = factor(
      classe_rwi,
      levels = c("Très pauvre", "Pauvre", "Riche", "Très riche")
    )
  )

# ── Carte : score p_rwi sur le réseau ─────────────────────────────────────────
# Le dégradé de couleur va du bleu foncé (zones pauvres) au rouge foncé (zones
# riches), ce qui est la convention cartographique habituelle pour les indices
# de richesse. La taille des points est proportionnelle au score p_rwi.
PALETTE_RWI <- c(
  "#08519C",   # Bleu foncé  — très pauvre (p_rwi < 0.25)
  "#6BAED6",   # Bleu clair  — pauvre      (p_rwi 0.25–0.50)
  "#FD8D3C",   # Orange      — riche       (p_rwi 0.50–0.75)
  "#A50026"    # Rouge foncé — très riche  (p_rwi > 0.75)
)

carte_rwi <- fond_carte() +
  
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(col = "#DDDDDD", lwd = 0.4) +
  
  tm_shape(entrepots_rwi_sf) +
  tm_dots(
    fill        = "p_rwi",
    fill.scale  = tm_scale_intervals(
      style  = "fixed",
      breaks = c(0, 0.25, 0.50, 0.75, 1.00),
      values = PALETTE_RWI
    ),
    fill.legend = tm_legend(title = "Richesse relative\n(p_rwi normalisé)"),
    size        = "taille_cercle",
    size.scale  = tm_scale(values.range = c(0.3, 2.5)),
    size.legend = tm_legend(show = FALSE)
  ) +
  
  tm_title(paste0(
    "Richesse relative des zones d'entrepôt\n",
    "Relative Wealth Index — Meta / CIESIN (IDW dans buffer ",
    BUFFER_RWI_M / 1000, " km)"
  )) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(
  carte_rwi,
  file.path(DIR_OUTPUT, "carte_rwi_zones.png"),
  width = 3000, height = 2400, dpi = 300
)
cat("  ✓ carte_rwi_zones.png\n")

# ── Carte : raster RWI Rwanda (vue d'ensemble des données brutes) ─────────────
# Cette carte montre les données RWI pour TOUT le Rwanda (pas seulement les
# entrepôts), ce qui permet de visualiser les gradients spatiaux de richesse
# et de vérifier que les entrepôts sont bien positionnés dans leur contexte.
if (rwi_ok && nrow(rwi_sf) > 0) {
  
  carte_rwi_raster <- fond_carte() +
    
    tm_shape(rwi_sf) +
    tm_dots(
      fill        = "rwi",
      fill.scale  = tm_scale_intervals(
        style  = "quantile",
        n      = 5,
        values = c("#08519C", "#6BAED6", "#F7F7F7", "#FD8D3C", "#A50026")
      ),
      fill.legend = tm_legend(title = "Score RWI\n(brut, centré 0)"),
      size        = 0.05,    # Petite taille : beaucoup de points (~1700 cellules)
      fill_alpha  = 0.7
    ) +
    
    # Superposition des entrepôts pour le repérage
    tm_shape(entrepots_rwi_sf) +
    tm_dots(
      fill        = "warehouse_type",
      fill.scale  = tm_scale(values = PALETTE_ZONE_TYPE),
      fill.legend = tm_legend(title = "Type d'entrepôt"),
      size        = 0.5,
      col         = "white",
      lwd         = 1
    ) +
    
    tm_title("Données RWI brutes — Rwanda\n(chaque point = cellule de ~2,4 km²)") +
    tm_layout(legend.outside = TRUE, frame = TRUE) +
    tm_scalebar(position = c("left", "bottom")) +
    tm_compass(position  = c("right", "top"))
  
  tmap_save(
    carte_rwi_raster,
    file.path(DIR_OUTPUT, "carte_rwi_rwanda_brut.png"),
    width = 3000, height = 2400, dpi = 300
  )
  cat("  ✓ carte_rwi_rwanda_brut.png\n")
}

# ── Graphique : corrélation RWI × population ──────────────────────────────────
# Ce graphique met en relation les deux enrichissements (IV.4 et IV.5) pour
# vérifier leur cohérence : on s'attend à ce que les zones à forte population
# (Kigali, Musanze…) aient aussi des scores RWI élevés — mais pas toujours,
# car les zones frontalières peuvent avoir une population élevée et un RWI faible.
if ("population_zone" %in% names(entreposages_fictifs)) {
  
  g_rwi_pop <- diag_rwi %>%
    left_join(
      entreposages_fictifs %>%
        select(nom, population_zone),
      by = c("nom_zone" = "nom")
    ) %>%
    filter(!is.na(population_zone), population_zone > 0) %>%
    mutate(
      Zone_court = str_trunc(str_remove(nom_zone, " - .*"), 22),
      pop_log    = log10(population_zone)
    ) %>%
    ggplot(aes(x = pop_log, y = p_rwi,
               color = type_zone, label = Zone_court)) +
    
    geom_point(size = 4, alpha = 0.85) +
    ggrepel::geom_text_repel(
      size = 3, max.overlaps = 12,
      segment.color = "#AAAAAA"
    ) +
    
    # Droite de régression pour visualiser la tendance générale
    geom_smooth(
      method  = "lm",
      se      = TRUE,
      color   = "#333333",
      linetype = "dashed",
      alpha   = 0.15
    ) +
    
    scale_color_manual(values = PALETTE_ZONE_TYPE, name = "Type de zone") +
    scale_x_continuous(
      labels = function(x) format(10^x, big.mark = " ", scientific = FALSE),
      name   = "Population (échelle log₁₀)"
    ) +
    scale_y_continuous(
      limits = c(0, 1),
      name   = "Score de richesse relative (p_rwi)"
    ) +
    
    labs(
      title    = "Richesse relative (RWI) × Population par zone d'entrepôt",
      subtitle = paste0(
        "Les zones en haut à droite (riches ET peuplées) génèrent la plus forte demande\n",
        "Sources : Meta RWI (2021), NISR RPHC-5 (2022)"
      )
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title    = element_text(face = "bold"),
      plot.subtitle = element_text(color = "#555555", size = 10),
      legend.position = "right"
    )+
    # Cercle rouge autour du point de référence
    geom_point(
      data = ~ filter(., str_trunc(str_remove(nom_zone, " - .*"), 22) ==
                        str_trunc(str_remove(zone_ref_pop_approx, " - .*"), 22)),
      aes(x = pop_log, y = p_rwi),
      shape  = 21,
      size   = 6,
      color  = "#CC0000",
      fill   = NA,
      stroke = 1.5,
      inherit.aes = FALSE
    ) +
    # Légende textuelle du cercle
    annotate(
      "text",
      x     = log10(max(diag_population$population_zone, na.rm = TRUE)) * 0.97,
      y     = 0.05,
      label = paste0("◄ Référence demande\n  (taille_composite = 1)"),
      hjust = 1,
      color = "#CC0000",
      size  = 3.0,
      fontface = "italic"
    )
  
  # ggrepel est nécessaire pour éviter les chevauchements de labels.
  # S'il n'est pas installé, on produit le graphique sans labels.
  if (!requireNamespace("ggrepel", quietly = TRUE)) {
    install.packages("ggrepel")
    library(ggrepel)
  }
  
  ggsave(
    file.path(DIR_OUTPUT, "graphique_rwi_vs_population.png"),
    g_rwi_pop, width = 12, height = 7, dpi = 300
  )
  cat("  ✓ graphique_rwi_vs_population.png\n\n")
}

cat("✓ Partie IV.5 terminée — p_rwi disponible dans :\n")
cat("  • reseau_rwanda  (attribut de nœud : rwi_brut, p_rwi)\n")
cat("  • DuckDB         (table richesse_entrepots)\n")
cat("  • entreposages_fictifs (colonnes rwi_brut, p_rwi, classe_rwi)\n\n")

################################################################################
# PARTIE IV.4.F — EMPLOI SECTORIEL RPHC5 2022 ET PROFILS D'OFFRE EMPIRIQUES
#
# OBJECTIF : Produire deux outputs à partir du RPHC5 2022 :
#
#   (A) profil_offre_empirique_all[i, s] — matrice nrow(entreposages_sf) × N_SECTEURS
#       Part du secteur s dans l'offre de la zone i, estimée par la part
#       de l'emploi du secteur s dans le district contenant la zone i.
#       Utilisé en VII.2 à la place de PROFILS_OFFRE[[type_zone]].
#
#   (B) emploi_total_par_entrepot[i] — vecteur de longueur nrow(entreposages_sf)
#       Emploi total du district de la zone i.
#       Utilisé dans la Transition IV.5→V pour calculer taille_composite_offre
#       (remplace la population côté offre).
#
# LOGIQUE ÉCONOMIQUE :
#   L'offre (ce qu'une zone peut exporter) est mieux captée par sa structure
#   productive (emploi par secteur) que par un type qualitatif attribué à la main.
#   Une zone taguée "marché" dans un district à fort emploi industriel devrait
#   avoir un profil d'offre orienté Industrie, pas le profil générique "marche".
#
#   La demande (ce qu'une zone importe) reste liée à la population résidentielle
#   et au niveau de richesse — justifiant deux tailles composites distinctes.
#
# DÉPENDANCES :
#   - entreposages_sf, entreposages_fictifs (Partie IV.3)
#   - rwanda_districts_gadm         (Partie IV.4.C — rechargé si absent)
#   - SECTEURS, N_SECTEURS, PROFILS_OFFRE (Paramètres)
#   - noeuds_entreposage, n_warehouses    (Partie IV.3)
# ALIMENTE :
#   - Transition IV.5→V   : emploi_total_par_entrepot, profil_offre_empirique_all
#   - DuckDB              : tables "profils_offre_empiriques", "diag_emploi"
################################################################################

cat("==========================================================\n")
cat("  PARTIE IV.4.F — EMPLOI SECTORIEL RPHC5 2022\n")
cat("==========================================================\n\n")

# ── Initialisation à des valeurs neutres (repli si RPHC5 indisponible) ────────
# Si le fichier n'est pas chargé, ces deux objets sont remplis avec
# les profils qualitatifs et la population — comportement identique à avant.
profil_offre_empirique_all <- matrix(
  NA_real_,
  nrow     = nrow(entreposages_sf),
  ncol     = N_SECTEURS,
  dimnames = list(entreposages_fictifs$nom, SECTEURS)
)
emploi_total_par_entrepot <- rep(NA_real_, nrow(entreposages_sf))
rphc5_emploi_ok           <- FALSE

# ── Téléchargement depuis MinIO si le fichier n'est pas en local ──────────────
if (!file.exists(RPHC5_EMPLOI_CSV_PATH)) {
  dir.create(dirname(RPHC5_EMPLOI_CSV_PATH), showWarnings = FALSE, recursive = TRUE)
  tryCatch({
    save_object(
      object    = RPHC5_EMPLOI_CSV_PATH,
      bucket    = MINIO_BUCKET,
      file      = RPHC5_EMPLOI_CSV_PATH,
      region    = "",
      use_https = TRUE,
      base_url  = MINIO_BASE_URL
    )
    cat("  ✓ Fichier emploi RPHC5 téléchargé depuis MinIO\n")
  }, error = function(e) {
    cat("  ⚠ Téléchargement MinIO échoué :", conditionMessage(e), "\n")
  })
}

if (file.exists(RPHC5_EMPLOI_CSV_PATH)) {
  
  tryCatch({
    
    # ── Chargement du CSV d'emploi RPHC5 ──────────────────────────────────────
    rphc5_emploi_raw <- read_csv(RPHC5_EMPLOI_CSV_PATH, show_col_types = FALSE)
    
    cat("  CSV emploi RPHC5 chargé :", nrow(rphc5_emploi_raw), "lignes\n")
    cat("  Colonnes :", paste(names(rphc5_emploi_raw), collapse = ", "), "\n\n")
    
    # Vérification des colonnes déclarées dans RPHC5_CORRESPONDANCE_SECTEURS
    cols_attendues  <- names(RPHC5_CORRESPONDANCE_SECTEURS)
    cols_manquantes <- cols_attendues[!cols_attendues %in% names(rphc5_emploi_raw)]
    if (length(cols_manquantes) > 0) {
      warning("  ⚠ Colonnes manquantes : ",
              paste(cols_manquantes, collapse = ", "),
              "\n  Adapter RPHC5_CORRESPONDANCE_SECTEURS dans les paramètres.")
    }
    
    # ── Nettoyage (même normalisation des noms de district que IV.4.C) ────────
    rphc5_emploi <- rphc5_emploi_raw %>%
      rename(district = any_of(RPHC5_COL_DISTRICT_EMPLOI)) %>%
      mutate(
        district_clean = iconv(str_to_lower(str_trim(district)),
                               from = "UTF-8", to = "ASCII//TRANSLIT"),
        # Suppression des espaces / virgules parfois présents dans les exports NISR
        across(all_of(intersect(cols_attendues, names(.))),
               ~ suppressWarnings(as.numeric(str_remove_all(as.character(.), "[,\\s]"))))
      ) %>%
      # Calcul de l'emploi total si la colonne n'est pas déjà dans le fichier
      {
        cols_presentes <- intersect(cols_attendues, names(.))
        if (!"emploi_total" %in% names(.) && length(cols_presentes) > 0) {
          mutate(., emploi_total = rowSums(select(., all_of(cols_presentes)),
                                           na.rm = TRUE))
        } else { . }
      }
    
    cat("  Districts après nettoyage :", nrow(rphc5_emploi), "\n")
    
    # ── Rechargement de GADM si absent de l'environnement ─────────────────────
    # rwanda_districts_gadm a été construit en IV.4.C. Si la session a été
    # interrompue entre IV.4.C et IV.4.F, on le retélécharge ici.
    if (!exists("rwanda_districts_gadm") || is.null(rwanda_districts_gadm)) {
      cat("  Retéléchargement des frontières GADM...\n")
      rwanda_districts_gadm <- tryCatch({
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
    
    if (!is.null(rwanda_districts_gadm)) {
      
      # ── Jointure GADM × emploi RPHC5 ────────────────────────────────────────
      cols_emploi_disponibles <- intersect(
        c(cols_attendues, "emploi_total"),
        names(rphc5_emploi)
      )
      
      districts_avec_emploi <- rwanda_districts_gadm %>%
        left_join(
          rphc5_emploi %>% select(district_clean, all_of(cols_emploi_disponibles)),
          by = "district_clean"
        )
      
      n_sans_emploi <- sum(is.na(districts_avec_emploi$emploi_total))
      cat("  Jointure GADM × emploi :",
          nrow(districts_avec_emploi) - n_sans_emploi, "/",
          nrow(districts_avec_emploi), "districts appariés\n")
      if (n_sans_emploi > 0) {
        cat("  Districts non appariés :",
            paste(districts_avec_emploi$district_gadm[
              is.na(districts_avec_emploi$emploi_total)], collapse = ", "), "\n")
        cat("  → Vérifier l'orthographe dans", RPHC5_EMPLOI_CSV_PATH, "\n")
      }
      
      # ── Jointure spatiale entrepôts × districts ─────────────────────────────
      # Même logique qu'en IV.4.C pour la population :
      # chaque entrepôt hérite des données du district qui le contient.
      entrepots_join_emploi <- entreposages_sf %>%
        st_join(
          districts_avec_emploi,
          join    = st_within,
          largest = TRUE
        )
      
      # Fallback nearest pour les entrepôts hors frontière (postes frontières…)
      manquants_idx_e <- which(is.na(entrepots_join_emploi$emploi_total))
      if (length(manquants_idx_e) > 0) {
        idx_proche_e <- st_nearest_feature(
          entreposages_sf[manquants_idx_e, ], districts_avec_emploi
        )
        for (col_e in cols_emploi_disponibles) {
          entrepots_join_emploi[[col_e]][manquants_idx_e] <-
            districts_avec_emploi[[col_e]][idx_proche_e]
        }
        cat("  Entrepôts hors district (fallback nearest) :",
            length(manquants_idx_e), "\n")
      }
      
      # ── Stockage de l'emploi total (vecteur nrow(entreposages_sf)) ──────────
      emploi_total_par_entrepot <- as.numeric(entrepots_join_emploi$emploi_total)
      emploi_total_par_entrepot[is.na(emploi_total_par_entrepot)] <-
        median(emploi_total_par_entrepot, na.rm = TRUE)
      
      cat("  Emploi total : min =", round(min(emploi_total_par_entrepot)),
          "| max =", round(max(emploi_total_par_entrepot)), "\n\n")
      
      # ── Construction des profils d'offre empiriques ─────────────────────────
      # Pour chaque zone i, le profil empirique est la part de l'emploi de
      # chaque secteur du modèle dans l'emploi total du district.
      # Il est ensuite fusionné avec le profil qualitatif de base via une
      # interpolation convexe (POIDS_PROFIL_EMPLOI_RPHC5 détermine le dosage).
      cat("  Construction des profils d'offre empiriques...\n")
      
      for (i in seq_len(nrow(entreposages_sf))) {
        
        emploi_total_i <- emploi_total_par_entrepot[i]
        if (is.na(emploi_total_i) || emploi_total_i == 0) next
        
        # Initialisation du vecteur d'emploi par secteur du modèle
        emploi_secteur_i <- numeric(N_SECTEURS)
        names(emploi_secteur_i) <- SECTEURS
        
        # Distribution de chaque colonne CSV vers les secteurs du modèle
        # selon RPHC5_CORRESPONDANCE_SECTEURS
        for (col_csv in intersect(cols_attendues,
                                  names(entrepots_join_emploi))) {
          val_col <- as.numeric(entrepots_join_emploi[[col_csv]][i])
          if (is.na(val_col) || val_col == 0) next
          
          # Récupération des secteurs cibles et de leurs parts
          corresp <- RPHC5_CORRESPONDANCE_SECTEURS[[col_csv]]
          for (secteur_m in names(corresp)) {
            if (secteur_m %in% SECTEURS) {
              emploi_secteur_i[secteur_m] <-
                emploi_secteur_i[secteur_m] + val_col * corresp[[secteur_m]]
            }
          }
        }
        
        # Normalisation : les parts doivent sommer à 1 (profil de probabilité)
        somme_emploi_i <- sum(emploi_secteur_i)
        if (somme_emploi_i == 0) next
        profil_empirique_brut <- emploi_secteur_i / somme_emploi_i
        
        # Fusion avec le profil qualitatif de base (interpolation convexe) :
        #   profil_final = α × profil_empirique_RPHC5 + (1-α) × profil_qualitatif
        # Cette fusion évite les profils dégénérés pour les zones dont le
        # district a peu d'emplois formels recensés (ex : zones frontalières).
        type_zone_i   <- entreposages_fictifs$type[i]
        profil_base_i <- PROFILS_OFFRE[[type_zone_i]]
        
        profil_fusionne <- POIDS_PROFIL_EMPLOI_RPHC5 * profil_empirique_brut +
          (1 - POIDS_PROFIL_EMPLOI_RPHC5) * profil_base_i
        
        # Renormalisation de sécurité (somme peut légèrement dériver de 1
        # à cause de la virgule flottante)
        profil_offre_empirique_all[i, ] <- profil_fusionne / sum(profil_fusionne)
      }
      
      rphc5_emploi_ok <- TRUE
      
    } else {
      cat("  ⚠ GADM indisponible — profils qualitatifs conservés\n\n")
    }
    
  }, error = function(e) {
    cat("  ⚠ Chargement RPHC5 emploi échoué :", conditionMessage(e), "\n")
    cat("    → profils qualitatifs PROFILS_OFFRE conservés\n\n")
  })
  
} else {
  cat("  Fichier RPHC5 emploi non trouvé :", RPHC5_EMPLOI_CSV_PATH, "\n")
  cat("  → Télécharger sur https://www.statistics.gov.rw/datasource/census-2022\n")
  cat("  → Partie IV.4.F ignorée — profils qualitatifs conservés\n\n")
}

# ── Imputation des zones sans profil empirique (NA restants) ──────────────────
# Garantit que profil_offre_empirique_all ne contient jamais de NA en VII.2.
# Les zones non couvertes par le RPHC5 reçoivent leur profil qualitatif de base.
for (i in seq_len(nrow(entreposages_sf))) {
  if (any(is.na(profil_offre_empirique_all[i, ]))) {
    type_zone_i <- entreposages_fictifs$type[i]
    profil_offre_empirique_all[i, ] <- PROFILS_OFFRE[[type_zone_i]]
  }
}

# ── Tableau de diagnostic et stockage dans DuckDB ─────────────────────────────
diag_emploi <- tibble(
  nom_zone      = entreposages_fictifs$nom,
  type_zone     = entreposages_fictifs$type,
  emploi_total  = emploi_total_par_entrepot,
  source_emploi = if_else(rphc5_emploi_ok &
                            !is.na(emploi_total_par_entrepot),
                          "RPHC5_2022", "NA_non_disponible")
)

cat("Répartition des sources d'emploi :\n")
print(diag_emploi %>%
        count(source_emploi) %>%
        mutate(pct = round(n / sum(n) * 100, 1)))

duck_write(
  as.data.frame(profil_offre_empirique_all) %>%
    rownames_to_column("nom_zone") %>%
    mutate(source = if_else(rphc5_emploi_ok, "RPHC5_empirique", "qualitatif")),
  "profils_offre_empiriques"
)
duck_write(diag_emploi, "diag_emploi")

cat("✓ Partie IV.4.F terminée — deux outputs disponibles :\n")
cat("  • profil_offre_empirique_all  [", nrow(entreposages_sf), "×", N_SECTEURS, "]\n")
cat("  • emploi_total_par_entrepot   [", nrow(entreposages_sf), "]\n\n")

################################################################################
# TRANSITION IV.5 → V — CALCUL DES TAILLES COMPOSITES OFFRE ET DEMANDE
#
# CHANGEMENT PAR RAPPORT À L'ANCIENNE VERSION :
#   Ancienne version : une seule taille_composite pour offre ET demande,
#                      basée sur la population et le RWI.
#   Nouvelle version : deux tailles composites distinctes.
#
# ── TAILLE COMPOSITE OFFRE (taille_composite_offre) ───────────────────────────
#   Capte la CAPACITÉ PRODUCTIVE d'une zone.
#   Basée sur l'emploi total du district (RPHC5 2022) si disponible,
#   sinon repli sur la population (comportement identique à l'ancienne version).
#
#   FORMULE :
#     taille_brute_offre_i = log10(emploi_i + 1)^ALPHA_LOG_EMPLOI
#                            × (1 + K_RWI_OFFRE × p_rwi_i)
#     taille_composite_offre_i = taille_brute_offre_i / ref_kigali_offre
#
# ── TAILLE COMPOSITE DEMANDE (taille_composite_demande) ───────────────────────
#   Capte la CAPACITÉ DE CONSOMMATION d'une zone.
#   Basée sur la population résidentielle et le RWI — INCHANGÉ.
#
#   FORMULE (identique à l'ancienne taille_composite) :
#     taille_brute_demande_i = log10(pop_i + 1)^ALPHA_LOG_POP
#                              × (1 + K_RWI_TAILLE × p_rwi_i)
#     taille_composite_demande_i = taille_brute_demande_i / ref_kigali_demande
#
# ── RÉTROCOMPATIBILITÉ ────────────────────────────────────────────────────────
#   taille_composite (moyenne géométrique des deux) est conservé pour les
#   Parties VIII et IX qui n'utilisent pas encore la distinction offre/demande.
#
# DÉPENDANCES :
#   - diag_population         (IV.4.D) → pop_i côté demande
#   - emploi_total_par_entrepot (IV.4.F) → emploi_i côté offre
#   - profil_offre_empirique_all (IV.4.F) → profils d'offre empiriques
#   - diag_rwi                (IV.5)   → p_rwi_i
################################################################################

cat("── Calcul des tailles composites offre et demande (RPHC5) ─────────────\n\n")

# ── Récupération de la population (côté demande) ──────────────────────────────
# Identique à l'ancienne version : on extrait pop_i depuis diag_population
# en faisant correspondre les noms de zones à noeuds_entreposage.
pop_i <- diag_population$population_zone[
  match(noeuds_entreposage$warehouse_name, diag_population$nom_zone)
]
pop_i <- replace_na(pop_i, median(pop_i, na.rm = TRUE))

# ── Correction du double comptage pour les entrepôts sur le même nœud ─────────
# [CONSERVER LE BLOC EXISTANT TEL QUEL — il corrige uniquement pop_i]
# Le bloc de correction (if (nrow(doublons_noeuds) > 0) { ... }) reste ici.
# Il n'a pas d'équivalent côté emploi (données districtuelles, pas raster).

# ── Récupération de l'emploi (côté offre) ─────────────────────────────────────
# On extrait emploi_total_par_entrepot (construit en IV.4.F, indexé sur
# entreposages_fictifs$nom) pour noeuds_entreposage, via le même match()
# que pour pop_i — assurant un alignement parfait des deux vecteurs.
if (rphc5_emploi_ok && !all(is.na(emploi_total_par_entrepot))) {
  
  emploi_i <- emploi_total_par_entrepot[
    match(noeuds_entreposage$warehouse_name, entreposages_fictifs$nom)
  ]
  emploi_i <- replace_na(emploi_i, median(emploi_i, na.rm = TRUE))
  emploi_i <- pmax(emploi_i, 1)   # Évite log10(0+1) = 0 qui annulerait la taille
  
  cat("  Source taille offre   : emploi RPHC5 2022\n")
  cat("  Emploi : min =", round(min(emploi_i)),
      "| max =", round(max(emploi_i)), "\n")
  
} else {
  # Repli sur la population si RPHC5 emploi indisponible :
  # taille_composite_offre ≈ taille_composite_demande (comportement avant RPHC5)
  emploi_i <- pop_i
  cat("  Source taille offre   : population (emploi RPHC5 non disponible)\n")
}

cat("  Source taille demande : population + RWI (inchangé)\n\n")

# ── Récupération du score de richesse p_rwi_i ─────────────────────────────────
# Utilisé des deux côtés (offre et demande) mais avec des coefficients différents :
#   K_RWI_OFFRE  (0.5) < K_RWI_TAILLE (1.0) : la richesse amplifie davantage
#   la consommation que la capacité productive.
p_rwi_i <- diag_rwi$p_rwi[
  match(noeuds_entreposage$warehouse_name, diag_rwi$nom_zone)
]
p_rwi_i <- replace_na(p_rwi_i, median(p_rwi_i, na.rm = TRUE))

stopifnot(
  length(pop_i)    == n_warehouses,
  length(emploi_i) == n_warehouses,
  length(p_rwi_i)  == n_warehouses
)

# ── Plafonnement de l'emploi pour les zones industrielles ─────────────────────
# Même logique que CAP_POP_INDUSTRIE côté demande : les zones industrielles
# dans Kigali héritent d'un emploi de district artificiellement élevé.
types_warehouse      <- noeuds_entreposage$warehouse_type
n_capees_emploi      <- sum(types_warehouse == "industrie" &
                              emploi_i > CAP_EMPLOI_INDUSTRIE, na.rm = TRUE)
if (n_capees_emploi > 0) {
  cat("  Plafonnement emploi pour", n_capees_emploi,
      "zones industrielles (cap =", CAP_EMPLOI_INDUSTRIE, ")\n")
  emploi_i <- ifelse(
    types_warehouse == "industrie" & emploi_i > CAP_EMPLOI_INDUSTRIE,
    CAP_EMPLOI_INDUSTRIE, emploi_i
  )
}

# ── Plafonnement de la population (côté demande — inchangé) ───────────────────
n_capees <- sum(types_warehouse == "industrie" &
                  pop_i > CAP_POP_INDUSTRIE, na.rm = TRUE)
if (n_capees > 0) {
  cat("  Plafonnement population pour", n_capees,
      "zones industrielles (cap =", CAP_POP_INDUSTRIE, "hab.)\n")
  pop_i <- ifelse(
    types_warehouse == "industrie" & pop_i > CAP_POP_INDUSTRIE,
    CAP_POP_INDUSTRIE, pop_i
  )
}

# ══════════════════════════════════════════════════════════════════════════════
# TAILLE COMPOSITE OFFRE (basée sur l'emploi RPHC5)
# ══════════════════════════════════════════════════════════════════════════════

# log10(emploi + 1) comprime les ordres de grandeur inter-districts.
# L'exposant ALPHA_LOG_EMPLOI étire l'échelle log (même raisonnement qu'ALPHA_LOG_POP).
# (1 + K_RWI_OFFRE × p_rwi) : amplificateur de richesse, plus faible côté offre
# (les zones riches ne produisent pas proportionnellement plus, contrairement
#  à leur consommation).
taille_brute_offre <- log10(emploi_i + 1)^ALPHA_LOG_EMPLOI *
  (1 + K_RWI_OFFRE * p_rwi_i)

# ── Normalisation par le maximum de l'échantillon ─────────────────────────────
ref_offre              <- max(taille_brute_offre, na.rm = TRUE)
idx_ref_offre          <- which.max(taille_brute_offre)
nom_ref_offre          <- noeuds_entreposage$warehouse_name[idx_ref_offre]
taille_composite_offre <- taille_brute_offre / ref_offre

cat("\n  Référence offre   (max) : '", nom_ref_offre,
    "' (idx =", idx_ref_offre, ") — masse brute emploi =",
    round(ref_offre, 3), "\n")

# ══════════════════════════════════════════════════════════════════════════════
# TAILLE COMPOSITE DEMANDE (basée sur la population — formule inchangée)
# ══════════════════════════════════════════════════════════════════════════════

# Identique à l'ancienne taille_composite :
#   log10(pop + 1)^ALPHA_LOG_POP × (1 + K_RWI_TAILLE × p_rwi)
taille_brute_demande <- log10(pop_i + 1)^ALPHA_LOG_POP *
  (1 + K_RWI_TAILLE * p_rwi_i)

# ── Normalisation par le maximum de l'échantillon ─────────────────────────────
ref_demande              <- max(taille_brute_demande, na.rm = TRUE)
idx_ref_demande          <- which.max(taille_brute_demande)
nom_ref_demande          <- noeuds_entreposage$warehouse_name[idx_ref_demande]
taille_composite_demande <- taille_brute_demande / ref_demande

cat("  Référence demande (max) : '", nom_ref_demande,
    "' (idx =", idx_ref_demande, ") — masse brute pop =",
    round(ref_demande, 3), "\n\n")

# ── Taille composite unifiée (rétrocompatibilité Parties VIII et IX) ──────────
# Les Parties VIII et IX n'utilisent pas encore la distinction offre/demande.
# On leur fournit la moyenne géométrique des deux, qui est un compromis neutre.
# Ancienne formule supprimée : taille_composite = (log10(pop+1)^α × (1+K×rwi)) / ref
taille_composite <- sqrt(taille_composite_offre * taille_composite_demande)

# ── Sommes de normalisation (une par côté + une unifiée) ──────────────────────
# Ces sommes sont utilisées dans VII.2 pour normer les volumes d'offre/demande.
somme_tailles_offre   <- sum(taille_composite_offre)
somme_tailles_demande <- sum(taille_composite_demande)
somme_tailles         <- sum(taille_composite)   # Rétrocompatibilité

# ── Extraction des profils d'offre empiriques pour noeuds_entreposage ─────────
# profil_offre_empirique_all a nrow(entreposages_sf) lignes (toutes les zones).
# On extrait ici la sous-matrice correspondant à noeuds_entreposage (zones actives
# après déduplication), en garantissant l'alignement via les noms de zones.
profil_offre_empirique <- profil_offre_empirique_all[
  match(noeuds_entreposage$warehouse_name, rownames(profil_offre_empirique_all)),
  , drop = FALSE
]
# profil_offre_empirique est maintenant n_warehouses × N_SECTEURS,
# prêt à être utilisé directement dans la boucle de VII.2.

# ── Mise à jour de entreposages_fictifs et DuckDB ─────────────────────────────
taille_lookup <- tibble(
  nom                      = noeuds_entreposage$warehouse_name,
  taille_composite         = taille_composite,
  taille_composite_offre   = taille_composite_offre,
  taille_composite_demande = taille_composite_demande
) %>% distinct(nom, .keep_all = TRUE)

entreposages_fictifs <- entreposages_fictifs %>%
  select(-any_of(c("taille_composite",
                   "taille_composite_offre",
                   "taille_composite_demande"))) %>%
  left_join(taille_lookup, by = "nom")

# ── Diagnostic et exclusion des zones sans taille composite ───────────────────
# Un NA après le left_join signifie que la zone n'a pas de correspondant dans
# noeuds_entreposage (zone dupliquée éliminée, zone hors réseau, etc.).
# On les signale explicitement puis on les exclut
zones_na_taille <- entreposages_fictifs %>%
  filter(is.na(taille_composite)) %>%
  pull(nom)

if (length(zones_na_taille) > 0) {
  warning(length(zones_na_taille),
          " zone(s) exclues faute de taille_composite : ",
          paste(zones_na_taille, collapse = ", "))
  entreposages_fictifs <- entreposages_fictifs %>%
    filter(!is.na(taille_composite))
}
duck_write(
  taille_lookup %>%
    left_join(entreposages_fictifs %>% select(nom, type, pays), by = "nom"),
  "tailles_composites"
)

cat("✓ Tailles composites calculées pour", n_warehouses, "zones\n")
cat("  Offre   (emploi RPHC5) : min / max =",
    round(min(taille_composite_offre),   3), "/",
    round(max(taille_composite_offre),   3), "\n")
cat("  Demande (population)   : min / max =",
    round(min(taille_composite_demande), 3), "/",
    round(max(taille_composite_demande), 3), "\n")
cat("  Composite (géom.)      : min / max =",
    round(min(taille_composite),         3), "/",
    round(max(taille_composite),         3), "\n\n")

# ==============================================================================
# SAUVEGARDE INTER-SCRIPTS
# ==============================================================================

cat("=== Sauvegarde des objets persistants (01_reseau) ===\n")

saveRDS(
  list(
    rwanda_boundary   = rwanda_boundary,
    rwanda_national   = rwanda_national,
    rwanda_provinces  = rwanda_provinces,
    lacs_raw          = lacs_raw,
    lacs_ok           = lacs_ok,
    parcs_raw         = if (exists("parcs_raw")) parcs_raw else NULL,
    parcs_ok          = parcs_ok,
    bbox_carto        = bbox_carto,
    villes_osm        = villes_osm
  ),
  PERSIST_GEODATA
)

saveRDS(
  list(
    reseau_rwanda     = reseau_rwanda,
    n_aretes_physiques = igraph::ecount(reseau_rwanda %>% as_tbl_graph()),
    date_creation     = Sys.time()
  ),
  PERSIST_RESEAU_BASE
)

saveRDS(
  list(
    entreposages_fictifs          = entreposages_fictifs,
    entreposages_sf               = entreposages_sf,
    entreposages_buffer           = entreposages_buffer,
    entreposages_avec_snap        = entreposages_avec_snap,
    noeuds_entreposage            = noeuds_entreposage,
    n_warehouses                  = n_warehouses,
    warehouse_nodes_base          = warehouse_nodes_base,
    # Tailles composites
    taille_composite              = taille_composite,
    taille_composite_offre        = taille_composite_offre,
    taille_composite_demande      = taille_composite_demande,
    somme_tailles                 = somme_tailles,
    somme_tailles_offre           = somme_tailles_offre,
    somme_tailles_demande         = somme_tailles_demande,
    profil_offre_empirique        = profil_offre_empirique,
    profil_offre_empirique_all    = profil_offre_empirique_all,
    # Landuse (pour 03_transport)
    part_urbain                   = part_urbain,
    part_industriel               = part_industriel,
    # Données démographiques
    diag_population               = diag_population,
    diag_rwi                      = diag_rwi,
    diag_emploi                   = if (exists("diag_emploi")) diag_emploi else NULL,
    zone_to_prov_placeholder      = NULL   # rempli dans 05_ario
  ),
  PERSIST_ENTREPOSAGES
)

cat("✓ persist_geodata.rds\n")
cat("✓ persist_reseau_base.rds\n")
cat("✓ persist_entreposages.rds\n\n")
cat("Lancer 02_couts.R pour la suite.\n")