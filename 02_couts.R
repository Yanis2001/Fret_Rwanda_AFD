################################################################################
# 02_couts.R
# RÔLE : Calcul des coûts généralisés par arête × véhicule (SQL DuckDB),
#        construction du graphe igraph multi-modal à 3 couches,
#        génération des cartes de coûts et d'émissions.
# ENTRÉES  : persist_reseau_base.rds, persist_entreposages.rds + DuckDB
# SORTIES  : persist_reseau_couts.rds, persist_graphe_mm.rds,
#            persist_mapping_mm.rds + cartes PNG
# DÉPEND DE : 00_parametres.R, 01_reseau.R
################################################################################
source("00_parametres.R")

cat("=== Chargement des objets de 01_reseau ===\n")

.geo   <- readRDS(PERSIST_GEODATA)
.ent   <- readRDS(PERSIST_ENTREPOSAGES)
.res   <- readRDS(PERSIST_RESEAU_BASE)

# Réhydratation des variables dans l'environnement courant
list2env(.geo, envir = .GlobalEnv)
list2env(.ent, envir = .GlobalEnv)
reseau_rwanda      <- .res$reseau_rwanda
n_aretes_physiques <- .res$n_aretes_physiques
rm(.geo, .ent, .res)

# Reconstruction de fond_carte() (dépend des objets géo)
source("utils_fond_carte.R")   # voir note ci-dessous

cat("✓ Objets chargés\n\n")

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
      END AS usure_usd_km,
      -- ── Facteurs d'émission récupérés depuis params_flotte ─────────────────
      -- Ces trois colonnes alimenteront les calculs des étapes suivantes.
      -- Le CO2 est une constante physique (combustion du gazole).
      -- Le NOx et les PM2.5 varient selon la norme Euro du moteur.
      f.facteur_emission_co2,
      f.facteur_emission_nox,
      f.facteur_emission_pm25
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
      * NULLIF(capacite_tonnes, 0))                           AS cost_per_tkm,
    -- ── Émissions absolues par arête (pour un trajet chargé) ─────────────────
    fuel_consumption_L * facteur_emission_co2          AS co2_kg,
    fuel_consumption_L * facteur_emission_nox          AS nox_g,
    fuel_consumption_L * facteur_emission_pm25         AS pm25_g,

    -- ── Intensité d'émission : CO2, PM2.5 et NOx par tonne-kilomètre ─────────
    -- NULLIF évite les divisions par zéro sur les arêtes dégénérées
    -- (longueur nulle ou capacité nulle ne doivent pas propager des NaN).
    (fuel_consumption_L * facteur_emission_co2)
      / NULLIF(length_km * capacite_tonnes, 0)         AS co2_kg_par_tkm,
    (fuel_consumption_L * facteur_emission_nox)
      / NULLIF(length_km * capacite_tonnes, 0)         AS nox_g_par_tkm,
    (fuel_consumption_L * facteur_emission_pm25)
      / NULLIF(length_km * capacite_tonnes, 0)         AS pm25_g_par_tkm
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

# ── Réintégration des émissions dans sfnetworks pour le véhicule de référence ─
# Même logique que pour les coûts : on récupère les valeurs depuis DuckDB
# pour le camion_moyen et on les ajoute comme attributs des arêtes du réseau.
# Cela permet de cartographier les émissions directement depuis reseau_rwanda
# sans requêter DuckDB à chaque fois.
aretes_ref_emissions <- duck_query(glue::glue("
  SELECT arete_id, co2_kg, nox_g, pm25_g, co2_kg_par_tkm, nox_g_par_tkm, pm25_g_par_tkm 
  FROM aretes_couts_tous
  WHERE vehicule_id = '{VEHICULE_REFERENCE}'
  ORDER BY arete_id
"))

reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    co2_kg         = aretes_ref_emissions$co2_kg,
    nox_g          = aretes_ref_emissions$nox_g,
    pm25_g         = aretes_ref_emissions$pm25_g,
    co2_kg_par_tkm = aretes_ref_emissions$co2_kg_par_tkm,
    nox_g_par_tkm  = aretes_ref_emissions$nox_g_par_tkm,
    pm25_g_par_tkm = aretes_ref_emissions$pm25_g_par_tkm
  )

cat("✓ Émissions intégrées dans reseau_rwanda (véhicule de référence)\n\n")

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
stocker_lourd("graphe_multimodal", igraph::graph_from_data_frame(
  all_edges_mm,
  directed = FALSE,
  vertices = vertices_mm
))

cat("✓ Graphe multi-modal construit\n")
cat("  Nœuds  :", igraph::vcount(recuperer_lourd("graphe_multimodal")),
    "(", n_noeuds, "×", n_vehicules, "couches)\n")
cat("  Arêtes :", igraph::ecount(recuperer_lourd("graphe_multimodal")),
    "dont", k, "transbordements\n\n")

# gc() force le garbage collector à libérer la mémoire physique.
# Sans gc(), R peut conserver l'objet en mémoire même après rm() jusqu'au
# prochain cycle automatique du collecteur.
invisible(gc(verbose = FALSE))

cat("✓ graphe_multimodal déplacé vers env_lourds (invisible dans RStudio)\n\n")

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

# ==============================================================================
# V.4 : Cartes d'émissions de GES
# Génère deux cartes pour le véhicule de référence :
#   - intensité carbone (co2_kg_par_tkm) : routes les plus émettrices
#   - émissions de NOx (nox_g_par_tkm)   : proxy de pollution locale
# Et un graphique comparatif des émissions totales par véhicule.
# ==============================================================================

# ── Palette d'émissions (vert pâle → rouge foncé) ─────────────────────────────
# Rouge = route très émettrice (pente forte + mauvaise surface + véhicule lourd)
# Vert  = route peu émettrice (plat, bitumée, camion léger)
PALETTE_EMISSIONS <- c("#1A9850", "#91CF60", "#FEE08B", "#FC8D59", "#D73027")

# ── Carte : intensité carbone (co2_kg_par_tkm) pour le véhicule de référence ──
# Cette carte identifie les segments routiers où chaque tonne-kilomètre
# transportée génère le plus de CO2 : pentes fortes, mauvaise surface,
# zones de congestion urbaine.
# Elle répond à la question : "Où décarboner le transport est-il le plus urgent ?"
carte_co2 <- fond_carte() +
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(
    col        = "co2_kg_par_tkm",
    col.scale  = tm_scale_intervals(style = "quantile", n = 5,
                                    values = PALETTE_EMISSIONS),
    col.legend = tm_legend(title = "CO₂\n(kg / tonne-km)"),
    lwd        = 1.5
  ) +
  tm_shape(entreposages_sf) + tm_dots(fill = "black", size = 0.2) +
  tm_title(paste0("Intensité carbone du réseau — ", VEHICULES_IDS$nom[
    VEHICULES_IDS$vehicule_id == VEHICULE_REFERENCE
  ])) +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(carte_co2,
          file.path(DIR_OUTPUT, "carte_emissions_co2_par_tkm.png"),
          width = 3000, height = 2400, dpi = 300)
cat("  ✓ carte_emissions_co2_par_tkm.png\n")

# ── Carte : intensité NOx (nox_g_par_tkm) ─────────────────────────────────────
# Le NOx est le principal polluant local du transport routier diesel.
# Sa distribution spatiale diffère du CO2 : elle dépend davantage
# des normes Euro des moteurs (plus sévère en ville) et de la congestion
# (ralentissements → régime moteur sous-optimal → émissions NOx élevées).
carte_nox <- fond_carte() +
  tm_shape(reseau_rwanda %>% activate("edges") %>% st_as_sf()) +
  tm_lines(
    col        = "nox_g_par_tkm",
    col.scale  = tm_scale_intervals(style = "quantile", n = 5,
                                    values = PALETTE_EMISSIONS),
    col.legend = tm_legend(title = "NOx\n(g / tonne-km)"),
    lwd        = 1.5
  ) +
  tm_shape(entreposages_sf) + tm_dots(fill = "black", size = 0.2) +
  tm_title("Intensité NOx du réseau (pollution locale)") +
  tm_layout(legend.outside = TRUE, frame = TRUE) +
  tm_scalebar(position = c("left", "bottom")) +
  tm_compass(position  = c("right", "top"))

tmap_save(carte_nox,
          file.path(DIR_OUTPUT, "carte_emissions_nox_par_tkm.png"),
          width = 3000, height = 2400, dpi = 300)
cat("  ✓ carte_emissions_nox_par_tkm.png\n")

# ── Graphique : émissions totales comparées par véhicule ──────────────────────
# Ce graphique répond à : "Quel véhicule est le plus émetteur sur le réseau ?"
# en agrégeant les émissions absolues sur l'ensemble du réseau.
# La décomposition par composante (CO2, NOx, PM2.5) montre que le classement
# peut différer selon le polluant : un camion lourd émet plus de CO2 mais
# un camion plus vieux peut émettre disproportionnellement plus de PM2.5.
emissions_par_vehicule <- duck_query("
  SELECT
    vehicule_id,
    vehicule_nom,
    -- Émissions totales sur l'ensemble des arêtes du réseau
    ROUND(SUM(co2_kg)  / 1000, 1)  AS co2_total_t,    -- CO2 en tonnes
    ROUND(SUM(nox_g)   / 1000, 1)  AS nox_total_kg,   -- NOx en kg
    ROUND(SUM(pm25_g)  / 1000, 1)  AS pm25_total_kg,  -- PM2.5 en kg
    -- Intensités moyennes pondérées par la longueur de l'arête
    ROUND(AVG(co2_kg_par_tkm), 4)  AS co2_intensite_moy,
    ROUND(AVG(nox_g_par_tkm),  4)  AS nox_intensite_moy
  FROM aretes_couts_tous
  GROUP BY vehicule_id, vehicule_nom
  ORDER BY co2_total_t DESC
")

cat("\nÉmissions totales par véhicule (réseau complet) :\n")
print(emissions_par_vehicule)

# Graphique en barres groupées : CO2 / NOx / PM2.5 côte à côte par véhicule.
# On normalise chaque polluant sur sa propre échelle (valeur relative entre
# véhicules) car les ordres de grandeur sont très différents (tonnes vs grammes).
# scale() centre et normalise chaque colonne entre 0 et 1 (min-max scaling).
emissions_long <- emissions_par_vehicule %>%
  select(vehicule_nom, co2_total_t, nox_total_kg, pm25_total_kg) %>%
  pivot_longer(-vehicule_nom,
               names_to  = "Polluant",
               values_to = "Valeur") %>%
  # Renommage pour la légende du graphique
  mutate(
    Polluant = recode(Polluant,
                      "co2_total_t"   = "CO₂ (t)",
                      "nox_total_kg"  = "NOx (kg)",
                      "pm25_total_kg" = "PM2.5 (kg)"),
    # Normalisation min-max par polluant pour rendre les barres comparables
    # entre polluants qui n'ont pas les mêmes unités.
    # group_by + mutate permet ici de normaliser chaque polluant séparément.
    Valeur_norm = Valeur / max(Valeur)
  ) %>%
  group_by(Polluant) %>%
  mutate(Valeur_norm = Valeur / max(Valeur)) %>%
  ungroup()

g_emissions <- ggplot(emissions_long,
                      aes(x = vehicule_nom, y = Valeur_norm, fill = Polluant)) +
  geom_col(position = "dodge", width = 0.65) +
  scale_fill_manual(values = c("CO₂ (t)"    = "#D73027",
                               "NOx (kg)"   = "#FC8D59",
                               "PM2.5 (kg)" = "#4575B4")) +
  scale_y_continuous(labels = scales::percent_format()) +
  labs(
    title    = "Émissions relatives par véhicule et par polluant",
    subtitle = "Normalisé par rapport au véhicule le plus émetteur de chaque polluant (100%)",
    x        = NULL,
    y        = "Niveau relatif d'émission",
    fill     = "Polluant"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#666666"),
    legend.position = "top"
  )

ggsave(file.path(DIR_OUTPUT, "graphique_emissions_par_vehicule.png"),
       g_emissions, width = 10, height = 6, dpi = 300)
cat("  ✓ graphique_emissions_par_vehicule.png\n\n")

