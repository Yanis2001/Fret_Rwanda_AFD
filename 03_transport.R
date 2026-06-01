################################################################################
# 03_transport.R
# RÔLE : Matrice OD par Dijkstra multi-modal, modèle Input-Output de Leontief,
#        modèle gravitaire sectoriel, affectation All-or-Nothing.
# ENTRÉES  : persist_reseau_couts.rds, persist_graphe_mm.rds,
#            persist_mapping_mm.rds, persist_entreposages.rds + DuckDB
# SORTIES  : persist_flux_fret.rds, persist_reseau_fret.rds
#            + od_cache.rds (déjà existant), affectation_cache.rds,
#            exports CSV/Parquet/GeoPackage
# DÉPEND DE : 00_parametres.R, 01_reseau.R, 02_couts.R
################################################################################

source("00_parametres.R")

cat("=== Chargement des objets de 01_reseau + 02_couts ===\n")

.ent  <- readRDS(PERSIST_ENTREPOSAGES)
.res  <- readRDS(PERSIST_RESEAU_COUTS)
.mm   <- readRDS(PERSIST_GRAPHE_MM)
.map  <- readRDS(PERSIST_MAPPING_MM)

list2env(.ent, envir = .GlobalEnv)
reseau_rwanda      <- .res$reseau_rwanda
n_noeuds           <- .mm$n_noeuds
n_vehicules        <- .mm$n_vehicules
n_aretes_physiques <- length(.map$lookup_type[.map$lookup_type == "route"]) / .mm$n_vehicules

# Recharger le graphe dans env_lourds
stocker_lourd("graphe_multimodal", .mm$graphe_multimodal)

# Récupérer les vecteurs de lookup
mapping_aretes_mm <- .map$mapping_aretes_mm
lookup_type       <- .map$lookup_type
lookup_physique   <- .map$lookup_physique
lookup_vehicule   <- .map$lookup_vehicule
max_idx_mm        <- .map$max_idx_mm
poids_mm          <- .map$poids_mm   # vecteur pré-extrait

rm(.ent, .res, .mm, .map)

# Redéfinition locale de node_multi avec n_noeuds chargé
node_multi <- function(v_idx, n_id) as.integer((v_idx - 1L) * n_noeuds + n_id)

cat("✓ Objets chargés\n\n")

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
CACHE_OD <- file.path(DIR_CACHE, "od_cache.rds")
cache_od_valide <- FALSE

# Colonnes attendues dans od_long — à mettre à jour si la structure change
OD_COLONNES_ATTENDUES <- c(
  "id_origine", "id_destination", "nom_origine", "nom_destination",
  "cout_usd", "distance_km", "temps_h",
  "vehicule_depart", "vehicule_arrivee", "n_transbordements",
  "co2_kg_trajet", "nox_g_trajet", "pm25_g_trajet"
)

# ── Empreinte de version du script ────────────────────────────────────────────
# Hash des paramètres structurels qui, s'ils changent, invalident le cache
empreinte_params <- digest::digest(
  list(
    vehicule_reference = VEHICULE_REFERENCE,
    n_vehicules        = nrow(VEHICULES_IDS),
    vehicules_ids      = sort(VEHICULES_IDS$vehicule_id)
  ),
  algo = "xxhash64"
)

# file.exists() : vérifie si le fichier cache existe déjà sur le disque.
if (file.exists(CACHE_OD)) {
  cache_od <- readRDS(CACHE_OD)  # Charge le fichier sauvegardé en mémoire R
  
  # ── Vérification 1 : métriques quantitatives ────────────────────────────────
  metriques_ok <- !is.null(cache_od$n_warehouses) &&
    cache_od$n_warehouses == n_warehouses &&
    !is.null(cache_od$n_aretes) &&
    cache_od$n_aretes == n_aretes_physiques
  
  # ── Vérification 2 : structure de od_long (colonnes présentes) ──────────────
  colonnes_ok <- !is.null(cache_od$od_long) &&
    all(OD_COLONNES_ATTENDUES %in% names(cache_od$od_long))
  
  
  version_ok <- !is.null(cache_od$empreinte_params) &&
    cache_od$empreinte_params == empreinte_params
  
  # ── Bilan ───────────────────────────────────────────────────────────────────
  if (metriques_ok && colonnes_ok && version_ok) {
    
    od_long         <- cache_od$od_long
    cache_od_valide <- TRUE
    
    cat("  ✓ Cache OD valide\n")
    cat("    Zones    :", n_warehouses, "\n")
    cat("    Arêtes   :", n_aretes_physiques, "\n")
    cat("    Colonnes :", ncol(od_long), "(", 
        length(OD_COLONNES_ATTENDUES), "attendues)\n\n")
    
  } else {
    
    # Diagnostic précis de la raison d'invalidation
    cat("  ⚠ Cache OD invalide — recalcul Dijkstra\n")
    if (!metriques_ok) {
      cat("    → Raison : métriques réseau modifiées\n")
      cat("      Cache : n_warehouses =", cache_od$n_warehouses,
          "| n_aretes =", cache_od$n_aretes, "\n")
      cat("      Actuel: n_warehouses =", n_warehouses,
          "| n_aretes =", n_aretes_physiques, "\n")
    }
    if (!colonnes_ok) {
      cat("    → Raison : structure de od_long modifiée\n")
      colonnes_manquantes <- setdiff(OD_COLONNES_ATTENDUES, 
                                     names(cache_od$od_long))
      colonnes_inattendues <- setdiff(names(cache_od$od_long), 
                                      OD_COLONNES_ATTENDUES)
      if (length(colonnes_manquantes)  > 0)
        cat("      Colonnes manquantes :", 
            paste(colonnes_manquantes, collapse = ", "), "\n")
      if (length(colonnes_inattendues) > 0)
        cat("      Colonnes inattendues:", 
            paste(colonnes_inattendues, collapse = ", "), "\n")
    }
    if (!version_ok) {
      cat("    → Raison : paramètres de flotte modifiés\n")
      cat("      Relancer la Partie V avant la Partie VI\n")
    }
    cat("\n")
  }
}

# Si pas de cache valide, on lance le calcul complet puis on sauvegarde.
if (!cache_od_valide) {
  
  # ── Extraction unique des attributs d'arêtes du graphe multi-modal ──────────
  e_length_km     <- igraph::E(recuperer_lourd("graphe_multimodal"))$length_km
  e_travel_time_h <- igraph::E(recuperer_lourd("graphe_multimodal"))$travel_time_h
  e_co2_kg        <- igraph::E(recuperer_lourd("graphe_multimodal"))$co2_kg
  e_nox_g         <- igraph::E(recuperer_lourd("graphe_multimodal"))$nox_g
  e_pm25_g        <- igraph::E(recuperer_lourd("graphe_multimodal"))$pm25_g
  e_type          <- igraph::E(recuperer_lourd("graphe_multimodal"))$type
  e_weight        <- igraph::E(recuperer_lourd("graphe_multimodal"))$weight
  
  cat("✓ Attributs d'arêtes extraits une seule fois (économie mémoire)\n\n")
  
  # ── Pré-allocation du data.frame de résultats ───────────────────────────────
  # n_paires_max = nombre maximal de paires OD possibles (sans la diagonale i=j)
  n_paires_max <- n_warehouses * (n_warehouses - 1)
  
  # data.frame() avec des vecteurs de la bonne taille et du bon type :
  # integer(n)   crée un vecteur d'entiers initialisé à 0
  # numeric(n)   crée un vecteur de doubles  initialisé à 0
  # character(n) crée un vecteur de chaînes  initialisé à ""
  # stringsAsFactors = FALSE évite la conversion automatique en facteurs
  # (comportement par défaut depuis R 4.0 mais on l'explicite par sécurité)
  od_long <- data.frame(
    id_origine        = integer(n_paires_max),
    id_destination    = integer(n_paires_max),
    nom_origine       = character(n_paires_max),
    nom_destination   = character(n_paires_max),
    cout_usd          = numeric(n_paires_max),
    distance_km       = numeric(n_paires_max),
    temps_h           = numeric(n_paires_max),
    vehicule_depart   = character(n_paires_max),
    vehicule_arrivee  = character(n_paires_max),
    n_transbordements = integer(n_paires_max),
    co2_kg_trajet     = numeric(n_paires_max),
    nox_g_trajet      = numeric(n_paires_max),
    pm25_g_trajet     = numeric(n_paires_max),
    stringsAsFactors  = FALSE
  )
  
  # idx : compteur de la ligne courante dans od_long.
  # 0L au lieu de 0 pour forcer le type entier (économie mémoire marginale
  # mais cohérence avec les compteurs entiers de la boucle).
  idx <- 0L
  
  # ── Système de checkpoint pour reprise après crash ──────────────────────────
  # Le calcul OD prend 30+ minutes sur le graphe multi-modal. 
  # Avec un checkpoint sauvegardé tous les 10 origines, on peut reprendre
  # là où on s'est arrêté — utile sur SSP Cloud où les crashs sont fréquents.
  # Le checkpoint est différent du cache final : il sert UNIQUEMENT à reprendre
  # un calcul interrompu. Il sera supprimé automatiquement à la fin du calcul.
  CHECKPOINT_OD <- file.path(DIR_CACHE, "od_checkpoint.rds")
  i_start <- 1L  # Origine de départ par défaut
  
  if (file.exists(CHECKPOINT_OD)) {
    
    cat("=== Checkpoint OD trouvé — reprise du calcul ===\n")
    cp <- readRDS(CHECKPOINT_OD)
    
    # Vérification que le checkpoint correspond à la session actuelle.
    # Si le réseau a changé (n_warehouses différent), on ignore le checkpoint
    # car les indices d'origine ne correspondraient plus.
    if (!is.null(cp$n_warehouses) && cp$n_warehouses == n_warehouses) {
      od_long <- cp$od_long
      idx     <- cp$idx
      i_start <- cp$i_next
      cat("  ✓ Reprise depuis l'origine", i_start, 
          "(", idx, "paires déjà calculées)\n\n")
    } else {
      cat("  ⚠ Checkpoint incompatible avec la session — calcul depuis 0\n\n")
      file.remove(CHECKPOINT_OD)
    }
  }
  
  # ── Boucle principale : Dijkstra multi-modal pour chaque origine ────────────
  
  cat("=== Calcul de la matrice OD multi-modale ===\n")
  cat("  Origines à traiter :", length(warehouse_nodes_base) - i_start + 1, "\n")
  cat("  Paires totales     :", n_paires_max, "\n\n")
  
  for (i in i_start:length(warehouse_nodes_base)) {
    
    # sources_i : indices des nœuds dans le graphe multi-modal correspondant
    # à l'entrepôt i dans chacune des 3 couches véhicule.
    # as.integer() force le type entier (igraph::distances exige des entiers)
    sources_i <- as.integer(sapply(seq_len(n_vehicules),
                                   function(v) node_multi(v, warehouse_nodes_base[i])))
    
    # Tous les nœuds destination (toutes couches × tous entrepôts) en une passe
    # as.vector() aplatit la matrice retournée par sapply en vecteur 1D
    targets_all <- as.integer(as.vector(sapply(
      seq_len(n_vehicules),
      function(v) node_multi(v, warehouse_nodes_base)
    )))
    
    # igraph::distances() : calcule les distances de Dijkstra depuis plusieurs
    # sources vers plusieurs cibles en une seule passe.
    # weights = e_weight : on utilise le vecteur d'attributs pré-extrait
    # plutôt que de relire E(graphe)$weight à chaque appel.
    # Résultat : matrice n_sources × n_targets de coûts optimaux.
    # Inf dans la matrice = impossible d'atteindre la cible depuis la source.
    dists_all <- igraph::distances(
      recuperer_lourd("graphe_multimodal"),
      v       = sources_i,
      to      = targets_all,
      weights = e_weight
    )
    
    for (j in seq_along(warehouse_nodes_base)) {
      if (i == j) next  # Pas de paire (i,i)
      
      # cols_j : colonnes de dists_all correspondant à l'entrepôt j
      # dans toutes les couches véhicule.
      cols_j   <- j + (seq_len(n_vehicules) - 1) * length(warehouse_nodes_base)
      
      # drop = FALSE : conserve la structure matricielle même si un seul
      # élément est extrait (évite que R "dégrade" en vecteur 1D)
      sub_dists <- dists_all[, cols_j, drop = FALSE]
      min_cout  <- min(sub_dists, na.rm = TRUE)
      if (is.infinite(min_cout)) next  # Entrepôts non connectés → pas de flux
      
      # Identifier la meilleure combinaison (couche départ, couche arrivée)
      # which(... arr.ind = TRUE) renvoie les indices ligne ET colonne.
      # [1, ] : on prend la première solution si plusieurs ont le même coût min.
      best_idx  <- which(sub_dists == min_cout, arr.ind = TRUE)[1, ]
      best_from <- sources_i[best_idx[1]]
      best_to   <- targets_all[cols_j[best_idx[2]]]
      
      # igraph::shortest_paths() : récupère le chemin lui-même (pas seulement
      # le coût). output = "epath" → retourne les indices des arêtes empruntées.
      path_obj  <- igraph::shortest_paths(
        recuperer_lourd("graphe_multimodal"),
        from    = best_from,
        to      = best_to,
        weights = e_weight,
        output  = "epath"
      )
      edges_path <- as.integer(path_obj$epath[[1]])
      
      # rm(path_obj) : libère immédiatement l'objet path_obj.
      # Sur 14 000 itérations, ces petits objets s'accumulent dans la RAM
      # entre les passages du garbage collector. Les supprimer explicitement
      # aide à maintenir la RAM stable.
      rm(path_obj)
      
      # Incrémentation du compteur et écriture directe dans le data.frame.
      # idx + 1L : 1L force l'addition en entier (sinon R promeut en double).
      # od_long$colonne[idx] <- valeur : écriture en place, pas de copie.
      idx <- idx + 1L
      
      od_long$id_origine[idx]        <- i
      od_long$id_destination[idx]    <- j
      od_long$nom_origine[idx]       <- noeuds_entreposage$warehouse_name[i]
      od_long$nom_destination[idx]   <- noeuds_entreposage$warehouse_name[j]
      od_long$cout_usd[idx]          <- min_cout
      
      # Distance, temps et émissions cumulées sur le chemin optimal.
      # On utilise les vecteurs pré-extraits (e_length_km, etc.) au lieu
      # de edge_data$length_km recalculé à chaque itération.
      # sum(..., na.rm = TRUE) agrège sur toutes les arêtes du chemin.
      od_long$distance_km[idx]       <- sum(e_length_km[edges_path],     na.rm = TRUE)
      od_long$temps_h[idx]           <- sum(e_travel_time_h[edges_path], na.rm = TRUE)
      od_long$vehicule_depart[idx]   <- VEHICULES_IDS$vehicule_id[best_idx[1]]
      od_long$vehicule_arrivee[idx]  <- VEHICULES_IDS$vehicule_id[best_idx[2]]
      
      # Comptage des transbordements : arêtes de type "transbordement"
      # sur le chemin (changements de véhicule dans un entrepôt intermédiaire).
      od_long$n_transbordements[idx] <- sum(e_type[edges_path] == "transbordement")
      
      # Émissions cumulées sur le trajet optimal (un trajet chargé à pleine
      # capacité entre i et j, en suivant le chemin de moindre coût).
      # Les arêtes de transbordement ont des émissions = 0 (opération
      # stationnaire dans un entrepôt, non modélisée ici).
      od_long$co2_kg_trajet[idx]     <- sum(e_co2_kg[edges_path],  na.rm = TRUE)
      od_long$nox_g_trajet[idx]      <- sum(e_nox_g[edges_path],   na.rm = TRUE)
      od_long$pm25_g_trajet[idx]     <- sum(e_pm25_g[edges_path],  na.rm = TRUE)
    }
    
    # Nettoyage de la matrice de distances avant l'itération suivante.
    # dists_all peut faire 50-100 Mo selon la taille du graphe.
    rm(dists_all)
    
    # Checkpoint et garbage collection tous les 10 origines.
    # Ce rythme est un compromis : trop fréquent → ralentit le calcul,
    # trop espacé → on perd plus de travail en cas de crash.
    if (i %% 10 == 0 || i == length(warehouse_nodes_base)) {
      
      # gc() force la libération mémoire. verbose = FALSE supprime l'affichage.
      invisible(gc(verbose = FALSE))
      
      # Sauvegarde du checkpoint : on stocke od_long, idx, i_next (pour reprise)
      # et n_warehouses (pour validation au rechargement).
      saveRDS(
        list(
          od_long      = od_long,
          idx          = idx,
          i_next       = i + 1L,
          n_warehouses = n_warehouses
        ),
        CHECKPOINT_OD
      )
      
      # Affichage de la progression et du suivi RAM.
      # sum(gc()[, 2]) somme les colonnes "used (MB)" du tableau gc()
      # pour donner la RAM totale utilisée par R à ce moment.
      ram_mb <- round(sum(gc()[, 2]), 0)
      cat("  OD multi-modal :",
          round(i / length(warehouse_nodes_base) * 100, 1), "% —",
          "RAM :", ram_mb, "MB —",
          "paires :", idx, "\n")
    }
  }
  
  # ── Finalisation : tronquer le data.frame à sa taille réelle ────────────────
  # On a pré-alloué n_paires_max lignes mais certaines paires peuvent ne pas
  # être connectées (cout = Inf, on les a "next" dans la boucle).
  # Les lignes correspondantes restent à leurs valeurs initiales (0, "").
  # On ne garde donc que les idx premières lignes effectivement remplies.
  od_long <- od_long[seq_len(idx), ]
  
  # Suppression du checkpoint maintenant que le calcul est terminé avec succès.
  # Le cache final (CACHE_OD) prend le relais pour les prochaines sessions.
  if (file.exists(CHECKPOINT_OD)) {
    file.remove(CHECKPOINT_OD)
    cat("  ✓ Checkpoint supprimé (calcul terminé avec succès)\n")
  }
  
  cat("\n✓ Matrice OD calculée :", nrow(od_long), "paires connectées\n\n")
  
  # saveRDS() : sauvegarde l'objet R dans un fichier binaire sur le disque.
  # On sauvegarde à la fois les résultats (od_long) et les paramètres de
  # validation (n_warehouses, n_aretes) pour pouvoir vérifier la validité
  # du cache lors des prochaines exécutions.
  saveRDS(
    list(
      od_long            = od_long,
      n_warehouses       = n_warehouses,
      n_aretes           = n_aretes_physiques,
      empreinte_params   = empreinte_params,       
      colonnes_od        = names(od_long),         
      date_creation      = Sys.time()
    ),
    CACHE_OD
  )
  cat("  ✓ Cache OD sauvegardé :", CACHE_OD, "\n")
  cat("  Colonnes sauvegardées :", paste(names(od_long), collapse = ", "), "\n\n")

}

# duck_write est hors du bloc if : il s'exécute que od_long vienne du calcul
# ou du cache, pour que matrice_od soit toujours disponible dans DuckDB.
duck_write(od_long, "matrice_od")

# Statistiques enrichies — placées après duck_write pour que matrice_od
# existe dans DuckDB, quel que soit le chemin emprunté (calcul ou cache).
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

# ── Construction de la table des arêtes finales enrichie ──────────────────────
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
st_write(aretes_finales, file.path(DIR_EXPORTS,"reseau_rwanda_aretes.gpkg"),
         delete_dsn=TRUE, quiet=TRUE)

noeuds_finaux <- reseau_rwanda %>%
  activate("nodes") %>%
  st_as_sf() %>%
  select(node_id, is_warehouse, warehouse_name, warehouse_type)
st_write(noeuds_finaux, file.path(DIR_EXPORTS, "reseau_rwanda_noeuds.gpkg"),
         delete_dsn=TRUE, quiet=TRUE)

# Exports CSV depuis DuckDB via COPY TO
# HEADER = TRUE : inclure les noms de colonnes en première ligne du fichier
dbExecute(con, paste0(
  "COPY (SELECT * FROM matrice_od) TO '",
  file.path(DIR_EXPORTS, "matrice_od_long.csv"),
  "' (FORMAT CSV, HEADER)"
))
dbExecute(con, paste0(
  "COPY (SELECT * FROM aretes_finales) TO '",
  file.path(DIR_EXPORTS,'aretes_finales.csv'),
  "'(FORMAT CSV, HEADER)"
))
dbExecute(con, paste0(
  "COPY (SELECT * FROM couts_prebordure) TO '",
  file.path(DIR_EXPORTS, "couts_prebordure.csv"),
  "' (FORMAT CSV, HEADER)"
))
cat("  ✓ couts_prebordure.csv\n")

# Exports Parquet depuis DuckDB
# Lisible directement avec : Python → pd.read_parquet() ; R → arrow::read_parquet()
dbExecute(con, paste0(
  "COPY (SELECT * FROM aretes_finales) TO '",
  file.path(DIR_EXPORTS, 'aretes_finales.parquet'), 
  "'(FORMAT PARQUET)"
))
dbExecute(con, paste0(
  "COPY (SELECT * FROM matrice_od) TO '",
  file.path(DIR_EXPORTS, 'matrice_od.parquet'), 
  "'(FORMAT PARQUET)"
))

cat("✓ Exports CSV + Parquet via DuckDB COPY TO\n\n")


################################################################################
# PARTIE VII — MODÈLE ÉCONOMIQUE
# Construit la chaîne économique complète :
#   Table IO → multiplicateurs Leontief → offres/demandes par zone
#   → modèle gravitaire avec friction sur coûts de transport (C_ij)
#     et nœuds RoW virtuels intégrant les coûts pré-frontière.
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

# ── Grandeurs dérivées de la table IO ─────────────────────────────────────────
# %*% : produit matriciel en R (différent de * qui est une multiplication élément par élément).
# A %*% x donne le vecteur des consommations intermédiaires : pour chaque secteur i,
# la somme de a_ij × production_j sur tous les secteurs j fournisseurs.
conso_interm   <- as.vector(A %*% production_totale)
# Valeur ajoutée = production - consommations intermédiaires
valeur_ajoutee <- production_totale - conso_interm
# Demande finale calibrée sur les comptes nationaux NISR (Supply-Use Tables 2022).
# DEMANDE_FINALE_NISR est un vecteur par secteur en M USD, défini dans 00_parametres.R.
# On réordonne selon l'ordre canonique SECTEURS pour garantir l'alignement des indices.
demande_finale <- DEMANDE_FINALE_NISR[SECTEURS]

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
# VII.2 : Offres et demandes par zone (modèle MRIO)
# Calcule pour chaque zone la production locale x[i,s], la demande totale
# d[i,s] (intermédiaire + finale), et en déduit les surplus/déficits sectoriels
# qui servent d'offre et de demande dans le modèle gravitaire.
# ==============================================================================

# Génération des matrices offre et demande (lignes = zones, colonnes = secteurs)
# matrix(0, n, m) : crée une matrice de zéros de dimensions n×m.
# dimnames : noms des lignes (zones) et colonnes (secteurs) pour lisibilité.
offre_zones   <- matrix(0, n_warehouses, N_SECTEURS,
                        dimnames=list(noeuds_entreposage$warehouse_name, SECTEURS))
demande_zones <- matrix(0, n_warehouses, N_SECTEURS,
                        dimnames=list(noeuds_entreposage$warehouse_name, SECTEURS))

# ==============================================================================
# VII.2.B : Modèle MRIO — allocation de la production et de la demande
#
# PRINCIPE (Input-Output interrégional) :
#   Offre et demande de chaque zone découlent de la même identité comptable,
#   garantissant l'équilibre agrégé avant le modèle gravitaire.
#
#   Production locale (allocation composite emploi + RWI) :
#     w[i,s] = α × (emp[i,s] / emp_national[s])
#            + (1-α) × (p_rwi[i] / Σ_j p_rwi[j])
#     x[i,s] = production_totale[s] × w[i,s]
#     α = ALPHA_EMPLOI_RWI (00_parametres.R) ; par construction Σ_i w[i,s] = 1.
#     Justification : forme additive car le RWI n'est pas sectoriel —
#     il agit comme un correcteur de productivité uniforme sur tous les secteurs.
#
#   Demande intermédiaire (via la matrice I/O nationale) :
#     d_inter[i,s] = Σ_r  A[s,r] × x[i,r]
#     → intrants du secteur s nécessaires à la production de tous les secteurs r.
#
#   Demande finale (allocation multiplicative population × RWI) :
#     z[i] = pop[i] × (p_rwi[i] + EPSILON_RWI)
#     d_finale[i,s] = demande_finale[s] × (z[i] / Σ_j z[j])
#     Justification : pop × richesse ≈ masse monétaire locale (Chi et al. 2022,
#     PNAS) ; EPSILON_RWI évite qu'une zone très pauvre mais peuplée reçoive
#     un poids nul.
#
#   Demande totale
#     d[i,s] = d_finale[i,s] + d_inter[i,s]
#
#   Surplus exportable et besoin importé :
#     offre_zones[i,s]   = max(0,  x[i,s] − d[i,s])
#     demande_zones[i,s] = max(0,  d[i,s] − x[i,s])
#
# ÉQUILIBRE AGRÉGÉ :
#   Σ_i x[i,s] = production_totale[s]  (par construction : Σ_i w[i,s] = 1)
#   Σ_i d[i,s] ≈ production_totale[s]  (identité ressources-emplois Leontief)
#   → Σ_i offre[i,s] ≈ Σ_i demande[i,s] ; les résidus = commerce avec le RoW.
#   La normalisation par moyenne géométrique dans furness_gravity() absorbe
#   ces résidus d'économie ouverte.
# ==============================================================================

# Emploi national par secteur = somme sur toutes les zones actives.
# Garantit que Σ_i x[i,s] = production_totale[s] (la production est entièrement
# allouée aux zones, sans fuite hors réseau).
emploi_national <- colSums(emploi_zone_secteur, na.rm = TRUE)
# Protection contre division par zéro si un secteur est absent du RPHC5.
emploi_national[emploi_national == 0] <- 1

# Population totale des zones actives — dénominateur de la demande finale.
pop_totale <- sum(pop_i, na.rm = TRUE)
if (pop_totale == 0) stop("pop_totale est nulle — vérifier diag_population dans persist.")

# ── Extraction du RWI normalisé aligné sur noeuds_entreposage ────────────────
# p_rwi est calculé dans 01_reseau.R (min-max sur [0,1]) et stocké dans diag_rwi.
# On réaligne sur noeuds_entreposage (même logique que pop_i ci-dessus).
p_rwi_zones <- diag_rwi$p_rwi[
  match(noeuds_entreposage$warehouse_name, diag_rwi$nom_zone)
]
p_rwi_zones <- replace_na(p_rwi_zones, median(p_rwi_zones, na.rm = TRUE))
stopifnot(length(p_rwi_zones) == n_warehouses)

# Dénominateur du terme RWI dans le poids de production (Σ_i p_rwi_i).
rwi_total <- sum(p_rwi_zones)
if (rwi_total == 0) stop("rwi_total est nul — vérifier diag_rwi dans persist.")

# Poids de demande finale z[i] = pop[i] × (p_rwi[i] + ε) — forme multiplicative.
# EPSILON_RWI évite un poids nul pour les zones très pauvres mais peuplées.
z_demande <- pop_i * (p_rwi_zones + EPSILON_RWI)
z_totale   <- sum(z_demande)
if (z_totale == 0) stop("z_totale est nul — vérifier pop_i et p_rwi_zones.")

cat("  Emploi national par secteur (dénominateur MRIO) :\n")
print(round(emploi_national))
cat("  Population totale zones actives :", round(pop_totale), "\n")
cat("  RWI total zones actives :", round(rwi_total, 3),
    "| z_totale (pop×rwi) :", round(z_totale), "\n\n")

for (i in seq_len(n_warehouses)) {

  # ── Production locale x[i,s] ────────────────────────────────────────────────
  # Poids composite w[i,s] = α × part_emploi[i,s] + (1-α) × part_rwi[i].
  # La part RWI est scalaire (même valeur pour tous les secteurs) car le RWI
  # est une caractéristique géographique de la zone, pas sectorielle.
  emp_i      <- emploi_zone_secteur[i, ]           # vecteur N_SECTEURS (effectifs bruts)
  part_emp_i <- emp_i / emploi_national            # part d'emploi par secteur (Σ_i = 1)
  part_rwi_i <- p_rwi_zones[i] / rwi_total         # part RWI scalaire (Σ_i = 1)
  w_i        <- ALPHA_EMPLOI_RWI * part_emp_i + (1 - ALPHA_EMPLOI_RWI) * part_rwi_i
  x_i        <- production_totale * w_i            # vecteur N_SECTEURS (M USD)

  # ── Demande intermédiaire d_inter[i,s] ──────────────────────────────────────
  # Pour chaque secteur s, quantité consommée comme intrant par la production
  # locale de tous les secteurs r : Σ_r A[s,r] × x[i,r] = (A %*% x_i)[s].
  # A %*% x_i : produit matriciel (N_SECTEURS × N_SECTEURS) × (N_SECTEURS) → N_SECTEURS.
  d_inter_i <- as.vector(A %*% x_i)
  names(d_inter_i) <- SECTEURS

  # ── Demande finale d_finale[i,s] ────────────────────────────────────────────
  # Pondération multiplicative z[i] = pop[i] × (p_rwi[i] + ε), calculée hors boucle.
  # Capture la masse monétaire locale (population × pouvoir d'achat).
  d_finale_i <- demande_finale * (z_demande[i] / z_totale)  # vecteur N_SECTEURS (M USD)

  # ── Demande totale et surplus/déficit ───────────────────────────────────────
  d_i <- d_inter_i + d_finale_i

  # pmax(0, ...) : max élément par élément entre 0 et le vecteur → remplace les
  # valeurs négatives par 0 (une zone ne peut pas avoir d'offre négative).
  offre_zones[i, ]   <- pmax(0, x_i - d_i)   # surplus exportable (M USD)
  demande_zones[i, ] <- pmax(0, d_i - x_i)   # besoin importé     (M USD)
}

# ── Stockage dans DuckDB des zones domestiques (format long) ─────────────────
# Format long (1 ligne = 1 zone × 1 secteur) plus adapté aux jointures SQL.
# Seules les zones domestiques (n_warehouses) sont stockées ici ; les lignes
# RoW seront ajoutées ci-dessous dans une table séparée (offre_zones_row).
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

cat("✓ Offres et demandes domestiques stockées dans DuckDB\n\n")

# ==============================================================================
# VII.2.C : Couche virtuelle RoW (Rest of World)
#
# PRINCIPE :
#   Les pays frontaliers (Ouganda, Tanzanie, RDC, Burundi) sont représentés comme
#   n_row = 4 nœuds virtuels ajoutés APRÈS les n_warehouses zones domestiques.
#   Ils ne snappent pas au réseau routier : leur coût vers toute destination j est
#   calculé comme le minimum sur les postes frontières b du pays :
#
#     C[RoW_pays, j, s] = min_b ( couts_prebordure[pays, s] + C_road[b, j] )
#
#   Ce minimum choisit automatiquement le poste frontière optimal pour chaque
#   destination — permettant par exemple à l'Ouganda d'utiliser Gatuna OU
#   Kagitumba selon la destination rwandaise.
#
#   Offre/demande des nœuds RoW : données de commerce extérieur NISR
#     offre[RoW_pays, s]   = importations du Rwanda   (M USD)
#     demande[RoW_pays, s] = exportations Rwanda (M USD)
#
# AFFECTATION AUX ROUTES :
#   Après le modèle gravitaire, les flux T[RoW_k, j] sont projetés sur le
#   nœud frontière b*(j) = argmin_b C_road[b,j] avant l'affectation All-or-Nothing.
#   Le segment pré-frontière (étranger) n'emprunte aucune route rwandaise.
# ==============================================================================

# ── Pays RoW et leurs postes frontières ──────────────────────────────────────
# COMMERCE_EXTERIEUR_NISR définit les pays RoW (ordre canonique).
# On récupère aussi les coûts pré-frontière depuis DuckDB.
pays_row <- unique(COMMERCE_EXTERIEUR_NISR$pays)   # vecteur des pays RoW
n_row    <- length(pays_row)                        # = 4

# Récupérer le pays de chaque poste frontière (depuis entreposages_fictifs).
# idx_frontiere_par_pays : tibble (idx = position dans noeuds_entreposage, pays)
# Utilisé plus bas pour calculer C[RoW,j] et pour la projection des flux.
idx_frontiere_par_pays <- noeuds_entreposage %>%
  mutate(idx = seq_len(n())) %>%
  left_join(
    entreposages_fictifs %>% select(nom, pays),
    by = c("warehouse_name" = "nom")
  ) %>%
  filter(warehouse_type == "frontiere", !is.na(pays)) %>%
  select(idx, warehouse_name, pays)

cat("  Postes frontières par pays :\n")
print(idx_frontiere_par_pays %>% select(warehouse_name, pays))

# Coûts pré-frontière depuis DuckDB (pays × secteur → cout_usd_tonne)
couts_prebordure <- duck_query("SELECT * FROM couts_prebordure")

# ── Extension des matrices offre/demande ──────────────────────────────────────
# offre_total   : (n_warehouses + n_row) × N_SECTEURS
# demande_total : (n_warehouses + n_row) × N_SECTEURS
# Les lignes 1..n_warehouses = zones domestiques (MRIO)
# Les lignes (n_warehouses+1)..(n_warehouses+n_row) = nœuds RoW (NISR commerce)
noms_row   <- paste0("RoW_", pays_row)
n_total    <- n_warehouses + n_row

offre_total <- rbind(
  offre_zones,
  matrix(0, n_row, N_SECTEURS, dimnames = list(noms_row, SECTEURS))
)
demande_total <- rbind(
  demande_zones,
  matrix(0, n_row, N_SECTEURS, dimnames = list(noms_row, SECTEURS))
)

# Remplir les lignes RoW depuis COMMERCE_EXTERIEUR_NISR
for (k in seq_along(pays_row)) {
  pays_k     <- pays_row[k]
  idx_k      <- n_warehouses + k
  commerce_k <- COMMERCE_EXTERIEUR_NISR %>% filter(pays == pays_k)
  for (s in SECTEURS) {
    row_s <- commerce_k %>% filter(secteur == s)
    # imports rwandais depuis le pays k = ce que le nœud RoW envoie vers le Rwanda
    offre_total[idx_k, s]   <- if (nrow(row_s) > 0) row_s$imports_musd else 0
    # exports rwandais vers le pays k = ce que le nœud RoW attire depuis le Rwanda
    demande_total[idx_k, s] <- if (nrow(row_s) > 0) row_s$exports_musd else 0
  }
}

# Stockage DuckDB des lignes RoW pour les visualisations
offre_row_df <- as.data.frame(offre_total[(n_warehouses+1):n_total, , drop = FALSE]) %>%
  rownames_to_column("zone") %>%
  pivot_longer(-zone, names_to = "secteur", values_to = "offre_musd")
duck_write(offre_row_df, "offre_zones_row")

demande_row_df <- as.data.frame(demande_total[(n_warehouses+1):n_total, , drop = FALSE]) %>%
  rownames_to_column("zone") %>%
  pivot_longer(-zone, names_to = "secteur", values_to = "demande_musd")
duck_write(demande_row_df, "demande_zones_row")

cat("✓ Couche RoW construite :", n_row, "pays,", n_total, "nœuds au total\n\n")

cat("Paramètres du modèle gravitaire:\n")
for (s in SECTEURS) {
  cat("  β(", s, ") =", BETA_SECTEUR[s], "\n")
}
cat("\n")

# ── Reconstruction de la matrice de coûts routiers (domestique) ───────────────
# On passe de la matrice OD format long (DuckDB, 1 ligne = 1 paire OD)
# au format matriciel carré (R, n_warehouses × n_warehouses) pour le calcul gravitaire.
# Cette matrice C_ij est étendue à n_total dans la boucle gravitaire ci-dessous.
matrice_couts <- matrix(0, n_warehouses, n_warehouses,
                        dimnames = list(noeuds_entreposage$warehouse_name,
                                        noeuds_entreposage$warehouse_name))
for (r in seq_len(nrow(od_long))) {
  i <- od_long$id_origine[r]; j <- od_long$id_destination[r]
  matrice_couts[i, j] <- od_long$cout_usd[r]
}

C_ij <- matrice_couts
diag(C_ij) <- NA      # Pas d'échange intrazone
C_ij[C_ij == 0] <- NA # Zones non connectées → pas de flux

# ==============================================================================
# VII.3 : Coût fixe de chargement/déchargement par véhicule
#
# PRINCIPE :
#   Tout trajet de fret implique deux opérations fixes indépendantes de la
#   distance : le chargement à l'origine et le déchargement à la destination.
#   Ces opérations mobilisent de la main-d'œuvre, du matériel (hayon, chariot)
#   et du temps — qu'on livre à 2 km ou à 200 km.
#
# RÔLE DANS LE MODÈLE GRAVITAIRE :
#   En ajoutant ce coût fixe à C_ij, le coût généralisé total d'un trajet
#   ne peut jamais descendre en dessous du coût de manutention, même pour deux
#   zones quasi-colocalisées. Cela évite l'explosion du terme C_ij^(-beta) qui
#   produisait des flux irréalistes entre zones très proches (ex : Karongi/Kibuye).
#
#   C_ij_total = C_ij_trajet + cout_fixe_par_tonne[vehicule]
#                ↑                  ↑
#          coût variable      coût fixe (présent même si distance → 0)
#
# CALCUL :
#   cout_fixe_par_tonne[v] = (cout_chargement_usd[v] + cout_dechargement_usd[v])
#                            / capacite_tonnes[v]
#
#   La division par la capacité convertit un coût par trajet (USD/trajet) en
#   coût par tonne transportée (USD/tonne) — l'unité attendue par C_ij.
#
#   Ce coût par tonne décroît avec la capacité du véhicule : un camion lourd
#   (20t) a un coût de manutention par tonne plus faible qu'une camionnette
#   (3.5t), même si son coût de manutention absolu est plus élevé. C'est
#   l'économie d'échelle à la manutention.
#
#   Exemple numérique :
#     Camionnette  (3.5t)  : (15 + 15) / 3.5  ≈  8.6 USD/tonne
#     Camion moyen (7.5t)  : (25 + 25) / 7.5  ≈  6.7 USD/tonne
#     Camion lourd  (20t)  : (40 + 40) / 20.0 =  4.0 USD/tonne
#
# VÉHICULE DE RÉFÉRENCE :
#   Le modèle gravitaire utilise une seule matrice C_ij (celle du véhicule de
#   référence, camion_moyen). On ajoute donc le coût fixe du véhicule de
#   référence. Le choix du véhicule de référence ne change pas le ROUTAGE
#   (Dijkstra reste multi-modal) mais calibre l'INTENSITÉ de la friction dans
#   le modèle gravitaire.
# ==============================================================================

# Récupération des coûts fixes depuis DuckDB
# La requête calcule directement le coût par tonne pour chaque véhicule,
# en divisant la somme des coûts de manutention par la capacité de chargement.
cout_fixe_par_vehicule <- duck_query("
  SELECT
    vehicule_id,
    nom,
    capacite_tonnes,
    cout_chargement_usd,
    cout_dechargement_usd,
    -- Coût fixe total par trajet (USD) = chargement + déchargement
    (cout_chargement_usd + cout_dechargement_usd)
      AS cout_fixe_trajet_usd,
    -- Coût fixe par tonne (USD/tonne) = coût par trajet / capacité
    -- C'est cette valeur qui s'ajoute à C_ij dans le modèle gravitaire
    (cout_chargement_usd + cout_dechargement_usd) / capacite_tonnes
      AS cout_fixe_par_tonne
  FROM params_flotte
  ORDER BY capacite_tonnes
")

cat("── Coûts fixes de manutention par véhicule ─────────────────────────────\n")
print(
  cout_fixe_par_vehicule %>%
    select(nom, capacite_tonnes, cout_fixe_trajet_usd, cout_fixe_par_tonne) %>%
    mutate(
      cout_fixe_trajet_usd = round(cout_fixe_trajet_usd, 1),
      cout_fixe_par_tonne  = round(cout_fixe_par_tonne,  2)
    ) %>%
    rename(
      Véhicule             = nom,
      `Capacité (t)`       = capacite_tonnes,
      `Coût trajet (USD)`  = cout_fixe_trajet_usd,
      `Coût/tonne (USD/t)` = cout_fixe_par_tonne
    )
)

# Extraction du coût fixe pour le véhicule de référence uniquement.
# Ce scalaire sera ajouté à l'ensemble de la matrice C_ij dans la boucle
# sectorielle ci-dessous.
cout_fixe_ref <- cout_fixe_par_vehicule$cout_fixe_par_tonne[
  cout_fixe_par_vehicule$vehicule_id == VEHICULE_REFERENCE
]

cat("\n  Coût fixe de référence (", VEHICULE_REFERENCE, ") :",
    round(cout_fixe_ref, 2), "USD/tonne\n")
cat("  → Ce montant sera ajouté à C_ij pour toutes les paires OD\n\n")


# ==============================================================================
# VII.4 : Modèle gravitaire DOUBLEMENT CONTRAINT (Wilson 1967 / Furness 1965)
#
#       T_ij^s = A_i^s × B_j^s × O_i^s × D_j^s × C_ij^(-beta_s)
#           → A_i^s et B_j^s sont des facteurs SPÉCIFIQUES à chaque zone,
#             calculés pour satisfaire EXACTEMENT les deux contraintes :
#               sum_j T_ij^s = O_i^s  (flux sortants = offre de i)
#               sum_i T_ij^s = D_j^s  (flux entrants = demande de j)
#
# COMPATIBILITÉ OFFRE / DEMANDE :
#   offre_total[i,s] et demande_total[j,s] (domestiques + RoW) sont construits
#   indépendamment (Parties VII.2 et VII.2.C) ; leur somme n'est pas nécessairement égale.
#   On normalise les deux cibles sur leur moyenne géométrique :
#     S^s = sqrt(sum_i O_i^s × sum_j D_j^s)
#   Cela préserve les distributions relatives tout en rendant les totaux compatibles.
#
# RÉFÉRENCES :
#   - Wilson (1967), Transportation Research 1(3), 253-269
#     → dérivation par maximisation d'entropie, preuve d'existence de A_i, B_j
#   - Furness (1965), Traffic Engineering and Control 7(7), 458-460
#     → algorithme IPF pour calculer A_i et B_j par itérations alternées
#   - Anderson & van Wincoop (2003), AER 93(1), 170-192
#     → A_i^s et B_j^s sont les "résistances multilatérales" de la théorie
#       structurelle du commerce international — même objet, deux littératures
# ==============================================================================


# ── Définition de la fonction d'équilibrage de Furness ────────────────────────
#
# furness_gravity() implémente l'algorithme de Furness (Iterative Proportional
# Fitting) qui calcule les facteurs A_i et B_j du modèle doublement contraint.
#
# ALGORITHME (3 étapes répétées jusqu'à convergence) :
#   Étape 0 : initialiser T_ij = O_i × D_j × friction_ij
#   Étape A : pour chaque ligne i,  T_ij ← T_ij × (target_O_i / sum_j T_ij)
#             → A_i implicite = target_O_i / sum_j T_ij_avant
#   Étape B : pour chaque colonne j, T_ij ← T_ij × (target_D_j / sum_i T_ij)
#             → B_j implicite = target_D_j / sum_i T_ij_avant
#   La convergence est garantie si friction > 0 partout (Sinkhorn & Knopp 1967).
#
# Paramètres :
#   O_s      — vecteur n_total des offres sectorielles en tonnes
#              (= offre_total[, s] × TONNES_PAR_musd[s])
#   D_s      — vecteur n_total des demandes sectorielles en tonnes (même convention)
#   friction — matrice n×n des termes (C_ij × TONNES_PAR_musd[s])^(-beta). Doit
#              avoir des NA là où les zones ne sont pas connectées (diag inclus).
#   secteur  — chaîne de caractères pour les messages de log uniquement
#
# Retourne :
#   Une matrice n×n de flux en tonnes, dont les marges (sommes de lignes et
#   de colonnes) correspondent aux offres et demandes cibles dans la limite
#   de la tolérance FURNESS_TOL.

furness_gravity <- function(O_s,
                            D_s,
                            friction,
                            secteur = "") {
  
  n <- length(O_s)
  stopifnot(
    length(D_s)    == n,
    nrow(friction) == n,
    ncol(friction) == n
  )
  
  # ── Calcul des cibles normalisées ────────────────────────────────────────────
  # offre_zones et demande_zones ont été construits indépendamment dans VII.2.
  # Leur somme totale n'est pas nécessairement égale (O_total ≠ D_total).
  # On les normalise toutes deux sur la moyenne géométrique de leurs totaux :
  #   S = sqrt(sum(O) × sum(D))
  # Cela équivaut à mettre à l'échelle chaque cible par un facteur constant :
  #   target_O_i = O_i × S / sum(O)  →  sum(target_O) = S
  #   target_D_j = D_j × S / sum(D)  →  sum(target_D) = S
  # La distribution RELATIVE entre zones est préservée ; seule l'échelle change.
  # Après normalisation, sum(target_O) == sum(target_D) == S, ce qui est
  # la condition nécessaire à l'existence d'une solution doublement contrainte.
  
  total_O <- sum(O_s, na.rm = TRUE)
  total_D <- sum(D_s, na.rm = TRUE)
  
  # Cas dégénéré : secteur sans offre ou sans demande (ex : secteur Mines dans
  # une zone uniquement résidentielle). On retourne une matrice nulle.
  if (total_O < 1e-12 || total_D < 1e-12) {
    cat("  [", secteur, "] Offre ou demande nulle — matrice de flux vide\n")
    return(matrix(0, nrow = n, ncol = n))
  }
  
  # Moyenne géométrique des totaux = cible commune pour les deux marges
  S_cible  <- sqrt(total_O * total_D)
  
  # Facteurs de normalisation : ratio entre la cible commune et le total actuel
  # target_O_i = O_i × (S / sum(O)) : redistributionne S en proportions de O_i
  # target_D_j = D_j × (S / sum(D)) : redistributionne S en proportions de D_j
  target_O <- O_s * (S_cible / total_O)
  target_D <- D_s * (S_cible / total_D)
  
  # Vérification numérique de la compatibilité (les deux doivent valoir S_cible)
  # L'écart relatif doit être inférieur à 1e-8 (erreur d'arrondi flottant)
  ecart_rel <- abs(sum(target_O) - sum(target_D)) / S_cible
  if (ecart_rel > 1e-6) {
    warning("  [", secteur, "] Déséquilibre offre/demande après normalisation : ",
            round(ecart_rel * 100, 6), "% — vérifier offre_zones et demande_zones")
  }
  
  # ── Initialisation de la matrice de flux ─────────────────────────────────────
  # T_ij = O_i^s × D_j^s × friction_ij
  # C'est le point de départ de Furness : la matrice "non contrainte" qui
  # respecte déjà la structure de friction mais pas les marges cibles.
  # outer(x, y) : produit extérieur — élément [i,j] = x[i] × y[j]
  T_mat <- outer(O_s, D_s) * friction
  
  # Nettoyage : les NA viennent de la friction (zones non connectées ou diagonale).
  # On les met à 0 : une zone non connectée ne peut pas échanger.
  T_mat[is.na(T_mat) | is.nan(T_mat) | is.infinite(T_mat)] <- 0
  diag(T_mat) <- 0   # Par sécurité : interdire les échanges intra-zone
  
  # ── Boucle de Furness ─────────────────────────────────────────────────────────
  # Chaque itération alterne deux équilibrages :
  #   Étape A (lignes)   : force la somme des flux sortants à target_O_i
  #   Étape B (colonnes) : force la somme des flux entrants à target_D_j
  # La preuve de convergence repose sur le fait que la matrice de friction est
  # non-négative et irréductible (Sinkhorn & Knopp 1967).
  
  for (iter in seq_len(FURNESS_MAX_ITER)) {
    
    # ── Étape A : équilibrage des LIGNES (contrainte sur les origines) ──────────
    # On veut sum_j T_ij = target_O_i pour tout i.
    # Le facteur d'ajustement A_i = target_O_i / (sum_j T_ij actuel)
    # Multiplier chaque ligne i par A_i recentre la somme de la ligne sur target_O_i.
    # pmax(..., 1e-12) : évite la division par zéro si une ligne est entièrement vide
    # (zone sans aucune connexion réseau → ses flux restent à 0 après multiplication)
    row_sums      <- rowSums(T_mat)
    row_sums_safe <- pmax(row_sums, 1e-12)
    A_i           <- target_O / row_sums_safe
    
    # Zones sans offre (target_O_i = 0) : facteur forcé à 0 pour ne pas
    # amplifier un résidu numérique (produit de flottants proche de zéro)
    A_i[target_O < 1e-12] <- 0
    
    # Multiplication ligne par ligne : T_ij ← A_i × T_ij
    # En R, la multiplication d'une matrice par un vecteur opère colonne par colonne
    # par défaut. Pour opérer ligne par ligne, on transpose (t()), multiplie, retranspose.
    T_mat <- T_mat * A_i   # broadcast ligne par ligne (vecteur recycled par R)
    
    # ── Étape B : équilibrage des COLONNES (contrainte sur les destinations) ────
    # Même logique que l'étape A, mais appliquée aux colonnes.
    # B_j = target_D_j / (sum_i T_ij actuel)
    col_sums      <- colSums(T_mat)
    col_sums_safe <- pmax(col_sums, 1e-12)
    B_j           <- target_D / col_sums_safe
    B_j[target_D < 1e-12] <- 0
    
    # t(t(T_mat) * B_j) : transposer, multiplier (ce qui fait un broadcast ligne),
    # retransposer → équivaut à multiplier chaque colonne j par B_j
    T_mat <- t(t(T_mat) * B_j)
    
    # ── Test de convergence ──────────────────────────────────────────────────────
    # On calcule l'erreur relative maximale sur les deux contraintes.
    # Erreur relative = |marge_actuelle - marge_cible| / marge_cible
    # On prend le max sur toutes les zones pour le critère d'arrêt.
    # Seules les zones avec une cible non nulle entrent dans le calcul
    # (les zones sans offre ou demande ne doivent pas être comptées).
    err_O <- max(
      abs(rowSums(T_mat)[target_O > 1e-12] - target_O[target_O > 1e-12]) /
        target_O[target_O > 1e-12]
    )
    err_D <- max(
      abs(colSums(T_mat)[target_D > 1e-12] - target_D[target_D > 1e-12]) /
        target_D[target_D > 1e-12]
    )
    err_max <- max(err_O, err_D)
    
    # Critère d'arrêt : les deux marges sont respectées à FURNESS_TOL près
    if (err_max < FURNESS_TOL) {
      cat("  [", secteur, "] Furness convergé — itération", iter,
          "| erreur max :", formatC(err_max * 100, format = "e", digits = 2), "%\n")
      break
    }
    
    # Avertissement si la boucle atteint la limite sans converger
    if (iter == FURNESS_MAX_ITER) {
      warning("  [", secteur, "] Furness non convergé après ", FURNESS_MAX_ITER,
              " itérations. Erreur finale : ", round(err_max * 100, 4), "%",
              "\n  → Vérifier les zones isolées (offre ou demande = 0).",
              "\n  → Augmenter FURNESS_MAX_ITER ou revoir C_IJ_PLANCHER.")
    }
  }
  
  # Remise à zéro de la diagonale par sécurité (peut avoir reçu un résidu
  # numérique lors des multiplications ligne/colonne successives)
  diag(T_mat) <- 0
  
  T_mat
}

# ── Application sectorielle du modèle doublement contraint ────────────────────

cat("Calcul des flux gravitaires (modèle doublement contraint)...\n\n")

flux_gravitaire <- list()   # Liste des matrices de flux par secteur (tonnes)

# Création des noms de zones UNIQUES (make.unique évite les doublons OSM).
# noms_zones_uniques : n_warehouses zones domestiques
# noms_total         : n_total = n_warehouses + n_row (inclut les nœuds RoW)
noms_zones_uniques <- make.unique(noeuds_entreposage$warehouse_name, sep = "_")
noms_total         <- c(noms_zones_uniques, noms_row)

# flux_total : matrice (n_total × n_total) qui accumule tous les secteurs.
# Les lignes/colonnes RoW représentent les flux import/export.
flux_total <- matrix(0, nrow = n_total, ncol = n_total,
                     dimnames = list(noms_total, noms_total))

for (s in SECTEURS) {

  beta_s <- BETA_SECTEUR[s]

  # ── Construction de la matrice de coûts totale (n_total × n_total) ───────────
  # Bloc domestique-domestique (n_warehouses × n_warehouses) : coût routier C_ij.
  # Lignes/colonnes RoW (n_row) : coût = min sur les postes frontières du pays.
  #
  # Pour un nœud RoW_k et une destination domestique j :
  #   C_total[RoW_k, j, s] = min_b ( couts_prebordure[pays_k, s] + C_road[b, j] )
  # Le minimum est pris sur tous les postes frontières b du pays k (ex. : Gatuna
  # ET Kagitumba pour l'Ouganda), ce qui sélectionne le passage optimal.
  # La matrice est symétrique : exporter coûte autant qu'importer (même route).
  C_total_s <- matrix(NA_real_, n_total, n_total,
                      dimnames = list(noms_total, noms_total))

  # Bloc domestique 
  C_total_s[1:n_warehouses, 1:n_warehouses] <- C_ij

  # Lignes/colonnes RoW : calculées par pays et par secteur
  cout_pb_s <- couts_prebordure %>% filter(secteur == s)

  for (k in seq_along(pays_row)) {
    pays_k  <- pays_row[k]
    idx_k   <- n_warehouses + k             # ligne/colonne RoW_k dans C_total_s
    idxs_b  <- idx_frontiere_par_pays %>%
      filter(pays == pays_k) %>%
      pull(idx)                             # indices des postes frontières du pays k

    if (length(idxs_b) == 0) next           # pays sans frontière connue : coût NA

    cout_pb_ks <- cout_pb_s %>%
      filter(pays == pays_k) %>%
      pull(cout_usd_tonne)
    if (length(cout_pb_ks) == 0) cout_pb_ks <- 0

    # Pour chaque destination domestique j, prendre le min sur les frontières b
    for (j in seq_len(n_warehouses)) {
      couts_via_b <- C_ij[idxs_b, j] + cout_pb_ks  # vecteur de longueur |idxs_b|
      couts_via_b <- couts_via_b[!is.na(couts_via_b)]
      if (length(couts_via_b) > 0) {
        C_total_s[idx_k, j] <- min(couts_via_b)
        C_total_s[j, idx_k] <- C_total_s[idx_k, j]  # symétrique
      }
    }
    # RoW ↔ RoW : non défini (NA) — le transit entre pays via le Rwanda est négligeable
  }

  # Diagonale NA (pas d'échange intrazone)
  diag(C_total_s) <- NA

  # ── Ajout du coût fixe de manutention ────────────────────────────────────────
  # Le coût fixe crée un plancher empêchant l'explosion de C^(-beta) pour les
  # paires très proches. Il s'additionne naturellement : NA + scalaire = NA.
  #   C_total_s_final = C_route_ou_RoW + cout_fixe_manutention
  C_total_s_final <- C_total_s + cout_fixe_ref

  # ── Calcul de la friction spatiale ───────────────────────────────────────────
  # On multiplie C_total_s_final (USD/tonne) par TONNES_PAR_musd[s] (tonnes/MUSD)
  # pour obtenir un coût adimensionné : USD dépensés en transport par MUSD de biens.
  # Ce rapport (coût de transport / valeur des biens) est la friction du modèle
  # iceberg. Beta mesure alors l'élasticité du commerce à ce coût RELATIF,
  # ce qui rend son interprétation cohérente entre secteurs et calibrable sur
  # des données empiriques.
  friction                  <- (C_total_s_final * TONNES_PAR_musd[s])^(-beta_s)
  friction[is.na(friction)] <- 0
  diag(friction)            <- 0

  # ── Appel à Furness (doublement contraint) ────────────────────────────────────
  # offre_total[, s] et demande_total[, s] sont en M USD ; on les convertit en
  # tonnes via TONNES_PAR_musd[s] pour que les marges cibles soient en tonnes.
  # La matrice résultante flux_gravitaire[[s]] est directement en tonnes.
  # Les n_row dernières entrées sont les flux d'import/export NISR (convertis).
  # La fonction normalise sur la moyenne géométrique pour assurer sum(O) = sum(D).
  flux_gravitaire[[s]] <- furness_gravity(
    O_s      = offre_total[, s]  * TONNES_PAR_musd[s],
    D_s      = demande_total[, s] * TONNES_PAR_musd[s],
    friction = friction,
    secteur  = s
  )

  # Accumulation dans la matrice de flux toutes-secteurs
  flux_total <- flux_total + flux_gravitaire[[s]]
}

# ── Vérification des contraintes de marges ────────────────────────────────────
# On contrôle que les flux sortants de chaque zone correspondent bien à son
# offre sectorielle, et idem pour les flux entrants et la demande.
# Un écart > 0.1% signale un problème de convergence ou de données.
# La vérification porte sur les n_total nœuds (domestiques + RoW).

cat("\n── Vérification des contraintes de marges ─────────────────────────────\n")

for (s in SECTEURS) {

  T_s      <- flux_gravitaire[[s]]
  O_s      <- offre_total[, s]  * TONNES_PAR_musd[s]
  D_s      <- demande_total[, s] * TONNES_PAR_musd[s]
  
  # Recalcul des cibles normalisées (même logique que dans furness_gravity)
  # pour comparer avec les marges effectives de la matrice obtenue
  total_O  <- sum(O_s, na.rm = TRUE)
  total_D  <- sum(D_s, na.rm = TRUE)
  
  if (total_O < 1e-12 || total_D < 1e-12) next
  
  S_cible  <- sqrt(total_O * total_D)
  target_O <- O_s * (S_cible / total_O)
  target_D <- D_s * (S_cible / total_D)
  
  # Erreur relative maximale : max sur toutes les zones non-nulles
  zones_O_actives <- target_O > 1e-12
  zones_D_actives <- target_D > 1e-12
  
  err_O <- if (any(zones_O_actives)) {
    max(abs(rowSums(T_s)[zones_O_actives] - target_O[zones_O_actives]) /
          target_O[zones_O_actives]) * 100
  } else 0
  
  err_D <- if (any(zones_D_actives)) {
    max(abs(colSums(T_s)[zones_D_actives] - target_D[zones_D_actives]) /
          target_D[zones_D_actives]) * 100
  } else 0
  
  statut <- if (max(err_O, err_D) < 0.01) "✓" else "⚠"
  cat("  ", statut, "[", formatC(s, width = 14), "]",
      "err. origine :", formatC(err_O, format = "f", digits = 4), "%",
      "| err. destin. :", formatC(err_D, format = "f", digits = 4), "%\n")
}

# ── Résultats globaux ─────────────────────────────────────────────────────────
flux_par_secteur_df <- tibble(
  Secteur           = SECTEURS,
  Beta              = unname(BETA_SECTEUR),
  Flux_total_tonnes = sapply(SECTEURS, function(s) round(sum(flux_gravitaire[[s]]), 0)),
  Flux_moyen_tonnes = sapply(SECTEURS, function(s) {
    f <- flux_gravitaire[[s]]
    round(mean(f[f > 0]), 1)
  })
)

cat("\nFlux par secteur (modèle doublement contraint):\n")
print(flux_par_secteur_df)

# Vérifier les doublons dans les noms de zones
cat("\nDoublons dans warehouse_name :\n")
print(noeuds_entreposage$warehouse_name[duplicated(noeuds_entreposage$warehouse_name)])

# Top 10 des paires OD
flux_total_long <- flux_total %>%
  as.data.frame() %>%
  setNames(make.unique(colnames(.), sep = "_")) %>%
  rownames_to_column("Origine") %>%
  pivot_longer(-Origine, names_to = "Destination", values_to = "flux_tonnes") %>%
  filter(flux_tonnes > 1) %>%
  arrange(desc(flux_tonnes))

cat("\nTop 10 des flux commerciaux bilatéraux (tonnes):\n")
print(head(flux_total_long, 10))
cat("\n")
cat("✓ Flux total modélisé:", format(round(sum(flux_total)), big.mark = " "), "tonnes\n")
cat("  Nombre de paires actives:", nrow(flux_total_long), "\n\n")

# ==============================================================================
# VII.5 : Projection des flux RoW sur le réseau routier rwandais
#
# PRINCIPE :
#   Les nœuds RoW sont virtuels (hors réseau routier). Pour l'affectation
#   All-or-Nothing (Partie VIII), chaque flux T[RoW_k, j] (ou T[j, RoW_k])
#   doit être attribué à un chemin physique. On l'injecte au poste frontière
#   optimal b*(j) = argmin_b C_road[b, j] sur les frontières du pays k.
#   Seul le segment rwandais b*(j) → j est affecté aux routes.
#
#   Remarque : le coût pré-frontière (segment étranger) est déjà intégré dans
#   le modèle gravitaire (C_total_s) et ne génère aucun trafic sur les routes
#   rwandaises.
#
# RÉSULTAT :
#   flux_tonnes_total : matrice (n_warehouses × n_warehouses) pour l'affectation.
#   Les flux RoW sont absorbés dans les lignes/colonnes des postes frontières.
# ==============================================================================

cat("── Projection des flux RoW sur les postes frontières ──────────────────\n")

# flux_gravitaire[[s]] est déjà en tonnes (O_s et D_s ont été convertis avant
# Furness) : on somme directement les secteurs sans conversion supplémentaire.
flux_tonnes_total_ext <- matrix(
  0,
  nrow = n_total, ncol = n_total,
  dimnames = list(noms_total, noms_total)
)
for (s in SECTEURS) {
  flux_tonnes_total_ext <- flux_tonnes_total_ext + flux_gravitaire[[s]]
}

# Projection des lignes/colonnes RoW sur les postes frontières correspondants.
# Pour chaque pays k : b*(j) = argmin sur les frontières du pays de C_road[b, j].
# Le même passage est utilisé pour les flux entrants (import) et sortants (export).
# Note : C_ij est sector-independent → argmin b ne dépend pas du secteur.
# Initialisation depuis le bloc domestique-domestique de la matrice étendue
flux_tonnes_total <- flux_tonnes_total_ext[1:n_warehouses, 1:n_warehouses]

for (k in seq_along(pays_row)) {
  pays_k <- pays_row[k]
  idx_k  <- n_warehouses + k
  idxs_b <- idx_frontiere_par_pays %>% filter(pays == pays_k) %>% pull(idx)

  if (length(idxs_b) == 0) next

  for (j in seq_len(n_warehouses)) {
    # Flux RoW_k → j (import)
    vol_import <- flux_tonnes_total_ext[idx_k, j]
    if (vol_import > 0) {
      couts_b <- C_ij[idxs_b, j]
      b_star  <- idxs_b[which.min(couts_b)]
      if (!is.na(b_star)) {
        flux_tonnes_total[b_star, j] <- flux_tonnes_total[b_star, j] + vol_import
      }
    }

    # Flux j → RoW_k (export)
    vol_export <- flux_tonnes_total_ext[j, idx_k]
    if (vol_export > 0) {
      couts_b <- C_ij[j, idxs_b]
      b_star  <- idxs_b[which.min(couts_b)]
      if (!is.na(b_star)) {
        flux_tonnes_total[j, b_star] <- flux_tonnes_total[j, b_star] + vol_export
      }
    }
  }
}

rm(flux_tonnes_total_ext)

tonnage_total <- sum(flux_tonnes_total)
cat("  Tonnage domestique après projection :",
    format(round(tonnage_total), big.mark = " "), "tonnes\n\n")

################################################################################
# PARTIE VIII — AFFECTATION DU FRET ET RÉSULTATS
# Affecte chaque flux OD (déjà en tonnes) au chemin optimal du graphe
# multi-modal (All-or-Nothing), puis produit les visualisations et exports.
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

# ── ÉTAPE 0 : Nettoyage agressif de la mémoire avant de commencer ─────────────
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

# ── ÉTAPE 1 : Pré-filtrage des paires OD à traiter ───────────────────────────
# flux_tonnes_total (n_warehouses × n_warehouses) a déjà été construit et projeté
# en VII.5 (flux RoW injectés sur les postes frontières optimaux).
# On filtre directement les paires actives à partir de cette matrice.
# Plutôt que de boucler sur n_warehouses² paires puis de filtrer par seuil,
# on construit d'abord la liste des paires pertinentes. Ça permet aussi
# d'avoir une barre de progression exacte et de mieux répartir les gc().

paires_actives <- which(flux_tonnes_total > SEUIL_FLUX_TONNES, arr.ind = TRUE)
paires_actives <- paires_actives[paires_actives[, 1] != paires_actives[, 2], ]

CACHE_AFFECTATION      <- file.path(DIR_CACHE, "affectation_cache.rds")
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
    flux_tonnes_total = flux_tonnes_total,    # dépend de BETA, TONNES, DEMANDE_FINALE_NISR, RoW
    seuil             = SEUIL_FLUX_TONNES,
    n_aretes          = n_aretes_physiques,
    n_warehouses      = n_warehouses,
    n_vehicules       = n_vehicules,
    n_aretes_mm       = igraph::ecount(recuperer_lourd("graphe_multimodal"))
  ),
  algo = "xxhash64"
)

if (file.exists(CACHE_AFFECTATION)) {
  
  cat("── Tentative de chargement du cache d'affectation ─────────────────────\n")
  cache_aff <- readRDS(CACHE_AFFECTATION)
  
  # Le cache est valide SEULEMENT si :
  #   1. l'empreinte correspond (entrées inchangées)
  #   2. le tableau 3D est présent 
  # Cette deuxième condition évite de charger un cache produit par une version
  # antérieure du script qui ne sauvegardait pas la dimension sectorielle.
  if (!is.null(cache_aff$empreinte) &&
      cache_aff$empreinte == empreinte_entrees &&
      !is.null(cache_aff$volume_trafic_mm_s)) {
    
    volume_trafic_mm_s       <- cache_aff$volume_trafic_mm_s   
    paires_traitees          <- cache_aff$paires_traitees
    paires_non_connectees    <- cache_aff$paires_non_connectees
    cache_affectation_valide <- TRUE
    
    cat("  ✓ Cache d'affectation valide\n")
    cat("    Paires traitées      :", paires_traitees, "\n")
    cat("    Paires non connectées:", paires_non_connectees, "\n")
    cat("    → Affectation ignorée (~2-5 min gagnées)\n\n")
    
  } else {
    cat("  ⚠ Cache d'affectation invalide ou obsolète — recalcul\n\n")
  }
}

# ── Diagnostic du filtre par seuil ────────────────────────────────────────────
# On calcule le nombre de paires exclues par le seuil SEUIL_FLUX_TONNES
# pour vérifier que le filtre n'élimine pas trop de flux économiquement
# significatifs. Une paire exclue = flux trop faible pour être affecté
# au réseau routier, mais qui contribue quand même au tonnage total.
# Ce diagnostic est affiché même quand le cache d'affectation est valide.

# Toutes les paires hors diagonale (i ≠ j), sans filtre de seuil
toutes_paires <- which(flux_tonnes_total > 0, arr.ind = TRUE)
toutes_paires <- toutes_paires[toutes_paires[, 1] != toutes_paires[, 2], ]

# Paires sous le seuil = toutes les paires actives MOINS celles retenues
n_paires_sous_seuil <- nrow(toutes_paires) - nrow(paires_actives)

# Tonnage total des paires exclues (pour juger de leur poids économique)
tonnage_exclu <- sum(flux_tonnes_total[toutes_paires]) -
  sum(flux_tonnes_total[paires_actives])
tonnage_total_avant_filtre <- sum(flux_tonnes_total[toutes_paires])

cat("── Diagnostic du filtre seuil (", SEUIL_FLUX_TONNES, "tonnes) ──────────\n")
flush.console()
cat("  Paires totales (flux > 0)  :", nrow(toutes_paires), "\n")
flush.console()
cat("  Paires retenues (> seuil)  :", nrow(paires_actives), "\n")
flush.console()
cat("  Paires exclues (< seuil)   :", n_paires_sous_seuil,
    "(", round(n_paires_sous_seuil / nrow(toutes_paires) * 100, 1), "%)\n")
flush.console()
cat("  Tonnage exclu              :", format(round(tonnage_exclu), big.mark = " "),
    "tonnes (", round(tonnage_exclu / tonnage_total_avant_filtre * 100, 2),
    "% du total)\n\n")
flush.console()
cat("  Paires OD à traiter :", format(nrow(paires_actives), big.mark = " "),
    "(sur", format(n_warehouses^2 - n_warehouses, big.mark = " "),
    "possibles)\n\n")
flush.console()

# ══════════════════════════════════════════════════════════════════════════════
# BLOC CONDITIONNEL : l'affectation ne s'exécute que si pas de cache valide
# ══════════════════════════════════════════════════════════════════════════════
if (!cache_affectation_valide) {

  # ── ÉTAPE 4 : Préparation des matrices de résultats ───────────────────────────
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
  # (évite l'accès répété à E(recuperer_lourd("graphe_multimodal"))$weight qui est coûteux)
  poids_mm <- igraph::E(recuperer_lourd("graphe_multimodal"))$weight
  
  # ── ÉTAPE 5 : Boucle principale par zone origine ──────────────────────────────
  # On parcourt les zones origine une par une. Pour chaque origine i, on calcule
  # en UNE SEULE fois les distances vers toutes les destinations (bien plus
  # efficace qu'une requête par paire).
  
  cat("Affectation du fret au réseau (All-or-Nothing multi-modal)...\n")
  
  # targets_all_global est un vecteur contenant les indices des nœuds-entrepôts
  # dans le graphe multi-modal, pour chaque couche véhicule. Par exemple, si
  # l'entrepôt A est au nœud 5 du réseau physique, il apparaît 3 fois :
  # une fois par couche véhicule : targets_all_global = (5,..., 1005,..., 2005,...).
  targets_all_global <- as.vector(sapply(
    seq_len(n_vehicules),
    function(v) node_multi(v, warehouse_nodes_base)
  ))
  
  # On regroupe les paires actives par origine pour traiter toute une origine
  # en une passe Dijkstra.
  # split(x, f) découpe le vecteur x en morceaux, un par valeur unique de f
  # paires_par_origine = list(
  #  "1" = c(2, 3, 5),    ← depuis l'origine 1, les destinations sont 2, 3 et 5
  #  "2" = c(1, 4),       ← depuis l'origine 2, les destinations sont 1 et 4
  #  "3" = c(1),          ← depuis l'origine 3, la destination est 1
  #  ...)
  paires_par_origine <- split(
    paires_actives[, 2],       # destinations
    paires_actives[, 1]        # origines (clé du split)
  )
  # Vecteur de l'indice des noeuds qui ont une destination après filtrage (si tous 
  # ont au moins une destination, origines_a_traiter = (1,2,...,n_warehouse))
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
    
    # Dijkstra en une passe : depuis les n_vehicules sources vers toutes les destinations.
    # Résultat : matrice n_vehicules × (n_warehouses × n_vehicules)
    dists_all <- igraph::distances(
      recuperer_lourd("graphe_multimodal"),
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
      # ventiler les volumes de TOUS les secteurs :
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
        recuperer_lourd("graphe_multimodal"),
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
      
      # ── Identification des arêtes physiques valides sur ce chemin ─────────────
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
      
      # ── Ventilation sectorielle sur le chemin trouvé ──────────────────────────
      # Le chemin est le même pour tous les secteurs (hypothèse de routage unique).
      # On affecte maintenant le volume de CHAQUE secteur séparément sur ce chemin.
      # Cela permet de savoir, arête par arête, combien de tonnes d'Agriculture,
      # de Mines, d'Industrie, etc. y transitent — sans recalculer Dijkstra.
      for (s in SECTEURS) {
        
        # ── Indice numérique du secteur dans la 3e dimension du tableau ─────────
        # Le tableau volume_trafic_mm_s a pour dimensions :
        #   [arête physique, véhicule, secteur]
        # Pour l'indexer efficacement, on a besoin de l'indice ENTIER du secteur
        # (1 pour "Agriculture", 2 pour "Mines", etc.) et pas de son nom texte.
        # match(s, SECTEURS) retourne la position de s dans le vecteur SECTEURS.
        # Exemple : match("Industrie", SECTEURS) → 4
        idx_s <- match(s, SECTEURS)
        
        # Volume en tonnes pour ce secteur entre i et j
        # flux_gravitaire[[s]] est directement en tonnes (converti avant Furness)
        flux_ij_s <- flux_gravitaire[[s]][i, j]
        
        # Si le flux sectoriel est négligeable, on passe au secteur suivant
        # pour ne pas alourdir inutilement les calculs
        if (is.na(flux_ij_s) || flux_ij_s < 1) next
        
        # ── Affectation vectorisée sur un tableau 3D ────────────────────────────
        # On veut ajouter flux_ij_s à TOUTES les cellules (a, v, s) où :
        #   - a parcourt les arêtes physiques du chemin (idx_phys_vec)
        #   - v parcourt les véhicules correspondants (col_veh_vec)
        #   - s est fixé au secteur courant (idx_s)
        #
        # Pour indexer un tableau à N dimensions, on passe une matrice à N colonnes :
        # chaque LIGNE de cette matrice = un triplet (arête, véhicule, secteur)
        # qui désigne UNE cellule unique du tableau 3D.
        #
        # cbind(idx_phys_vec, col_veh_vec, idx_s) construit cette matrice :
        #   - idx_phys_vec et col_veh_vec sont des vecteurs de même longueur
        #     (autant que d'arêtes du chemin)
        #   - idx_s est un scalaire : R le RECYCLE automatiquement pour qu'il
        #     apparaisse sur chaque ligne
        # Résultat : une matrice à 3 colonnes avec une ligne par arête du chemin.
        indices_3d <- cbind(idx_phys_vec, col_veh_vec, idx_s)
        
        # volume_trafic_mm_s[indices_3d] : quand on passe une matrice à N colonnes
        # à un tableau à N dimensions, R interprète CHAQUE LIGNE comme un jeu
        # d'indices. C'est l'équivalent vectorisé de faire :
        #   volume_trafic_mm_s[arete1, veh1, s] += flux_ij_s
        #   volume_trafic_mm_s[arete2, veh2, s] += flux_ij_s
        #   ...
        # mais sans boucle R explicite, donc beaucoup plus rapide.
        volume_trafic_mm_s[indices_3d] <-
          volume_trafic_mm_s[indices_3d] + flux_ij_s
      }
      
      paires_traitees <- paires_traitees + 1
    }
    
    # Nettoyage explicite de dists_all avant l'itération suivante
    rm(dists_all)
    
    # gc() nettoie la RAM à chaque itération pour éviter les pics de RAM qui font crasher R.
    if (idx_i %% 5 == 0) {
      invisible(gc(verbose = FALSE))
    }
    
    pb_aff$tick()
  }
  
  
  # ── SAUVEGARDE DU CACHE ───────────────────────────────────────────────────────
  cat("=== Sauvegarde du cache d'affectation ===\n")
  
  saveRDS(
    list(
      volume_trafic_mm_s    = volume_trafic_mm_s,
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
# Reconstruction des matrices agrégées (exécutée DANS TOUS LES CAS)
# ══════════════════════════════════════════════════════════════════════════════
# Que l'on vienne du cache ou d'un recalcul complet, on dispose maintenant
# de volume_trafic_mm_s (le tableau 3D complet [arête, véhicule, secteur]).
# On en dérive les agrégations nécessaires pour la suite du script :
#   - volume_trafic_mm   : total par (arête, véhicule), sommé sur secteurs
#   - volume_trafic      : total par arête, sommé sur véhicules ET secteurs
#   - volume_par_secteur : total par (arête, secteur), sommé sur véhicules

# apply(X, MARGIN, FUN) : applique une fonction sur certaines dimensions d'un tableau.
# MARGIN = c(1, 2) signifie "pour chaque combinaison des dimensions 1 et 2,
# applique FUN sur la dimension restante (ici la 3e = secteurs)".
# Résultat : une matrice 2D [arête, véhicule] avec la somme sur tous les secteurs.
volume_trafic_mm  <- apply(volume_trafic_mm_s, c(1, 2), sum)

# rowSums() : somme sur les colonnes, pour chaque ligne.
# Appliqué à volume_trafic_mm (matrice [arête, véhicule]), ça donne le
# total par arête, tous véhicules confondus.
volume_trafic     <- rowSums(volume_trafic_mm)

# Cette fois on somme sur la dimension 2 (véhicules) en gardant les
# dimensions 1 (arêtes) et 3 (secteurs) → matrice [arête, secteur].
volume_par_secteur <- apply(volume_trafic_mm_s, c(1, 3), sum)

# Conversion en data.frame pour l'export CSV en Partie VIII.3.
# Les noms de colonnes sont préfixés par "vol_t_" pour indiquer "volume en tonnes".
volume_par_secteur_df <- as.data.frame(volume_par_secteur)
colnames(volume_par_secteur_df) <- paste0("vol_t_", SECTEURS)

# ── Calcul des émissions totales affectées sur le réseau ──────────────────────
# On calcule les émissions absolues (CO2, NOx, PM2.5) générées par l'ensemble
# des flux de fret modélisés, arête par arête.
#
# Principe : pour chaque arête, on multiplie son intensité d'émission
# par tonne-km par le volume de trafic affecté et par la longueur de l'arête.
#   Émissions_arête = intensité_par_tkm × volume_tonnes × length_km
#
# co2_kg_par_tkm, nox_g_par_tkm et pm25_g_par_tkm sont les intensités
# unitaires calculées en Partie V.1 et intégrées dans reseau_rwanda.
# volume_trafic est le vecteur de tonnes affectées par arête (calculé juste
# au-dessus via rowSums()).
# length_km est la longueur de chaque arête en kilomètres.

# Récupération des attributs d'émissions et de longueur pour toutes les arêtes.
# On extrait ces trois colonnes depuis reseau_rwanda en un seul appel pour
# éviter de réactiver le réseau plusieurs fois.
aretes_emissions_base <- reseau_rwanda %>%
  activate("edges") %>%
  as_tibble() %>%
  select(length_km, co2_kg_par_tkm, nox_g_par_tkm, pm25_g_par_tkm)

# Calcul vectorisé des émissions totales par arête.
# replace_na(..., 0) : les arêtes sans données d'émissions (ex : arêtes
# topologiques créées lors de la subdivision, arêtes de longueur nulle)
# contribuent 0 au total au lieu de propager des NA dans toute la colonne.
# Le produit est vectorisé : R multiplie élément par élément les trois
# vecteurs de même longueur (n_aretes lignes chacun).
emissions_co2_aretes  <- replace_na(aretes_emissions_base$co2_kg_par_tkm,  0) *
  volume_trafic *
  replace_na(aretes_emissions_base$length_km, 0)

emissions_nox_aretes  <- replace_na(aretes_emissions_base$nox_g_par_tkm,   0) *
  volume_trafic *
  replace_na(aretes_emissions_base$length_km, 0)

emissions_pm25_aretes <- replace_na(aretes_emissions_base$pm25_g_par_tkm,  0) *
  volume_trafic *
  replace_na(aretes_emissions_base$length_km, 0)

# Intégration dans reseau_rwanda comme attributs des arêtes.
# Les unités sont converties pour rester lisibles dans les exports :
#   CO2  : kg → tonnes  (÷ 1 000) — ordre de grandeur typique : quelques t/arête
#   NOx  : g  → kg      (÷ 1 000) — ordre de grandeur typique : quelques kg/arête
#   PM2.5: g  → kg      (÷ 1 000) — ordre de grandeur typique : < 1 kg/arête
#     (les PM2.5 sont émises en quantités bien inférieures au NOx,
#      d'où l'importance de garder la colonne en kg et non en tonnes
#      pour ne pas afficher des valeurs trop proches de zéro)
reseau_rwanda <- reseau_rwanda %>%
  activate("edges") %>%
  mutate(
    emissions_co2_t    = emissions_co2_aretes  / 1000,
    emissions_nox_kg   = emissions_nox_aretes  / 1000,
    emissions_pm25_kg  = emissions_pm25_aretes / 1000
  )

# Rapport global d'émissions (pour le log console).
# Ces totaux agrègent toutes les arêtes du réseau et donc tous les flux OD
# modélisés. Ils constituent un ordre de grandeur de l'empreinte carbone
# et polluante du fret routier rwandais dans le modèle.
co2_total_reseau_t   <- sum(emissions_co2_aretes,  na.rm = TRUE) / 1000
nox_total_reseau_kg  <- sum(emissions_nox_aretes,  na.rm = TRUE) / 1000
pm25_total_reseau_kg <- sum(emissions_pm25_aretes, na.rm = TRUE) / 1000

cat("── Émissions totales du fret modélisé ──────────────────────────────\n")
cat("  CO2   total :", format(round(co2_total_reseau_t),   big.mark = " "), "tonnes\n")
cat("  NOx   total :", format(round(nox_total_reseau_kg),  big.mark = " "), "kg\n")
cat("  PM2.5 total :", format(round(pm25_total_reseau_kg), big.mark = " "), "kg\n\n")

# ── Sanity check : le tonnage affecté doit être cohérent ──────────────────────
# Note : on somme sur toutes les dimensions de volume_trafic_mm_s (tableau 3D).
# Le tonnage affecté sera plusieurs fois supérieur au tonnage attendu, car
# chaque flux OD est compté sur TOUTES les arêtes de son chemin (un flux
# de 100t qui emprunte 20 arêtes contribue 100×20 = 2000 tonnes-arêtes).
# Ce qui compte, c'est que le ratio soit stable et qu'il n'y ait pas de NA.
tonnage_affecte <- sum(volume_par_secteur)
tonnage_attendu <- sum(flux_tonnes_total[paires_actives])

cat("  Tonnage affecté au réseau (cumulé sur toutes les arêtes) :",
    format(round(tonnage_affecte), big.mark = " "), "tonnes-arêtes\n")
cat("  Tonnage OD attendu (paires > seuil) :",
    format(round(tonnage_attendu), big.mark = " "), "tonnes\n")
cat("  Ratio moyen (≈ longueur moyenne de chemin en arêtes) :",
    round(tonnage_affecte / tonnage_attendu, 1), "\n")

# ══════════════════════════════════════════════════════════════════════════════
# SUITE : calculs rapides toujours exécutés (qu'on ait un cache ou non)
# ══════════════════════════════════════════════════════════════════════════════

# ── Statistiques de répartition modale ────────────────────────────────────────
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

# ── Étape 6 : Intégration des volumes au réseau ───────────────────────────────
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

# Identification des arêtes les plus empruntées
reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  mutate(arete_idx = row_number()) %>%
  st_drop_geometry() %>%
  select(osm_id, name, road_type, surface, longueur_m, volume_tonnes) %>%
  filter(!is.na(volume_tonnes), volume_tonnes > 0) %>%
  arrange(desc(volume_tonnes)) %>%
  slice_head(n = 20) %>%
  mutate(
    longueur_km   = round(longueur_m / 1000, 2),
    volume_tonnes = round(volume_tonnes)
  ) %>%
  select(osm_id, name, road_type, surface, longueur_km, volume_tonnes) %>%
  print(n = 20)

# ==============================================================================
# TRANSITION VIII.1 → VIII.2
# Recrée les objets de statistiques (volumes_par_zone, stats_trafic) qui
# avaient été supprimés du patch de VIII.1 et qui sont requis en VIII.2.
# ==============================================================================

# ── Statistiques de trafic sur le réseau ──────────────────────────────────────
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

# ── Zones les plus actives (origines + destinations cumulées) ─────────────────
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

cat("✓ Transition VIII.1 → VIII.2 terminée\n\n")


# ==============================================================================
# VIII.3 : Exports finaux
# Exporte toutes les matrices (flux tonnes, offre/demande en M USD, IO)
# en CSV et le réseau complet avec volumes fret en GeoPackage.
# ==============================================================================

cat("Export des données du modèle de fret...\n")

write.csv(as.data.frame(A),
          file.path(DIR_EXPORTS,"table_io_coefficients.csv"),
          row.names = TRUE)
write.csv(recap_io,
          file.path(DIR_EXPORTS,"table_io_recap.csv"),
          row.names = FALSE)
write.csv(as.data.frame(flux_total) %>% rownames_to_column("Zone"),
          file.path(DIR_EXPORTS,"matrice_flux_gravitaire_tonnes.csv"),
          row.names = FALSE)
write.csv(as.data.frame(flux_tonnes_total) %>% rownames_to_column("Zone"),
          file.path(DIR_EXPORTS,"matrice_flux_fret_tonnes.csv"),
          row.names = FALSE)
write.csv(recap_zones,
          file.path(DIR_EXPORTS,"offre_demande_zones.csv"),
          row.names = FALSE)

# ── Export complémentaire : réseau avec volumes fret ──────────────────────────
aretes_fret_export <- reseau_rwanda %>%
  activate("edges") %>%
  st_as_sf() %>%
  select(osm_id, name, road_type, surface,
         length_km, speed_kmh, cost_per_tkm,
         volume_tonnes, classe_trafic,
         volume_camionnette, volume_camion_moyen, volume_camion_lourd,
         part_camion_lourd)

st_write(aretes_fret_export,
         file.path(DIR_EXPORTS, "reseau_rwanda_avec_fret.gpkg"),
         delete_dsn = TRUE, quiet = TRUE)
cat("✓ GeoPackage avec volumes fret exporté\n")

# Export du détail sectoriel par arête
# Ce fichier permet de répondre à des questions comme :
# "Quelle part du trafic sur la RN1 est de l'Agriculture ?"
aretes_fret_sectoriel <- aretes_fret_export %>%
  st_drop_geometry() %>%
  bind_cols(volume_par_secteur_df)

write.csv(aretes_fret_sectoriel,
          file.path(DIR_EXPORTS, "volumes_fret_par_secteur.csv"),
          row.names = FALSE)
cat("✓ Export sectoriel par arête sauvegardé\n")

# ==============================================================================
# SAUVEGARDE INTER-SCRIPTS
# ==============================================================================

cat("=== Sauvegarde des objets persistants (03_transport) ===\n")

# ── Sauvegarde 1 : flux et matrices gravitaires ───────────────────────────────
# On sauvegarde en premier les matrices de flux (plus légères), puis on les
# supprime immédiatement pour libérer de la RAM avant la sauvegarde du réseau.
saveRDS(
  list(
    flux_gravitaire     = flux_gravitaire,
    flux_total          = flux_total,          # n_total × n_total (inclut RoW)
    flux_tonnes_total   = flux_tonnes_total,   # n_warehouses × n_warehouses (projeté)
    offre_zones         = offre_zones,         # n_warehouses × N_SECTEURS (domestique)
    demande_zones       = demande_zones,       # n_warehouses × N_SECTEURS (domestique)
    offre_total         = offre_total,         # n_total × N_SECTEURS (domestique + RoW)
    demande_total       = demande_total,       # n_total × N_SECTEURS (domestique + RoW)
    noms_zones_uniques  = noms_zones_uniques,  # noms des n_warehouses zones domestiques
    noms_total          = noms_total,          # noms des n_total nœuds (incl. RoW)
    flux_par_secteur_df = flux_par_secteur_df,
    recap_zones         = recap_zones,
    date_creation       = Sys.time()
  ),
  PERSIST_FLUX_FRET
)
cat("✓ persist_flux_fret.rds\n")

# Libération immédiate après sauvegarde : ces objets ne sont plus nécessaires
# pour la sauvegarde du réseau qui suit. Sans ce rm(), ils resteraient en
# mémoire et doubleraient le pic RAM lors du saveRDS suivant.
rm(flux_gravitaire, flux_total, flux_tonnes_total,
   offre_zones, demande_zones, offre_total, demande_total,
   flux_par_secteur_df, recap_zones)
invisible(gc(full = TRUE))
invisible(gc(full = TRUE))
afficher_ram("entre les deux sauvegardes")

# ── Sauvegarde 2 : réseau enrichi avec volumes fret ───────────────────────────
# On ne sauvegarde que les objets effectivement lus par les scripts en aval
# (viz_fret.R, viz_vulnerabilite.R, 04_vulnerabilite.R).
# volume_trafic_mm_s / volume_trafic / volume_trafic_mm sont intentionnellement
# exclus : aucun script en aval ne les lit depuis ce fichier (ils sont déjà
# intégrés dans les arêtes de reseau_rwanda ou disponibles via affectation_cache).
saveRDS(
  list(
    reseau_rwanda         = reseau_rwanda,
    volume_par_secteur    = volume_par_secteur,
    volume_par_secteur_df = volume_par_secteur_df,
    volumes_par_zone      = volumes_par_zone,
    date_creation         = Sys.time()
  ),
  PERSIST_RESEAU_FRET
)
cat("✓ persist_reseau_fret.rds\n\n")

# ── Nettoyage final explicite ─────────────────────────────────────────────────
# Quand un script R se termine, la session tente de libérer automatiquement
# tous les objets en mémoire. Les gros objets (sfnetworks, igraph, tableaux 3D)
# peuvent provoquer un crash lors de ce nettoyage automatique si la RAM est
# saturée. On les détruit explicitement ici pour prévenir ce crash.
cat("── Nettoyage final ─────────────────────────────────────────────────────\n")

objets_fin <- c(
  "reseau_rwanda", "volume_trafic_mm_s", "volume_trafic",
  "volume_trafic_mm", "volume_par_secteur", "volume_par_secteur_df",
  "volumes_par_zone", "paires_actives", "aretes_fret_export",
  "aretes_fret_sectoriel", "aretes_emissions_base", "longueurs_km",
  "emissions_co2_aretes", "emissions_nox_aretes", "emissions_pm25_aretes",
  "noms_zones_uniques", "flux_total_long", "od_long"
)
rm(list = intersect(objets_fin, ls(envir = .GlobalEnv)), envir = .GlobalEnv)

# Vider env_lourds : contient le graphe multi-modal (plusieurs centaines de MB)
rm(list = ls(envir = env_lourds), envir = env_lourds)

invisible(gc(full = TRUE))
invisible(gc(full = TRUE))
afficher_ram("fin de script")

cat("✓ Nettoyage terminé — session stable\n")
cat("Lancer 04_vulnerabilite.R ou un script viz_*.R pour la suite.\n")