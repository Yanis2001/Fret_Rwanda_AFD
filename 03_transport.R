################################################################################
# 03_transport.R
# RÔLE : Matrice OD par Dijkstra multi-modal, modèle Input-Output de Leontief,
#        modèle gravitaire sectoriel, projection des flux RoW sur les frontières.
#        L'AFFECTATION du fret sur le réseau a été déplacée dans 04_affectation.R.
# ENTRÉES  : persist_reseau_couts.rds, persist_graphe_mm.rds,
#            persist_mapping_mm.rds, persist_entreposages.rds + DuckDB
# SORTIES  : persist_flux_fret.rds
#            + od_cache.rds (déjà existant), exports CSV/Parquet/GeoPackage
# DÉPEND DE : 00_parametres.R, 01_reseau.R, 02_couts.R
################################################################################

source("00_parametres.R")

cat("=== Chargement des objets de 01_reseau + 02_couts ===\n")

.ent  <- readRDS(PERSIST_ENTREPOSAGES)
.res  <- readRDS(PERSIST_RESEAU_COUTS)
.mm   <- readRDS(PERSIST_GRAPHE_MM)
.map  <- readRDS(PERSIST_MAPPING_MM)

list2env(.ent, envir = .GlobalEnv)
reseau      <- .res$reseau
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
graphe_igraph      <- reseau %>%
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
# DIR_CACHE_SCENARIO = DIR_CACHE en run de référence, et un sous-dossier dédié
# en test de sensibilité : la matrice OD dépend des paramètres surchargés
# (betas, valeur du temps, tonnages), elle ne doit donc jamais écraser le cache
# de référence.
CACHE_OD <- file.path(DIR_CACHE_SCENARIO, "od_cache.rds")
cache_od_valide <- FALSE

# Colonnes attendues dans od_long — à mettre à jour si la structure change
OD_COLONNES_ATTENDUES <- c(
  "id_origine", "id_destination", "nom_origine", "nom_destination",
  "cout_rwf", "distance_km", "temps_h",
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
    cout_rwf          = numeric(n_paires_max),
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
      od_long$cout_rwf[idx]          <- min_cout
      
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
    ROUND(AVG(cout_rwf), 2)           AS cout_moyen_rwf,
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
cat("  Coût moyen                   :", od_stats$cout_moyen_rwf, "RWF\n")
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
aretes_finales <- reseau %>%
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
st_write(aretes_finales, file.path(DIR_EXPORTS,"reseau_aretes.gpkg"),
         delete_dsn=TRUE, quiet=TRUE)

noeuds_finaux <- reseau %>%
  activate("nodes") %>%
  st_as_sf() %>%
  select(node_id, is_warehouse, warehouse_name, warehouse_type)
st_write(noeuds_finaux, file.path(DIR_EXPORTS, "reseau_noeuds.gpkg"),
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
# Définit les 11 secteurs, la matrice des coefficients techniques A,
# les productions totales et les facteurs de conversion valeur → tonnes.
# Calcule les multiplicateurs de Leontief (I-A)^(-1) et stocke dans DuckDB.
# NOTE : données fictives de démonstration. Pour utiliser les données réelles,
# remplacer A et production_totale ici uniquement.
# ==============================================================================

# La table Input-Output de Leontief modélise les interdépendances sectorielles :
#   a_ij = part de la production du secteur j consommée en intrant par le secteur i
#   Production totale : X = (I - A)^(-1) × D  [équation de Leontief]
#   où D = demande finale et (I - A)^(-1) = matrice des multiplicateurs de Leontief
#
# Interprétation de (I - A)^(-1) :
#   Un élément [i,j] donne l'augmentation de production du secteur i nécessaire
#   pour satisfaire une augmentation de 1 RWF de demande finale dans le secteur j.
#
# En termes concrets : si les ménages dépensent 1 RWF de plus en
# produits alimentaires (Agro_industrie), combien cela génère-t-il de production
# supplémentaire dans l'Agriculture (pour fournir les matières premières) ?
# C'est ce que calcule la matrice de Leontief.

# ── Grandeurs dérivées de la table IO ─────────────────────────────────────────
# %*% : produit matriciel en R (différent de * qui est une multiplication élément par élément).
# A %*% x donne le vecteur des consommations intermédiaires : pour chaque secteur i,
# la somme de a_ij × production_j sur tous les secteurs j fournisseurs.
conso_interm   <- as.vector(A %*% production_totale)

# Valeur ajoutée par secteur : reprise directe de lire_sam() (00_parametres.R)
valeur_ajoutee <- sam$va[SECTEURS]

# Demande finale extraite de la SAM IFPRI (cf. 00_parametres.R).
# DEMANDE_FINALE_SAM est un vecteur par secteur en MILLIARDS DE RWF.
# On réordonne selon l'ordre canonique SECTEURS pour garantir l'alignement des indices.
demande_finale <- DEMANDE_FINALE_SAM[SECTEURS]

# ── Stockage dans DuckDB ──────────────────────────────────────────────────────
io_table <- tibble(
  secteur             = SECTEURS,
  production_mrd_rwf     = production_totale,
  conso_interm_mrd_rwf   = conso_interm,
  valeur_ajoutee_mrd_rwf = valeur_ajoutee,
  demande_finale_mrd_rwf = demande_finale,
  tonnes_par_mrd_rwf     = TONNES_PAR_mrd_RWF
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
# L[i,j] = augmentation de production du secteur i nécessaire pour fournir 1 RWF
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

# prod_zones / dem_zones : production locale x[i,s] et demande totale d[i,s] en
# BRUT (avant tout netting). offre_zones/demande_zones n'en gardent que le solde
# net (max(0, x−d)), ce qui efface la production d'un secteur importateur net et
# l'empêche d'alimenter ses exports. On conserve donc ici les valeurs brutes :
# elles servent à décomposer le commerce extérieur (export tiré de la production
# brute, import couvrant la demande brute) dans la partie gravitaire VII.4.
prod_zones <- matrix(0, n_warehouses, N_SECTEURS,
                     dimnames=list(noeuds_entreposage$warehouse_name, SECTEURS))
dem_zones  <- matrix(0, n_warehouses, N_SECTEURS,
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
# ÉQUILIBRE AGRÉGÉ (prix de base) :
#   Σ_i x[i,s] = production_totale[s]  (par construction : Σ_i w[i,s] = 1)
#   Σ_i d[i,s] = conso_interm[s] + demande_finale[s]
#              = production_totale[s] + imports[s] − exports[s]  (bilan SAM)
#   → Σ_i offre[i,s] − Σ_i demande[i,s] = exports[s] − imports[s]
#   → Σ_i offre[i,s] + imports[s] = Σ_i demande[i,s] + exports[s]  ✓
#
#   Le résidu (exports − imports) n'est PAS négligeable pour les secteurs à
#   fort déséquilibre commercial (ex. Chimie_petrole : imports >> production).
#   Il est absorbé par les nœuds RoW (offre_row = imports, demande_row = exports)
#   ajoutés dans VII.2.C, et NON par la normalisation du Furness.
#
#   Les grandeurs production_totale, demande_finale, A sont toutes issues de
#   lire_sam() (00_parametres.R), au PRIX DE BASE : trc (marges de
#   distribution), stax et mtax (taxes sur produits) ont été retirés via le
#   facteur k = 1 − wedge / usages_acquisition. Le bilan est vérifié
#   explicitement ci-dessous après construction de offre_zones / demande_zones.
# ==============================================================================

# Postes-frontière "passage" (00_parametres.R, cf. warehouse_passage_uniquement
# dans 01_reseau.R Partie IV.3-bis) : sans cellule de Voronoï propre, ils n'ont
# ni population ni emploi (Partie IV.6/IV.4.F) et doivent rester à l'écart de
# la production/demande domestiques — ils ne servent qu'au commerce extérieur
# (idx_frontiere_par_pays, plus bas). emp_i est déjà nul pour eux ; is_passage_seul
# sert plus bas à annuler aussi le terme RWI (non pondéré par population), seul
# canal qui leur donnerait sinon une part de production non nulle.
is_passage_seul <- replace_na(noeuds_entreposage$warehouse_passage_uniquement, FALSE)

# Emploi national par secteur = somme sur les zones "ville" actives.
# Garantit que Σ_i x[i,s] = production_totale[s] (la production est entièrement
# allouée aux zones "ville", sans fuite hors réseau).
emploi_national <- colSums(emploi_zone_secteur[!is_passage_seul, , drop = FALSE], na.rm = TRUE)
# Protection contre division par zéro si un secteur est absent du RPHC5.
emploi_national[emploi_national == 0] <- 1

# Population totale des zones actives — dénominateur de la demande finale.
pop_totale <- sum(pop_i, na.rm = TRUE)
if (pop_totale == 0) stop("pop_totale est nulle — vérifier diag_population dans persist.")

# ── Extraction du RWI normalisé aligné sur noeuds_entreposage ─────────────────
# p_rwi est calculé dans 01_reseau.R (min-max sur [0,1]) et stocké dans diag_rwi.
# On réaligne sur noeuds_entreposage (même logique que pop_i ci-dessus).
p_rwi_zones <- diag_rwi$p_rwi[
  match(noeuds_entreposage$warehouse_name, diag_rwi$nom_zone)
]
p_rwi_zones <- replace_na(p_rwi_zones, median(p_rwi_zones, na.rm = TRUE))
stopifnot(length(p_rwi_zones) == n_warehouses)

# Dénominateur du terme RWI dans le poids de production (Σ_i p_rwi_i), calculé
# sur les seules zones "ville" (cf. is_passage_seul ci-dessus).
rwi_total <- sum(p_rwi_zones[!is_passage_seul])
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

# ── Demande finale par groupe de ménages ──────────────────────────────────────
# Chaque zone reçoit un MÉLANGE de paniers de consommation : sa population est
# répartie entre les 10 groupes de ménages de la SAM (strate urbain/rural ×
# quintile national de consommation), et elle consomme le panier de chaque groupe
# au prorata de la population qu'elle y détient :
#     d_finale_menages[i, s] = Σ_g C[s, g] × pop_groupe_zone[i, g] / N[g]
# avec C = DEMANDE_FINALE_GROUPES_SAM et N[g] = Σ_i pop_groupe_zone[i, g].
# Par construction Σ_i d_finale_menages[i, s] = Σ_g C[s, g] : le total national
# par secteur est exactement préservé.
#
# La consommation publique (gov, s-i, marges Commerce), sans dimension
# revenu/strate, reste spatialisée par le poids pop × RWI (z[i]).

groupes_sam <- colnames(DEMANDE_FINALE_GROUPES_SAM)


# On réordonne les colonnes sur l'ordre des groupes de la SAM et on vérifie que la
# population par zone est cohérente avec pop_i (le recalage a été fait en 01 ;
# on ne fait que le contrôler ici).
pop_groupe_zone <- pop_groupe_zone[, groupes_sam, drop = FALSE]
ecart_pop <- max(abs(rowSums(pop_groupe_zone) - pop_i))
if (ecart_pop > 1e-3) {
  stop("pop_groupe_zone incohérente avec pop_i (écart max = ", round(ecart_pop, 3),
       ") — relancer 01_reseau.R.")
}

# Nombre de groupes effectivement représentés dans chaque zone : mesure directe de
# ce que la méthode apporte (1 = équivalent à une classification par cellule).
n_groupes_par_zone <- rowSums(pop_groupe_zone > 0)
cat("  Groupes représentés par zone : médiane =", median(n_groupes_par_zone),
    "| min =", min(n_groupes_par_zone), "| max =", max(n_groupes_par_zone), "\n")

# Population nationale par groupe = dénominateur d'allocation des paniers.
N_groupe <- colSums(pop_groupe_zone)


# ── Demande finale des ménages, calculée en une fois pour toutes les zones ─────
# part_groupe[i,g] = pop_groupe_zone[i,g] / N[g] = part de la zone i dans la
# population du groupe g (Σ_i part_groupe[i,g] = 1 pour tout groupe peuplé).
# Le produit matriciel C × t(part_groupe) donne directement la matrice
# (secteur × zone) de la demande finale des ménages.
part_groupe <- sweep(pop_groupe_zone, 2, pmax(N_groupe, 1), "/")
d_finale_menages_mat <- DEMANDE_FINALE_GROUPES_SAM[SECTEURS, groupes_sam, drop = FALSE] %*%
                        t(part_groupe)

# Contrôle : le total national par secteur doit être exactement conservé.
stopifnot(max(abs(rowSums(d_finale_menages_mat) -
                  rowSums(DEMANDE_FINALE_GROUPES_SAM[SECTEURS, , drop = FALSE]))) < 1e-6)

cat("  Population par groupe SAM (milliers) :\n")
print(round(N_groupe / 1e3, 1))
cat("\n")

# Demande publique (gov, s-i, marges Commerce) : sans dimension revenu/strate,
# spatialisée par pop × RWI (z[i]) plutôt que par groupe.
demande_finale_pub_eff <- DEMANDE_FINALE_PUBLIQUE_SAM[SECTEURS]

for (i in seq_len(n_warehouses)) {

  # ── Production locale x[i,s] ────────────────────────────────────────────────
  # Poids composite w[i,s] = α × part_emploi[i,s] + (1-α) × part_rwi[i].
  # La part RWI est scalaire (même valeur pour tous les secteurs) car le RWI
  # est une caractéristique géographique de la zone, pas sectorielle.
  emp_i      <- emploi_zone_secteur[i, ]           # vecteur N_SECTEURS (effectifs bruts)
  part_emp_i <- emp_i / emploi_national            # part d'emploi par secteur (Σ_i = 1)
  # part_rwi_i forcée à 0 pour un poste-frontière "passage" : sans cette
  # exclusion il recevrait quand même une part de production via ce seul
  # terme RWI (non pondéré par population, contrairement à la demande finale).
  part_rwi_i <- if (is_passage_seul[i]) 0 else p_rwi_zones[i] / rwi_total  # part RWI scalaire (Σ_i = 1)
  w_i        <- ALPHA_EMPLOI_RWI * part_emp_i + (1 - ALPHA_EMPLOI_RWI) * part_rwi_i
  x_i        <- production_totale * w_i            # vecteur N_SECTEURS (mrd RWF)

  # ── Demande intermédiaire d_inter[i,s] ──────────────────────────────────────
  # Pour chaque secteur s, quantité consommée comme intrant par la production
  # locale de tous les secteurs r : Σ_r A[s,r] × x[i,r] = (A %*% x_i)[s].
  # A %*% x_i : produit matriciel (N_SECTEURS × N_SECTEURS) × (N_SECTEURS) → N_SECTEURS.
  d_inter_i <- as.vector(A %*% x_i)
  names(d_inter_i) <- SECTEURS

  # ── Demande finale d_finale[i,s] ────────────────────────────────────────────
  # Mélange des paniers des groupes présents dans la zone, chacun alloué au
  # prorata de la part de la zone dans la population de son groupe
  # (Σ_i pop_groupe_zone[i,g]/N_g = 1), AUQUEL s'ajoute la demande publique
  # (gov, s-i) spatialisée par pop × RWI.
  # Par construction Σ_i d_finale[i,s] = demande_finale[s] (total national inchangé).
  d_finale_menages_i <- d_finale_menages_mat[, i]
  d_finale_pub_i     <- demande_finale_pub_eff * (z_demande[i] / z_totale)
  d_finale_i <- d_finale_menages_i + d_finale_pub_i
  names(d_finale_i) <- SECTEURS

  # ── Demande totale et surplus/déficit ───────────────────────────────────────
  d_i <- d_inter_i + d_finale_i

  # pmax(0, ...) : max élément par élément entre 0 et le vecteur → remplace les
  # valeurs négatives par 0 (une zone ne peut pas avoir d'offre négative).
  offre_zones[i, ]   <- pmax(0, x_i - d_i)   # surplus exportable (mrd RWF)
  demande_zones[i, ] <- pmax(0, d_i - x_i)   # besoin importé     (mrd RWF)

  # Conservation des grandeurs brutes (non nettées) pour la décomposition
  # du commerce extérieur en VII.4.
  prod_zones[i, ] <- x_i                     # production locale brute (mrd RWF)
  dem_zones[i, ]  <- d_i                     # demande totale brute    (mrd RWF)
}

# ── Stockage dans DuckDB des zones domestiques (format long) ─────────────────
# Format long (1 ligne = 1 zone × 1 secteur) plus adapté aux jointures SQL.
# Seules les zones domestiques (n_warehouses) sont stockées ici ; les lignes
# RoW seront ajoutées ci-dessous dans une table séparée (offre_zones_row).
offre_long_df <- as.data.frame(offre_zones) %>%
  rownames_to_column("zone") %>%
  pivot_longer(-zone, names_to = "secteur", values_to = "offre_mrd_rwf")
duck_write(offre_long_df, "offre_zones")

demande_long_df <- as.data.frame(demande_zones) %>%
  rownames_to_column("zone") %>%
  pivot_longer(-zone, names_to = "secteur", values_to = "demande_mrd_rwf")
duck_write(demande_long_df, "demande_zones")

# Bilan par zone calculé directement en SQL
recap_zones <- duck_query("
  SELECT
    o.zone,
    ROUND(SUM(o.offre_mrd_rwf), 2)                  AS offre_totale_mrd_rwf,
    ROUND(SUM(d.demande_mrd_rwf), 2)                AS demande_totale_mrd_rwf,
    ROUND(SUM(o.offre_mrd_rwf - d.demande_mrd_rwf), 2) AS solde_mrd_rwf
  FROM offre_zones o
  JOIN demande_zones d ON o.zone = d.zone AND o.secteur = d.secteur
  GROUP BY o.zone
  ORDER BY offre_totale_mrd_rwf DESC
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
#   Kagitumba selon la destination interne.
#
#   Offre/demande des nœuds RoW : données de commerce extérieur NISR
#     offre[RoW_pays, s]   = importations du pays étudié (mrd RWF)
#     demande[RoW_pays, s] = exportations du pays étudié (mrd RWF)
#
# AFFECTATION AUX ROUTES :
#   Après le modèle gravitaire, les flux T[RoW_k, j] sont projetés sur le
#   nœud frontière b*(j) = argmin_b C_road[b,j] avant l'affectation All-or-Nothing.
#   Le segment pré-frontière (étranger) n'emprunte aucune route du pays étudié.
# ==============================================================================

# ── Pays RoW et leurs postes frontières ──────────────────────────────────────
# COMMERCE_EXTERIEUR_NISR définit les pays RoW (ordre canonique).
pays_row <- unique(COMMERCE_EXTERIEUR_NISR$pays)   # vecteur des pays RoW
n_row    <- length(pays_row)                       # = 4

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

# Coûts pré-frontière depuis DuckDB (pays × secteur → cout_rwf_tonne)
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
    # imports du pays depuis le pays k = ce que le nœud RoW envoie vers le pays
    offre_total[idx_k, s]   <- if (nrow(row_s) > 0) row_s$imports_mrd_rwf else 0
    # exports du pays vers le pays k = ce que le nœud RoW attire depuis le pays
    demande_total[idx_k, s] <- if (nrow(row_s) > 0) row_s$exports_mrd_rwf else 0
  }
}

# Stockage DuckDB des lignes RoW pour les visualisations
offre_row_df <- as.data.frame(offre_total[(n_warehouses+1):n_total, , drop = FALSE]) %>%
  rownames_to_column("zone") %>%
  pivot_longer(-zone, names_to = "secteur", values_to = "offre_mrd_rwf")
duck_write(offre_row_df, "offre_zones_row")

demande_row_df <- as.data.frame(demande_total[(n_warehouses+1):n_total, , drop = FALSE]) %>%
  rownames_to_column("zone") %>%
  pivot_longer(-zone, names_to = "secteur", values_to = "demande_mrd_rwf")
duck_write(demande_row_df, "demande_zones_row")

cat("✓ Couche RoW construite :", n_row, "pays,", n_total, "nœuds au total\n\n")

cat("Paramètres du modèle gravitaire:\n")
for (s in SECTEURS_FRET) {
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
  matrice_couts[i, j] <- od_long$cout_rwf[r]
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
#   cout_fixe_par_tonne[v] = (cout_chargement_rwf[v] + cout_dechargement_rwf[v])
#                            / capacite_tonnes[v]
#
#   La division par la capacité convertit un coût par trajet (RWF/trajet) en
#   coût par tonne transportée (RWF/tonne) — l'unité attendue par C_ij.
#
#   Ce coût par tonne décroît avec la capacité du véhicule : un camion lourd
#   (20t) a un coût de manutention par tonne plus faible qu'une camionnette
#   (3.5t), même si son coût de manutention absolu est plus élevé. C'est
#   l'économie d'échelle à la manutention.
#
#   Exemple numérique :
#     Camionnette  (3.5t)  : (14820 + 14820) / 3.5  ≈  8469 RWF/tonne
#     Camion moyen (7.5t)  : (24700 + 24700) / 7.5  ≈  6587 RWF/tonne
#     Camion lourd  (20t)  : (39520 + 39520) / 20.0 =  3952 RWF/tonne
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
    cout_chargement_rwf,
    cout_dechargement_rwf,
    -- Coût fixe total par trajet (RWF) = chargement + déchargement
    (cout_chargement_rwf + cout_dechargement_rwf)
      AS cout_fixe_trajet_rwf,
    -- Coût fixe par tonne (RWF/tonne) = coût par trajet / capacité
    -- C'est cette valeur qui s'ajoute à C_ij dans le modèle gravitaire
    (cout_chargement_rwf + cout_dechargement_rwf) / capacite_tonnes
      AS cout_fixe_par_tonne
  FROM params_flotte
  ORDER BY capacite_tonnes
")

cat("── Coûts fixes de manutention par véhicule ─────────────────────────────\n")
print(
  cout_fixe_par_vehicule %>%
    select(nom, capacite_tonnes, cout_fixe_trajet_rwf, cout_fixe_par_tonne) %>%
    mutate(
      cout_fixe_trajet_rwf = round(cout_fixe_trajet_rwf, 1),
      cout_fixe_par_tonne  = round(cout_fixe_par_tonne,  2)
    ) %>%
    rename(
      Véhicule             = nom,
      `Capacité (t)`       = capacite_tonnes,
      `Coût trajet (RWF)`  = cout_fixe_trajet_rwf,
      `Coût/tonne (RWF/t)` = cout_fixe_par_tonne
    )
)

# Extraction du coût fixe pour le véhicule de référence uniquement.
# Ce scalaire sera ajouté à l'ensemble de la matrice C_ij dans la boucle
# sectorielle ci-dessous.
cout_fixe_ref <- cout_fixe_par_vehicule$cout_fixe_par_tonne[
  cout_fixe_par_vehicule$vehicule_id == VEHICULE_REFERENCE
]

cat("\n  Coût fixe de référence (", VEHICULE_REFERENCE, ") :",
    round(cout_fixe_ref, 2), "RWF/tonne\n")
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
#              (= offre_total[, s] × TONNES_PAR_mrd_RWF[s])
#   D_s      — vecteur n_total des demandes sectorielles en tonnes (même convention)
#   friction — matrice n×n des termes (C_ij × TONNES_PAR_mrd_RWF[s])^(-beta). Doit
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
  
  # ── Contrôle de faisabilité du problème biproportionnel ─────────────────────
  # L'IPF ne converge que si sum(O) = sum(D). 

  total_O <- sum(O_s, na.rm = TRUE)
  total_D <- sum(D_s, na.rm = TRUE)

  # Cas dégénéré : secteur sans offre ou sans demande (ex : secteur Mines dans
  # une zone uniquement résidentielle). On retourne une matrice nulle.
  # Ce test précède le contrôle d'équilibre car un total nul des deux côtés est
  # un cas légitime (secteur absent), pas un déséquilibre.
  if (total_O < 1e-12 || total_D < 1e-12) {
    cat("  [", secteur, "] Offre ou demande nulle — matrice de flux vide\n")
    return(matrix(0, nrow = n, ncol = n))
  }

  # Écart relatif entre les deux totaux, rapporté au plus grand des deux.
  # Attendu : ~1e-8
  # Tout écart supérieur à TOL_EQUILIBRE_MARGES traduit une incohérence amont
  # réelle et rend le problème infaisable : on interrompt plutôt que de produire
  # une matrice de flux dont les marges ne veulent rien dire.
  ecart_rel <- abs(total_O - total_D) / max(total_O, total_D)
  if (ecart_rel > TOL_EQUILIBRE_MARGES) {
    stop("  [", secteur, "] Marges déséquilibrées : ΣO = ", round(total_O, 3),
         ", ΣD = ", round(total_D, 3), " (écart ", round(ecart_rel * 100, 6),
         " %).\n  → Vérifier l'allocation MRIO VII.2.B ",
         "(Σ_i prod_zones[i,s] doit valoir production_totale[s]).")
  }

  # Cibles de l'IPF 
  target_O <- O_s
  target_D <- D_s

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
              "\n  → Augmenter FURNESS_MAX_ITER.")
    }
  }
  
  # Remise à zéro de la diagonale par sécurité (peut avoir reçu un résidu
  # numérique lors des multiplications ligne/colonne successives)
  diag(T_mat) <- 0

  T_mat
}

# ── Variante RECTANGULAIRE de Furness (commerce extérieur) ────────────────────
#
# furness_rect() applique le même algorithme d'équilibrage biproportionnel (IPF)
# mais à une matrice RECTANGULAIRE nO × nD, sans notion de diagonale.
# Elle sert aux deux jambes du commerce extérieur, où origines et destinations
# sont des ensembles de nœuds DISJOINTS :
#   - jambe EXPORT : origines = zones domestiques (nO), destinations = nœuds RoW (nD)
#   - jambe IMPORT : origines = nœuds RoW (nO),        destinations = zones (nD)
#
# Contrairement à furness_gravity (carrée, échanges intra-zone interdits via la
# diagonale), ici aucune cellule n'est exclue : tout couple (origine, destination)
# du bloc est un échange potentiel.
#
# Paramètres :
#   O_s      — vecteur nO des offres (origines), en tonnes
#   D_s      — vecteur nD des demandes (destinations), en tonnes
#   friction — matrice nO × nD des termes (C_ij × TONNES)^(-beta), NA mis à 0
#   secteur  — chaîne pour les messages d'avertissement uniquement
#
# Retourne : matrice nO × nD de flux en tonnes respectant les deux marges.
# Par construction (sum(O) = sum(D), cf. VII.4), la normalisation est neutre.
furness_rect <- function(O_s, D_s, friction, secteur = "") {

  nO <- length(O_s); nD <- length(D_s)
  stopifnot(nrow(friction) == nO, ncol(friction) == nD)

  total_O <- sum(O_s, na.rm = TRUE)
  total_D <- sum(D_s, na.rm = TRUE)

  # Cas dégénéré : secteur sans export (ou sans import) → bloc de flux nul.
  if (total_O < 1e-12 || total_D < 1e-12) return(matrix(0, nO, nD))

  # Contrôle de faisabilité, identique à furness_gravity : les marges des jambes
  # export et import sont équilibrées par construction (les totaux RoW sont les
  # totaux SAM ventilés par des parts pays normalisées à 1, cf. VII.4-bis), donc
  # un écart dépassant le résidu d'équilibrage de la SAM est une erreur fatale.
  ecart_rel <- abs(total_O - total_D) / max(total_O, total_D)
  if (ecart_rel > TOL_EQUILIBRE_MARGES) {
    stop("  [", secteur, "] Marges déséquilibrées : ΣO = ", round(total_O, 3),
         ", ΣD = ", round(total_D, 3), " (écart ", round(ecart_rel * 100, 6),
         " %).\n  → Vérifier la cohérence entre les totaux SAM (imports/exports) ",
         "et les marges spatialisées e_zones / m_zones.")
  }

  # Cibles de l'IPF 
  target_O <- O_s
  target_D <- D_s

  # Matrice initiale T_ij = O_i × D_j × friction_ij, NA/Inf remis à 0.
  T_mat <- outer(O_s, D_s) * friction
  T_mat[is.na(T_mat) | is.nan(T_mat) | is.infinite(T_mat)] <- 0

  for (iter in seq_len(FURNESS_MAX_ITER)) {
    # Étape A : équilibrage des lignes (origines)
    A_i <- target_O / pmax(rowSums(T_mat), 1e-12)
    A_i[target_O < 1e-12] <- 0
    T_mat <- T_mat * A_i
    # Étape B : équilibrage des colonnes (destinations)
    B_j <- target_D / pmax(colSums(T_mat), 1e-12)
    B_j[target_D < 1e-12] <- 0
    T_mat <- t(t(T_mat) * B_j)

    # Test de convergence sur les marges à cible non nulle
    err_O <- if (any(target_O > 1e-12)) max(
      abs(rowSums(T_mat)[target_O > 1e-12] - target_O[target_O > 1e-12]) /
        target_O[target_O > 1e-12]) else 0
    err_D <- if (any(target_D > 1e-12)) max(
      abs(colSums(T_mat)[target_D > 1e-12] - target_D[target_D > 1e-12]) /
        target_D[target_D > 1e-12]) else 0
    if (max(err_O, err_D) < FURNESS_TOL) break

    if (iter == FURNESS_MAX_ITER) {
      warning("  [", secteur, "] Furness rectangulaire non convergé après ",
              FURNESS_MAX_ITER, " itérations. Erreur finale : ",
              round(max(err_O, err_D) * 100, 4), "%")
    }
  }

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

# ==============================================================================
# VII.4-bis : Décomposition brut/net des marges (commerce extérieur en brut)
#
#   On sépare trois flux physiquement distincts et on les résout séparément :
#     1. EXPORT      : zones (production brute) → nœuds RoW
#     2. IMPORT      : nœuds RoW → zones (demande brute)
#     3. DOMESTIQUE  : zone → zone, sur le solde net après commerce extérieur
#
# HYPOTHÈSE D'EXPOSITION UNIFORME AU COMMERCE EXTÉRIEUR :
#   Chaque zone exporte la même fraction τ_E de sa production et importe la même
#   fraction τ_M de sa demande, où :
#       τ_E[s] = exports[s]  / production_totale[s]   (propension à exporter)
#       τ_M[s] = imports[s]  / Σ_i d[i,s]             (pénétration des imports)
#   Comme exports[s] ≤ production_totale[s] pour tous les secteurs, τ_E ∈ [0,1] : 
#   les exports proviennent de la production domestique (pas de ré-export). 
#   De même τ_M ∈ [0,1].
#
#   Marges par zone qui en découlent :
#       e_zones[i,s]     = τ_E[s] × x[i,s]                (origine de la jambe export)
#       m_zones[i,s]     = τ_M[s] × d[i,s]                (destination jambe import)
#       o_dom_zones[i,s] = max(0, x[i,s](1−τ_E) − d[i,s](1−τ_M))  (surplus domestique)
#       q_dom_zones[i,s] = max(0, d[i,s](1−τ_M) − x[i,s](1−τ_E))  (déficit domestique)
#
# ÉQUILIBRE DE CHAQUE JAMBE (condition de faisabilité du biproportionnel) :
#   Σ_i e_zones[i,s] = τ_E × X = exports[s] = Σ_k exports_RoW[k]   (export équilibré)
#   Σ_i m_zones[i,s] = τ_M × D = imports[s] = Σ_k imports_RoW[k]   (import équilibré)
#   Σ_i o_dom_zones  = Σ_i q_dom_zones = X − exports = D − imports (domestique équil.)
#   La dernière égalité découle du bilan SAM (X + imports = D + exports).
# ==============================================================================

# Agrégats de commerce extérieur de la SAM, au prix de base (mrd RWF).
# Ce sont les totaux nationaux à répartir entre les zones par les propensions
# ci-dessous ; ils servent aussi de cibles de marges aux jambes export/import.
imports_s <- sam$imports[SECTEURS]
exports_s <- sam$exports[SECTEURS]

# Propensions sectorielles (uniformes entre zones)
tau_E <- ifelse(production_totale[SECTEURS] > 1e-12,
                exports_s / production_totale[SECTEURS], 0)
D_nat <- colSums(dem_zones)                          # demande domestique totale par secteur
tau_M <- ifelse(D_nat > 1e-12, imports_s / D_nat, 0)
names(tau_E) <- SECTEURS; names(tau_M) <- SECTEURS

# Matrices de marges (zone × secteur), en mrd RWF — sweep multiplie chaque
# colonne s par le scalaire tau correspondant (recyclage par colonne).
e_zones     <- sweep(prod_zones, 2, tau_E,     `*`)  # production exportée
m_zones     <- sweep(dem_zones,  2, tau_M,     `*`)  # demande couverte par import
prod_dom    <- sweep(prod_zones, 2, 1 - tau_E, `*`)  # production restant au marché domestique
dem_dom     <- sweep(dem_zones,  2, 1 - tau_M, `*`)  # demande restant au marché domestique
o_dom_zones <- pmax(prod_dom - dem_dom, 0)           # surplus domestique (origine domestique)
q_dom_zones <- pmax(dem_dom - prod_dom, 0)           # déficit domestique (destination domestique)

# Indices des blocs domestique / RoW dans les matrices n_total × n_total
idx_dom <- 1:n_warehouses
idx_row <- (n_warehouses + 1):n_total

for (s in SECTEURS_FRET) {

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
      pull(cout_rwf_tonne)
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
    # RoW ↔ RoW : non défini (NA) — le transit entre pays via le pays étudié est négligeable
  }

  # Diagonale NA (pas d'échange intrazone)
  diag(C_total_s) <- NA

  # ── Ajout du coût fixe de manutention ────────────────────────────────────────
  # Le coût fixe crée un plancher empêchant l'explosion de C^(-beta) pour les
  # paires très proches. Il s'additionne naturellement : NA + scalaire = NA.
  #   C_total_s_final = C_route_ou_RoW + cout_fixe_manutention
  C_total_s_final <- C_total_s + cout_fixe_ref

  # ── Calcul de la friction spatiale ───────────────────────────────────────────
  # On multiplie C_total_s_final (RWF/tonne) par TONNES_PAR_mrd_RWF[s] (tonnes/mrd RWF)
  # pour obtenir un coût adimensionné : RWF dépensés en transport par mrd RWF de biens.
  # Ce rapport (coût de transport / valeur des biens) est la friction du modèle
  # iceberg. Beta mesure alors l'élasticité du commerce à ce coût RELATIF,
  # ce qui rend son interprétation cohérente entre secteurs et calibrable sur
  # des données empiriques.
  friction                  <- (C_total_s_final * TONNES_PAR_mrd_RWF[s])^(-beta_s)
  friction[is.na(friction)] <- 0
  diag(friction)            <- 0

  # ── Découpage de la friction en blocs ────────────────────────────────────────
  # friction est n_total × n_total. On en extrait trois blocs correspondant aux
  # trois jambes. Le bloc RoW↔RoW (transit) n'est volontairement pas utilisé.
  Tcoef <- TONNES_PAR_mrd_RWF[s]                     # facteur mrd RWF → tonnes
  F_dom <- friction[idx_dom, idx_dom, drop = FALSE]  # zone → zone
  F_exp <- friction[idx_dom, idx_row, drop = FALSE]  # zone → frontière (export)
  F_imp <- friction[idx_row, idx_dom, drop = FALSE]  # frontière → zone (import)

  # ── Trois sous-problèmes biproportionnels (marges converties en tonnes) ───────
  # 1. DOMESTIQUE — carré, échanges intra-zone interdits (diagonale nulle) :
  #    origine = surplus domestique, destination = déficit domestique.
  flux_dom <- furness_gravity(
    O_s      = o_dom_zones[, s] * Tcoef,
    D_s      = q_dom_zones[, s] * Tcoef,
    friction = F_dom,
    secteur  = paste0(s, " · dom.")
  )
  # 2. EXPORT — rectangulaire : origine = production exportée des zones,
  #    destination = exports par poste-pays (lignes RoW de demande_total).
  flux_exp <- furness_rect(
    O_s      = e_zones[, s]            * Tcoef,
    D_s      = demande_total[idx_row, s] * Tcoef,
    friction = F_exp,
    secteur  = paste0(s, " · exp.")
  )
  # 3. IMPORT — rectangulaire : origine = imports par poste-pays (lignes RoW
  #    d'offre_total), destination = demande des zones couverte par import.
  flux_imp <- furness_rect(
    O_s      = offre_total[idx_row, s] * Tcoef,
    D_s      = m_zones[, s]            * Tcoef,
    friction = F_imp,
    secteur  = paste0(s, " · imp.")
  )

  # ── Assemblage de la matrice de flux n_total × n_total (tonnes) ───────────────
  # Le bloc RoW↔RoW reste nul (pas de transit). Structure compatible avec la
  # projection des flux RoW (VII.5) et l'affectation (VIII).
  T_s <- matrix(0, n_total, n_total, dimnames = list(noms_total, noms_total))
  T_s[idx_dom, idx_dom] <- flux_dom
  T_s[idx_dom, idx_row] <- flux_exp
  T_s[idx_row, idx_dom] <- flux_imp
  flux_gravitaire[[s]] <- T_s

  # Accumulation dans la matrice de flux toutes-secteurs
  flux_total <- flux_total + T_s
}

# ── Vérification des contraintes de marges ────────────────────────────────────
# On contrôle que les flux sortants/entrants de chaque nœud correspondent bien
# à ses marges cibles, décomposées (cf. VII.4-bis) :
#   - nœud domestique i : sortie attendue  = surplus domestique + production exportée
#                         entrée  attendue = déficit domestique + demande importée
#   - nœud RoW k        : sortie attendue  = imports du pays k
#                         entrée  attendue = exports vers le pays k
# Les cibles étant équilibrées par jambe (Σ origines = Σ destinations), chaque
# marge doit être respectée exactement : un écart > 0.01 % signale un défaut de
# convergence. La vérification porte sur les n_total nœuds (domestiques + RoW).

cat("\n── Vérification des contraintes de marges ─────────────────────────────\n")

for (s in SECTEURS_FRET) {

  T_s   <- flux_gravitaire[[s]]
  Tcoef <- TONNES_PAR_mrd_RWF[s]

  # Cibles complètes par nœud (tonnes) : on concatène le bloc domestique et le
  # bloc RoW. Origine = ce qui doit sortir du nœud ; destination = ce qui entre.
  target_O <- c(o_dom_zones[, s] + e_zones[, s],          # zones : surplus + export
                offre_total[idx_row, s]) * Tcoef          # RoW   : imports
  target_D <- c(q_dom_zones[, s] + m_zones[, s],          # zones : déficit + import
                demande_total[idx_row, s]) * Tcoef        # RoW   : exports

  # Erreur relative maximale sur les seules marges non nulles
  zones_O_actives <- target_O > 1e-9
  zones_D_actives <- target_D > 1e-9

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
  Secteur           = SECTEURS_FRET,
  Beta              = unname(BETA_SECTEUR[SECTEURS_FRET]),
  Flux_total_tonnes = sapply(SECTEURS_FRET, function(s) round(sum(flux_gravitaire[[s]]), 0)),
  Flux_moyen_tonnes = sapply(SECTEURS_FRET, function(s) {
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
# VII.5 : Projection des flux RoW sur le réseau routier du pays
#
# PRINCIPE :
#   Les nœuds RoW sont virtuels (hors réseau routier). Pour l'affectation
#   All-or-Nothing (04_affectation.R), chaque flux T[RoW_k, j] (ou T[j, RoW_k])
#   doit être attribué à un chemin physique. On l'injecte au poste frontière
#   optimal b*(j) = argmin_b C_road[b, j] sur les frontières du pays k.
#   Seul le segment intérieur b*(j) → j est affecté aux routes.
#
#   Remarque : le coût pré-frontière (segment étranger) est déjà intégré dans
#   le modèle gravitaire (C_total_s) et ne génère aucun trafic sur les routes
#   du pays étudié.
#
# RÉSULTAT :
#   flux_gravitaire[[s]] : matrices (n_warehouses × n_warehouses) PROJETÉES,
#     une par secteur. Ce sont elles que 04/05 lisent pour affecter les volumes.
#   flux_tonnes_total    : leur somme, utilisée pour sélectionner les paires OD
#     actives et pour l'empreinte du cache d'affectation.
#   flux_gravitaire_ext  : les matrices d'origine (n_total × n_total, RoW en
#     lignes/colonnes séparées), conservées pour les diagnostics et les exports.
#
# POURQUOI LA PROJECTION EST FAITE SECTEUR PAR SECTEUR :
#   L'affectation lit flux_gravitaire[[s]][i, j] sur des indices 1..n_warehouses
#   issus de flux_tonnes_total. Si les matrices sectorielles restaient en
#   n_total × n_total, tout le bloc RoW (lignes/colonnes n_warehouses+1..n_total)
#   ne serait jamais lu : le tonnage importé et exporté disparaîtrait de
#   l'affectation, et d'autant plus fortement que le secteur dépend du commerce
#   extérieur. La projection doit donc produire directement les matrices
#   sectorielles utilisées en aval, et non seulement leur somme.
# ==============================================================================

cat("── Projection des flux RoW sur les postes frontières ──────────────────\n")

# ── Choix du poste frontière par pays et par zone ─────────────────────────────
# Pour chaque pays k et chaque zone j : b*(j) = argmin_b C_road[b, j] sur les
# postes frontières du pays k. C_ij ne dépend pas du secteur (le coût routier
# est le même quelle que soit la marchandise), donc ce choix est calculé UNE
# FOIS ici et réutilisé pour tous les secteurs — d'où deux tables d'indices :
#   b_import[k, j] : poste d'entrée du flux RoW_k → j
#   b_export[k, j] : poste de sortie du flux j → RoW_k
# NA signifie « aucun poste frontière connu pour ce pays » : le flux est alors
# ignoré (comptabilisé plus bas comme tonnage non projeté).
b_import <- matrix(NA_integer_, nrow = length(pays_row), ncol = n_warehouses)
b_export <- matrix(NA_integer_, nrow = length(pays_row), ncol = n_warehouses)

# Sélection du poste le moins coûteux parmi une liste de candidats. Une zone
# peut n'être reliée à aucun poste frontière du pays par une route connue :
# C_ij vaut alors NA ou Inf pour tous les candidats, et which.min() renverrait
# un vecteur vide. On renvoie NA dans ce cas — le flux correspondant sera
# ignoré à la projection et signalé par le contrôle de conservation.
choisir_poste <- function(couts, idxs_b) {
  finis <- which(is.finite(couts))
  if (length(finis) == 0) return(NA_integer_)
  as.integer(idxs_b[finis[which.min(couts[finis])]])
}

for (k in seq_along(pays_row)) {
  idxs_b <- idx_frontiere_par_pays %>% filter(pays == pays_row[k]) %>% pull(idx)
  if (length(idxs_b) == 0) next
  for (j in seq_len(n_warehouses)) {
    b_import[k, j] <- choisir_poste(C_ij[idxs_b, j], idxs_b)
    b_export[k, j] <- choisir_poste(C_ij[j, idxs_b], idxs_b)
  }
}

# ── Projection d'une matrice étendue vers le réseau domestique ────────────────
# Prend une matrice n_total × n_total et renvoie sa version n_warehouses ×
# n_warehouses : le bloc domestique est repris tel quel, et chaque flux
# impliquant un nœud RoW est réinjecté sur le poste frontière retenu ci-dessus.
projeter_row_sur_frontieres <- function(M_ext) {

  M <- M_ext[seq_len(n_warehouses), seq_len(n_warehouses), drop = FALSE]

  for (k in seq_along(pays_row)) {
    idx_k <- n_warehouses + k

    for (j in seq_len(n_warehouses)) {
      # Import RoW_k → j : le trafic naît au poste frontière b_import
      vol_import <- M_ext[idx_k, j]
      if (vol_import > 0 && !is.na(b_import[k, j])) {
        M[b_import[k, j], j] <- M[b_import[k, j], j] + vol_import
      }
      # Export j → RoW_k : le trafic s'arrête au poste frontière b_export
      vol_export <- M_ext[j, idx_k]
      if (vol_export > 0 && !is.na(b_export[k, j])) {
        M[j, b_export[k, j]] <- M[j, b_export[k, j]] + vol_export
      }
    }
  }

  M
}

# ── Application secteur par secteur ───────────────────────────────────────────
# On conserve les matrices étendues sous flux_gravitaire_ext (diagnostics,
# export CSV, lecture des échanges avec chaque pays), et flux_gravitaire devient
# la version projetée : c'est le seul objet lu par 04_affectation.R et
# 05_vulnerabilite.R.
flux_gravitaire_ext <- flux_gravitaire
flux_gravitaire     <- lapply(flux_gravitaire_ext, projeter_row_sur_frontieres)
names(flux_gravitaire) <- names(flux_gravitaire_ext)

# flux_gravitaire[[s]] est déjà en tonnes (O_s et D_s ont été convertis avant
# Furness) : la somme sectorielle donne directement la matrice d'affectation.
flux_tonnes_total <- Reduce(`+`, flux_gravitaire)

# ── Contrôle de conservation du tonnage ───────────────────────────────────────
# La projection est un simple déplacement de masse : rien ne doit se perdre,
# sauf les flux d'un pays sans poste frontière renseigné. Un écart inattendu
# signalerait une désynchronisation entre les matrices sectorielles et la
# matrice d'affectation — exactement le défaut que cette section corrige.
tonnage_avant <- sum(sapply(flux_gravitaire_ext, sum))
tonnage_total <- sum(flux_tonnes_total)
ecart_projection <- tonnage_avant - tonnage_total

cat("  Tonnage sectoriel avant projection  :",
    format(round(tonnage_avant), big.mark = " "), "tonnes\n")
cat("  Tonnage domestique après projection :",
    format(round(tonnage_total), big.mark = " "), "tonnes\n")

if (abs(ecart_projection) > 1e-6 * max(1, tonnage_avant)) {
  warning(sprintf(
    paste0("Projection RoW : %s tonnes non projetees (%.3f %% du total). ",
           "Verifier que chaque pays RoW dispose d'au moins un poste frontiere ",
           "dans idx_frontiere_par_pays."),
    format(round(ecart_projection), big.mark = " "),
    100 * ecart_projection / tonnage_avant
  ))
} else {
  cat("  ✓ Conservation du tonnage vérifiée (écart nul)\n")
}

# Les dimensions doivent être cohérentes : c'est l'invariant sur lequel repose
# l'indexation de flux_gravitaire[[s]][i, j] dans l'affectation.
stopifnot(
  all(sapply(flux_gravitaire, nrow) == n_warehouses),
  all(sapply(flux_gravitaire, ncol) == n_warehouses),
  identical(dim(flux_tonnes_total), c(n_warehouses, n_warehouses))
)
cat("  ✓ Matrices sectorielles projetées :", length(flux_gravitaire), "secteurs en",
    n_warehouses, "×", n_warehouses, "\n\n")


################################################################################
# SAUVEGARDE INTER-SCRIPTS (modèle économique et gravitaire)
################################################################################

cat("=== Sauvegarde des objets persistants (03_transport) ===\n")

saveRDS(
  list(
    flux_gravitaire     = flux_gravitaire,     # liste par secteur, n_warehouses × n_warehouses (PROJETÉ) — lu par 04/05
    flux_gravitaire_ext = flux_gravitaire_ext,  # liste par secteur, n_total × n_total (RoW séparés) — diagnostics
    flux_total          = flux_total,          # n_total × n_total (inclut RoW)
    flux_tonnes_total   = flux_tonnes_total,   # n_warehouses × n_warehouses (somme des secteurs projetés)
    offre_zones         = offre_zones,         # n_warehouses × N_SECTEURS (domestique, NET : max(0,x−d))
    demande_zones       = demande_zones,       # n_warehouses × N_SECTEURS (domestique, NET : max(0,d−x))
    prod_zones          = prod_zones,          # n_warehouses × N_SECTEURS (production locale BRUTE x[i,s], avant netting)
    dem_zones           = dem_zones,           # n_warehouses × N_SECTEURS (demande totale BRUTE d[i,s], avant netting)
    e_zones             = e_zones,             # n_warehouses × N_SECTEURS (exports par zone = τ_E·prod_zones, jambe gravitaire export)
    m_zones             = m_zones,             # n_warehouses × N_SECTEURS (imports par zone = τ_M·dem_zones, jambe gravitaire import)
    offre_total         = offre_total,         # n_total × N_SECTEURS (domestique + RoW)
    demande_total       = demande_total,       # n_total × N_SECTEURS (domestique + RoW)
    noms_zones_uniques  = noms_zones_uniques,  # noms des n_warehouses zones domestiques
    noms_total          = noms_total,          # noms des n_total nœuds (incl. RoW)
    flux_par_secteur_df = flux_par_secteur_df,
    recap_zones         = recap_zones,
    A                   = A,                    # table IO (coefficients techniques), exportée par 04
    recap_io            = recap_io,             # récapitulatif IO (output, demande finale…), exporté par 04
    date_creation       = Sys.time()
  ),
  PERSIST_FLUX_FRET
)
cat("✓ persist_flux_fret.rds\n\n")

# ── Nettoyage final explicite ─────────────────────────────────────────────────
# Quand un script R se termine, la session tente de libérer automatiquement tous
# les objets en mémoire. Les gros objets (sfnetworks, igraph) peuvent provoquer
# un crash lors de ce nettoyage si la RAM est saturée. On les détruit donc
# explicitement ici. 04_affectation.R recharge de toute façon ses entrées depuis
# les fichiers persist_*.rds, ce nettoyage ne le pénalise pas.
cat("── Nettoyage final ─────────────────────────────────────────────────────\n")

objets_fin <- c(
  "flux_gravitaire", "flux_gravitaire_ext", "flux_total", "flux_tonnes_total",
  "offre_zones", "demande_zones", "prod_zones", "dem_zones",
  "e_zones", "m_zones", "offre_total", "demande_total",
  "flux_par_secteur_df", "recap_zones", "A", "recap_io",
  "reseau", "graphe_igraph", "od_long", "flux_total_long",
  "noms_zones_uniques"
)
rm(list = intersect(objets_fin, ls(envir = .GlobalEnv)), envir = .GlobalEnv)

# Vider env_lourds : contient le graphe multi-modal (plusieurs centaines de MB).
# 04_affectation.R le recharge depuis persist_graphe_mm.rds.
rm(list = ls(envir = env_lourds), envir = env_lourds)

invisible(gc(full = TRUE))
invisible(gc(full = TRUE))

cat("✓ 03_transport terminé — affectation déléguée à 04_affectation.R\n")
