################################################################################
# outils_affectation_equilibre.R
# RÔLE : SOURCE UNIQUE de l'affectation du fret au réseau À L'ÉQUILIBRE (BPR/MSA).
#        Appelée par 04_affectation.R (affectation principale) ET
#        05_vulnerabilite.R (ré-équilibre sur réseau intact/dégradé).
#
# POURQUOI une fonction partagée : garantir que 04 et 05 utilisent EXACTEMENT la
#        même méthode d'équilibre (pas de dérive entre deux copies du code).
#          - 04 : appel sans arête bloquée → affectation + comptabilité EOQ ;
#          - 05 baseline : appel sans arête bloquée → poids congestionnés de réf. ;
#          - 05 dégradé  : appel avec arêtes bloquées → report de trafic + re-congestion.
#
# ENTRÉES (objets globaux préparés par le script appelant — 04 ou 05) :
#   - graphe_multimodal (via recuperer_lourd) avec attributs weight, weight_temps,
#     travel_time_h
#   - lookup_type / lookup_physique / lookup_vehicule, max_idx_mm
#   - n_aretes_physiques, n_vehicules, n_warehouses, node_multi(), warehouse_nodes_base
#   - reseau (pour le road_type des arêtes physiques)
#   - flux_gravitaire (matrices OD tonnes par secteur, n_warehouses × n_warehouses,
#     flux RoW déjà projetés sur les postes frontières), flux_tonnes_total
#   - paramètres 00 : CONGESTION, BPR_ALPHA/BETA, MSA_MAX_ITER/TOL, SECTEURS,
#     VEHICULES_IDS, params_flotte_df, capacites_route_df, TAUX_CHARGEMENT,
#     JOURS_TRAFIC_AN, TAUX_DETENTION_STOCK, EOQ_REMPLISSAGE_MIN,
#     VALEUR_RWF_PAR_TONNE, HEURES_PAR_AN, SEUIL_FLUX_TONNES
################################################################################


# ──────────────────────────────────────────────────────────────────────────────
# preparer_congestion() : construit les objets invariants de la congestion.
# (Utilisée aussi par 04 pour le chemin « cache valide », où l'affectation n'est
#  pas rejouée mais où C_phys / conv_v restent nécessaires au taux de saturation.)
# Renvoie une liste avec, par arête physique ou par véhicule :
#   C_phys          : capacité d'écoulement (PCU/jour) par arête physique
#   conv_v          : coefficient tonnes/an → PCU/jour, par véhicule
#   K_vec           : coût fixe par trajet (chargement+déchargement), par véhicule
#   cap_vec         : capacité de chargement (tonnes), par véhicule
# ──────────────────────────────────────────────────────────────────────────────
preparer_congestion <- function() {

  # road_type de chaque arête PHYSIQUE, dans l'ordre des indices physiques
  # (1..n_aretes_physiques = ordre des arêtes de `reseau`).
  road_type_phys <- reseau %>%
    activate("edges") %>%
    as_tibble() %>%
    pull(road_type)

  # Capacité (PCU/jour) déduite du type de route ; types inconnus/NA → capacité
  # minimale (évite une division par NA dans le facteur BPR).
  C_phys <- capacites_route_df$capacite_pcu_jour[
    match(road_type_phys, capacites_route_df$road_type)
  ]
  C_phys[is.na(C_phys)] <- min(capacites_route_df$capacite_pcu_jour)

  # Conversion tonnes/an → PCU/jour, un coefficient par véhicule dans l'ordre des
  # colonnes du tableau de trafic (VEHICULES_IDS$vehicule_id) :
  #   conv_v = facteur_pcu / (capacite_tonnes × TAUX_CHARGEMENT × JOURS_TRAFIC_AN)
  .veh_order <- VEHICULES_IDS$vehicule_id
  .m_veh     <- match(.veh_order, params_flotte_df$vehicule_id)
  conv_v <- params_flotte_df$facteur_pcu[.m_veh] /
    (params_flotte_df$capacite_tonnes[.m_veh] * TAUX_CHARGEMENT * JOURS_TRAFIC_AN)

  # Paramètres EOQ par véhicule, MÊME ordre que les colonnes de trafic :
  #   K_vec = coût fixe par trajet ; cap_vec = capacité de chargement (tonnes)
  K_vec           <- params_flotte_df$cout_chargement_rwf[.m_veh] +
                     params_flotte_df$cout_dechargement_rwf[.m_veh]
  cap_vec         <- params_flotte_df$capacite_tonnes[.m_veh]

  list(
    C_phys          = C_phys,
    conv_v          = conv_v,
    K_vec           = K_vec,
    cap_vec         = cap_vec,
    road_type_phys  = road_type_phys
  )
}


# ──────────────────────────────────────────────────────────────────────────────
# affecter_equilibre_msa(aretes_bloquees) : affectation du fret à l'équilibre.
#
# Rejoue la boucle d'équilibre MSA/BPR : à chaque itération, une affectation
# All-or-Nothing multimodale sectorielle (avec taille d'envoi q* de Wilson et
# comptabilité EOQ) sur des coûts congestionnés, puis moyennage de la charge
# (pas 1/n) jusqu'à convergence (gap < MSA_TOL). Si CONGESTION = FALSE, une seule
# passe AON à coûts libres.
#
# ARGUMENT :
#   aretes_bloquees : vecteur d'indices d'arêtes PHYSIQUES à retirer du réseau
#                     (poids Inf dans toutes les couches véhicule). Vide par
#                     défaut → réseau intact.
#
# RENVOIE une liste :
#   volume_trafic_mm_s : charge d'équilibre (tonnes) [arête, véhicule, secteur]
#   compta_eoq         : ventilation des coûts logistiques par secteur (6 colonnes)
#   V_phys             : charge physique d'équilibre par arête physique (PCU/jour)
#   saturation_phys    : V/C par arête physique
#   C_phys             : capacité PCU/jour par arête physique
#   poids_mm           : poids multimodaux CONGESTIONNÉS d'équilibre (RWF/tonne),
#                        Inf sur les arêtes bloquées → réutilisables tels quels
#                        dans un Dijkstra (utilisé par 05)
#   temps_mm_c         : temps de trajet congestionné par arête mm (heures)
#   paires_traitees / paires_non_connectees : comptages (dernière itération)
# ──────────────────────────────────────────────────────────────────────────────
affecter_equilibre_msa <- function(aretes_bloquees = integer(0)) {

  # ── Objets invariants de la congestion (capacités, conversions PCU, EOQ) ──────
  prep            <- preparer_congestion()
  C_phys          <- prep$C_phys
  conv_v          <- prep$conv_v
  K_vec           <- prep$K_vec
  cap_vec         <- prep$cap_vec

  N_SECTEURS <- length(SECTEURS)

  # ── Poids de référence à charge nulle et décomposition temps / hors-temps ─────
  # On n'applique la congestion (BPR) qu'à la composante TEMPS du coût généralisé
  # (carburant + usure ne dépendent pas de l'encombrement). weight_temps est
  # calculé en 02_couts.R.
  g              <- recuperer_lourd("graphe_multimodal")
  poids_mm_libre <- igraph::E(g)$weight

  temps_mm <- igraph::E(g)$travel_time_h
  temps_mm[is.na(temps_mm)] <- 0

  poids_temps_mm <- igraph::E(g)$weight_temps
  if (is.null(poids_temps_mm))
    stop("Attribut d'arête 'weight_temps' absent du graphe multimodal : ",
         "relancer 02_couts.R pour régénérer persist_graphe_mm.rds.")
  poids_temps_mm[is.na(poids_temps_mm)] <- 0
  poids_horstemps_mm <- pmax(poids_mm_libre - poids_temps_mm, 0)

  # ── Arêtes multimodales à bloquer (toutes les couches véhicule des arêtes
  # physiques perturbées) : poids Inf → jamais empruntées, charge nulle. ─────────
  .est_route <- lookup_type == "route"
  indices_mm_bloques <- if (length(aretes_bloquees) > 0) {
    which(.est_route & lookup_physique %in% aretes_bloquees)
  } else {
    integer(0)
  }

  # ── Garde-fou : cohérence dimensionnelle des matrices de flux ────────────────
  # Les paires OD sont sélectionnées sur flux_tonnes_total, puis le volume est
  # lu dans flux_gravitaire[[s]][i, j] avec les MÊMES indices. Si les matrices
  # sectorielles étaient plus grandes (nœuds RoW non projetés en lignes/colonnes
  # supplémentaires), tout le bloc excédentaire ne serait jamais lu et le
  # tonnage importé/exporté disparaîtrait silencieusement de l'affectation.
  # On vérifie donc l'invariant avant toute affectation.
  dims_secteurs <- sapply(flux_gravitaire, function(M) paste(dim(M), collapse = "x"))
  dim_attendue  <- paste(dim(flux_tonnes_total), collapse = "x")
  if (any(dims_secteurs != dim_attendue)) {
    stop("Dimensions incoherentes entre flux_gravitaire et flux_tonnes_total : ",
         "attendu ", dim_attendue, ", obtenu ",
         paste(sprintf("%s=%s", names(dims_secteurs), dims_secteurs), collapse = ", "),
         ".\n  Les flux RoW doivent etre projetes secteur par secteur en 03_transport.R (VII.5).")
  }

  # ── Paires OD actives (flux > seuil, hors diagonale), regroupées par origine ──
  paires_actives <- which(flux_tonnes_total > SEUIL_FLUX_TONNES, arr.ind = TRUE)
  paires_actives <- paires_actives[paires_actives[, 1] != paires_actives[, 2], , drop = FALSE]
  paires_par_origine <- split(paires_actives[, 2], paires_actives[, 1])
  origines_a_traiter <- as.integer(names(paires_par_origine))
  n_origines         <- length(origines_a_traiter)

  # targets_all_global : indices des nœuds-entrepôts dans chaque couche véhicule.
  targets_all_global <- as.vector(sapply(
    seq_len(n_vehicules),
    function(v) node_multi(v, warehouse_nodes_base)
  ))

  # ── État d'équilibre accumulé (MSA) ───────────────────────────────────────────
  volume_eq_s <- array(0, dim = c(n_aretes_physiques, n_vehicules, N_SECTEURS),
                       dimnames = list(NULL, VEHICULES_IDS$vehicule_id, SECTEURS))
  V_phys <- rep(0, n_aretes_physiques)

  # Nombre d'itérations : 1 seule passe (AON à coûts libres) si CONGESTION = FALSE.
  n_iter_msa <- if (isTRUE(CONGESTION)) MSA_MAX_ITER else 1L

  cat("    ── Équilibre MSA (", length(aretes_bloquees), "arêtes bloquées ) —",
      if (isTRUE(CONGESTION)) "congestion ACTIVÉE" else "AON libre", "──\n")

  # Facteur BPR par arête mm (mis à jour à chaque itération ; sert aussi à
  # recomposer les poids d'équilibre finaux après la boucle).
  f_edge <- rep(1, length(poids_mm_libre))

  # Sorties conservées entre itérations (reflètent la DERNIÈRE passe AON).
  volume_trafic_mm_s    <- volume_eq_s
  compta_eoq            <- matrix(0, nrow = N_SECTEURS, ncol = 6,
    dimnames = list(SECTEURS, c("cout_commande", "cout_transport",
                                "cout_stock_cyclique", "cout_stock_transit",
                                "flux_tonnes", "flux_x_qopt")))
  paires_traitees       <- 0
  paires_non_connectees <- 0

  # ════════════════════════════════════════════════════════════════════════════
  # BOUCLE D'ÉQUILIBRE (MSA)
  # ════════════════════════════════════════════════════════════════════════════
  for (iter_msa in seq_len(n_iter_msa)) {

    # ── Coûts congestionnés de l'itération : coût libre × facteur BPR ───────────
    # f_bpr_phys = 1 + α·(V/C)^β par arête physique (=1 à l'itér. 1 car V=0).
    # Congestion appliquée au TEMPS uniquement ; transbordements jamais congestionnés.
    if (isTRUE(CONGESTION)) {
      f_bpr_phys <- 1 + BPR_ALPHA * (V_phys / C_phys)^BPR_BETA
      f_edge[]   <- 1
      f_edge[.est_route] <- f_bpr_phys[lookup_physique[.est_route]]
    }
    poids_mm   <- poids_horstemps_mm + poids_temps_mm * f_edge
    temps_mm_c <- temps_mm * f_edge
    # Blocage des arêtes perturbées (après recomposition, pour écraser leur poids).
    if (length(indices_mm_bloques) > 0) poids_mm[indices_mm_bloques] <- Inf

    # ── Résultats de CETTE passe AON (réinitialisés chaque itération) ───────────
    volume_trafic_mm_s <- array(0, dim = c(n_aretes_physiques, n_vehicules, N_SECTEURS),
                                dimnames = list(NULL, VEHICULES_IDS$vehicule_id, SECTEURS))
    # Comptabilité EOQ par secteur (identité de Wilson, ventilation en 4 postes).
    compta_eoq <- matrix(0, nrow = N_SECTEURS, ncol = 6,
      dimnames = list(SECTEURS, c("cout_commande", "cout_transport",
                                  "cout_stock_cyclique", "cout_stock_transit",
                                  "flux_tonnes", "flux_x_qopt")))

    paires_traitees       <- 0
    paires_non_connectees <- 0

    pb_aff <- progress_bar$new(
      format = paste0("  Itér. ", iter_msa, "/", n_iter_msa,
                      " [:bar] :percent | ETA: :eta | :current/:total"),
      total = n_origines, clear = FALSE, width = 70
    )

    for (i in origines_a_traiter) {

      destinations_i <- paires_par_origine[[as.character(i)]]

      sources_i <- as.integer(sapply(
        seq_len(n_vehicules),
        function(v) node_multi(v, warehouse_nodes_base[i])
      ))

      # Dijkstra en une passe depuis les couches véhicule de l'origine i.
      dists_all <- igraph::distances(g, v = sources_i,
                                     to = targets_all_global, weights = poids_mm)

      for (j in destinations_i) {

        cols_j   <- j + (seq_len(n_vehicules) - 1) * n_warehouses
        min_cout <- min(dists_all[, cols_j], na.rm = TRUE)
        if (is.infinite(min_cout)) {
          paires_non_connectees <- paires_non_connectees + 1
          next
        }

        # Meilleure combinaison (couche départ, couche arrivée)
        best_idx_mat <- which(dists_all[, cols_j] == min_cout, arr.ind = TRUE)
        if (!is.matrix(best_idx_mat)) best_idx_mat <- matrix(best_idx_mat, nrow = 1)
        best_from <- sources_i[best_idx_mat[1, 1]]
        best_to   <- targets_all_global[cols_j[best_idx_mat[1, 2]]]

        # Reconstruction du chemin optimal (arêtes empruntées)
        path_obj <- igraph::shortest_paths(g, from = best_from, to = best_to,
                                           weights = poids_mm, output = "epath")
        edges_path_mm <- as.integer(path_obj$epath[[1]])
        rm(path_obj)
        if (length(edges_path_mm) == 0) {
          paires_non_connectees <- paires_non_connectees + 1
          next
        }

        # Arêtes "route" du chemin (hors transbordements) + indices physiques/véhicules
        edges_valides <- edges_path_mm[edges_path_mm <= max_idx_mm]
        types_e       <- lookup_type[edges_valides]
        edges_routes  <- edges_valides[types_e == "route"]
        if (length(edges_routes) == 0) {
          paires_traitees <- paires_traitees + 1
          next
        }

        idx_phys_vec <- lookup_physique[edges_routes]
        veh_id_vec   <- lookup_vehicule[edges_routes]
        valides <- idx_phys_vec >= 1 & idx_phys_vec <= n_aretes_physiques & veh_id_vec != ""
        if (!any(valides)) {
          paires_traitees <- paires_traitees + 1
          next
        }
        idx_phys_vec <- idx_phys_vec[valides]
        veh_id_vec   <- veh_id_vec[valides]
        col_veh_vec  <- match(veh_id_vec, VEHICULES_IDS$vehicule_id)

        # ── Découpage du chemin en JAMBES (segments à véhicule constant) ─────────
        # Une jambe = arêtes consécutives dans la même couche véhicule (les ruptures
        # de col_veh_vec délimitent les jambes). Pour chaque jambe on précalcule
        # (indépendamment du secteur) son véhicule v, son coût réalisé c (RWF/tonne,
        # congestion incluse) et son temps de transit tau_an (fraction d'année).
        runs   <- rle(col_veh_vec)
        fin    <- cumsum(runs$lengths)
        debut  <- fin - runs$lengths + 1L
        jambes <- lapply(seq_along(runs$values), function(gr) {
          v      <- runs$values[gr]
          mm_ids <- idx_phys_vec[debut[gr]:fin[gr]] + (v - 1L) * n_aretes_physiques
          list(v      = v,
               c      = sum(poids_mm[mm_ids],   na.rm = TRUE),
               tau_an = sum(temps_mm_c[mm_ids], na.rm = TRUE) / HEURES_PAR_AN)
        })

        # ── Ventilation sectorielle : routage et véhicule communs à tous les
        # secteurs, seul le VOLUME change. Pour chaque secteur : taille d'envoi q*
        # (Wilson bornée), comptabilité EOQ jambe par jambe, et affectation du flux
        # au tableau 3D.
        for (s in SECTEURS_FRET) {

          idx_s     <- match(s, SECTEURS)
          flux_ij_s <- flux_gravitaire[[s]][i, j]
          if (is.na(flux_ij_s) || flux_ij_s < 1) next

          Vs         <- VALEUR_RWF_PAR_TONNE[s]
          # q*_v = √(2·Q·K_v/(V_s·r)), plafonné à la capacité, planché à un
          # remplissage minimal (évite un q* nul → trajets explosifs).
          q_star_vec <- pmin(
            pmax(sqrt(2 * flux_ij_s * K_vec / (Vs * TAUX_DETENTION_STOCK)),
                 EOQ_REMPLISSAGE_MIN * cap_vec),
            cap_vec
          )

          # Comptabilité logistique annuelle, ventilée jambe par jambe puis sommée.
          for (jb in jambes) {
            q_leg <- q_star_vec[jb$v]
            compta_eoq[idx_s, "cout_commande"]       <- compta_eoq[idx_s, "cout_commande"] +
              (flux_ij_s / q_leg) * K_vec[jb$v]
            compta_eoq[idx_s, "cout_transport"]      <- compta_eoq[idx_s, "cout_transport"] +
              flux_ij_s * jb$c
            compta_eoq[idx_s, "cout_stock_cyclique"] <- compta_eoq[idx_s, "cout_stock_cyclique"] +
              (q_leg / 2) * Vs * TAUX_DETENTION_STOCK
            compta_eoq[idx_s, "cout_stock_transit"]  <- compta_eoq[idx_s, "cout_stock_transit"] +
              flux_ij_s * jb$tau_an * Vs * TAUX_DETENTION_STOCK
            compta_eoq[idx_s, "flux_tonnes"]         <- compta_eoq[idx_s, "flux_tonnes"] +
              flux_ij_s
            compta_eoq[idx_s, "flux_x_qopt"]         <- compta_eoq[idx_s, "flux_x_qopt"] +
              flux_ij_s * q_leg
          }

          # Affectation vectorisée du flux sectoriel sur le tableau 3D.
          indices_3d <- cbind(idx_phys_vec, col_veh_vec, idx_s)
          volume_trafic_mm_s[indices_3d] <- volume_trafic_mm_s[indices_3d] + flux_ij_s
        }

        paires_traitees <- paires_traitees + 1
      }

      rm(dists_all)
      pb_aff$tick()
    }
    # ── fin de la passe AON ─────────────────────────────────────────────────────

    # ── Mise à jour d'équilibre (MSA, pas 1/n) ──────────────────────────────────
    volume_eq_s <- volume_eq_s + (1 / iter_msa) * (volume_trafic_mm_s - volume_eq_s)

    # Charge physique (PCU/jour) d'équilibre : remplissage FIXE — le tonnage
    # d'équilibre par arête×véhicule est converti en PCU/jour via conv_v
    # (TAUX_CHARGEMENT), le produit matriciel sommant sur les véhicules.
    .vol_eq_mm <- apply(volume_eq_s, c(1, 2), sum)
    V_new      <- as.vector(.vol_eq_mm %*% conv_v)
    rm(.vol_eq_mm)

    gap_msa <- sum(abs(V_new - V_phys)) / max(sum(V_new), 1)
    V_phys  <- V_new

    .sat_iter <- V_phys / C_phys
    cat(sprintf("      → Itér. %d/%d : gap = %.4f | saturation max = %.2f | V/C>1 : %d\n",
                iter_msa, n_iter_msa, gap_msa, max(.sat_iter, na.rm = TRUE),
                sum(.sat_iter > 1, na.rm = TRUE)))
    rm(.sat_iter)
    invisible(gc(verbose = FALSE))

    # Arrêt anticipé si la charge ne bouge quasiment plus (équilibre atteint).
    if (isTRUE(CONGESTION) && iter_msa > 1 && gap_msa < MSA_TOL) {
      cat("      ✓ Convergence MSA (gap <", MSA_TOL, ") à l'itération", iter_msa, "\n")
      break
    }
  }
  # ── fin de la boucle d'équilibre MSA ──────────────────────────────────────────

  # La charge retenue est la charge d'ÉQUILIBRE (moyennée), pas celle de la
  # dernière passe AON. compta_eoq / paires_* reflètent la dernière passe (≈
  # équilibre après convergence), comportement identique au 04.
  volume_trafic_mm_s <- volume_eq_s

  # ── Poids d'équilibre FINAUX : recomposés à partir de la charge convergée ─────
  if (isTRUE(CONGESTION)) {
    f_bpr_phys <- 1 + BPR_ALPHA * (V_phys / C_phys)^BPR_BETA
    f_edge[]   <- 1
    f_edge[.est_route] <- f_bpr_phys[lookup_physique[.est_route]]
  }
  poids_mm   <- poids_horstemps_mm + poids_temps_mm * f_edge
  temps_mm_c <- temps_mm * f_edge
  if (length(indices_mm_bloques) > 0) poids_mm[indices_mm_bloques] <- Inf

  list(
    volume_trafic_mm_s    = volume_trafic_mm_s,
    compta_eoq            = compta_eoq,
    V_phys                = V_phys,
    saturation_phys       = V_phys / C_phys,
    C_phys                = C_phys,
    poids_mm              = poids_mm,
    temps_mm_c            = temps_mm_c,
    paires_traitees       = paires_traitees,
    paires_non_connectees = paires_non_connectees
  )
}
