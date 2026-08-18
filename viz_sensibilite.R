################################################################################
# viz_sensibilite.R
# RÔLE : Mettre en évidence les DIVERGENCES entre le scénario de RÉFÉRENCE et les
#        scénarios de l'analyse de sensibilité par hypercube latin produits par
#        run_sensibilite.R.
#
# ENTRÉES (toutes déjà écrites sur disque, aucun modèle n'est relancé)
#   • outputs/exports/sensibilite/plan_lhs.csv
#       → plan d'expérience : multiplicateurs d'ENTRÉE (beta_<sect>,
#         valtonne_<sect>, vot) tirés pour chaque scénario ;
#   • volumes_fret_par_secteur.csv / comptabilite_couts_eoq.csv
#       → flux affectés et comptabilité des coûts ;
#   • offre_demande_zones.csv
#       → offre et demande par zone, en mrd RWF : localisation de la production
#         et de la consommation ;
#   • impact_od_<scenario>.csv / criticite_aretes_<scenario>.csv
#       → surcoûts de la rupture par paire OD et classement de criticité des
#         arêtes, pour le scénario désigné par SENS_SCENARIO_VULNERAB ;
#     chacun lu dans outputs/exports/ (RÉFÉRENCE) et dans
#     outputs/exports/sensibilite/<id>/ (SCÉNARIOS) ;
#   • outputs/exports/reseau_avec_fret.gpkg   (géométrie des arêtes)
#     outputs/exports/reseau_noeuds.gpkg      (géométrie des zones/entrepôts)
#
# SORTIES : outputs/cartes/sensibilite/_synthese/
#   A. sensibilite_enveloppe_indicateurs.png — dispersion des indicateurs
#      agrégés autour de la référence (enveloppe d'incertitude) ;
#   B. sensibilite_tornado_tkm.png           — indices de sensibilité : quels
#      paramètres d'entrée pilotent le plus les tonnes·km ;
#   C. sensibilite_scatter_entrees.png       — nuages de points sortie vs entrée,
#      un panneau par paramètre : relation brute derrière chaque indice du
#      tornado (repère une non-linéarité qu'une simple corrélation masquerait) ;
#   D. sensibilite_heatmap_indices.png       — |corrélation| entrée × sortie ;
#   E. sensibilite_divergence_sectorielle.png — volatilité du tonnage par secteur ;
#   F. sensibilite_carte_robustesse.png      — coefficient de variation du volume
#      par arête : où le réseau est le plus sensible à l'incertitude ;
#   G. sensibilite_localisation_zones.png    — volatilité de la PART de chaque
#      zone dans l'offre et dans la demande nationales ;
#   H. sensibilite_carte_localisation.png    — même grandeur, cartographiée :
#      où la localisation de la production et de la consommation est la moins
#      assurée ;
#   I. sensibilite_stabilite_criticite.png   — stabilité du classement de
#      criticité : les axes critiques du scénario de référence le restent-ils
#      sous d'autres hypothèses ?
#   + sensibilite_table_indicateurs.csv      — récapitulatif chiffré par
#      indicateur agrégé (référence, quartiles d'écart, paramètre dominant) ;
#   + sensibilite_table_secteurs.csv         — même récapitulatif par secteur.
#      Ces deux tables donnent les nombres à citer dans le texte, que les
#      figures ne permettent que de commenter.
#
# Ce script se lance en mode RÉFÉRENCE (SCENARIO_ID reste "reference") : il ne
# porte donc pas le marquage "TEST DE SENSIBILITÉ" — c'est bien la synthèse qui
# COMPARE la référence aux tests, pas un test de plus.
################################################################################

source("00_parametres.R")

# ==============================================================================
# 0. Localisation des dossiers et garde-fous de présence
# ==============================================================================
# En mode référence, DIR_EXPORTS = outputs/exports et DIR_CARTES = outputs/cartes.
DIR_SENS_EXPORTS <- file.path(DIR_EXPORTS, "sensibilite")            # exports des scénarios
DIR_SYNTHESE     <- file.path(DIR_CARTES, "sensibilite", "_synthese")# figures de synthèse
dir.create(DIR_SYNTHESE, showWarnings = FALSE, recursive = TRUE)

f_plan <- file.path(DIR_SENS_EXPORTS, "plan_lhs.csv")
if (!file.exists(f_plan)) {
  stop("Plan LHS introuvable (", f_plan, ").\n",
       "Lancez d'abord run_sensibilite.R pour générer les scénarios.")
}

plan <- readr::read_csv(f_plan, show_col_types = FALSE)
cat("✓ Plan LHS chargé :", nrow(plan), "scénarios,",
    ncol(plan) - 1, "paramètres variés\n")

# Colonnes du plan qui sont des multiplicateurs d'entrée (tout sauf l'id)
cols_entree <- setdiff(names(plan), "id")

# ── Choix du scénario de rupture analysé ─────────────────────────────────────
# Le module 05 écrit un jeu de fichiers par scénario de rupture, suffixé par le
# nom du scénario (impact_od_<nom>.csv). La référence en contient souvent
# plusieurs, les tirages un seul : il faut donc désigner celui sur lequel porte
# la comparaison, faute de quoi on comparerait des ruptures différentes entre
# elles. SENS_SCENARIO_VULNERAB (00_parametres.R) fixe ce nom ; à NULL, on
# retient le scénario présent dans la référence ET dans le plus grand nombre de
# dossiers de tirages.
noms_scenarios_vuln <- function(dir_exp) {
  f <- list.files(dir_exp, pattern = "^impact_od_.*\\.csv$")
  sub("^impact_od_(.*)\\.csv$", "\\1", f)
}

SCEN_VULN <- SENS_SCENARIO_VULNERAB
if (is.null(SCEN_VULN)) {
  .dispo_ref  <- noms_scenarios_vuln(DIR_EXPORTS)
  .dispo_scen <- unlist(lapply(plan$id,
                    function(id) noms_scenarios_vuln(file.path(DIR_SENS_EXPORTS, id))))
  .communs <- intersect(.dispo_ref, unique(.dispo_scen))
  if (length(.communs) == 0) {
    SCEN_VULN <- NA_character_
    cat("⚠ Aucun scénario de rupture commun à la référence et aux tirages :",
        "les figures de surcoût et de criticité seront ignorées.\n")
  } else {
    # Le plus représenté parmi les tirages ; départage alphabétique.
    .compte  <- sort(table(.dispo_scen[.dispo_scen %in% .communs]), decreasing = TRUE)
    SCEN_VULN <- names(.compte)[1]
    cat("✓ Scénario de rupture analysé :", SCEN_VULN,
        sprintf("(présent dans %d/%d tirages)\n", .compte[[1]], nrow(plan)))
  }
} else {
  cat("✓ Scénario de rupture analysé (imposé) :", SCEN_VULN, "\n")
}

# Étiquettes lisibles pour les colonnes de paramètres : "beta_Agriculture" →
# "β Agriculture" (élasticité) ; "valtonne_Mines" → "RWF/t Mines" (valeur
# unitaire en RWF par tonne) ; "vot" → "Valeur du temps" (axe unique, partagé
# par les trois véhicules). Réutilisée pour la table de synthèse (section 1)
# et les indices de sensibilité (section 3).
jolie_entree <- function(x) {
  x <- sub("^beta_",     "β ",     x)
  x <- sub("^valtonne_", "RWF/t ", x)
  x[x == "vot"] <- "Valeur du temps"
  x
}

# ==============================================================================
# 1. Lecture des INDICATEURS de sortie pour un dossier d'exports
# ==============================================================================
# Renvoie une liste à trois composantes pour un scénario (ou la référence) :
#   $agg     : indicateurs agrégés (1 valeur chacun) ;
#   $secteur : tonnage total par secteur (vecteur nommé) ;
#   $arete   : volume par arête (vecteur, aligné sur l'ordre des arêtes — le
#              réseau étant partagé, cet ordre est identique entre scénarios).
# Renvoie NULL si les fichiers attendus manquent (scénario en échec).
# ==============================================================================
lire_indicateurs <- function(dir_exp) {

  f_vol <- file.path(dir_exp, "volumes_fret_par_secteur.csv")
  f_cpt <- file.path(dir_exp, "comptabilite_couts_eoq.csv")
  if (!file.exists(f_vol)) return(NULL)

  vol <- readr::read_csv(f_vol, show_col_types = FALSE)

  # ── Surcoûts de la rupture (module 05) ────────────────────────────────────
  # impact_od_<scenario>.csv donne, paire OD par paire OD, le surcoût de la
  # rupture et le fait que la paire reste connectée. On en tire deux
  # indicateurs agrégés, ajoutés aux sorties suivies : le surcoût pondéré par
  # les tonnages (le chiffrage du chapitre de vulnérabilité) et le tonnage qui
  # perd tout accès. Les deux valent NA si le scénario n'est pas disponible
  # pour ce dossier : ils sont alors écartés des figures, sans bloquer le reste.
  # ── Longueur moyenne d'acheminement ───────────────────────────────────────
  # Distance parcourue par une tonne moyenne, en km : somme des distances de
  # chaque paire origine-destination pondérée par le tonnage qui l'emprunte,
  # divisée par le tonnage total. Les deux fichiers sont complémentaires —
  # matrice_od_long.csv porte la distance de chaque paire mais pas son tonnage,
  # matrice_flux_fret_tonnes.csv porte le tonnage mais pas la distance — et
  # s'apparient par les noms de zone d'origine et de destination.
  # C'est l'indicateur de la section « Volume affecté et longueur des
  # acheminements » : il résume à lui seul si les paramètres rapprochent ou
  # éloignent les échanges, là où le tonnage total n'en dit rien.
  long_moy <- NA_real_
  f_odl <- file.path(dir_exp, "matrice_od_long.csv")
  f_mat <- file.path(dir_exp, "matrice_flux_fret_tonnes.csv")
  if (file.exists(f_odl) && file.exists(f_mat)) {
    odl <- readr::read_csv(f_odl, show_col_types = FALSE)
    mat <- readr::read_csv(f_mat, show_col_types = FALSE)
    if (all(c("nom_origine", "nom_destination", "distance_km") %in% names(odl)) &&
        ncol(mat) >= 2) {
      zn <- mat[[1]]
      Mt <- as.matrix(mat[, -1, drop = FALSE])
      # Tonnage de chaque paire OD, lu dans la matrice par indexation
      # ligne/colonne : cbind(i, j) sélectionne les cellules une à une.
      i_o <- match(odl$nom_origine,     zn)
      i_d <- match(odl$nom_destination, zn)
      ok  <- !is.na(i_o) & !is.na(i_d)
      if (any(ok)) {
        t_paire <- Mt[cbind(i_o[ok], i_d[ok])]
        d_paire <- odl$distance_km[ok]
        if (sum(t_paire, na.rm = TRUE) > 0)
          long_moy <- sum(t_paire * d_paire, na.rm = TRUE) / sum(t_paire, na.rm = TRUE)
      }
    }
  }

  surcout_tot <- NA_real_
  tonnage_dec <- NA_real_
  if (!is.na(SCEN_VULN)) {
    f_imp <- file.path(dir_exp, paste0("impact_od_", SCEN_VULN, ".csv"))
    if (file.exists(f_imp)) {
      imp <- readr::read_csv(f_imp, show_col_types = FALSE)
      if ("surcout_pondere_rwf" %in% names(imp))
        surcout_tot <- sum(imp$surcout_pondere_rwf, na.rm = TRUE)
      if (all(c("connecte", "tonnage_paire") %in% names(imp))) {
        # connecte est lu tantôt en logique, tantôt en texte "true"/"false"
        # selon le moteur d'écriture : on normalise avant de filtrer.
        est_connecte <- if (is.logical(imp$connecte)) imp$connecte
                        else tolower(as.character(imp$connecte)) %in% c("true", "1")
        tonnage_dec <- sum(imp$tonnage_paire[!est_connecte], na.rm = TRUE)
      }
    }
  }

  # Colonnes de tonnage par secteur (préfixe "vol_t_")
  cols_sect <- grep("^vol_t_", names(vol), value = TRUE)

  # Coût de transport total : somme de la colonne cout_transport de la
  # comptabilité EOQ (peut manquer si 04 a échoué → NA, l'indicateur sera exclu).
  cout_transport_tot <- NA_real_
  if (file.exists(f_cpt)) {
    cpt <- readr::read_csv(f_cpt, show_col_types = FALSE)
    if ("cout_transport" %in% names(cpt))
      cout_transport_tot <- sum(cpt$cout_transport, na.rm = TRUE)
  }

  list(
    agg = c(
      # Charge totale du réseau : chaque arête compte pour volume × longueur.
      `Tonnes-km réseau`       = sum(vol$volume_tonnes * vol$length_km, na.rm = TRUE),
      # Tonnage total circulant (somme des volumes d'arête).
      `Tonnage total réseau`   = sum(vol$volume_tonnes, na.rm = TRUE),
      # Part du poids lourd dans le tonnage transporté (choix de véhicule).
      `Part camion lourd`      = sum(vol$volume_camion_lourd, na.rm = TRUE) /
                                 sum(vol$volume_tonnes, na.rm = TRUE),
      # Coût de transport annuel total (comptabilité EOQ).
      `Coût transport total`   = cout_transport_tot,
      # Surcoût de la rupture, pondéré par les tonnages des paires touchées.
      `Surcoût rupture`        = surcout_tot,
      # Tonnage des paires OD qui perdent tout accès pendant la rupture.
      `Tonnage déconnecté`     = tonnage_dec,
      # Distance moyenne parcourue par une tonne, en km.
      `Longueur d'acheminement` = long_moy
    ),
    secteur = vapply(cols_sect, function(cc) sum(vol[[cc]], na.rm = TRUE), numeric(1)) |>
              setNames(sub("^vol_t_", "", cols_sect)),
    arete   = vol$volume_tonnes
  )
}

# ── Référence ────────────────────────────────────────────────────────────────
ref <- lire_indicateurs(DIR_EXPORTS)
if (is.null(ref))
  stop("Exports de référence introuvables dans ", DIR_EXPORTS,
       " : lancez run_all.R.")

# ── Scénarios ────────────────────────────────────────────────────────────────
# On lit chaque scénario du plan ; ceux dont les exports manquent (échec) sont
# écartés avec un avertissement, sans interrompre la synthèse.
res_scen <- lapply(plan$id, function(id) lire_indicateurs(file.path(DIR_SENS_EXPORTS, id)))
names(res_scen) <- plan$id

ids_ok <- plan$id[!vapply(res_scen, is.null, logical(1))]
if (length(ids_ok) < length(plan$id)) {
  cat("⚠", length(plan$id) - length(ids_ok),
      "scénario(s) sans exports (ignorés) :",
      paste(setdiff(plan$id, ids_ok), collapse = ", "), "\n")
}
if (length(ids_ok) < 2)
  stop("Moins de 2 scénarios exploitables : synthèse impossible.")

res_scen <- res_scen[ids_ok]
plan     <- plan[plan$id %in% ids_ok, , drop = FALSE]

# Palette de couleurs par scénario : une couleur fixe attribuée à chaque id de
# tirage LHS, réutilisée dans toutes les figures de synthèse (A et D) pour
# pouvoir suivre visuellement un même scénario d'un graphique à l'autre.
# hcl.colors(..., "Dynamic") est une palette qualitative qui reste lisible
# même pour un grand nombre de catégories.
PALETTE_SCENARIOS <- setNames(
  grDevices::hcl.colors(length(ids_ok), palette = "Dynamic"),
  ids_ok
)

# ==============================================================================
# 1bis. Table de synthèse des paramètres tirés par scénario
# ==============================================================================
# Écrit (à chaque exécution) un CSV qui explicite, pour chaque id de scénario
# LHS (identifiant utilisé comme couleur dans les figures A et D), la valeur
# exacte de tous les multiplicateurs tirés (β et RWF/t par secteur, plus la
# valeur du temps). Sert de légende détaillée à consulter à côté des figures,
# puisque les couleurs seules ne permettent pas de lire les valeurs des
# paramètres.
table_parametres <- plan
names(table_parametres) <- c("id", jolie_entree(cols_entree))
table_parametres[jolie_entree(cols_entree)] <-
  lapply(table_parametres[jolie_entree(cols_entree)], round, digits = 3)
readr::write_csv(table_parametres,
                  file.path(DIR_SYNTHESE, "sensibilite_table_parametres.csv"))
cat("  ✓ Table des paramètres par scénario → sensibilite_table_parametres.csv\n")

# ==============================================================================
# 2. Table longue des indicateurs agrégés (écart % à la référence)
# ==============================================================================
# Pour comparer sur un même axe des indicateurs d'unités différentes (tonnes-km,
# RWF, %), on exprime chaque scénario en ÉCART RELATIF à la référence :
#   ecart_pct = 100 × (valeur_scenario − valeur_ref) / valeur_ref.
# La référence est donc, par construction, la ligne 0 %.
# ==============================================================================
agg_long <- do.call(rbind, lapply(names(res_scen), function(id) {
  a  <- res_scen[[id]]$agg
  data.frame(
    id        = id,
    indicateur = names(a),
    valeur    = as.numeric(a),
    ref       = as.numeric(ref$agg[names(a)]),
    stringsAsFactors = FALSE
  )
}))
agg_long$ecart_pct <- 100 * (agg_long$valeur - agg_long$ref) / agg_long$ref
# On retire les indicateurs dont la référence est manquante (ex. coût si 04 KO).
agg_long <- agg_long[is.finite(agg_long$ecart_pct), , drop = FALSE]

# ── FIGURE A : enveloppe d'incertitude des indicateurs agrégés ───────────────
# Les points sont colorés par id de scénario (PALETTE_SCENARIOS) : on peut
# ainsi repérer un même tirage LHS sur les différents indicateurs (et, comme
# la palette est partagée, le retrouver aussi sur la figure D).
gA <- ggplot(agg_long, aes(x = indicateur, y = ecart_pct)) +
  geom_hline(yintercept = 0, linewidth = 0.6, color = "#B22222") +
  geom_boxplot(width = 0.45, fill = "#BDD7E7", outlier.shape = NA, alpha = 0.7) +
  geom_jitter(aes(color = id), width = 0.12, height = 0, size = 1.8, alpha = 0.85) +
  scale_color_manual(values = PALETTE_SCENARIOS, name = "Scénario") +
  scale_y_continuous(labels = function(x) paste0(x, " %")) +
  labs(
    title    = "Sensibilité des indicateurs agrégés aux incertitudes de paramètres",
    subtitle = sprintf("Écart à la référence sur %d tirages (hypercube latin) — la ligne rouge est la référence (0 %%)",
                       length(res_scen)),
    x = NULL, y = "Écart à la référence",
    caption = note_lecture({
      .etendue_v <- agg_long %>% dplyr::group_by(indicateur) %>%
        dplyr::summarise(etendue = diff(range(ecart_pct)), .groups = "drop") %>%
        dplyr::arrange(dplyr::desc(etendue)) %>% dplyr::slice(1)
      sprintf("l'indicateur « %s » est le plus dispersé : ses tirages s'étendent sur %.0f points d'écart à la référence.",
              .etendue_v$indicateur, .etendue_v$etendue)
    }, largeur_car = 132)
  ) +
  guides(color = guide_legend(ncol = 1, override.aes = list(size = 2.5, alpha = 1))) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title   = element_text(face = "bold"),
    plot.subtitle = element_text(color = "#666666"),
    axis.text.x  = element_text(angle = 15, hjust = 1),
    legend.text  = element_text(size = 7),
    legend.key.height = grid::unit(0.35, "cm")
  ) +
  THEME_NOTE_LECTURE
ggsave(file.path(DIR_SYNTHESE, "sensibilite_enveloppe_indicateurs.png"),
       gA, width = 12.5, height = 6.8, dpi = 300, bg = "white")
cat("  ✓ A. sensibilite_enveloppe_indicateurs.png\n")

# ==============================================================================
# 3. Indices de sensibilité : corrélation entrée → sortie
# ==============================================================================
# L'intérêt d'un plan LHS est de pouvoir attribuer les variations de SORTIE aux
# variations d'ENTRÉE. Comme chaque paramètre a été tiré indépendamment, la
# corrélation de rang de Spearman entre un multiplicateur d'entrée et un
# indicateur de sortie mesure directement l'influence de ce paramètre :
#   > 0 → augmenter le paramètre augmente l'indicateur ; |corr| ≈ 1 → très
#   influent ; ≈ 0 → sans effet. On préfère Spearman (rang) car la relation
#   n'est pas nécessairement linéaire.
# ==============================================================================

# Matrice des sorties agrégées par scénario (lignes = scénarios, cols = indics)
sorties_mat <- do.call(rbind, lapply(names(res_scen), function(id) res_scen[[id]]$agg))
rownames(sorties_mat) <- names(res_scen)
# On ne garde que les indicateurs entièrement renseignés sur tous les scénarios,
# ET qui varient réellement : un indicateur constant (ex. part camion lourd
# saturée à 100 %) donnerait une corrélation indéfinie (aucune information de
# sensibilité). Il reste toutefois affiché dans la figure A (enveloppe).
sorties_mat <- sorties_mat[, colSums(is.finite(sorties_mat)) == nrow(sorties_mat), drop = FALSE]
sorties_mat <- sorties_mat[, apply(sorties_mat, 2, stats::sd) > 0, drop = FALSE]

# Matrice des entrées, dans le même ordre de scénarios que sorties_mat.
entrees_mat <- as.matrix(plan[match(rownames(sorties_mat), plan$id), cols_entree, drop = FALSE])

# Corrélation de Spearman entre chaque entrée et chaque sortie.
indices <- expand.grid(entree = cols_entree,
                       sortie = colnames(sorties_mat),
                       stringsAsFactors = FALSE)
indices$corr <- mapply(function(e, s)
  suppressWarnings(cor(entrees_mat[, e], sorties_mat[, s], method = "spearman")),
  indices$entree, indices$sortie)

# Étiquettes lisibles (fonction jolie_entree définie section 0).
indices$entree_lbl <- jolie_entree(indices$entree)

# ── FIGURE B : tornado pour l'indicateur principal (Tonnes-km réseau) ────────
indic_principal <- "Tonnes-km réseau"
if (indic_principal %in% colnames(sorties_mat)) {
  tor <- indices[indices$sortie == indic_principal, ]
  # Tri par corrélation croissante pour un tornado lisible (barres du bas = plus
  # négatives, barres du haut = plus positives).
  tor <- tor[order(tor$corr), ]
  tor$entree_lbl <- factor(tor$entree_lbl, levels = tor$entree_lbl)

  gB <- ggplot(tor, aes(x = corr, y = entree_lbl, fill = corr > 0)) +
    geom_col(width = 0.7) +
    geom_vline(xintercept = 0, color = "grey30") +
    scale_fill_manual(values = c(`TRUE` = "#2171B5", `FALSE` = "#D94701"),
                      labels = c(`TRUE` = "effet positif", `FALSE` = "effet négatif"),
                      name = NULL) +
    scale_x_continuous(limits = c(-1, 1)) +
    labs(
      title    = paste0("Indices de sensibilité — ", indic_principal),
      subtitle = "Corrélation de rang (Spearman) entre chaque paramètre et la sortie",
      x = "Corrélation avec l'indicateur", y = NULL,
      caption = note_lecture(sprintf(
        "le paramètre le plus influent sur %s est %s, avec une corrélation de %.2f.",
        indic_principal, tor$entree_lbl[which.max(abs(tor$corr))], tor$corr[which.max(abs(tor$corr))]
      ), largeur_car = 108)
    ) +
    theme_minimal(base_size = 12) +
    theme(plot.title = element_text(face = "bold"),
          plot.subtitle = element_text(color = "#666666"),
          legend.position = "top") +
    THEME_NOTE_LECTURE
  ggsave(file.path(DIR_SYNTHESE, "sensibilite_tornado_tkm.png"),
         gB, width = 9, height = 8.8, dpi = 300, bg = "white")
  cat("  ✓ B. sensibilite_tornado_tkm.png\n")

  # ── FIGURE C : nuages de points sortie vs entrée (indicateur principal) ────
  # Le tornado (figure B) résume la relation entrée → sortie par un seul
  # nombre (la corrélation de rang). Ce nombre peut masquer une relation non
  # monotone ou un effet de seuil. On trace donc, pour l'indicateur principal,
  # un panneau par paramètre : en abscisse le multiplicateur tiré (1 =
  # référence), en ordonnée l'écart de sortie à la référence. Les panneaux
  # sont ordonnés du paramètre le plus influent (|corrélation| la plus forte,
  # même ordre que le tornado) au moins influent.
  # entrees_mat n'a pas forcément les id de scénario en rownames (héritées du
  # numéro de ligne de "plan" lors du match) : on utilise rownames(sorties_mat),
  # qui portent explicitement les id et sont dans le même ordre que entrees_mat
  # (les deux matrices ont été alignées ligne à ligne via ce même match).
  ids_princ <- rownames(sorties_mat)

  # Écarts de sortie de l'indicateur principal, rangés dans un vecteur NOMMÉ par
  # id de scénario. agg_long contient une ligne par couple (scénario ×
  # indicateur) : on isole donc d'abord les lignes de l'indicateur principal,
  # puis on indexe ce sous-ensemble par les id, dans l'ordre des lignes de
  # entrees_mat. Indexer agg_long directement par des positions calculées sur
  # le sous-ensemble reviendrait à lire les mauvaises lignes (celles des autres
  # indicateurs).
  agg_princ   <- agg_long[agg_long$indicateur == indic_principal, ]
  ecart_princ <- setNames(agg_princ$ecart_pct, agg_princ$id)

  scatter_princ <- do.call(rbind, lapply(cols_entree, function(e) {
    data.frame(
      entree_lbl    = jolie_entree(e),
      valeur_entree = unname(entrees_mat[, e]),
      ecart_pct     = unname(ecart_princ[ids_princ]),
      stringsAsFactors = FALSE
    )
  }))
  # Ordre des panneaux : du paramètre le plus influent au moins influent,
  # d'après |corrélation| dans le tornado (variable "tor" ci-dessus).
  ordre_scatter <- tor$entree_lbl[order(-abs(tor$corr))]
  scatter_princ$entree_lbl <- factor(scatter_princ$entree_lbl, levels = ordre_scatter)

  gC <- ggplot(scatter_princ, aes(x = valeur_entree, y = ecart_pct)) +
    geom_hline(yintercept = 0, linewidth = 0.5, color = "#B22222") +
    geom_vline(xintercept = 1, linewidth = 0.5, color = "grey60", linetype = "dashed") +
    geom_smooth(method = "loess", formula = y ~ x, se = FALSE,
                color = "grey40", linewidth = 0.5) +
    geom_point(color = "#2171B5", size = 2, alpha = 0.8) +
    scale_y_continuous(labels = function(x) paste0(x, " %")) +
    facet_wrap(~ entree_lbl, ncol = 4) +
    labs(
      title    = paste0("Relation brute entrée → sortie — ", indic_principal),
      subtitle = "Un panneau par paramètre tiré (hypercube latin) ; complète le tornado (figure B) en montrant la forme de la relation",
      x = "Multiplicateur d'entrée tiré (1 = référence)",
      y = "Écart de sortie à la référence",
      caption = note_lecture(
        "chaque point est un tirage LHS ; la ligne grise est une tendance lissée (loess) qui permet de repérer une relation non linéaire ou un effet de seuil que la seule corrélation du tornado ne capturerait pas.",
        largeur_car = 132)
    ) +
    theme_minimal(base_size = 11) +
    theme(plot.title    = element_text(face = "bold"),
          plot.subtitle = element_text(color = "#666666"),
          strip.text    = element_text(face = "bold")) +
    THEME_NOTE_LECTURE
  ggsave(file.path(DIR_SYNTHESE, "sensibilite_scatter_entrees.png"),
         gC, width = 13, height = 11, dpi = 300, bg = "white")
  cat("  ✓ C. sensibilite_scatter_entrees.png\n")
}

# ── FIGURE D : heatmap des indices |corr| (toutes entrées × toutes sorties) ──
indices$abscorr <- abs(indices$corr)
# Ordonner les entrées par influence moyenne (les plus influentes en haut).
ordre_entree <- indices |>
  dplyr::group_by(entree_lbl) |>
  dplyr::summarise(m = mean(abscorr, na.rm = TRUE), .groups = "drop") |>
  dplyr::arrange(m)
indices$entree_lbl <- factor(indices$entree_lbl, levels = ordre_entree$entree_lbl)

gD <- ggplot(indices, aes(x = sortie, y = entree_lbl, fill = abscorr)) +
  geom_tile(color = "white", linewidth = 0.4) +
  geom_text(aes(label = sprintf("%.2f", corr)), size = 3,
            color = ifelse(indices$abscorr > 0.6, "white", "grey20")) +
  scale_fill_gradientn(colors = c("#F7FBFF", "#6BAED6", "#08306B"),
                       limits = c(0, 1), name = "|corr|") +
  labs(
    title    = "Carte de sensibilité : influence de chaque paramètre sur chaque sortie",
    subtitle = "Valeur affichée = corrélation signée ; couleur = intensité |corr|",
    x = NULL, y = NULL,
    caption = note_lecture(sprintf(
      "la corrélation la plus forte du graphique est entre %s et %s, à %.2f.",
      indices$entree_lbl[which.max(indices$abscorr)], indices$sortie[which.max(indices$abscorr)],
      indices$corr[which.max(indices$abscorr)]
    ), largeur_car = 120)
  ) +
  theme_minimal(base_size = 11) +
  theme(plot.title = element_text(face = "bold"),
        plot.subtitle = element_text(color = "#666666"),
        axis.text.x = element_text(angle = 20, hjust = 1)) +
  THEME_NOTE_LECTURE
ggsave(file.path(DIR_SYNTHESE, "sensibilite_heatmap_indices.png"),
       gD, width = 10, height = 8.8, dpi = 300, bg = "white")
cat("  ✓ D. sensibilite_heatmap_indices.png\n")

# ==============================================================================
# 4. Divergence sectorielle : volatilité du tonnage par secteur
# ==============================================================================
# Pour chaque secteur, écart % du tonnage total transporté à la référence, sur
# l'ensemble des tirages. Un secteur à large boîte est très sensible aux
# hypothèses ; un secteur resserré autour de 0 est robuste.
# ==============================================================================
sect_long <- do.call(rbind, lapply(names(res_scen), function(id) {
  s <- res_scen[[id]]$secteur
  r <- ref$secteur[names(s)]
  data.frame(id = id, secteur = names(s),
             ecart_pct = 100 * (s - r) / r,
             stringsAsFactors = FALSE)
}))
sect_long <- sect_long[is.finite(sect_long$ecart_pct), , drop = FALSE]
# Ordonner les secteurs par amplitude de dispersion (plus volatils à gauche).
ordre_sect <- sect_long |>
  dplyr::group_by(secteur) |>
  dplyr::summarise(etendue = diff(range(ecart_pct)), .groups = "drop") |>
  dplyr::arrange(dplyr::desc(etendue))
sect_long$secteur <- factor(sect_long$secteur, levels = ordre_sect$secteur)

# Les boîtes restent colorées par secteur (PALETTE_SECTEURS) ; les points sont
# eux colorés par id de scénario (PALETTE_SCENARIOS, la même que sur la
# figure A) pour pouvoir suivre un tirage donné à travers les secteurs.
gE <- ggplot(sect_long, aes(x = secteur, y = ecart_pct)) +
  geom_hline(yintercept = 0, linewidth = 0.6, color = "#B22222") +
  geom_boxplot(aes(fill = secteur), width = 0.55, outlier.shape = NA, alpha = 0.85) +
  geom_jitter(aes(color = id), width = 0.12, height = 0, size = 1.4, alpha = 0.6) +
  scale_fill_manual(values = PALETTE_SECTEURS, guide = "none") +
  scale_color_manual(values = PALETTE_SCENARIOS, name = "Scénario") +
  scale_y_continuous(labels = function(x) paste0(x, " %")) +
  labs(
    title    = "Volatilité du tonnage sectoriel face aux incertitudes de paramètres",
    subtitle = sprintf("Écart à la référence du tonnage total transporté par secteur — %d tirages",
                       length(res_scen)),
    x = NULL, y = "Écart à la référence",
    caption = note_lecture(sprintf(
      "le secteur %s est le plus volatil : son tonnage varie sur %.0f points d'écart à la référence selon les tirages.",
      ordre_sect$secteur[1], ordre_sect$etendue[1]
    ), largeur_car = 132)
  ) +
  guides(color = guide_legend(ncol = 1, override.aes = list(size = 2.5, alpha = 1))) +
  theme_minimal(base_size = 12) +
  theme(plot.title = element_text(face = "bold"),
        plot.subtitle = element_text(color = "#666666"),
        axis.text.x = element_text(angle = 20, hjust = 1),
        legend.text  = element_text(size = 7),
        legend.key.height = grid::unit(0.35, "cm")) +
  THEME_NOTE_LECTURE
ggsave(file.path(DIR_SYNTHESE, "sensibilite_divergence_sectorielle.png"),
       gE, width = 12.5, height = 6.8, dpi = 300, bg = "white")
cat("  ✓ E. sensibilite_divergence_sectorielle.png\n")

# ==============================================================================
# 5. Carte de robustesse spatiale : coefficient de variation par arête
# ==============================================================================
# Pour chaque arête, on calcule la moyenne et l'écart-type du volume à travers
# tous les tirages, puis le coefficient de variation CV = écart-type / moyenne.
# CV élevé = corridor dont la charge dépend fortement des hypothèses (sensible) ;
# CV faible = corridor robuste, chargé quels que soient les paramètres.
#
# Alignement : le module 01 (géographie) n'étant PAS relancé en sensibilité,
# l'ordre des arêtes est identique entre tous les scénarios et la référence. On
# empile donc les vecteurs $arete par colonne, avec un garde-fou sur la longueur.
#
# La carte est produite avec tmap (et non ggplot2) pour reprendre le même fond
# de carte (provinces, frontière, lacs, parcs) que toutes les autres cartes du
# projet. fond_carte() est une fonction tmap sauvegardée par 01_reseau.R dans
# persist_fond_carte.rds ; on la recharge ici comme le font les autres scripts
# viz_*.R.
# ==============================================================================

# ── Correction du device PNG pour tmap sur macOS sans XQuartz ────────────────
# tmap v4 force type="cairo-png" en dur dans sa fonction interne plot_device.
# Sur macOS sans XQuartz installé, cairo n'est pas disponible : tmap_save()
# échoue silencieusement (avertissement "failed to load cairo DLL", aucun
# fichier créé, pas d'erreur levée). Ce bloc détecte l'absence de cairo et
# remplace automatiquement le device par type="quartz" (rendu natif macOS).
# Sur les systèmes où cairo fonctionne (Linux, macOS + XQuartz), le patch est
# silencieusement ignoré.
local({
  .f      <- tempfile(fileext = ".png")
  .echec  <- FALSE
  withCallingHandlers(
    grDevices::png(.f, type = "cairo-png", width = 10, height = 10,
                   res = 72, units = "px"),
    warning = function(w) {
      .echec <<- TRUE
      invokeRestart("muffleWarning")
    }
  )
  try(dev.off(), silent = TRUE)
  unlink(.f, force = TRUE)

  if (.echec) {
    .orig <- tmap:::plot_device
    .patched <- function(device, ext, filename, dpi, units_target) {
      if (is.null(device) && identical(ext, "png")) {
        force(dpi)
        force(units_target)
        return(function(..., width, height) {
          grDevices::png(..., type = "quartz", width = width, height = height,
                         res = dpi, units = units_target)
        })
      }
      .orig(device, ext, filename, dpi, units_target)
    }
    assignInNamespace("plot_device", .patched, ns = "tmap")
    cat("  ✓ tmap : device PNG patché (quartz au lieu de cairo-png, XQuartz absent)\n")
  }
})

# Fonction de fond de carte (provinces, frontière, lacs, parcs), commune à
# toutes les cartes tmap du projet.
fond_carte <- readRDS(file.path(DIR_CARTES, "persist_fond_carte.rds"))

f_gpkg <- file.path(DIR_EXPORTS, "reseau_avec_fret.gpkg")
long_ref <- length(ref$arete)
longueurs_ok <- vapply(res_scen, function(x) length(x$arete) == long_ref, logical(1))

if (!file.exists(f_gpkg)) {
  cat("  ⚠ F. carte de robustesse ignorée : géométrie", f_gpkg, "absente\n")
} else if (!all(longueurs_ok)) {
  cat("  ⚠ F. carte de robustesse ignorée : nombre d'arêtes incohérent entre scénarios\n")
} else {

  # Matrice arêtes × scénarios des volumes.
  vol_mat <- vapply(res_scen, function(x) x$arete, numeric(long_ref))

  moy_arete <- rowMeans(vol_mat)
  # sd par ligne ; apply est acceptable ici (une seule passe sur ~35 000 lignes).
  sd_arete  <- apply(vol_mat, 1, sd)
  cv_arete  <- ifelse(moy_arete > 0, sd_arete / moy_arete, NA_real_)

  # Géométrie de référence (même ordre de lignes que les volumes).
  reseau_geo <- sf::st_read(f_gpkg, quiet = TRUE)

  if (nrow(reseau_geo) != long_ref) {
    cat("  ⚠ F. carte de robustesse ignorée : géométrie (", nrow(reseau_geo),
        ") et volumes (", long_ref, ") de tailles différentes\n")
  } else {
    reseau_geo$cv_volume  <- cv_arete
    reseau_geo$moy_volume <- moy_arete
    # On ne cartographie que les arêtes réellement empruntées en moyenne
    # (le CV n'a pas de sens sur les arêtes à volume quasi nul).
    reseau_cv <- reseau_geo[is.finite(reseau_geo$cv_volume) &
                            reseau_geo$moy_volume > SEUIL_FLUX_TONNES, ]
    # CV exprimé en % pour la légende (tm_scale_continuous n'a pas d'équivalent
    # à scales::percent_format utilisé côté ggplot2).
    reseau_cv$cv_pct <- 100 * reseau_cv$cv_volume

    carte_robustesse <- fond_carte() +

      # Réseau de base en gris très clair, pour situer les corridors colorés
      # dans l'ensemble du réseau routier.
      tm_shape(reseau_geo) +
      tm_lines(col = "#DDDDDD", lwd = 0.3) +

      # Arêtes empruntées, colorées par CV (vert = robuste, rouge = sensible
      # aux hypothèses) et épaissies selon le volume moyen transporté.
      tm_shape(reseau_cv) +
      tm_lines(
        col        = "cv_pct",
        col.scale  = tm_scale_continuous(
          values = c("#1A9850", "#91CF60", "#FEE08B", "#FC8D59", "#D73027")
        ),
        col.legend = tm_legend(title = "Coef. de\nvariation (%)"),
        lwd        = "moy_volume",
        lwd.scale  = tm_scale(values.range = c(0.2, 3)),
        lwd.legend = tm_legend(show = FALSE)
      ) +

      tm_title("Robustesse spatiale des flux face aux incertitudes de paramètres") +
      tm_credits(
        note_lecture(sprintf(
          "l'arête la plus sensible a un coefficient de variation de %.0f %% sur les %d tirages.",
          max(reseau_cv$cv_pct, na.rm = TRUE), length(res_scen)
        )),
        position = tm_pos_out("center", "bottom", "left", "top"),
        size     = 0.65
      ) +
      tm_layout(legend.outside = TRUE, frame = TRUE) +
      tm_scalebar(position = c("left", "bottom")) +
      tm_compass(position  = c("right", "top"))

    tmap_save(
      carte_robustesse,
      file.path(DIR_SYNTHESE, "sensibilite_carte_robustesse.png"),
      width = 3000, height = 2400, dpi = 300
    )
    cat("  ✓ F. sensibilite_carte_robustesse.png\n")
  }
}


# ==============================================================================
# 6. Localisation de la production et de la consommation
# ==============================================================================
# La source utilisée ici est matrice_flux_fret_tonnes.csv, la matrice
# origine-destination des flux EN TONNES : la somme d'une ligne donne ce qu'une
# zone expédie, la somme d'une colonne ce qu'elle reçoit.
#
# Pourquoi pas offre_demande_zones.csv, qui porte pourtant les mots « offre » et
# « demande » ? Parce que ce fichier est libellé en mrd RWF et que ces montants
# sont produits en amont des paramètres testés : ils viennent de la matrice de
# comptabilité sociale et de sa spatialisation, que ni les élasticités, ni les
# valeurs au tonne, ni la valeur du temps ne touchent. Vérification faite sur
# les tirages, chaque zone n'y prend qu'une seule valeur : ce fichier est
# invariant par construction et ne peut rien dire d'une sensibilité.
# La conversion en tonnes, elle, dépend directement des valeurs au tonne, et la
# répartition des flux des élasticités : c'est donc bien en tonnes que la
# localisation de la production et de la consommation est susceptible de bouger.
#
# On raisonne en PART de chaque zone dans le total national plutôt qu'en niveau.
# Les valeurs au tonne déplacent aussi le tonnage TOTAL du pays, effet déjà
# suivi par l'indicateur « Tonnage total réseau » de la figure A ; passer en
# part isole ce qui nous intéresse ici, la LOCALISATION, de ce changement
# d'échelle d'ensemble.
# ==============================================================================

lire_zones <- function(dir_exp) {
  f <- file.path(dir_exp, "matrice_flux_fret_tonnes.csv")
  if (!file.exists(f)) return(NULL)
  m <- readr::read_csv(f, show_col_types = FALSE)
  if (ncol(m) < 2) return(NULL)

  noms <- m[[1]]
  M    <- as.matrix(m[, -1, drop = FALSE])
  # Les colonnes doivent désigner les mêmes zones que les lignes, dans le même
  # ordre, pour que la somme d'une colonne soit bien la réception de la zone
  # correspondante. On le vérifie plutôt que de le supposer.
  if (!identical(as.character(colnames(M)), as.character(noms))) return(NULL)

  expedie <- rowSums(M, na.rm = TRUE)   # ce que la zone produit et envoie
  recu    <- colSums(M, na.rm = TRUE)   # ce que la zone consomme et reçoit

  data.frame(
    zone           = noms,
    Offre          = expedie / sum(expedie),
    Demande        = recu    / sum(recu),
    offre_niveau   = expedie,
    demande_niveau = recu,
    stringsAsFactors = FALSE
  )
}

zones_ref   <- lire_zones(DIR_EXPORTS)
zones_scen  <- lapply(names(res_scen), function(id) lire_zones(file.path(DIR_SENS_EXPORTS, id)))
names(zones_scen) <- names(res_scen)
zones_scen  <- zones_scen[!vapply(zones_scen, is.null, logical(1))]

if (is.null(zones_ref) || length(zones_scen) < 2) {

  cat("  ⚠ G/H. localisation ignorée : matrice_flux_fret_tonnes.csv absent ou trop rare\n")

} else {

  # Table longue : une ligne par (tirage × zone × grandeur), en écart RELATIF de
  # part à la référence. Le passage en écart relatif permet de comparer sur un
  # même axe une grande zone et une petite : « la part de cette zone est
  # inférieure de 8 % à celle du scénario de référence ».
  loc_long <- do.call(rbind, lapply(names(zones_scen), function(id) {
    z <- zones_scen[[id]]
    m <- merge(z[, c("zone", "Offre", "Demande")],
               zones_ref[, c("zone", "Offre", "Demande")],
               by = "zone", suffixes = c("", "_ref"))
    rbind(
      data.frame(id = id, zone = m$zone, grandeur = "Production expédiée",
                 part = m$Offre,   part_ref = m$Offre_ref,   stringsAsFactors = FALSE),
      data.frame(id = id, zone = m$zone, grandeur = "Consommation reçue",
                 part = m$Demande, part_ref = m$Demande_ref, stringsAsFactors = FALSE)
    )
  }))
  loc_long$ecart_pct <- 100 * (loc_long$part - loc_long$part_ref) / loc_long$part_ref
  loc_long <- loc_long[is.finite(loc_long$ecart_pct), , drop = FALSE]
  loc_long$grandeur <- factor(loc_long$grandeur,
                              levels = c("Production expédiée", "Consommation reçue"))

  # ── FIGURE G : zones dont la part est la plus instable ────────────────────
  # Les 91 zones ne tiennent pas sur un graphique lisible : on ne retient que
  # les SENS_N_ZONES_VOLATILES dont la part varie le plus (étendue maximale
  # entre les deux grandeurs), classées de la plus instable à la moins instable.
  # Plancher de poids : le classement se fait sur une dispersion RELATIVE, qui
  # récompense mécaniquement les zones minuscules (passer de 0,05 % à 0,15 % du
  # tonnage national est un écart de +200 % pour un déplacement de fret
  # négligeable). On écarte donc les zones sous SENS_PART_MIN_ZONE, faute de
  # quoi le graphique classe du bruit et son échelle devient inexploitable.
  parts_ref <- pmax(zones_ref$Offre, zones_ref$Demande)
  zones_eligibles <- zones_ref$zone[parts_ref >= SENS_PART_MIN_ZONE]
  loc_long <- loc_long[loc_long$zone %in% zones_eligibles, , drop = FALSE]

  etendue_zone <- loc_long |>
    dplyr::group_by(zone, grandeur) |>
    dplyr::summarise(etendue = diff(range(ecart_pct)), .groups = "drop") |>
    dplyr::group_by(zone) |>
    dplyr::summarise(etendue = max(etendue), .groups = "drop") |>
    dplyr::arrange(dplyr::desc(etendue))

  zones_top <- utils::head(etendue_zone$zone, SENS_N_ZONES_VOLATILES)
  loc_top   <- loc_long[loc_long$zone %in% zones_top, , drop = FALSE]
  # Ordre des modalités : la plus volatile en HAUT du graphique. Avec un axe y
  # discret, le premier niveau se dessine en bas — on inverse donc l'ordre.
  loc_top$zone <- factor(loc_top$zone, levels = rev(zones_top))

  gG <- ggplot(loc_top, aes(x = ecart_pct, y = zone)) +
    geom_vline(xintercept = 0, linewidth = 0.6, color = "#B22222") +
    geom_boxplot(fill = "#BDD7E7", color = "#4A6D80", linewidth = 0.35,
                 outlier.shape = NA, alpha = 0.75, width = 0.6) +
    geom_jitter(height = 0.14, width = 0, size = 1.5, alpha = 0.55, color = "#2171B5") +
    facet_wrap(~ grandeur, ncol = 2) +
    scale_x_continuous(labels = function(x) paste0(x, " %")) +
    labs(
      title    = "Où la localisation de l'activité dépend-elle des hypothèses ?",
      subtitle = sprintf("Écart à la référence de la part de chaque zone dans le tonnage national, sur %d tirages",
                         length(zones_scen)),
      x = "Écart à la référence de la part de la zone", y = NULL,
      caption = note_lecture(sprintf(
        "les parts sont calculées sur la matrice origine-destination en tonnes ; raisonner en part neutralise la variation du tonnage national et isole les déplacements de localisation. Seules les %d zones pesant au moins %s %% du tonnage sont classées, et les %d plus instables sont représentées. La zone %s est la plus instable, avec une part qui s'étend sur %.0f points d'écart à la référence.",
        length(zones_eligibles), format(100 * SENS_PART_MIN_ZONE, decimal.mark = ","),
        length(zones_top), etendue_zone$zone[1], etendue_zone$etendue[1]),
        largeur_car = 150)
    ) +
    theme_minimal(base_size = 12) +
    theme(
      plot.title       = element_text(face = "bold"),
      plot.subtitle    = element_text(color = "#666666"),
      strip.text       = element_text(face = "bold"),
      panel.grid.major.y = element_blank(),
      panel.grid.minor   = element_blank()
    ) +
    THEME_NOTE_LECTURE
  ggsave(file.path(DIR_SYNTHESE, "sensibilite_localisation_zones.png"),
         gG, width = 13, height = 7.5, dpi = 300, bg = "white")
  cat("  ✓ G. sensibilite_localisation_zones.png\n")

  # ── FIGURE H : carte de la volatilité de la localisation ──────────────────
  # Coefficient de variation de la part de chaque zone entre tirages, porté sur
  # la position géographique des entrepôts. La couleur code la robustesse (vert
  # = part stable, rouge = part qui dépend des hypothèses), la taille du symbole
  # le tonnage de la zone dans le scénario de référence.
  cv_zone <- loc_long |>
    dplyr::group_by(zone, grandeur) |>
    dplyr::summarise(cv_pct = 100 * sd(part) / mean(part), .groups = "drop")

  f_noeuds <- file.path(DIR_EXPORTS, "reseau_noeuds.gpkg")
  if (!file.exists(f_noeuds)) {

    cat("  ⚠ H. carte de localisation ignorée : géométrie", f_noeuds, "absente\n")

  } else {

    noeuds <- sf::st_read(f_noeuds, quiet = TRUE)
    noeuds <- noeuds[!is.na(noeuds$warehouse_name), ]

    # Appariement des noms de zone entre le CSV et le GeoPackage. Les deux
    # sources traversent des encodages différents (UTF-8 côté CSV, encodage du
    # driver GDAL côté GPKG) : « Frontière Nemba » peut donc ne pas coïncider
    # caractère pour caractère. On compare donc des clés normalisées, sans
    # accent ni ponctuation, plutôt que les libellés bruts.
    normaliser_nom <- function(x) {
      y <- iconv(x, to = "ASCII//TRANSLIT")
      y <- ifelse(is.na(y), x, y)
      toupper(gsub("[^A-Za-z0-9]+", "", y))
    }
    noeuds$cle  <- normaliser_nom(noeuds$warehouse_name)
    cv_zone$cle <- normaliser_nom(cv_zone$zone)

    # Poids de la zone dans le scénario de référence, en tonnes : sert à la
    # taille des symboles, pour ne pas donner le même poids visuel à une zone
    # marginale très instable qu'à un pôle qui porte l'essentiel du fret.
    poids_zone <- data.frame(
      cle                   = normaliser_nom(zones_ref$zone),
      `Production expédiée` = zones_ref$offre_niveau,
      `Consommation reçue`  = zones_ref$demande_niveau,
      check.names = FALSE, stringsAsFactors = FALSE
    )

    # Bornes de couleur COMMUNES aux deux cartes : sans cela, tmap ajuste
    # l'échelle panneau par panneau et un même vert ne désigne plus la même
    # volatilité à gauche et à droite — les deux cartes ne seraient plus
    # comparables, ce qui est pourtant tout l'intérêt de les mettre côte à côte.
    cv_max <- max(cv_zone$cv_pct[is.finite(cv_zone$cv_pct)], na.rm = TRUE)

    carte_grandeur <- function(lib, col_poids, titre) {
      d <- cv_zone[cv_zone$grandeur == lib, ]
      d$poids <- poids_zone[[col_poids]][match(d$cle, poids_zone$cle)]
      g <- merge(noeuds[, c("cle", "warehouse_name")], d, by = "cle")
      g <- g[is.finite(g$cv_pct) & is.finite(g$poids), ]

      fond_carte() +
        tm_shape(g) +
        tm_symbols(
          fill        = "cv_pct",
          fill.scale  = tm_scale_continuous(limits = c(0, cv_max),
                                            values = PALETTE_ROBUSTESSE),
          fill.legend = tm_legend(title = "Coef. de\nvariation (%)"),
          size        = "poids",
          # Racine carrée : les tonnages par zone sont très inégaux (Kigali pèse
          # plusieurs ordres de grandeur de plus qu'une zone rurale). Sans
          # transformation, tout sauf les deux ou trois pôles majeurs se réduit
          # à un point invisible et la carte ne montre plus rien.
          size.scale  = tm_scale_continuous(trans = "sqrt",
                                            values.range = c(0.25, 1.5)),
          size.legend = tm_legend(show = FALSE),
          col         = "white",     # anneau clair : sépare deux symboles qui se chevauchent
          col_alpha   = 0.9,
          lwd         = 0.6
        ) +
        tm_title(titre) +
        tm_layout(legend.outside = TRUE, frame = TRUE) +
        tm_scalebar(position = c("left", "bottom"))
    }

    carte_offre   <- carte_grandeur("Production expédiée", "Production expédiée",
                                    "Production expédiée (tonnes)")
    carte_demande <- carte_grandeur("Consommation reçue",  "Consommation reçue",
                                    "Consommation reçue (tonnes)")

    tmap_save(
      tmap_arrange(carte_offre, carte_demande, ncol = 2),
      file.path(DIR_SYNTHESE, "sensibilite_carte_localisation.png"),
      width = 3600, height = 2000, dpi = 300
    )
    cat("  ✓ H. sensibilite_carte_localisation.png\n")
  }
}

# ==============================================================================
# 7. Stabilité du classement de criticité des arêtes
# ==============================================================================
# Le chapitre de vulnérabilité classe les arêtes par surcoût pondéré et met en
# avant les plus critiques. Ce classement est-il un fait robuste, ou l'artefact
# d'un jeu de paramètres particulier ? On reprend les SENS_TOP_CRITICITE
# premières arêtes du classement de RÉFÉRENCE et on regarde, tirage par tirage,
# le rang qu'elles occupent. Une arête dont le rang reste resserré est
# critique quelles que soient les hypothèses ; une arête dont le rang se
# disperse — ou qui disparaît du classement — ne l'est que sous la référence.
# ==============================================================================

# Le module 05 classe la criticité à deux niveaux : par ROUTE nommée
# (criticite_routes_*, une ligne par voie OSM, avec son nom) et par ARÊTE du
# graphe (criticite_aretes_*, une ligne par tronçon). On préfère la route :
# c'est l'unité dont parle le mémoire — « les axes critiques » — et elle porte
# un nom lisible, alors qu'une route est découpée en plusieurs arêtes qui
# occupent sinon une dizaine de lignes du graphique sous un index technique.
# On retombe sur l'arête si le classement par route n'a pas été exporté.
fichier_criticite <- function(dir_exp, unite) {
  if (is.na(SCEN_VULN)) return(NA_character_)
  f <- file.path(dir_exp, sprintf("criticite_%s_%s.csv", unite, SCEN_VULN))
  if (file.exists(f)) f else NA_character_
}

# Renvoie un classement normalisé : une clé, un libellé lisible, un rang —
# quelle que soit l'unité, pour que la suite du code n'ait pas à s'en soucier.
lire_criticite <- function(dir_exp, unite) {
  f <- fichier_criticite(dir_exp, unite)
  if (is.na(f)) return(NULL)
  d <- readr::read_csv(f, show_col_types = FALSE)
  col_cle <- if (identical(unite, "routes")) "osm_id" else "arete_idx"
  if (!all(c(col_cle, "rang") %in% names(d))) return(NULL)

  nom  <- if ("name" %in% names(d))      as.character(d$name)      else rep(NA_character_, nrow(d))
  type <- if ("road_type" %in% names(d)) as.character(d$road_type) else rep("", nrow(d))
  data.frame(
    cle     = as.character(d[[col_cle]]),
    # Beaucoup de tronçons du réseau rwandais n'ont pas de nom dans OSM : on
    # retombe alors sur le type de route.
    libelle = ifelse(!is.na(nom) & nzchar(trimws(nom)), nom, type),
    rang    = d$rang,
    stringsAsFactors = FALSE
  )
}

# Unité retenue, décidée UNE FOIS pour la référence et tous les tirages : les
# comparer sur des unités différentes n'aurait aucun sens.
# On ne prend pas la route par principe mais par couverture : si une version
# antérieure du module 05 n'a exporté le classement par route que pour une
# poignée de tirages, préférer la route reviendrait à jeter les autres et à
# conclure sur trois ou quatre scénarios. À couverture égale, la route
# l'emporte, parce qu'elle est nommée et que c'est l'unité du mémoire.
compte_unite <- function(unite) {
  sum(vapply(names(res_scen),
             function(id) !is.na(fichier_criticite(file.path(DIR_SENS_EXPORTS, id), unite)),
             logical(1)))
}
n_routes <- if (is.null(lire_criticite(DIR_EXPORTS, "routes"))) 0L else compte_unite("routes")
n_aretes <- if (is.null(lire_criticite(DIR_EXPORTS, "aretes"))) 0L else compte_unite("aretes")

UNITE_CRIT <- if (n_routes >= n_aretes && n_routes >= 2) "routes" else "aretes"
LIB_UNITE  <- if (identical(UNITE_CRIT, "routes")) "routes" else "arêtes"
LIB_UNITE_SING <- if (identical(UNITE_CRIT, "routes")) "route" else "arête"
if (max(n_routes, n_aretes) >= 2)
  cat("✓ Criticité analysée par", LIB_UNITE,
      sprintf("(routes : %d tirages, arêtes : %d)\n", n_routes, n_aretes))

crit_ref  <- lire_criticite(DIR_EXPORTS, UNITE_CRIT)
crit_scen <- lapply(names(res_scen), function(id) lire_criticite(file.path(DIR_SENS_EXPORTS, id), UNITE_CRIT))
names(crit_scen) <- names(res_scen)
crit_scen <- crit_scen[!vapply(crit_scen, is.null, logical(1))]

if (is.null(crit_ref) || length(crit_scen) < 2) {

  cat("  ⚠ I. stabilité de la criticité ignorée : classements absents ou trop rares\n")

} else {

  # Les SENS_TOP_CRITICITE éléments les plus critiques de la référence.
  crit_ref <- crit_ref[order(crit_ref$rang), , drop = FALSE]
  top_ref  <- utils::head(crit_ref, SENS_TOP_CRITICITE)

  # Rang de chacun dans chaque tirage (NA = sorti du classement).
  rangs <- do.call(rbind, lapply(names(crit_scen), function(id) {
    d <- crit_scen[[id]]
    data.frame(id = id, cle = top_ref$cle,
               rang = d$rang[match(top_ref$cle, d$cle)],
               stringsAsFactors = FALSE)
  }))

  # Présence : nombre de tirages où l'élément figure encore au classement.
  presence <- tapply(!is.na(rangs$rang), rangs$cle, sum)

  # Deux voies peuvent porter le même nom OSM (« KK 3 Rd » désigne plusieurs
  # tronçons distincts). On ne rappelle l'identifiant que dans ce cas, pour ne
  # pas alourdir des libellés déjà lisibles.
  double <- duplicated(top_ref$libelle) | duplicated(top_ref$libelle, fromLast = TRUE)
  nom_affiche <- ifelse(double,
                        paste0(top_ref$libelle, " · ", top_ref$cle),
                        top_ref$libelle)
  etiquette <- sprintf("%2d. %s  (%d/%d)", top_ref$rang, nom_affiche,
                       presence[top_ref$cle], length(crit_scen))

  # L'ordre des niveaux suit le rang de référence ; comme l'axe discret dessine
  # le premier niveau en bas, on inverse pour que le rang 1 soit en haut.
  corresp <- setNames(etiquette, top_ref$cle)
  rangs$etiquette <- factor(corresp[rangs$cle], levels = rev(etiquette))

  ref_pts <- data.frame(etiquette = factor(etiquette, levels = rev(etiquette)),
                        rang = top_ref$rang, stringsAsFactors = FALSE)

  # Étendue observée entre tirages, tracée en trait fin sous les points.
  etendues <- rangs |>
    dplyr::filter(!is.na(rang)) |>
    dplyr::group_by(etiquette) |>
    dplyr::summarise(rmin = min(rang), rmax = max(rang), .groups = "drop")

  gI <- ggplot() +
    geom_linerange(data = etendues,
                   aes(y = etiquette, xmin = rmin, xmax = rmax),
                   color = "#C9D3DA", linewidth = 1.6) +
    # La référence est tracée AVANT les tirages, et les tirages sont légèrement
    # dispersés en hauteur : quand un rang ne bouge pas d'un tirage à l'autre,
    # tous les points se superposeraient exactement au losange et la ligne
    # paraîtrait vide alors qu'elle décrit le cas le plus stable qui soit.
    geom_point(data = ref_pts,
               aes(x = rang, y = etiquette, color = "Référence"),
               size = 3.2, shape = 18) +
    geom_jitter(data = rangs[!is.na(rangs$rang), ],
                aes(x = rang, y = etiquette, color = "Tirages"),
                height = 0.16, width = 0, size = 1.7, alpha = 0.65) +
    scale_color_manual(values = c(Tirages = "#6BAED6", `Référence` = "#B22222"),
                       breaks = c("Référence", "Tirages"), name = NULL) +
    scale_x_continuous(breaks = scales::pretty_breaks()) +
    labs(
      title    = "Les axes critiques le restent-ils sous d'autres hypothèses ?",
      subtitle = sprintf("Rang au classement de criticité des %d %s les plus critiques de la référence — %s",
                         nrow(top_ref), LIB_UNITE,
                         if (identical(SCEN_VULN, "Scenario_default")) "scénario de rupture par défaut"
                         else paste("scénario", gsub("_", " ", SCEN_VULN))),
      x = sprintf("Rang au classement de criticité (1 = %s la plus critique)", LIB_UNITE_SING),
      y = NULL,
      caption = note_lecture(sprintf(
        "chaque point bleu est un tirage, le losange rouge le rang de référence ; le trait gris relie les rangs extrêmes observés. Le nombre entre parenthèses indique dans combien de tirages sur %d l'axe figure encore au classement — un axe absent en est sorti.",
        length(crit_scen)), largeur_car = 132)
    ) +
    theme_minimal(base_size = 11) +
    theme(
      plot.title       = element_text(face = "bold"),
      plot.subtitle    = element_text(color = "#666666"),
      legend.position  = "top",
      axis.text.y      = element_text(family = "mono", size = 8),
      panel.grid.major.y = element_blank(),
      panel.grid.minor   = element_blank()
    ) +
    THEME_NOTE_LECTURE
  ggsave(file.path(DIR_SYNTHESE, "sensibilite_stabilite_criticite.png"),
         gI, width = 11, height = 7.5, dpi = 300, bg = "white")
  cat("  ✓ I. sensibilite_stabilite_criticite.png\n")
}


# ==============================================================================
# 8. Tables chiffrées
# ==============================================================================
# Les figures montrent des formes ; la rédaction demande des nombres. Ce bloc
# écrit deux CSV qui donnent, pour chaque indicateur et pour chaque secteur, la
# valeur de référence et la distribution des écarts observés entre tirages —
# de quoi écrire « l'indicateur varie de −12 % à +18 %, médiane +2 % » sans
# relire les graphiques au pixel près.
#
# Les écarts sont exprimés en pourcentage de la valeur de référence, comme dans
# toutes les figures, de sorte que les nombres cités dans le texte et ceux lus
# sur les axes soient les mêmes.
# ==============================================================================

# Arrondi commun : deux décimales suffisent pour des pourcentages d'écart, et
# évitent des colonnes illisibles de chiffres non significatifs.
arrondir <- function(x) round(x, 2)

# ── Table 1 : indicateurs agrégés ───────────────────────────────────────────
# Pour chaque indicateur : sa valeur de référence, les quartiles de l'écart
# relatif sur les tirages, et — quand l'indicateur varie assez pour que la
# corrélation ait un sens — le paramètre d'entrée qui le pilote le plus.
table_indic <- agg_long |>
  dplyr::group_by(indicateur) |>
  dplyr::summarise(
    # signif() plutôt qu'un arrondi décimal fixe : la colonne mélange des RWF
    # (10^10), des tonnes, des kilomètres et un ratio, et un nombre de décimales
    # unique serait absurde pour au moins l'un d'entre eux.
    valeur_reference = signif(dplyr::first(ref), 6),
    n_tirages        = dplyr::n(),
    ecart_min_pct    = arrondir(min(ecart_pct)),
    ecart_q1_pct     = arrondir(stats::quantile(ecart_pct, 0.25)),
    ecart_median_pct = arrondir(stats::median(ecart_pct)),
    ecart_q3_pct     = arrondir(stats::quantile(ecart_pct, 0.75)),
    ecart_max_pct    = arrondir(max(ecart_pct)),
    etendue_pct      = arrondir(diff(range(ecart_pct))),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(etendue_pct))

# Paramètre dominant : celui dont la corrélation de rang avec l'indicateur est
# la plus forte en valeur absolue. indices ne couvre que les indicateurs
# retenus en section 3 (renseignés partout et non constants) : les autres
# reçoivent NA plutôt qu'une valeur trompeuse.
dominant <- indices |>
  dplyr::group_by(sortie) |>
  dplyr::slice_max(abs(corr), n = 1, with_ties = FALSE) |>
  dplyr::ungroup() |>
  dplyr::transmute(indicateur = sortie,
                   parametre_dominant = entree_lbl,
                   correlation_spearman = arrondir(corr))

table_indic <- dplyr::left_join(table_indic, dominant, by = "indicateur")
readr::write_csv(table_indic, file.path(DIR_SYNTHESE, "sensibilite_table_indicateurs.csv"))
cat("  ✓ Table chiffrée des indicateurs → sensibilite_table_indicateurs.csv\n")

# ── Table 2 : tonnage par secteur ───────────────────────────────────────────
# Même lecture, appliquée au tonnage transporté de chaque secteur : c'est le
# chiffrage de la figure E.
table_sect <- sect_long |>
  dplyr::group_by(secteur) |>
  dplyr::summarise(
    n_tirages        = dplyr::n(),
    ecart_min_pct    = arrondir(min(ecart_pct)),
    ecart_median_pct = arrondir(stats::median(ecart_pct)),
    ecart_max_pct    = arrondir(max(ecart_pct)),
    etendue_pct      = arrondir(diff(range(ecart_pct))),
    .groups = "drop"
  ) |>
  dplyr::arrange(dplyr::desc(etendue_pct))

# Tonnage de référence du secteur, pour situer les écarts relatifs : un écart
# de ±30 % ne pèse pas la même chose sur un secteur marginal et sur le premier
# contributeur au fret.
table_sect$tonnage_reference <- signif(ref$secteur[as.character(table_sect$secteur)], 6)
table_sect <- table_sect[, c("secteur", "tonnage_reference", "n_tirages",
                             "ecart_min_pct", "ecart_median_pct",
                             "ecart_max_pct", "etendue_pct")]

readr::write_csv(table_sect, file.path(DIR_SYNTHESE, "sensibilite_table_secteurs.csv"))
cat("  ✓ Table chiffrée des secteurs → sensibilite_table_secteurs.csv\n")

cat("\n✓ Synthèse de sensibilité terminée →", DIR_SYNTHESE, "\n")
