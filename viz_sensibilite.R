################################################################################
# viz_sensibilite.R
# RÔLE : Mettre en évidence les DIVERGENCES entre le scénario de RÉFÉRENCE et les
#        scénarios de l'analyse de sensibilité par hypercube latin produits par
#        run_sensibilite.R.
#
# ENTRÉES (toutes déjà écrites sur disque, aucun modèle n'est relancé)
#   • outputs/exports/sensibilite/plan_lhs.csv
#       → plan d'expérience : multiplicateurs d'ENTRÉE (beta_<sect>,
#         valtonne_<sect>) tirés pour chaque scénario ;
#   • outputs/exports/volumes_fret_par_secteur.csv         (RÉFÉRENCE)
#     outputs/exports/comptabilite_couts_eoq.csv           (RÉFÉRENCE)
#   • outputs/exports/sensibilite/<id>/volumes_fret_par_secteur.csv
#     outputs/exports/sensibilite/<id>/comptabilite_couts_eoq.csv   (SCÉNARIOS)
#     outputs/exports/reseau_avec_fret.gpkg                (géométrie du réseau)
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
#      par arête : où le réseau est le plus sensible à l'incertitude.
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

# Étiquettes lisibles pour les colonnes de paramètres : "beta_Agriculture" →
# "β Agriculture" (élasticité) ; "valtonne_Mines" → "RWF/t Mines" (valeur
# unitaire en RWF par tonne). Réutilisée pour la table de synthèse (section 1)
# et les indices de sensibilité (section 3).
jolie_entree <- function(x) {
  x <- sub("^beta_",     "β ",     x)
  x <- sub("^valtonne_", "RWF/t ", x)
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
      `Coût transport total`   = cout_transport_tot
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
# exacte des 16 multiplicateurs tirés (β et RWF/t par secteur). Sert de
# légende détaillée à consulter à côté des figures, puisque les couleurs
# seules ne permettent pas de lire les valeurs des paramètres.
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
       gA, width = 12.5, height = 6.8, dpi = 300)
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
         gB, width = 9, height = 8.8, dpi = 300)
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
  scatter_princ <- do.call(rbind, lapply(cols_entree, function(e) {
    data.frame(
      entree_lbl    = jolie_entree(e),
      valeur_entree = entrees_mat[, e],
      ecart_pct     = agg_long$ecart_pct[
        match(ids_princ, agg_long$id[agg_long$indicateur == indic_principal])
      ],
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
         gC, width = 13, height = 9, dpi = 300)
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
       gD, width = 10, height = 8.8, dpi = 300)
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
       gE, width = 12.5, height = 6.8, dpi = 300)
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

cat("\n✓ Synthèse de sensibilité terminée →", DIR_SYNTHESE, "\n")
