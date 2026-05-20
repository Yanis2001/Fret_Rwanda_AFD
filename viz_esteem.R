################################################################################
# viz_esteem.R
# RÔLE : Visualisations du modèle ESTEEM — Dynamic I-O Traverse Disequilibrium
#        (Magacho & Spinola, 2025) couplé au modèle de transport de fret Rwanda.
#
# PEUT TOURNER SANS RELANCER 06_esteem.R si persist_esteem.rds est à jour.
# RELANCER 06_esteem.R avant ce script si :
#   → les paramètres ESTEEM_BETA_Y, ESTEEM_BETA_P, ESTEEM_MU_1 ont changé
#   → le scénario de perturbation (NOM_SCENARIO) a changé
#   → les flux de fret (flux_gravitaire) ont été recalculés
#
# NOTE sur la dépendance au solveur EDO (graphique 8 — sensibilité) :
#   Le graphique 8 relance des simulations EDO complètes pour tester 16
#   combinaisons de paramètres. Il utilise esteem_ode() et state0 qui sont
#   sauvegardés dans persist_esteem.rds depuis 06_esteem.R. S'assurer que
#   la version de 06_esteem.R utilisée est à jour avant de lancer ce script.
#
# GRAPHIQUES PRODUITS (dans outputs/cartes/) :
#   1. graphique_esteem_production.png  — Trajectoires de production agrégée
#   2. graphique_esteem_secteurs.png    — Pertes/prix sectoriels comparés
#   3. graphique_esteem_prix.png        — CPI, prix sectoriels, inflation
#   4. graphique_esteem_inventaires.png — Stocks et markups par secteur
#   5. graphique_esteem_biophysique.png — Terres agricoles, prix foncier
#   6. graphique_esteem_vs_ario.png     — Comparaison ESTEEM vs ARIO (si dispo)
#   7. graphique_esteem_chocs.png       — Profil sectoriel des chocs δy et δc
#   8. graphique_esteem_sensibilite.png — Heatmap β_y × β_p (robustesse param.)
#
# FICHIERS LUS : persist_esteem.rds, persist_vulnerabilite.rds + 00_parametres.R
################################################################################

source("00_parametres.R")
fond_carte <- readRDS(file.path(DIR_CARTES, "persist_fond_carte.rds"))

# ── Package solveur EDO (requis pour le graphique 8 — analyse de sensibilité) ──
# Le graphique 8 relance des simulations complètes du modèle ESTEEM avec
# différentes valeurs de β_y et β_p. deSolve est nécessaire pour cela.
if (!requireNamespace("deSolve", quietly = TRUE)) {
  install.packages("deSolve", dependencies = TRUE)
}
library(deSolve)

# Chemin de persistance ESTEEM (cohérent avec 06_esteem.R)
PERSIST_ESTEEM <- file.path(DIR_PERSIST, "persist_esteem.rds")

# ── Chargement des résultats ESTEEM ───────────────────────────────────────────
if (!file.exists(PERSIST_ESTEEM)) {
  stop("Fichier persist_esteem.rds introuvable.\n",
       "→ Lancer 06_esteem.R avant viz_esteem.R\n",
       "→ Chemin attendu : ", PERSIST_ESTEEM)
}

cat("=== Chargement des résultats ESTEEM pour visualisation ===\n")
.est <- readRDS(PERSIST_ESTEEM)
list2env(.est, envir = .GlobalEnv)
rm(.est)

# ── Vérification des objets requis pour le graphique 8 ────────────────────────
# state0 et esteem_ode sont persistés depuis 06_esteem.R (depuis la version
# corrigée). Si absent (ancienne version du .rds), le graphique 8 est ignoré.
sensibilite_possible <- exists("state0") && exists("esteem_ode") &&
  exists("ESTEEM_DT_ANS") && is.function(esteem_ode)
if (!sensibilite_possible) {
  cat("  ⚠ state0/esteem_ode absents du .rds — relancer 06_esteem.R pour\n")
  cat("    activer le graphique 8 (sensibilité aux paramètres)\n")
}

# Chargement optionnel des résultats ARIO pour comparaison
ario_dispo <- file.exists(PERSIST_ARIO)
if (ario_dispo) {
  .ario <- readRDS(PERSIST_ARIO)
  cat("  ✓ Résultats ARIO chargés pour comparaison\n")
}

cat("✓ Résultats ESTEEM chargés — scénario :", NOM_SCENARIO, "\n\n")

################################################################################
# CONFIGURATION GRAPHIQUE
#
# On définit ici une palette de couleurs et un thème ggplot cohérents
# avec le reste du script (utilise les mêmes conventions que viz_reseau.R).
################################################################################

# ── Palette de couleurs par secteur ──────────────────────────────────────────
# 8 couleurs distinctes et lisibles pour les 8 secteurs de l'économie rwandaise.
# Basées sur ColorBrewer "Set1" + ajustements pour les paires proches.
PALETTE_SECTEUR <- c(
  Agriculture    = "#2CA02C",   # Vert foncé       — nature, agriculture
  Mines          = "#7F7F7F",   # Gris ardoise      — minerais
  Agro_industrie = "#98DF8A",   # Vert clair        — transformation agricole
  Industrie      = "#1F77B4",   # Bleu classique    — industrie manufacturière
  Construction   = "#BCBD22",   # Jaune-vert        — BTP, infrastructures
  Commerce       = "#FF7F0E",   # Orange            — échanges commerciaux
  Transport      = "#D62728",   # Rouge             — routes, logistique
  Services       = "#9467BD"    # Violet            — services financiers/sociaux
)

# ── Thème graphique ESTEEM ────────────────────────────────────────────────────
# Thème ggplot2 minimal adapté pour les graphiques de séries temporelles
# économiques : fond blanc, grille légère, police lisible.
theme_esteem <- theme_minimal(base_size = 12) +
  theme(
    plot.title    = element_text(face = "bold", size = 14, hjust = 0),
    plot.subtitle = element_text(size = 10, color = "#555555"),
    plot.caption  = element_text(size = 9, color = "#888888", hjust = 0),
    panel.grid.minor = element_blank(),
    legend.position = "right",
    legend.title    = element_text(size = 10, face = "bold"),
    strip.text      = element_text(face = "bold")
  )

# Ligne verticale de fin du choc (repère temporel sur tous les graphiques)
xintercept_choc <- DUREE_JOURS   # En jours

cat("✓ Configuration graphique définie\n\n")

################################################################################
# GRAPHIQUE 1 — TRAJECTOIRES DE PRODUCTION AGRÉGÉE
#
# Ce graphique montre l'évolution de la production totale (M USD/an) pour les
# deux scénarios (référence et choc) sur l'horizon de simulation. On peut y
# lire la profondeur et la durée de la récession induite par le choc.
################################################################################

cat("── Graphique 1 : Production agrégée ────────────────────────────────────\n")

# Construction du tableau de données pour ggplot
df_prod <- tibble(
  Jour      = rep(res_ref$j, 2),
  Scenario  = rep(c("Référence", NOM_SCENARIO), each = length(res_ref$j)),
  Production = c(res_ref$x_total, res_choc$x_total),
  CPI        = c(res_ref$pc, res_choc$pc)
) %>%
  mutate(
    Scenario = factor(Scenario, levels = c("Référence", NOM_SCENARIO))
  )

g1 <- ggplot(df_prod, aes(x = Jour, y = Production, color = Scenario,
                          linetype = Scenario)) +
  # Zone ombrée représentant les pertes (entre référence et choc)
  geom_ribbon(
    data = tibble(
      Jour  = res_ref$j,
      y_ref = res_ref$x_total,
      y_choc = res_choc$x_total
    ),
    aes(x = Jour, ymin = y_choc, ymax = y_ref),
    inherit.aes = FALSE,
    fill = "#FF7F7F", alpha = 0.25
  ) +
  geom_line(linewidth = 1.1) +
  # Ligne verticale : fin de la perturbation
  geom_vline(xintercept = xintercept_choc, color = "#CC0000",
             linetype = "dashed", linewidth = 0.8) +
  annotate("text", x = xintercept_choc + 1, y = max(df_prod$Production) * 0.99,
           label = paste("Fin du choc\n(J", xintercept_choc, ")"),
           hjust = 0, size = 3.5, color = "#CC0000") +
  scale_color_manual(values = setNames(c("#333333", "#D62728"), c("Référence", NOM_SCENARIO)),
                     name = "Scénario") +
  scale_linetype_manual(values = setNames(c("dashed", "solid"), c("Référence", NOM_SCENARIO)),
                        name = "Scénario") +
  scale_y_continuous(labels = scales::label_number(suffix = " M$")) +
  labs(
    title    = "Production économique totale — Modèle ESTEEM",
    subtitle = paste0(
      "Scénario : ", NOM_SCENARIO, "\n",
      "Dynamique d'ajustement hors-équilibre après le choc de transport\n",
      "β_y = ", ESTEEM_BETA_Y, "/an | β_p = ", ESTEEM_BETA_P, "/an | η = ", ESTEEM_ETA
    ),
    x       = "Jour de simulation",
    y       = "Production totale (M USD/an)",
    caption = paste0(
      "Source : Modèle ESTEEM — Magacho & Spinola (2025) | Rwanda ",
      format(Sys.Date(), "%Y")
    )
  ) +
  theme_esteem

# Inset : courbe de perte en %
df_perte <- tibble(Jour = res_ref$j, Perte_pct = perte_pct)
g1_perte <- ggplot(df_perte, aes(x = Jour, y = Perte_pct)) +
  geom_area(fill = "#FF7F7F", alpha = 0.4) +
  geom_line(color = "#D62728", linewidth = 0.8) +
  geom_vline(xintercept = xintercept_choc, color = "#CC0000",
             linetype = "dashed", linewidth = 0.6) +
  geom_hline(yintercept = 0, color = "#333333", linewidth = 0.3) +
  scale_y_continuous(labels = scales::label_number(suffix = "%")) +
  labs(title = "Perte de production (%)", x = NULL, y = NULL) +
  theme_esteem +
  theme(plot.title = element_text(size = 9, face = "bold"),
        axis.text  = element_text(size = 8),
        plot.background = element_rect(fill = "white", color = "#CCCCCC"))

# Assemblage avec cowplot ou patchwork si disponible, sinon ggsave direct
if (requireNamespace("gridExtra", quietly = TRUE)) {
  g1_final <- gridExtra::arrangeGrob(g1, ncol = 1)
  ggsave(file.path(DIR_CARTES, "graphique_esteem_production.png"),
         g1_final, width = 14, height = 7, dpi = 300)
} else {
  ggsave(file.path(DIR_CARTES, "graphique_esteem_production.png"),
         g1, width = 14, height = 7, dpi = 300)
}
cat("  ✓ graphique_esteem_production.png\n\n")

################################################################################
# GRAPHIQUE 2 — PERTES SECTORIELLES
#
# Ce graphique à double panneau montre :
#   (a) la perte de production moyenne par secteur (impact quantitatif)
#   (b) la hausse de prix finale par secteur (impact inflationniste)
# Il révèle le compromis entre secteurs qui perdent de la production et ceux
# qui font face à une inflation sectorielle prononcée.
################################################################################

cat("── Graphique 2 : Pertes sectorielles ───────────────────────────────────\n")

# Panneau gauche : pertes de production
df_sect_prod <- perte_par_secteur %>%
  mutate(
    Secteur = factor(Secteur, levels = rev(perte_par_secteur$Secteur)),
    Type_impact = case_when(
      Perte_prod_pct > 1.0  ~ "Fort",
      Perte_prod_pct > 0.25 ~ "Modéré",
      TRUE                  ~ "Faible"
    )
  )

g2a <- ggplot(df_sect_prod,
              aes(x = Secteur, y = Perte_prod_pct,
                  fill = Secteur, alpha = Type_impact)) +
  geom_col(width = 0.7) +
  geom_text(aes(label = paste0(round(Perte_prod_pct, 2), "%")),
            hjust = -0.1, size = 3.5, color = "#333333") +
  coord_flip() +
  scale_fill_manual(values = PALETTE_SECTEUR, guide = "none") +
  scale_alpha_manual(values = c("Fort" = 1.0, "Modéré" = 0.75, "Faible" = 0.5),
                     name = "Intensité") +
  scale_y_continuous(limits = c(0, max(df_sect_prod$Perte_prod_pct) * 1.3),
                     labels = scales::label_number(suffix = "%")) +
  labs(
    title    = "Perte de production\npar secteur (moyenne sur l'horizon)",
    subtitle = "Choc via canal demande + coûts",
    x = NULL, y = "Perte moy. (% de la référence)"
  ) +
  theme_esteem

# Panneau droit : hausses de prix
df_sect_prix <- perte_par_secteur %>%
  mutate(
    Secteur = factor(Secteur, levels = rev(perte_par_secteur$Secteur))
  )

g2b <- ggplot(df_sect_prix,
              aes(x = Secteur, y = Hausse_prix_pct, fill = Secteur)) +
  geom_col(width = 0.7) +
  geom_text(aes(label = paste0("+", round(Hausse_prix_pct, 2), "%")),
            hjust = -0.1, size = 3.5, color = "#333333") +
  coord_flip() +
  scale_fill_manual(values = PALETTE_SECTEUR, guide = "none") +
  scale_y_continuous(limits = c(0, max(df_sect_prix$Hausse_prix_pct) * 1.3),
                     labels = scales::label_number(suffix = "%")) +
  labs(
    title    = "Hausse de prix finale\npar secteur (fin de simulation)",
    subtitle = "Transmission via la règle de markup endogène",
    x = NULL, y = "Hausse de prix (%)"
  ) +
  theme_esteem +
  theme(axis.text.y = element_blank())   # Éviter la répétition des labels

# Assemblage des deux panneaux
if (requireNamespace("patchwork", quietly = TRUE)) {
  library(patchwork)
  g2_final <- g2a + g2b +
    plot_annotation(
      title   = paste0("Impact sectoriel du choc — Scénario : ", NOM_SCENARIO),
      caption = "Modèle ESTEEM | Rwanda"
    )
  ggsave(file.path(DIR_CARTES, "graphique_esteem_secteurs.png"),
         g2_final, width = 16, height = 7, dpi = 300)
} else {
  ggsave(file.path(DIR_CARTES, "graphique_esteem_secteurs.png"),
         g2a, width = 10, height = 6, dpi = 300)
}
cat("  ✓ graphique_esteem_secteurs.png\n\n")

################################################################################
# GRAPHIQUE 3 — DYNAMIQUES DES PRIX ET INFLATION
#
# Ce graphique illustre la dynamique inflationniste induite par le choc :
#   (a) Évolution du CPI (Indice des Prix à la Consommation) comparée
#       entre référence et choc
#   (b) Taux d'inflation annualisé instantané (en %)
#   (c) Prix sectoriel pour les 3 secteurs les plus impactés
# C'est le résultat le plus distinctif d'ESTEEM par rapport à ARIO.
################################################################################

cat("── Graphique 3 : Dynamiques des prix ───────────────────────────────────\n")

# Panneau A : CPI comparé
df_cpi <- tibble(
  Jour     = rep(res_ref$j, 2),
  Scenario = rep(c("Référence", NOM_SCENARIO), each = length(res_ref$j)),
  CPI      = c(res_ref$pc, res_choc$pc)
) %>% mutate(CPI_ecart_pct = (CPI - 1) * 100)

g3a <- ggplot(df_cpi %>% filter(Scenario == NOM_SCENARIO),
              aes(x = Jour, y = (CPI - 1) * 100)) +
  geom_hline(yintercept = 0, color = "#999999", linewidth = 0.5) +
  geom_ribbon(
    data = tibble(
      Jour = res_ref$j,
      y_ref  = (res_ref$pc  - 1) * 100,
      y_choc = (res_choc$pc - 1) * 100
    ),
    aes(x = Jour, ymin = y_ref, ymax = y_choc),
    inherit.aes = FALSE,
    fill = "#FF9900", alpha = 0.3
  ) +
  geom_line(aes(y = (res_ref$pc - 1) * 100),
            color = "#333333", linetype = "dashed", linewidth = 0.8,
            data = tibble(Jour = res_ref$j,
                          Dummy = NOM_SCENARIO)) +
  geom_line(color = "#FF4400", linewidth = 1.1) +
  geom_vline(xintercept = xintercept_choc, color = "#CC0000",
             linetype = "dashed", linewidth = 0.7) +
  scale_y_continuous(labels = scales::label_number(suffix = "%")) +
  labs(
    title    = "Indice des prix à la consommation (CPI)",
    subtitle = "Écart par rapport au niveau normalisé à 1",
    x = "Jour", y = "CPI − 1 (%)"
  ) +
  theme_esteem

# Panneau B : Prix sectoriel pour les 3 secteurs les plus inflatés
top3_secteurs <- perte_par_secteur %>%
  arrange(desc(Hausse_prix_pct)) %>%
  slice_head(n = 3) %>%
  pull(Secteur)

df_prix_secteurs <- tibble(
  Jour    = rep(res_choc$j, length(top3_secteurs)),
  Secteur = rep(top3_secteurs, each = length(res_choc$j)),
  Prix    = as.vector(res_choc$p[, top3_secteurs]),
  Prix_ref = as.vector(res_ref$p[, top3_secteurs])
) %>%
  mutate(
    Secteur   = factor(Secteur, levels = top3_secteurs),
    Hausse_pct = (Prix / Prix_ref - 1) * 100
  )

g3b <- ggplot(df_prix_secteurs, aes(x = Jour, y = Hausse_pct,
                                    color = Secteur)) +
  geom_hline(yintercept = 0, color = "#999999", linewidth = 0.4) +
  geom_line(linewidth = 1.0) +
  geom_vline(xintercept = xintercept_choc, color = "#CC0000",
             linetype = "dashed", linewidth = 0.7) +
  scale_color_manual(values = PALETTE_SECTEUR[top3_secteurs], name = "Secteur") +
  scale_y_continuous(labels = scales::label_number(suffix = "%")) +
  labs(
    title    = "Hausse de prix relative (top 3 secteurs)",
    subtitle = "Prix choc / prix référence − 1",
    x = "Jour", y = "Hausse de prix (%)"
  ) +
  theme_esteem

# Panneau C : taux d'inflation journalier annualisé
df_pi <- tibble(
  Jour = res_choc$j,
  Inflation_ref  = res_ref$pi,
  Inflation_choc = res_choc$pi
) %>%
  # Lissage sur 14 jours pour réduire le bruit numérique
  mutate(
    Inf_ref_lissee  = zoo::rollmean(Inflation_ref,  k = 14, fill = NA, align = "right"),
    Inf_choc_lissee = zoo::rollmean(Inflation_choc, k = 14, fill = NA, align = "right")
  )

# zoo requis pour rollmean — installation si absent
if (!requireNamespace("zoo", quietly = TRUE)) install.packages("zoo")

df_pi <- tibble(
  Jour = res_choc$j,
  Inflation_ref  = res_ref$pi,
  Inflation_choc = res_choc$pi
)

g3c <- ggplot(df_pi, aes(x = Jour)) +
  geom_hline(yintercept = 0, color = "#999999", linewidth = 0.4) +
  geom_line(aes(y = Inflation_ref),  color = "#333333", linetype = "dashed",
            linewidth = 0.7, alpha = 0.7) +
  geom_line(aes(y = Inflation_choc), color = "#FF4400", linewidth = 0.9) +
  geom_vline(xintercept = xintercept_choc, color = "#CC0000",
             linetype = "dashed", linewidth = 0.7) +
  scale_y_continuous(labels = scales::label_number(suffix = "%/an")) +
  labs(
    title    = "Taux d'inflation instantané",
    subtitle = "Annualisé (gris = référence, rouge = choc)",
    x = "Jour", y = "Inflation (%/an)"
  ) +
  theme_esteem

# Assemblage
if (requireNamespace("patchwork", quietly = TRUE)) {
  g3_final <- (g3a | g3b) / g3c +
    plot_annotation(
      title   = paste0("Dynamiques des prix — ESTEEM — ", NOM_SCENARIO),
      caption = paste0("β_p = ", ESTEEM_BETA_P, "/an (stickiness des prix) | Rwanda")
    )
  ggsave(file.path(DIR_CARTES, "graphique_esteem_prix.png"),
         g3_final, width = 18, height = 10, dpi = 300)
} else {
  ggsave(file.path(DIR_CARTES, "graphique_esteem_prix.png"),
         g3a, width = 12, height = 6, dpi = 300)
}
cat("  ✓ graphique_esteem_prix.png\n\n")

################################################################################
# GRAPHIQUE 4 — INVENTAIRES ET MARKUP SECTORIEL
#
# Ce graphique illustre la dynamique des stocks et des marges commerciales :
#   (a) Inventaires pour les secteurs les plus affectés (en % du niveau désiré)
#   (b) Markup moyen pour les mêmes secteurs
#
# Ces deux variables sont au cœur du mécanisme d'ESTEEM :
#   - Quand les stocks chutent sous le niveau désiré, le markup monte
#     (les firmes profitent de la rareté pour restaurer leurs marges)
#   - Quand les stocks sont excédentaires, le markup baisse
#     (pression à la baisse pour écouler les stocks)
################################################################################

cat("── Graphique 4 : Inventaires et markups ────────────────────────────────\n")

# Top 4 secteurs par perte de production pour ce graphique
top4_sect <- perte_par_secteur %>%
  arrange(desc(abs(Perte_prod_pct))) %>%
  slice_head(n = 4) %>%
  pull(Secteur)

# Inventaires en % du niveau désiré (v/v_d × 100)
df_stocks <- tibble(
  Jour    = rep(res_choc$j, length(top4_sect)),
  Secteur = rep(top4_sect, each = length(res_choc$j)),
  v_pct   = as.vector(res_choc$v[, top4_sect]) /
    rep(v_d_calibre[top4_sect], each = length(res_choc$j)) * 100,
  v_pct_ref = as.vector(res_ref$v[, top4_sect]) /
    rep(v_d_calibre[top4_sect], each = length(res_choc$j)) * 100,
  mu_pct  = as.vector(res_choc$mu[, top4_sect]) * 100,
  mu_pct_ref = as.vector(res_ref$mu[, top4_sect]) * 100
) %>%
  mutate(Secteur = factor(Secteur, levels = top4_sect))

# Panneau A : inventaires (% du niveau désiré)
g4a <- ggplot(df_stocks, aes(x = Jour, color = Secteur)) +
  geom_hline(yintercept = 100, color = "#555555", linewidth = 0.5,
             linetype = "dashed") +
  annotate("text", x = max(res_choc$j) * 0.02, y = 101,
           label = "Niveau désiré (100%)", size = 3, color = "#555555") +
  geom_line(aes(y = v_pct_ref), linewidth = 0.6, linetype = "dashed", alpha = 0.5) +
  geom_line(aes(y = v_pct),     linewidth = 1.0) +
  geom_vline(xintercept = xintercept_choc, color = "#CC0000",
             linetype = "dashed", linewidth = 0.7) +
  facet_wrap(~Secteur, ncol = 2, scales = "free_y") +
  scale_color_manual(values = PALETTE_SECTEUR[top4_sect], guide = "none") +
  scale_y_continuous(labels = scales::label_number(suffix = "%")) +
  labs(
    title    = "Inventaires sectoriels (% du niveau désiré v_d)",
    subtitle = "Trait plein = choc | Tiret = référence | Ligne noire = niveau cible",
    x = "Jour", y = "v(t) / v_d × 100 (%)"
  ) +
  theme_esteem

# Panneau B : markup en %
g4b <- ggplot(df_stocks, aes(x = Jour, color = Secteur)) +
  geom_hline(aes(yintercept = mu_pct_ref),
             color = "#999999", linewidth = 0.4, linetype = "dashed") +
  geom_line(aes(y = mu_pct), linewidth = 1.0) +
  geom_vline(xintercept = xintercept_choc, color = "#CC0000",
             linetype = "dashed", linewidth = 0.7) +
  facet_wrap(~Secteur, ncol = 2, scales = "free_y") +
  scale_color_manual(values = PALETTE_SECTEUR[top4_sect], guide = "none") +
  scale_y_continuous(labels = scales::label_number(suffix = "%")) +
  labs(
    title    = "Markup sectoriel μ(t) (en % des coûts)",
    subtitle = "Réagit aux déviations d'inventaires : v < v_d → μ ↑",
    x = "Jour", y = "Markup μ(t) (%)"
  ) +
  theme_esteem

if (requireNamespace("patchwork", quietly = TRUE)) {
  g4_final <- g4a / g4b +
    plot_annotation(
      title   = paste0("Dynamiques stocks & markups — ", NOM_SCENARIO),
      caption = paste0("μ₁ = ", ESTEEM_MU_1, " | β_y = ", ESTEEM_BETA_Y, "/an")
    )
  ggsave(file.path(DIR_CARTES, "graphique_esteem_inventaires.png"),
         g4_final, width = 14, height = 12, dpi = 300)
} else {
  ggsave(file.path(DIR_CARTES, "graphique_esteem_inventaires.png"),
         g4a, width = 12, height = 8, dpi = 300)
}
cat("  ✓ graphique_esteem_inventaires.png\n\n")

################################################################################
# GRAPHIQUE 5 — RESSOURCES BIOPHYSIQUES
#
# Ce graphique illustre le canal biophysique du modèle ESTEEM :
#   (a) Usage des terres agricoles (en % de la disponibilité q_l,s)
#       → Si la ligne dépasse 100%, la contrainte est active
#   (b) Prix des terres (prix de rente ricardien normalisé)
#   (c) Contribution sectorielle à l'usage des terres
#
# Ce canal est particulièrement pertinent pour le Rwanda, pays agricole où
# les terres cultivables sont déjà utilisées à 90-95% de leur potentiel.
################################################################################

cat("── Graphique 5 : Ressources biophysiques ───────────────────────────────\n")

df_bio <- tibble(
  Jour          = res_choc$j,
  Land_choc_pct = land_choc / q_l_s * 100,
  Land_ref_pct  = land_ref  / q_l_s * 100,
  Prix_terres_choc = res_choc$pl,
  Prix_terres_ref  = res_ref$pl
)

# Panneau A : pression foncière
g5a <- ggplot(df_bio, aes(x = Jour)) +
  # Zone de dépassement de la contrainte (rouge si > 100%)
  geom_hline(yintercept = 100, color = "#CC0000", linewidth = 0.8, linetype = "dashed") +
  annotate("text", x = max(res_choc$j) * 0.02, y = 101.5,
           label = "Limite biophysique (100%)", size = 3, color = "#CC0000") +
  geom_ribbon(aes(ymin = Land_ref_pct, ymax = Land_choc_pct),
              fill = "#2CA02C", alpha = 0.2) +
  geom_line(aes(y = Land_ref_pct),  color = "#333333", linetype = "dashed",
            linewidth = 0.8, alpha = 0.7) +
  geom_line(aes(y = Land_choc_pct), color = "#2CA02C", linewidth = 1.1) +
  geom_vline(xintercept = xintercept_choc, color = "#CC0000",
             linetype = "dashed", linewidth = 0.7) +
  scale_y_continuous(limits = c(
    min(df_bio$Land_ref_pct, df_bio$Land_choc_pct) * 0.99,
    max(df_bio$Land_choc_pct) * 1.02
  ), labels = scales::label_number(suffix = "%")) +
  labs(
    title    = "Pression sur les terres agricoles",
    subtitle = paste0("En % de la disponibilité totale (q_l,s = ",
                      round(q_l_s, 2), " unités norm.)\n",
                      if (contrainte_active) "⚠ Contrainte biophysique ACTIVE"
                      else "Contrainte non active sur l'horizon simulé"),
    x = "Jour", y = "Utilisation / Disponibilité (%)"
  ) +
  theme_esteem

# Panneau B : prix des terres (rente ricardienne)
g5b <- ggplot(df_bio, aes(x = Jour)) +
  geom_hline(yintercept = 1.0, color = "#555555", linewidth = 0.4, linetype = "dashed") +
  annotate("text", x = max(res_choc$j) * 0.02, y = 1.005,
           label = "Prix équilibre (1.0)", size = 3, color = "#555555") +
  geom_line(aes(y = Prix_terres_ref),  color = "#333333", linetype = "dashed",
            linewidth = 0.8, alpha = 0.7) +
  geom_line(aes(y = Prix_terres_choc), color = "#98DF8A", linewidth = 1.1) +
  geom_vline(xintercept = xintercept_choc, color = "#CC0000",
             linetype = "dashed", linewidth = 0.7) +
  labs(
    title    = "Prix de rente des terres agricoles",
    subtitle = "Prix normalisé à 1 à l'équilibre | Logique ricardienne (Eq. 16)",
    x = "Jour", y = "Prix des terres p_l(t) (norm.)"
  ) +
  theme_esteem

# Panneau C : contribution sectorielle à la demande foncière
df_land_secteur <- tibble(
  Jour = rep(res_choc$j, N_SECTEURS),
  Secteur = rep(SECTEURS, each = length(res_choc$j)),
  Land = as.vector(t(t(res_choc$x) * q_l))
) %>%
  filter(Land > 0)   # Ne garder que les secteurs utilisant des terres

g5c <- ggplot(df_land_secteur, aes(x = Jour, y = Land, fill = Secteur)) +
  geom_area(position = "stack", alpha = 0.8) +
  geom_hline(yintercept = q_l_s, color = "#CC0000", linewidth = 0.8,
             linetype = "dashed") +
  geom_vline(xintercept = xintercept_choc, color = "#CC0000",
             linetype = "dashed", linewidth = 0.7) +
  scale_fill_manual(values = PALETTE_SECTEUR, name = "Secteur") +
  labs(
    title    = "Décomposition sectorielle de la demande foncière",
    subtitle = "Ligne rouge = disponibilité totale des terres",
    x = "Jour", y = "Demande foncière totale (unités norm.)"
  ) +
  theme_esteem

if (requireNamespace("patchwork", quietly = TRUE)) {
  g5_final <- (g5a | g5b) / g5c +
    plot_annotation(
      title   = paste0("Canal biophysique ESTEEM — ", NOM_SCENARIO),
      caption = "Rwanda — Terres agricoles ≈ 1.37 M ha (MINAGRI 2022)"
    )
  ggsave(file.path(DIR_CARTES, "graphique_esteem_biophysique.png"),
         g5_final, width = 18, height = 12, dpi = 300)
} else {
  ggsave(file.path(DIR_CARTES, "graphique_esteem_biophysique.png"),
         g5a, width = 12, height = 6, dpi = 300)
}
cat("  ✓ graphique_esteem_biophysique.png\n\n")

################################################################################
# GRAPHIQUE 6 — COMPARAISON ESTEEM vs ARIO
#
# ARIO  : modélise le rationnement de la capacité productive → perte directe
# ESTEEM: modélise l'ajustement prix × quantités → perte amortie ou amplifiée
#          selon la vitesse d'ajustement et les effets inflationnistes
#
# Cette comparaison est centrale car elle montre quelle dynamique domine :
#   - Si ESTEEM > ARIO : l'inflation exacerbe les pertes de production
#   - Si ESTEEM < ARIO : l'ajustement des prix amortit le choc
################################################################################

cat("── Graphique 6 : Comparaison ESTEEM vs ARIO ────────────────────────────\n")

# Construction du tableau de comparaison
if (ario_dispo && !is.null(.ario$production_totale_j)) {
  
  n_j_comm <- min(
    length(perte_pct),
    length(.ario$production_totale_j)
  )
  
  df_comp <- tibble(
    Jour         = res_choc$j[1:n_j_comm],
    Perte_ESTEEM = perte_pct[1:n_j_comm],
    Perte_ARIO   = (1 - .ario$production_totale_j[1:n_j_comm] /
                      .ario$production_totale_j[1]) * 100
  ) %>%
    pivot_longer(cols = c(Perte_ESTEEM, Perte_ARIO),
                 names_to = "Modele", values_to = "Perte_pct") %>%
    mutate(Modele = recode(Modele,
                           "Perte_ESTEEM" = "ESTEEM (prix + quantités)",
                           "Perte_ARIO"   = "ARIO (rationnement)"))
  
  g6 <- ggplot(df_comp, aes(x = Jour, y = Perte_pct,
                            color = Modele, linetype = Modele)) +
    geom_hline(yintercept = 0, color = "#999999", linewidth = 0.4) +
    geom_line(linewidth = 1.2) +
    geom_vline(xintercept = xintercept_choc, color = "#CC0000",
               linetype = "dashed", linewidth = 0.8) +
    scale_color_manual(
      values = c("ESTEEM (prix + quantités)" = "#D62728",
                 "ARIO (rationnement)"       = "#1F77B4"),
      name = "Modèle"
    ) +
    scale_linetype_manual(
      values = c("ESTEEM (prix + quantités)" = "solid",
                 "ARIO (rationnement)"       = "dashed"),
      name = "Modèle"
    ) +
    scale_y_continuous(labels = scales::label_number(suffix = "%")) +
    labs(
      title    = "Comparaison des pertes de production : ESTEEM vs ARIO",
      subtitle = paste0(
        "ARIO : choc de capacité directe (rationnement intrants)\n",
        "ESTEEM : choc de demande + coûts avec ajustement dynamique β_y = ",
        ESTEEM_BETA_Y, "/an, β_p = ", ESTEEM_BETA_P, "/an"
      ),
      x       = "Jour de simulation",
      y       = "Perte de production (% de la référence)",
      caption = paste0(
        "Scénario : ", NOM_SCENARIO, " | Rwanda ",
        format(Sys.Date(), "%Y")
      )
    ) +
    theme_esteem +
    theme(legend.position = "top")
  
  ggsave(file.path(DIR_CARTES, "graphique_esteem_vs_ario.png"),
         g6, width = 14, height = 7, dpi = 300)
  cat("  ✓ graphique_esteem_vs_ario.png\n\n")
  
} else {
  cat("  ⚠ Résultats ARIO absents — graphique de comparaison ignoré\n\n")
}

################################################################################
# GRAPHIQUE 7 — PROFIL DES CHOCS DE TRANSPORT PAR SECTEUR
#
# Ce graphique synthétise les deux canaux de transmission du choc de transport
# vers l'économie : le choc de demande (axe horizontal) et le choc de coûts
# (axe vertical). La taille des points représente l'impact combiné.
# Il permet d'identifier les secteurs selon leur exposition :
#   - Zone haut-droite : doublement exposés (demande ET coûts)
#   - Zone bas-droite  : principalement touchés par la demande
#   - Zone haut-gauche : principalement touchés par les coûts
################################################################################

cat("── Graphique 7 : Profil des chocs de transport ─────────────────────────\n")

df_chocs_plot <- chocs_df %>%
  left_join(perte_par_secteur %>%
              select(Secteur, Perte_prod_pct, Hausse_prix_pct),
            by = "Secteur") %>%
  mutate(
    Type = case_when(
      Choc_demande > median(Choc_demande) & Choc_cout > median(Choc_cout) ~ "Doublement exposé",
      Choc_demande > median(Choc_demande)  ~ "Principalement demande",
      Choc_cout    > median(Choc_cout)     ~ "Principalement coûts",
      TRUE ~ "Peu exposé"
    ),
    Type = factor(Type, levels = c("Doublement exposé", "Principalement demande",
                                   "Principalement coûts", "Peu exposé"))
  )

g7 <- ggplot(df_chocs_plot,
             aes(x = Choc_demande, y = Choc_cout,
                 size = Impact_combine, color = Secteur, label = Secteur)) +
  
  # Quadrant de référence
  geom_hline(yintercept = median(df_chocs_plot$Choc_cout),
             color = "#CCCCCC", linetype = "dashed") +
  geom_vline(xintercept = median(df_chocs_plot$Choc_demande),
             color = "#CCCCCC", linetype = "dashed") +
  
  annotate("text", x = max(df_chocs_plot$Choc_demande) * 0.5,
           y = max(df_chocs_plot$Choc_cout),
           label = "Doublement\nexposé",
           size = 3.5, color = "#888888", hjust = 0) +
  
  geom_point(alpha = 0.8) +
  ggrepel::geom_text_repel(
    size = 3.5, max.overlaps = 15,
    segment.color = "#AAAAAA"
  ) +
  
  scale_color_manual(values = PALETTE_SECTEUR, guide = "none") +
  scale_size_continuous(range = c(4, 12), name = "Impact combiné (%)") +
  
  scale_x_continuous(labels = scales::label_number(suffix = "%")) +
  scale_y_continuous(labels = scales::label_number(suffix = "%")) +
  
  labs(
    title    = "Profil des chocs de transport par secteur",
    subtitle = paste0(
      "Axe X = choc de demande (δy_s) | Axe Y = surcoût de transport (δc_s)\n",
      "Taille = impact combiné | Scénario : ", NOM_SCENARIO
    ),
    x       = "Choc de demande δy_s (%)",
    y       = "Surcoût de transport δc_s (%)",
    caption = "Agrégation spatiale pondérée par les flux de fret inter-zones"
  ) +
  theme_esteem

ggsave(file.path(DIR_CARTES, "graphique_esteem_chocs.png"),
       g7, width = 12, height = 8, dpi = 300)
cat("  ✓ graphique_esteem_chocs.png\n\n")

################################################################################
# GRAPHIQUE 8 — HEATMAP DE SENSIBILITÉ AUX PARAMÈTRES COMPORTEMENTAUX
#
# Ce graphique reproduit l'analyse des figures A2.1 et A2.2 de Magacho &
# Spinola (2025) pour le contexte Rwanda : il montre comment les pertes de
# production varient selon β_y et β_p pour le scénario de choc actuel.
# Il aide à identifier si les résultats sont robustes aux choix de paramètres.
#
# PRÉREQUIS : ce graphique nécessite que state0 et esteem_ode aient été
# sauvegardés dans persist_esteem.rds (06_esteem.R version corrigée).
# Si ces objets sont absents, le graphique est ignoré gracieusement.
################################################################################

cat("── Graphique 8 : Heatmap de sensibilité ────────────────────────────────\n")

if (!sensibilite_possible) {
  cat("  ↷ Graphique 8 ignoré (state0 ou esteem_ode absent du .rds)\n")
  cat("  → Relancer 06_esteem.R pour activer cette analyse\n\n")
} else {
  
  # Grille de paramètres à tester (version réduite pour temps de calcul raisonnable)
  # β_y : 4 valeurs (lent → rapide)
  # β_p : 4 valeurs (rigide → flexible)
  beta_y_grid <- c(0.5, 1.0, 3.0, 12.0)   # /an
  beta_p_grid <- c(0.5, 1.0, 3.0, 12.0)   # /an
  
  # Temps réduit de simulation pour l'analyse de sensibilité (1 an)
  times_sens <- seq(0, 1.0, by = ESTEEM_DT_ANS)
  
  cat("  Simulation de la grille de sensibilité (",
      length(beta_y_grid) * length(beta_p_grid), "scénarios)...\n")
  
  # Tableau de résultats pour la heatmap
  sensib_df <- expand.grid(
    beta_y = beta_y_grid,
    beta_p = beta_p_grid
  ) %>%
    mutate(
      Perte_max_pct      = NA_real_,
      Inflation_fin_pct  = NA_real_,
      Land_max_pct       = NA_real_
    )
  
  for (k in seq_len(nrow(sensib_df))) {
    
    params_k <- params_choc
    params_k$beta_y <- sensib_df$beta_y[k]
    params_k$beta_p <- sensib_df$beta_p[k]
    
    params_ref_k        <- params_base
    params_ref_k$beta_y <- sensib_df$beta_y[k]
    params_ref_k$beta_p <- sensib_df$beta_p[k]
    
    tryCatch({
      sim_k_choc <- ode(y = state0, times = times_sens, func = esteem_ode,
                        parms = params_k,     method = "lsoda")
      sim_k_ref  <- ode(y = state0, times = times_sens, func = esteem_ode,
                        parms = params_ref_k, method = "lsoda")
      
      x_tot_choc <- rowSums(sim_k_choc[, 2:(N_SECTEURS + 1)])
      x_tot_ref  <- rowSums(sim_k_ref[,  2:(N_SECTEURS + 1)])
      perte_k    <- (x_tot_ref - x_tot_choc) / x_tot_ref * 100
      
      # CPI fin choc vs fin ref
      pc_k_choc <- as.numeric(t(y0) %*% pmax(sim_k_choc[nrow(sim_k_choc),
                                                        (2 * N_SECTEURS + 2):(3 * N_SECTEURS + 1)], 0.01)) /
        as.numeric(t(y0) %*% rep(1, N_SECTEURS))
      
      pc_k_ref  <- as.numeric(t(y0) %*% pmax(sim_k_ref[nrow(sim_k_ref),
                                                       (2 * N_SECTEURS + 2):(3 * N_SECTEURS + 1)], 0.01)) /
        as.numeric(t(y0) %*% rep(1, N_SECTEURS))
      
      sensib_df$Perte_max_pct[k]     <- max(perte_k, na.rm = TRUE)
      sensib_df$Inflation_fin_pct[k] <- (pc_k_choc / pc_k_ref - 1) * 100
      sensib_df$Land_max_pct[k]      <- max(
        rowSums(t(t(pmax(sim_k_choc[, 2:(N_SECTEURS + 1)], 0)) * q_l)) /
          q_l_s * 100 - 100,
        0, na.rm = TRUE
      )
      
    }, error = function(e) {
      # Si une simulation diverge, on laisse NA
      NULL
    })
    
    if (k %% 4 == 0) cat("    ", k, "/", nrow(sensib_df), "scénarios simulés\n")
  }
  
  # Formatage des labels des axes
  beta_y_labels <- paste0("β_y = ", beta_y_grid, "/an")
  beta_p_labels <- paste0("β_p = ", beta_p_grid, "/an")
  
  sensib_df <- sensib_df %>%
    mutate(
      beta_y_label = factor(paste0("β_y = ", beta_y, "/an"), levels = beta_y_labels),
      beta_p_label = factor(paste0("β_p = ", beta_p, "/an"), levels = beta_p_labels)
    )
  
  # Panneau A : perte de production max
  g8a <- ggplot(sensib_df %>% filter(!is.na(Perte_max_pct)),
                aes(x = beta_y_label, y = beta_p_label, fill = Perte_max_pct)) +
    geom_tile(color = "white", linewidth = 0.5) +
    geom_text(aes(label = round(Perte_max_pct, 2)), size = 4, color = "white") +
    scale_fill_gradientn(
      colors = c("#2CA02C", "#FFDD00", "#FF7F00", "#D62728"),
      name   = "Perte max (%)"
    ) +
    labs(
      title = "Perte de prod. max. (%)",
      x = "Vitesse d'ajustement quantités", y = "Vitesse d'ajustement prix"
    ) +
    theme_esteem +
    theme(axis.text.x = element_text(angle = 30, hjust = 1))
  
  # Panneau B : inflation finale
  g8b <- ggplot(sensib_df %>% filter(!is.na(Inflation_fin_pct)),
                aes(x = beta_y_label, y = beta_p_label, fill = Inflation_fin_pct)) +
    geom_tile(color = "white", linewidth = 0.5) +
    geom_text(aes(label = round(Inflation_fin_pct, 3)), size = 4, color = "white") +
    scale_fill_gradientn(
      colors = c("#1F77B4", "#98C8E8", "#FFD700", "#FF4400"),
      name   = "Inflation fin (pts %)"
    ) +
    labs(
      title = "Surcoût inflationniste (pts %)",
      x = "Vitesse d'ajustement quantités", y = NULL
    ) +
    theme_esteem +
    theme(axis.text.x = element_text(angle = 30, hjust = 1),
          axis.text.y = element_blank())
  
  if (requireNamespace("patchwork", quietly = TRUE)) {
    g8_final <- g8a | g8b +
      plot_annotation(
        title   = paste0("Sensibilité aux paramètres comportementaux — ", NOM_SCENARIO),
        subtitle = paste0(
          "Chaque cellule = 1 simulation ESTEEM complète | ",
          "Croix rouge = paramètres de référence (β_y = ", ESTEEM_BETA_Y,
          ", β_p = ", ESTEEM_BETA_P, ")"
        ),
        caption = paste0("Grille ", length(beta_y_grid), "×", length(beta_p_grid),
                         " | Horizon 1 an | Rwanda")
      )
    ggsave(file.path(DIR_CARTES, "graphique_esteem_sensibilite.png"),
           g8_final, width = 16, height = 7, dpi = 300)
  } else {
    ggsave(file.path(DIR_CARTES, "graphique_esteem_sensibilite.png"),
           g8a, width = 10, height = 6, dpi = 300)
  }
  cat("  ✓ graphique_esteem_sensibilite.png\n\n")
  
}   # fin du if (sensibilite_possible) — graphique 8

################################################################################
# BILAN FINAL DES VISUALISATIONS
################################################################################

cat("==========================================================\n")
cat("  BILAN DES VISUALISATIONS ESTEEM\n")
cat("==========================================================\n\n")

cat("Scénario         :", NOM_SCENARIO, "\n\n")

cat("Fichiers produits dans", DIR_CARTES, ":\n")
fichiers_esteem <- c(
  "graphique_esteem_production.png",
  "graphique_esteem_secteurs.png",
  "graphique_esteem_prix.png",
  "graphique_esteem_inventaires.png",
  "graphique_esteem_biophysique.png",
  if (ario_dispo)          "graphique_esteem_vs_ario.png",
  "graphique_esteem_chocs.png",
  if (sensibilite_possible) "graphique_esteem_sensibilite.png"
)
for (f in fichiers_esteem) {
  chemin_f <- file.path(DIR_CARTES, f)
  statut   <- if (file.exists(chemin_f)) "✓" else "✗ (non généré)"
  taille   <- if (file.exists(chemin_f))
    paste0("(", round(file.size(chemin_f) / 1024), " Ko)") else ""
  cat("  ", statut, f, taille, "\n")
}

cat("\nIndicateurs ESTEEM résumés :\n")
cat("  Perte max. production      :", round(perte_max, 2), "% (jour", jour_perte_max, ")\n")
cat("  Surcoût inflationniste     :", round(inflation_surcomp, 3), "pts de %\n")
cat("  Secteur le plus affecté    :", perte_par_secteur$Secteur[1], "\n")
cat("  Canal dominant             :",
    if (max(delta_demande) > max(delta_cout)) "demande (δy > δc)"
    else "coûts (δc > δy)", "\n")
cat("  Contrainte foncière        :",
    if (contrainte_active) "ACTIVE ⚠" else "non active", "\n\n")