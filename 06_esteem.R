################################################################################
# 06_esteem.R
# RÔLE : Modèle ESTEEM (Extended Structural Traverse Econometric and
#        Environmental Model) couplé au modèle de transport de fret Rwanda.
#
# FONDEMENT THÉORIQUE :
#   Basé sur Magacho & Spinola (2025) — "Dynamic Adjustments in Environmental
#   Input-Output Models: Incorporating Quantity and Price Traverse
#   Disequilibrium". Ce modèle étend le cadre input-output de Leontief en
#   introduisant des ajustements dynamiques HORS-ÉQUILIBRE sur les prix ET
#   les quantités simultanément, au lieu de traiter l'un ou l'autre en isolation.
#
# ── PRINCIPE DU COUPLAGE TRANSPORT → ESTEEM ────────────────────────────────────
#   Le choc d'inondation (Partie IX) perturbe le réseau routier et réduit les
#   flux de fret entre zones économiques. ESTEEM traduit ces perturbations en
#   deux chocs sectoriels qui alimentent la dynamique macroéconomique :
#
#   Canal 1 — DEMANDE : Les routes coupées empêchent certains produits d'atteindre
#     leurs destinations. Pour chaque secteur s, la fraction de flux perturbée
#     constitue un choc de demande finale négatif δy_s :
#
#     δy_s = Σᵢⱼ [fraction_perdue(i,j) × flux_gravitaire_s(i,j)]
#            ─────────────────────────────────────────────────────
#            Σᵢⱼ [flux_gravitaire_s(i,j)]
#
#   Canal 2 — COÛTS : Les détours imposés par les routes coupées alourdissent
#     les coûts logistiques de chaque secteur. Ce surcoût δc_s s'intègre dans
#     le calcul des prix via la règle de markup endogène du modèle :
#
#     δc_s = Σᵢⱼ [surcout_relatif(i,j) × flux_gravitaire_s(i,j)]
#            ────────────────────────────────────────────────────────
#            Σᵢⱼ [flux_gravitaire_s(i,j)]
#
# ── DIFFÉRENCES CLÉS AVEC LE MODÈLE ARIO (05_ario.R) ─────────────────────────
#   ARIO  : choc de CAPACITÉ productive → rationnement des intrants
#           → perte de production directe. Pas de dynamique des prix.
#
#   ESTEEM : choc de DEMANDE + COÛTS → ajustement graduel simultané
#           des quantités ET des prix, gouverné par une rationalité limitée :
#           • Inventaires comme tampon entre offre et demande
#           • Anticipations adaptatives (β_y : vitesse de révision des attentes)
#           • Prix collants (β_p : délai de transmission des coûts aux prix)
#           • Markup endogène : varie avec les niveaux de stocks
#           • Contrainte biophysique : terres agricoles pouvant devenir limitantes
#           • Demande finale sensible aux prix relatifs (élasticité η)
#
# ── RÉSUMÉ DES ÉQUATIONS (Magacho & Spinola, 2025) ────────────────────────────
#   (6)  Demande totale   : x_D(t) = A·x(t) + y(t)
#   (7)  Production       : x(t) = min(x_e(t)+v_d−v(t), contrainte_foncière)
#   (8)  Inventaires      : dv/dt = x(t) − x_D(t)
#   (9)  Ventes anticipées: dx_e/dt = β_y·(x_D(t) − x_e(t))
#   (10) Demande finale   : y(t) = y_0·(p(t)/p_c(t))^η · (1 − δy·intensity(t))
#   (11) Prix désiré      : p_d(t) = (1+μ(t))·(w + A'·p(t) + q_l·p_l(t) + δc·intensity(t))
#   (12) Prix             : dp/dt = β_p·(p_d(t) − p(t))
#   (13) Prix terres      : dp_l/dt = β_p·(p_l,d(t) − p_l(t))
#   (14) Markup           : μ(t) = μ_0 + μ_1·(v_d/v(t) − 1)
#   (15) CPI              : p_c(t) = (y_0'·p(t)) / (y_0'·ι)
#   (16) Prix terres désiré: p_l,d(t) = λ_0·(q_l'·x_aspiré / q_l,s)^λ_1
#
# ENTRÉES  : persist_vulnerabilite.rds, persist_flux_fret.rds,
#            persist_entreposages.rds + DuckDB + 00_parametres.R
# SORTIES  : persist_esteem.rds + exports CSV (outputs/exports/)
# DÉPEND DE : 00_parametres.R → 01_reseau.R → 03_transport.R → 04_vulnerabilite.R
#
# RÉFÉRENCES :
#   Magacho, G. & Spinola, D. (2025). Dynamic Adjustments in Environmental
#     Input-Output Models. French Development Agency Working Paper.
#   Metzler, L.A. (1941). The nature and stability of inventory cycles.
#     Review of Economics and Statistics, 23(3), 113-129.
#   Leontief, W. (1941). The structure of the American economy. Harvard U. Press.
#   Klenow, P.J. & Malin, B.A. (2010). Microeconomic evidence on price-setting.
#     Handbook of Monetary Economics, 3, 231-284.
################################################################################

source("00_parametres.R")

# ── Chemin de persistance ESTEEM ──────────────────────────────────────────────
# À ajouter dans 00_parametres.R pour une utilisation globale :
#   PERSIST_ESTEEM <- file.path(DIR_PERSIST, "persist_esteem.rds")
PERSIST_ESTEEM <- file.path(DIR_PERSIST, "persist_esteem.rds")

# ── Solveur d'équations différentielles ordinaires ────────────────────────────
# deSolve est le package R standard pour la résolution numérique d'EDO.
# LSODA (Livermore Solver for Ordinary Differential Equations with Automatic
# method switching) bascule automatiquement entre méthodes "stiff" et
# "non-stiff", ce qui le rend robuste pour les systèmes économiques hétérogènes.
if (!requireNamespace("deSolve", quietly = TRUE)) {
  install.packages("deSolve", dependencies = TRUE)
}
library(deSolve)

cat("=== Chargement des objets pour 06_esteem.R ===\n")

# Chargement des résultats de vulnérabilité (Partie IX)
.vuln <- readRDS(PERSIST_VULNERAB)
od_compare           <- .vuln$od_compare
fraction_perdue_zone <- .vuln$fraction_perdue_zone

# Chargement des flux de fret par secteur (Partie VII)
.fret <- readRDS(PERSIST_FLUX_FRET)
flux_gravitaire <- .fret$flux_gravitaire   # liste de 8 matrices [n_zones × n_zones]

# Chargement des métadonnées des zones d'entrepôt
.ent <- readRDS(PERSIST_ENTREPOSAGES)
noeuds_entreposage <- .ent$noeuds_entreposage
n_warehouses       <- .ent$n_warehouses

rm(.vuln, .fret, .ent)

# ── Grandeurs macro-économiques dérivées de 00_parametres.R ───────────────────
# Lorsqu'on source 00_parametres.R, les vecteurs suivants sont automatiquement
# calculés (voir la section "Paramètres du modèle économique") :
#   conso_interm   = A %*% production_totale
#   valeur_ajoutee = production_totale - conso_interm
#   demande_finale = valeur_ajoutee * PART_DEMANDE_FINALE
#
# On calcule ici la production d'équilibre de Leontief : x* = (I-A)^{-1} y_f
L           <- solve(diag(N_SECTEURS) - A)   # Inverse de Leontief
x_equilibre <- as.vector(L %*% demande_finale)
names(x_equilibre) <- SECTEURS

cat("✓ Objets chargés — production d'équilibre calculée\n\n")

################################################################################
# SECTION I — PARAMÈTRES COMPORTEMENTAUX ESTEEM
#
# Ces paramètres gouvernent les vitesses d'ajustement et les sensibilités
# comportementales du modèle. Ils peuvent être déplacés dans 00_parametres.R.
# Les valeurs proposées sont cohérentes avec la littérature empirique et
# avec les paramètres ARIO déjà utilisés dans 05_ario.R.
################################################################################

cat("==========================================================\n")
cat("  ESTEEM — PARAMÈTRES COMPORTEMENTAUX\n")
cat("==========================================================\n\n")

# ── Vitesse d'ajustement des quantités β_y ────────────────────────────────────
# β_y mesure la rapidité avec laquelle les firmes révisent leurs anticipations
# de ventes en fonction de la demande observée (rationalité limitée adaptative).
# β_y = 3/an → temps caractéristique τ = 1/β_y ≈ 4 mois (Iyetomi et al., 2011)
# β_y → ∞  : modèle de Leontief statique (équilibre instantané, cas extrême)
# β_y = 0.5 : ajustement très lent (production quasi-rigide sur 2 ans)
ESTEEM_BETA_Y <- 3.0

# ── Vitesse d'ajustement des prix β_p ─────────────────────────────────────────
# β_p capture la "stickiness" (rigidité) des prix. Les firmes ne répercutent
# pas immédiatement les hausses de coûts sur leurs tarifs (coûts de menu,
# contrats, concurrence).
# β_p = 1/an → temps caractéristique τ ≈ 12 mois (Klenow & Malin, 2010)
# β_p = 0   : prix parfaitement rigides (modèle de Leontief en quantités pures)
# β_p → ∞  : transmission instantanée (modèle de Leontief en prix purs)
ESTEEM_BETA_P <- 1.0

# ── Sensibilité du markup aux stocks μ₁ ──────────────────────────────────────
# μ₁ quantifie l'ampleur avec laquelle les firmes modifient leurs marges en
# réponse aux déviations des inventaires par rapport au niveau désiré :
# - Sous-stock (v < v_d) → pouvoir de marché accru → μ monte
# - Sur-stock  (v > v_d) → pression concurrentielle → μ baisse
# Calibré à 0.65 d'après Magacho & Spinola (2025, Section 4).
ESTEEM_MU_1 <- 0.65

# ── Élasticité-prix de la demande finale η ────────────────────────────────────
# η < 0 : une hausse des prix réduit la demande (cas normal).
# η = -1 : élasticité unitaire (réduction de 1% pour chaque 1% de hausse de prix).
# Pour Rwanda, on utilise la valeur de référence du papier : η = -1.
ESTEEM_ETA <- -1.0

# ── Paramètres du marché foncier (ressource biophysique) ──────────────────────
# λ₀ : prix des terres à l'équilibre (normalisé à 1 par convention)
# λ₁ : élasticité du prix à la pression sur la ressource
#   λ₁ = 1 → doublement de la demande foncière → doublement du prix de rente
#   Logique ricardienne : c'est l'exploitant marginal qui détermine le prix.
ESTEEM_LAMBDA_0 <- 1.0
ESTEEM_LAMBDA_1 <- 1.0

# ── Horizon et pas de temps ───────────────────────────────────────────────────
# On simule sur 2× la durée de la perturbation (phase de rétablissement incluse)
# avec un plancher de 180 jours et un plafond de 3 ans.
ESTEEM_HORIZON_ANS <- min(max(DUREE_JOURS * 2 / 365, 0.5), 3.0)
ESTEEM_DT_ANS      <- 1 / 365      # Pas journalier exprimé en années
ESTEEM_DUREE_ANS   <- DUREE_JOURS / 365   # Durée du choc en années
ESTEEM_TAU_RECUP   <- (DUREE_JOURS / 2) / 365  # Temps de récupération (même loi qu'ARIO)

# Phase d'entrée progressive (évite les discontinuités dans le solveur EDO)
ESTEEM_PHASE_IN_JOURS <- min(7, DUREE_JOURS)

cat("  β_y (vitesse quantités)  :", ESTEEM_BETA_Y,
    "/an → τ ≈", round(365 / ESTEEM_BETA_Y), "jours\n")
cat("  β_p (vitesse prix)       :", ESTEEM_BETA_P,
    "/an → τ ≈", round(365 / ESTEEM_BETA_P), "jours\n")
cat("  μ₁  (sensibilité markup) :", ESTEEM_MU_1, "\n")
cat("  η   (élasticité demande) :", ESTEEM_ETA, "\n")
cat("  λ₀  (prix terres équil.) :", ESTEEM_LAMBDA_0, "\n")
cat("  λ₁  (élasticité foncière):", ESTEEM_LAMBDA_1, "\n")
cat("  Horizon simulation       :", round(ESTEEM_HORIZON_ANS * 365), "jours (",
    round(ESTEEM_HORIZON_ANS, 2), "ans)\n")
cat("  Durée du choc            :", DUREE_JOURS, "jours\n\n")

################################################################################
# SECTION II — CALIBRATION SUR LES DONNÉES RWANDA
#
# Le modèle requiert plusieurs paramètres structurels calibrés sur l'économie
# rwandaise. En l'absence de données sectorielles granulaires, on utilise des
# estimations cohérentes avec les structures Africaines Sub-Sahariennes et les
# paramètres déjà définis dans 00_parametres.R.
#
# MÉTHODE DE CALIBRATION DES COÛTS SALARIAUX w :
#   À l'équilibre de prix normalisé (p = 1, p_l = 1), l'équation de prix (11)
#   donne : 1 = (1 + μ_0) × (w_j + Σᵢ a_ij + q_l,j)
#   On déduit : w_j = 1/(1+μ_0) − Σᵢ a_ij − q_l,j
#   Cette procédure assure la cohérence interne du modèle à l'équilibre initial.
################################################################################

cat("── Section II : Calibration sur les données Rwanda ────────────────────\n\n")

# ── Markup initial par secteur (μ₀) ──────────────────────────────────────────
# Part des profits dans la valeur ajoutée, estimée par secteur pour le Rwanda.
# Sources : Banque Mondiale (2022), NISR, estimation sur structure Afrique Sub-Saharienne.
# Agriculture     : faibles marges (petits producteurs, prix volatils, concurrence)
# Mines           : marges élevées (oligopole ; coltan, cassitérite représentent ~97%
#                   des exportations de minerais rwandaises)
# Services/Commerce: marges intermédiaires à élevées (moins de concurrence)
markup_initial <- c(
  Agriculture    = 0.20,
  Mines          = 0.45,
  Agro_industrie = 0.28,
  Industrie      = 0.32,
  Construction   = 0.25,
  Commerce       = 0.35,
  Transport      = 0.30,
  Services       = 0.38
)
names(markup_initial) <- SECTEURS

# ── Besoins en terres agricoles par unité de production (q_l) ─────────────────
# Agriculture et Agro-industrie (plantations de thé, café) sont les principaux
# utilisateurs de terres. Les autres secteurs ont des besoins marginaux.
# La valeur 1.0 pour Agriculture est une référence normalisée.
q_l <- c(
  Agriculture    = 1.00,   # Cultures vivrières, thé, café, pyrèthre
  Mines          = 0.02,   # Exploitation minière (usage de surface résiduel)
  Agro_industrie = 0.08,   # Plantations industrielles pour transformation
  Industrie      = 0.01,   # Emprises industrielles très faibles
  Construction   = 0.05,   # Chantiers temporaires (retour à état naturel)
  Commerce       = 0.00,   # Service pur, pas de terres agricoles
  Transport      = 0.00,   # Infrastructure non agricole
  Services       = 0.00    # Services purs
)
names(q_l) <- SECTEURS

# ── Disponibilité des terres agricoles (q_l,s) ────────────────────────────────
# Fixée à 130% de l'utilisation d'équilibre pour laisser 30% de marge avant
# que la contrainte ne devienne active. Cela reflète les terres encore cultivables
# au Rwanda (potentiel d'extension estimé à 20-35% par MINAGRI 2022).
q_l_equilibre <- as.numeric(t(q_l) %*% x_equilibre)
q_l_s         <- 1.3 * q_l_equilibre

cat("  Utilisation terres à l'équilibre  :", round(q_l_equilibre, 3), "(norm.)\n")
cat("  Disponibilité totale q_l,s (130%) :", round(q_l_s, 3), "\n")
cat("  Marge biophysique disponible      :",
    round((q_l_s / q_l_equilibre - 1) * 100, 0), "%\n\n")

# ── Vecteur de coûts salariaux (w) ────────────────────────────────────────────
# Déduit de la condition de cohérence à l'équilibre de prix (voir ci-dessus).
# col_sums_A[j] = Σᵢ a_ij = coûts intermédiaires par unité de production du secteur j
col_sums_A <- colSums(A)
w_calibre  <- (1 / (1 + markup_initial)) - col_sums_A - q_l
w_calibre  <- pmax(w_calibre, 0.05)   # Plancher de 5% (toujours du travail)
names(w_calibre) <- SECTEURS

cat("  Calibration des coûts salariaux w :\n")
cat("  ", formatC("Secteur",        width = 15), "|",
    formatC("w (salaires)",   width = 14), "|",
    formatC("μ₀ (markup)",    width = 12), "|",
    formatC("Σa_ij (intrants)",width = 16), "\n")
cat("  ", paste(rep("-", 62), collapse = ""), "\n")
for (s in SECTEURS) {
  cat("  ", formatC(s,                    width = 15), "|",
      formatC(round(w_calibre[s], 3), width = 14), "|",
      formatC(round(markup_initial[s], 2), width = 12), "|",
      formatC(round(col_sums_A[s], 3), width = 16), "\n")
}
cat("\n")

# ── Inventaires cibles par secteur (v_d) ──────────────────────────────────────
# On réutilise ARIO_INV_DUREE_JOURS (défini dans 00_parametres.R) pour garantir
# la cohérence entre ESTEEM et ARIO sur la représentation des stocks sectoriels.
# v_d[s] = (durée_stock_s / 365) × production_équilibre_s   [M USD]
v_d_calibre <- (ARIO_INV_DUREE_JOURS / 365) * x_equilibre
names(v_d_calibre) <- SECTEURS

cat("  Inventaires cibles v_d (jours de production) :\n")
for (s in SECTEURS) {
  cat("    ", formatC(s, width = 14, flag = "-"), ":",
      ARIO_INV_DUREE_JOURS[s], "j →",
      round(v_d_calibre[s], 2), "M USD/an\n")
}
cat("\n")

################################################################################
# SECTION III — TRADUCTION DES CHOCS DE TRANSPORT EN CHOCS SECTORIELS
#
# Le choc d'inondation produit deux types de perturbations spatiales :
#   1. Des flux interrompus (fraction_perdue_zone) → choc de demande
#   2. Des surcoûts de transport (od_compare$surcout_relatif_pct) → choc de coûts
#
# On agrège ces perturbations spatiales (n_warehouses × n_warehouses) vers
# l'espace sectoriel (N_SECTEURS) en pondérant par les flux de fret.
################################################################################

cat("── Section III : Chocs de transport → chocs sectoriels ────────────────\n\n")

# ── Canal 1 : choc de demande sectoriel (δy_s) ────────────────────────────────
# Pour chaque secteur s, la fraction de ses flux interzonaux perturbés constitue
# une réduction équivalente de sa demande finale effective.
# Raisonnement : si 15% des flux d'Agriculture sont bloqués, la demande effective
# pour ce secteur diminue de 15% pendant la durée du choc.
delta_demande <- sapply(SECTEURS, function(s) {
  flux_s <- flux_gravitaire[[s]]
  total  <- sum(flux_s, na.rm = TRUE)
  if (total < 1e-10) return(0)
  sum(fraction_perdue_zone * flux_s, na.rm = TRUE) / total
})
names(delta_demande) <- SECTEURS

# ── Canal 2 : choc de coûts sectoriel (δc_s) ─────────────────────────────────
# Pour chaque secteur s, le surcoût moyen de transport pondéré par ses flux.
# Ce surcoût s'ajoute aux coûts logistiques (coûts de transport ≈ coûts de
# distribution, traités comme un composant des coûts fixes sectoriels).

# Reconstruction de la matrice de surcoûts relatifs depuis od_compare
surcout_mat <- matrix(0, nrow = n_warehouses, ncol = n_warehouses)
for (k in seq_len(nrow(od_compare))) {
  i_k <- od_compare$id_origine[k]
  j_k <- od_compare$id_destination[k]
  if (i_k >= 1 && j_k >= 1 &&
      i_k <= n_warehouses && j_k <= n_warehouses) {
    pct <- od_compare$surcout_relatif_pct[k]
    if (!is.na(pct)) {
      surcout_mat[i_k, j_k] <- pct / 100   # Convertir % → fraction décimale
    }
  }
}

delta_cout <- sapply(SECTEURS, function(s) {
  flux_s <- flux_gravitaire[[s]]
  total  <- sum(flux_s, na.rm = TRUE)
  if (total < 1e-10) return(0)
  sum(surcout_mat * flux_s, na.rm = TRUE) / total
})
names(delta_cout) <- SECTEURS

# ── Tableau récapitulatif des chocs sectoriels ─────────────────────────────────
chocs_df <- tibble(
  Secteur         = SECTEURS,
  Choc_demande    = round(delta_demande * 100, 2),   # En %
  Choc_cout       = round(delta_cout    * 100, 2),   # En %
  # Score composite pour identifier les secteurs les plus exposés
  Impact_combine  = round((delta_demande + delta_cout / 2) * 100, 2)
) %>% arrange(desc(Impact_combine))

cat("Chocs sectoriels issus de la perturbation de transport :\n")
cat("  ", formatC("Secteur",           width = 15), "|",
    formatC("Choc demande (%)", width = 17), "|",
    formatC("Surcoût coûts (%)", width = 17), "|",
    formatC("Score combiné (%)", width = 17), "\n")
cat("  ", paste(rep("-", 70), collapse = ""), "\n")
for (i in seq_len(nrow(chocs_df))) {
  cat("  ", formatC(chocs_df$Secteur[i],       width = 15), "|",
      formatC(chocs_df$Choc_demande[i],  width = 17), "|",
      formatC(chocs_df$Choc_cout[i],     width = 17), "|",
      formatC(chocs_df$Impact_combine[i],width = 17), "\n")
}
cat("\n  Secteur le plus affecté (demande) :", SECTEURS[which.max(delta_demande)],
    "(", round(max(delta_demande) * 100, 1), "%)\n")
cat("  Secteur le plus affecté (coûts)   :", SECTEURS[which.max(delta_cout)],
    "(", round(max(delta_cout) * 100, 1), "%)\n\n")

################################################################################
# SECTION IV — CONDITIONS INITIALES
#
# On initialise le modèle à l'état d'équilibre dynamique :
#   - Production = solution de Leontief : x* = (I-A)^{-1}·y_f
#   - Inventaires = inventaires désirés : v(0) = v_d (pas d'excès ni de manque)
#   - Ventes anticipées = production équilibre : x_e(0) = x*
#   - Prix sectoriels = 1 (tous normalisés)
#   - Prix des terres = 1 (normalisé)
# À t=0, aucune variable ne dévie de sa valeur d'équilibre → dérivées = 0.
################################################################################

cat("── Section IV : Conditions initiales ──────────────────────────────────\n\n")

y0    <- demande_finale                      # Demande finale de référence [M USD/an]
names(y0) <- SECTEURS

v0    <- v_d_calibre                         # Inventaires initiaux = cibles
xe0   <- x_equilibre                         # Ventes anticipées = production équilibre
p0    <- setNames(rep(1.0, N_SECTEURS), SECTEURS)  # Prix normalisés à 1
pl0   <- 1.0                                 # Prix des terres normalisé à 1

# Vecteur d'état initial : ordre [v | xe | p | pl]
# Dimension totale : 3×N + 1 = 3×8 + 1 = 25 pour Rwanda
state0 <- c(v0, xe0, p0, pl0)
names(state0) <- c(
  paste0("v_",  SECTEURS),
  paste0("xe_", SECTEURS),
  paste0("p_",  SECTEURS),
  "pl"
)

cat("  Vecteur d'état initial — dimension :", length(state0), "\n")
cat("  Production d'équilibre par secteur (M USD/an) :\n")
for (s in SECTEURS) {
  cat("    ", formatC(s, width = 14, flag = "-"), ":", round(x_equilibre[s]), "M USD/an",
      "| v_d =", round(v_d_calibre[s], 1), "M USD\n")
}
cat("\n  Vérification cohérence à t=0 :")
# À t=0, la dérivée du vecteur d'état devrait être nulle si on est à l'équilibre
# (sans choc). Vérification rapide sur les inventaires et les ventes anticipées :
xD_init <- as.vector(A %*% x_equilibre) + y0
cat(" x_D - x =", round(sum(abs(xD_init - x_equilibre)), 4),
    "(doit être ≈ 0 pour un équilibre parfait)\n\n")

################################################################################
# SECTION V — DÉFINITION DU SYSTÈME D'ÉQUATIONS DIFFÉRENTIELLES ORDINAIRES
#
# La fonction esteem_ode() implémente les équations (6)–(16) de Magacho &
# Spinola (2025) dans le format requis par deSolve::ode().
#
# Convention d'état : state = [v₁..vₙ | xe₁..xeₙ | p₁..pₙ | p_l]
#   N = N_SECTEURS = 8 pour Rwanda
#
# UNITÉS DE TEMPS : ANNÉES
#   Les paramètres β_y, β_p, λ sont exprimés par an.
#   Le pas de temps dt = 1/365 an (résolution journalière).
#
# GESTION DES DISCONTINUITÉS :
#   La contrainte biophysique (min dans Eq. 7) est implémentée via une formule
#   "douce" pour éviter les discontinuités qui pourraient déstabiliser le solveur.
################################################################################

cat("── Section V : Définition du système EDO ───────────────────────────────\n\n")

esteem_ode <- function(t, state, params) {
  
  # ── Extraction des composantes de l'état ──────────────────────────────────
  N  <- params$N
  v  <- pmax(state[1:N], 0)                    # Inventaires (≥ 0)
  xe <- pmax(state[(N + 1):(2 * N)], 0)        # Ventes anticipées (≥ 0)
  p  <- pmax(state[(2 * N + 1):(3 * N)], 0.01) # Prix sectoriels (> 0)
  pl <- max(state[3 * N + 1], 0.001)           # Prix des terres (> 0)
  
  # ── Intensité temporelle du choc ──────────────────────────────────────────
  # La fonction d'intensité prend 3 phases :
  #   Phase 1 — Phase-in [0, phase_in]    : montée progressive (évite sauts)
  #   Phase 2 — Choc plein [phase_in, d]  : perturbation maximale
  #   Phase 3 — Récupération [d, +∞]      : décroissance exponentielle
  # Cette forme lisse est préférable à un créneau (Heaviside) pour la stabilité
  # numérique du solveur LSODA.
  d        <- params$duree_choc
  phase_in <- params$phase_in
  tau_r    <- params$tau_recup
  
  intensity <- if (t <= 0) {
    0.0
  } else if (t < phase_in) {
    t / phase_in                          # Montée linéaire
  } else if (t <= d) {
    1.0                                   # Choc plein
  } else {
    exp(-(t - d) / tau_r)                 # Décroissance exponentielle
  }
  intensity <- max(0, min(1, intensity))  # Clampé dans [0, 1]
  
  # ── Indice des prix à la consommation (CPI) — Eq. (15) ───────────────────
  # Moyenne pondérée des prix sectoriels, avec la structure de demande initiale
  # comme poids. Mesure la pression globale sur le pouvoir d'achat.
  pc <- as.numeric(t(params$y0) %*% p) /
    as.numeric(t(params$y0) %*% rep(1, N))
  pc <- max(pc, 0.01)   # Protection numérique contre la division par zéro
  
  # ── Demande finale endogène — Eq. (10) ────────────────────────────────────
  # Deux effets simultanés :
  #   (a) Effet-prix : hausse des prix → réduction de demande via η < 0
  #   (b) Choc de transport : routes coupées → demande effective réduite
  y_prix <- params$y0 * (p / pc)^params$eta
  y      <- pmax(0, y_prix * (1 - intensity * params$delta_demande))
  
  # ── Production aspirée (avant contrainte biophysique) — Eq. (7) ──────────
  # La firme souhaite produire assez pour couvrir les ventes anticipées ET
  # reconstituer les stocks jusqu'au niveau désiré.
  x_aspire <- pmax(0, xe + params$vd - v)
  
  # ── Contrainte biophysique (terres agricoles) ─────────────────────────────
  # Si la demande totale de terres excède la disponibilité (q_l,s), les secteurs
  # utilisant des terres sont contraints. On utilise un facteur de rationnement
  # proportionnel qui "pénalise" davantage les secteurs les plus land-intensifs.
  # Cette approche "douce" évite la non-dérivabilité de la fonction min().
  land_demand <- as.numeric(t(params$ql) %*% x_aspire)
  
  if (land_demand > params$qls && land_demand > 0) {
    # Ratio de rationnement : production maximale permise / production aspirée
    ratio_land <- params$qls / land_demand
    # Les secteurs avec q_l > 0 sont contraints proportionnellement à leur
    # intensité foncière. Les autres (q_l = 0) ne sont pas rationnés.
    facteur_contrainte <- ifelse(
      params$ql > 0,
      ratio_land + (1 - ratio_land) * (1 - params$ql / max(params$ql)),
      1.0
    )
    x <- x_aspire * facteur_contrainte
  } else {
    x <- x_aspire
  }
  x <- pmax(0, x)
  
  # ── Demande totale sectorielle — Eq. (6) ─────────────────────────────────
  # Demande intermédiaire (A×x : inputs requis par la production) +
  # Demande finale (y : consommation des ménages et investissement)
  x_D <- pmax(0, as.vector(params$A %*% x) + y)
  
  # ── Markup endogène — Eq. (14) ────────────────────────────────────────────
  # Quand v < v_d (stocks bas), la firme profite de la rareté pour augmenter
  # son markup. Quand v > v_d (surstocks), elle le baisse pour écouler ses stocks.
  v_safe <- pmax(v, 0.001 * pmax(params$vd, 1e-10))   # Évite 0/0
  mu     <- params$mu0 + params$mu1 * (params$vd / v_safe - 1)
  mu     <- pmax(-0.90, pmin(mu, 3.0))   # Markup borné : [-90%, +300%]
  
  # ── Coûts logistiques de transport — Canal 2 ──────────────────────────────
  # Le surcoût δc_s s'ajoute aux coûts salariaux effectifs comme un surcoût
  # de distribution/logistique (traité comme un coût variable proportionnel à w).
  # Interprétation : les entreprises doivent payer des frais supplémentaires de
  # transport pour faire parvenir leurs intrants et produits malgré les routes
  # coupées (détours, stockage temporaire, reroutage).
  w_effectif <- params$w * (1 + intensity * params$delta_cout)
  
  # ── Prix désiré par coûts — Eq. (11) ─────────────────────────────────────
  # La firme souhaite fixer son prix à (1 + markup) fois ses coûts unitaires.
  # Coûts unitaires = salaires + intrants valorisés aux prix courants + rente foncière
  cout_intermediaire <- as.vector(t(params$A) %*% p)     # Intrants (A'×p)
  cout_foncier       <- params$ql * pl                    # Rente des terres
  cout_total         <- w_effectif + cout_intermediaire + cout_foncier
  
  p_d <- (1 + mu) * cout_total
  p_d <- pmax(0.01, p_d)   # Prix toujours positif
  
  # ── Prix désiré des terres — logique ricardienne — Eq. (16) ──────────────
  # Le prix de rente de la terre est déterminé par la pression sur la ressource :
  # plus la demande se rapproche de la disponibilité, plus la rente monte.
  # C'est l'exploitant sur la "terre marginale" (la moins fertile encore exploitée)
  # qui fixe le prix pour l'ensemble du marché foncier (Ricardo).
  land_aspire <- as.numeric(t(params$ql) %*% pmax(0, xe + params$vd - v))
  if (params$qls > 0 && land_aspire > 0) {
    pression_fonciere <- land_aspire / params$qls
    pl_d <- params$lambda0 * pression_fonciere^params$lambda1
  } else {
    pl_d <- params$lambda0
  }
  pl_d <- max(0.01, pl_d)
  
  # ── Équations différentielles ─────────────────────────────────────────────
  
  # dv/dt — Eq. (8) : variation des stocks
  # Les inventaires s'accumulent quand on produit plus qu'on ne vend,
  # et se déstockent dans le cas inverse.
  dv <- x - x_D
  
  # dx_e/dt — Eq. (9) : révision des anticipations de ventes
  # Les firmes, à rationalité limitée, mettent à jour progressivement leurs
  # anticipations de ventes en direction de la demande réelle observée.
  # β_y = vitesse de cette mise à jour (plus élevé = anticipations plus réactives).
  dxe <- params$beta_y * (x_D - xe)
  
  # dp/dt — Eq. (12) : ajustement graduel des prix ("stickiness")
  # Les prix convergent vers les prix désirés à vitesse β_p. Une valeur faible
  # représente des prix rigides (contrats, coûts de menu, stratégie).
  dp <- params$beta_p * (p_d - p)
  
  # dp_l/dt — Eq. (13) : ajustement du prix des terres
  # La rente foncière s'ajuste vers la valeur ricardienne à la même vitesse β_p.
  dpl <- params$beta_p * (pl_d - pl)
  
  # Retour sous forme de liste — convention deSolve
  list(c(dv, dxe, dp, dpl))
}

cat("  ✓ Système EDO ESTEEM défini\n")
cat("  Dimension du vecteur d'état : 3×", N_SECTEURS, "+ 1 =",
    3 * N_SECTEURS + 1, "équations différentielles\n\n")

################################################################################
# SECTION VI — SIMULATION
#
# On effectue deux simulations :
#   A) Scénario de RÉFÉRENCE : sans choc de transport (δy = δc = 0)
#      → Etablit la trajectoire de l'économie à l'état stationnaire
#   B) Scénario PERTURBÉ     : avec les chocs calculés en Section III
#      → Simule l'impact de l'inondation sur l'économie rwandaise
#
# La méthode LSODA est préférable ici car le système peut exhiber des
# comportements "stiff" (certaines variables s'ajustent très rapidement,
# d'autres très lentement), ce qui rend les méthodes explicites (rk4)
# soit trop lentes soit instables.
################################################################################

cat("── Section VI : Simulation ESTEEM ──────────────────────────────────────\n\n")

# Liste des paramètres pour le scénario de référence (sans choc)
params_base <- list(
  N             = N_SECTEURS,
  A             = A,
  w             = w_calibre,
  mu0           = markup_initial,
  mu1           = ESTEEM_MU_1,
  eta           = ESTEEM_ETA,
  beta_y        = ESTEEM_BETA_Y,
  beta_p        = ESTEEM_BETA_P,
  vd            = v_d_calibre,
  ql            = q_l,
  qls           = q_l_s,
  lambda0       = ESTEEM_LAMBDA_0,
  lambda1       = ESTEEM_LAMBDA_1,
  y0            = y0,
  duree_choc    = ESTEEM_DUREE_ANS,
  tau_recup     = ESTEEM_TAU_RECUP,
  phase_in      = ESTEEM_PHASE_IN_JOURS / 365,
  delta_demande = setNames(rep(0, N_SECTEURS), SECTEURS),   # Pas de choc
  delta_cout    = setNames(rep(0, N_SECTEURS), SECTEURS)    # Pas de choc
)

# Liste des paramètres pour le scénario perturbé (avec chocs de transport)
params_choc <- params_base
params_choc$delta_demande <- delta_demande
params_choc$delta_cout    <- delta_cout

# Séquence temporelle (en années, résolution journalière)
times_ans <- seq(0, ESTEEM_HORIZON_ANS, by = ESTEEM_DT_ANS)

cat("  Simulation A — Référence (sans choc) ...\n")
cat("    Pas de temps :", length(times_ans),
    "(résolution :", round(ESTEEM_DT_ANS * 365, 0), "jours)\n")

# ── Simulation de référence ───────────────────────────────────────────────────
# tryCatch : si LSODA échoue (systèmes très stiff), on bascule vers rk4
# (Runge-Kutta d'ordre 4, moins robuste mais toujours utilisable en secours).
sim_ref <- tryCatch({
  ode(
    y      = state0,
    times  = times_ans,
    func   = esteem_ode,
    parms  = params_base,
    method = "lsoda"
  )
}, error = function(e) {
  cat("    ⚠ LSODA échoué, bascule vers rk4 :", conditionMessage(e), "\n")
  ode(y = state0, times = times_ans, func = esteem_ode,
      parms = params_base, method = "rk4")
})

cat("    ✓ Référence simulée —", nrow(sim_ref), "pas de temps\n\n")

cat("  Simulation B — Perturbé (", NOM_SCENARIO, ") ...\n")

# ── Simulation du scénario perturbé ───────────────────────────────────────────
sim_choc <- tryCatch({
  ode(
    y      = state0,
    times  = times_ans,
    func   = esteem_ode,
    parms  = params_choc,
    method = "lsoda"
  )
}, error = function(e) {
  cat("    ⚠ LSODA échoué, bascule vers rk4 :", conditionMessage(e), "\n")
  ode(y = state0, times = times_ans, func = esteem_ode,
      parms = params_choc, method = "rk4")
})

cat("    ✓ Choc simulé —", nrow(sim_choc), "pas de temps\n\n")

################################################################################
# SECTION VII — RECONSTRUCTION DES VARIABLES DÉRIVÉES
#
# Les variables d'état (v, xe, p, pl) sont directement dans la sortie de deSolve.
# Les variables "algébriques" (x, x_D, y, mu, pc) sont déterminées à chaque pas
# par les équations internes du modèle. On les recalcule ici pour l'analyse.
################################################################################

cat("── Section VII : Reconstruction des variables dérivées ─────────────────\n\n")

# Fonction générique de reconstruction depuis une sortie ode()
reconstruire_variables <- function(sim_out, params_run, nom_sc = "") {
  
  N   <- params_run$N
  n_t <- nrow(sim_out)
  
  # Pré-allocation des matrices de résultats
  res <- list(
    t   = sim_out[, "time"],
    j   = round(sim_out[, "time"] * 365),    # Numéro de jour
    v   = matrix(NA, n_t, N, dimnames = list(NULL, SECTEURS)),  # Inventaires
    xe  = matrix(NA, n_t, N, dimnames = list(NULL, SECTEURS)),  # Ventes anticipées
    p   = matrix(NA, n_t, N, dimnames = list(NULL, SECTEURS)),  # Prix
    x   = matrix(NA, n_t, N, dimnames = list(NULL, SECTEURS)),  # Production
    xD  = matrix(NA, n_t, N, dimnames = list(NULL, SECTEURS)),  # Demande totale
    y   = matrix(NA, n_t, N, dimnames = list(NULL, SECTEURS)),  # Demande finale
    mu  = matrix(NA, n_t, N, dimnames = list(NULL, SECTEURS)),  # Markup
    pc  = numeric(n_t),     # CPI
    pi  = numeric(n_t),     # Taux d'inflation journalier
    pl  = numeric(n_t),     # Prix des terres
    scenario = nom_sc
  )
  
  pc_prev <- 1.0   # CPI initial
  
  for (i in seq_len(n_t)) {
    
    t_i <- sim_out[i, "time"]
    v_i  <- pmax(sim_out[i, 2:(N + 1)], 0)
    xe_i <- pmax(sim_out[i, (N + 2):(2 * N + 1)], 0)
    p_i  <- pmax(sim_out[i, (2 * N + 2):(3 * N + 1)], 0.01)
    pl_i <- max(sim_out[i, 3 * N + 2], 0.001)
    
    # Intensité du choc à ce pas de temps
    d        <- params_run$duree_choc
    phase_in <- params_run$phase_in
    tau_r    <- params_run$tau_recup
    intensity <- if (t_i <= 0) 0
    else if (t_i < phase_in) t_i / phase_in
    else if (t_i <= d) 1.0
    else exp(-(t_i - d) / tau_r)
    intensity <- max(0, min(1, intensity))
    
    # CPI et inflation annualisée
    pc_i <- as.numeric(t(params_run$y0) %*% p_i) /
      as.numeric(t(params_run$y0) %*% rep(1, N))
    pc_i <- max(pc_i, 0.01)
    # Inflation annualisée (%) = variation de CPI × 365 jours
    pi_i <- if (i > 1 && pc_prev > 0) (pc_i - pc_prev) / pc_prev * 365 * 100 else 0
    
    # Demande finale
    y_i <- pmax(0, params_run$y0 * (p_i / pc_i)^params_run$eta *
                  (1 - intensity * params_run$delta_demande))
    
    # Production aspirée
    x_aspire <- pmax(0, xe_i + params_run$vd - v_i)
    
    # Contrainte biophysique
    land_demand <- as.numeric(t(params_run$ql) %*% x_aspire)
    if (land_demand > params_run$qls && land_demand > 0) {
      ratio_land <- params_run$qls / land_demand
      facteur    <- ifelse(params_run$ql > 0,
                           ratio_land + (1 - ratio_land) * (1 - params_run$ql / max(params_run$ql)),
                           1.0)
      x_i <- pmax(0, x_aspire * facteur)
    } else {
      x_i <- x_aspire
    }
    
    # Demande totale et markup
    xD_i   <- pmax(0, as.vector(params_run$A %*% x_i) + y_i)
    v_safe <- pmax(v_i, 0.001 * pmax(params_run$vd, 1e-10))
    mu_i   <- pmax(-0.9, pmin(params_run$mu0 + params_run$mu1 *
                                (params_run$vd / v_safe - 1), 3.0))
    
    # Stockage dans les matrices
    res$v[i, ]  <- v_i
    res$xe[i, ] <- xe_i
    res$p[i, ]  <- p_i
    res$x[i, ]  <- x_i
    res$xD[i, ] <- xD_i
    res$y[i, ]  <- y_i
    res$mu[i, ] <- mu_i
    res$pc[i]   <- pc_i
    res$pi[i]   <- pi_i
    res$pl[i]   <- pl_i
    
    pc_prev <- pc_i
  }
  
  # Agrégats
  res$x_total <- rowSums(res$x)
  res$y_total <- rowSums(res$y)
  res
}

cat("  Post-traitement du scénario de référence...\n")
res_ref  <- reconstruire_variables(sim_ref,  params_base, "reference")
cat("  Post-traitement du scénario perturbé...\n")
res_choc <- reconstruire_variables(sim_choc, params_choc, NOM_SCENARIO)
cat("  ✓ Variables dérivées calculées\n\n")

################################################################################
# SECTION VIII — ANALYSE DES RÉSULTATS
################################################################################

cat("── Section VIII : Analyse des résultats ESTEEM ─────────────────────────\n\n")

# ── Pertes de production ──────────────────────────────────────────────────────
perte_pct     <- (res_ref$x_total - res_choc$x_total) / res_ref$x_total * 100
perte_max     <- max(perte_pct, na.rm = TRUE)
jour_perte_max <- res_choc$j[which.max(perte_pct)]
perte_cum_pctj <- sum(pmax(0, perte_pct) * ESTEEM_DT_ANS, na.rm = TRUE)

cat("Pertes de production agrégées :\n")
cat("  Perte maximale instantanée :", round(perte_max, 2), "% (jour", jour_perte_max, ")\n")
cat("  Perte cumulée              :", round(perte_cum_pctj * 365, 2), "% × jours\n\n")

# ── Impact inflationniste ─────────────────────────────────────────────────────
cpi_ref_fin  <- res_ref$pc[length(res_ref$pc)]
cpi_choc_fin <- res_choc$pc[length(res_choc$pc)]
cpi_peak_choc <- max(res_choc$pc)
inflation_surcomp <- (cpi_choc_fin - cpi_ref_fin) / cpi_ref_fin * 100

cat("Impact inflationniste :\n")
cat("  CPI fin référence     :", round(cpi_ref_fin, 4), "\n")
cat("  CPI fin choc          :", round(cpi_choc_fin, 4), "\n")
cat("  CPI pic choc          :", round(cpi_peak_choc, 4),
    "(jour", res_choc$j[which.max(res_choc$pc)], ")\n")
cat("  Surcoût inflationniste:", round(inflation_surcomp, 3), "points de %\n\n")

# ── Résultats sectoriels ──────────────────────────────────────────────────────
perte_par_secteur <- tibble(
  Secteur               = SECTEURS,
  x_ref_moyen           = round(colMeans(res_ref$x),  1),
  x_choc_moyen          = round(colMeans(res_choc$x), 1),
  Perte_prod_pct        = round((colMeans(res_ref$x) - colMeans(res_choc$x)) /
                                  colMeans(res_ref$x) * 100, 2),
  Perte_prod_pic_pct    = round(apply(res_ref$x - res_choc$x, 2, max) /
                                  colMeans(res_ref$x) * 100, 2),
  Prix_final_ref        = round(res_ref$p[nrow(sim_ref), ],  4),
  Prix_final_choc       = round(res_choc$p[nrow(sim_choc), ], 4),
  Hausse_prix_pct       = round((res_choc$p[nrow(sim_choc), ] /
                                   res_ref$p[nrow(sim_ref), ] - 1) * 100, 2),
  Choc_demande_pct      = round(delta_demande * 100, 1),
  Choc_cout_pct         = round(delta_cout    * 100, 1)
) %>% arrange(desc(Perte_prod_pct))

cat("Résultats sectoriels ESTEEM (classement par perte de production) :\n")
print(
  perte_par_secteur %>%
    select(Secteur, Perte_prod_pct, Hausse_prix_pct, Choc_demande_pct, Choc_cout_pct) %>%
    rename(
      `Perte prod. moy. (%)` = Perte_prod_pct,
      `Hausse prix fin. (%)` = Hausse_prix_pct,
      `Choc demande (%)`     = Choc_demande_pct,
      `Choc coût (%)`        = Choc_cout_pct
    )
)
cat("\n")

# ── Dynamique des terres agricoles ────────────────────────────────────────────
land_ref  <- rowSums(t(t(res_ref$x)  * q_l))
land_choc <- rowSums(t(t(res_choc$x) * q_l))
land_ref_moy  <- mean(land_ref,  na.rm = TRUE)
land_choc_moy <- mean(land_choc, na.rm = TRUE)
land_delta_pct <- (land_choc_moy - land_ref_moy) / land_ref_moy * 100
contrainte_active <- any(land_choc > q_l_s * 0.99, na.rm = TRUE)

cat("Usage des terres agricoles :\n")
cat("  Utilisation moyenne référence  :", round(land_ref_moy,  3), "(norm.)\n")
cat("  Utilisation moyenne choc       :", round(land_choc_moy, 3), "\n")
cat("  Variation                      :", round(land_delta_pct, 1), "%\n")
cat("  Contrainte biophysique active  :", if (contrainte_active) "OUI ⚠" else "non", "\n\n")

# ── Comparaison ESTEEM vs ARIO (si disponible) ────────────────────────────────
if (file.exists(PERSIST_ARIO)) {
  .ario <- readRDS(PERSIST_ARIO)
  if (!is.null(.ario$production_totale_j)) {
    n_j_esteem <- length(res_choc$j[res_choc$j <= DUREE_JOURS])
    n_j_ario   <- min(DUREE_JOURS, length(.ario$production_totale_j))
    
    perte_moy_esteem <- mean(
      (res_ref$x_total[1:n_j_esteem] - res_choc$x_total[1:n_j_esteem]) /
        res_ref$x_total[1:n_j_esteem] * 100,
      na.rm = TRUE
    )
    perte_moy_ario <- mean(
      (1 - .ario$production_totale_j[1:n_j_ario] /
         .ario$production_totale_j[1]) * 100,
      na.rm = TRUE
    )
    
    cat("── Comparaison ESTEEM vs ARIO ──────────────────────────────────────────\n")
    cat("  Perte de production moyenne (ESTEEM) :", round(perte_moy_esteem, 2), "%\n")
    cat("  Perte de production moyenne (ARIO)   :", round(perte_moy_ario,   2), "%\n")
    cat("  Différence                           :",
        round(perte_moy_esteem - perte_moy_ario, 2), "points\n")
    cat("  Interprétation : les dynamiques prix de l'ESTEEM amortissent (ou\n")
    cat("  amplifient) les effets quantitatifs par rapport à l'ARIO pur.\n\n")
    ario_compare <- list(
      perte_moy_esteem = perte_moy_esteem,
      perte_moy_ario   = perte_moy_ario
    )
  } else {
    ario_compare <- NULL
  }
  rm(.ario)
} else {
  ario_compare <- NULL
  cat("  (Fichier PERSIST_ARIO absent — comparaison ESTEEM/ARIO ignorée)\n\n")
}

################################################################################
# SECTION IX — EXPORTS
################################################################################

cat("── Section IX : Exports des résultats ──────────────────────────────────\n\n")

# ── Export CSV principal : trajectoires complètes ──────────────────────────────
# Ce tableau contient tous les indicateurs macro-économiques jour par jour,
# utile pour les comparaisons temporelles et les analyses post-traitement.
trajectoires_csv <- tibble(
  Jour              = res_choc$j,
  Annee             = round(res_choc$t, 4),
  Scenario          = NOM_SCENARIO,
  Prod_tot_ref      = round(res_ref$x_total,  3),
  Prod_tot_choc     = round(res_choc$x_total, 3),
  Perte_prod_pct    = round(perte_pct, 4),
  CPI_ref           = round(res_ref$pc,  6),
  CPI_choc          = round(res_choc$pc, 6),
  Inflation_ref_an  = round(res_ref$pi,  4),
  Inflation_choc_an = round(res_choc$pi, 4),
  Prix_terres_ref   = round(res_ref$pl,  4),
  Prix_terres_choc  = round(res_choc$pl, 4),
  Land_use_ref      = round(land_ref,  4),
  Land_use_choc     = round(land_choc, 4)
) %>%
  # Ajout des productions sectorielles
  bind_cols(as_tibble(round(res_choc$x, 3)) %>%
              setNames(paste0("x_choc_", SECTEURS))) %>%
  bind_cols(as_tibble(round(res_ref$x, 3)) %>%
              setNames(paste0("x_ref_", SECTEURS))) %>%
  bind_cols(as_tibble(round(res_choc$p, 4)) %>%
              setNames(paste0("prix_", SECTEURS))) %>%
  bind_cols(as_tibble(round(res_choc$v, 3)) %>%
              setNames(paste0("stock_", SECTEURS))) %>%
  bind_cols(as_tibble(round(res_choc$mu, 4)) %>%
              setNames(paste0("markup_", SECTEURS)))

write.csv(trajectoires_csv,
          file.path(DIR_EXPORTS, paste0("esteem_trajectoires_", NOM_SCENARIO, ".csv")),
          row.names = FALSE)
cat("  ✓ esteem_trajectoires_", NOM_SCENARIO, ".csv (", ncol(trajectoires_csv), "colonnes)\n", sep = "")

write.csv(perte_par_secteur,
          file.path(DIR_EXPORTS, paste0("esteem_bilan_sectoriel_", NOM_SCENARIO, ".csv")),
          row.names = FALSE)
cat("  ✓ esteem_bilan_sectoriel_", NOM_SCENARIO, ".csv\n", sep = "")

write.csv(chocs_df,
          file.path(DIR_EXPORTS, paste0("esteem_chocs_", NOM_SCENARIO, ".csv")),
          row.names = FALSE)
cat("  ✓ esteem_chocs_", NOM_SCENARIO, ".csv\n\n", sep = "")

# ── Rapport de synthèse ────────────────────────────────────────────────────────
cat("==========================================================\n")
cat("  RAPPORT FINAL — MODÈLE ESTEEM\n")
cat("==========================================================\n\n")
cat("Scénario              :", NOM_SCENARIO, "\n")
cat("Description           :", DESCRIPTION_SCENARIO, "\n")
cat("Durée du choc         :", DUREE_JOURS, "jours (", round(ESTEEM_DUREE_ANS, 3), "an)\n")
cat("Horizon simulé        :", round(ESTEEM_HORIZON_ANS * 365), "jours\n\n")

cat("Paramètres comportementaux :\n")
cat("  β_y (ajustement quantités)   :", ESTEEM_BETA_Y, "/an\n")
cat("  β_p (ajustement prix)        :", ESTEEM_BETA_P, "/an\n")
cat("  μ₁  (sensibilité markup)     :", ESTEEM_MU_1, "\n")
cat("  η   (élasticité prix)        :", ESTEEM_ETA, "\n\n")

cat("Résultats agrégés :\n")
cat("  Perte max. de production      :", round(perte_max, 2), "% (jour", jour_perte_max, ")\n")
cat("  Surcoût inflationniste total  :", round(inflation_surcomp, 3), "points de %\n")
cat("  Secteur le plus affecté       :", perte_par_secteur$Secteur[1],
    "(perte moy.", perte_par_secteur$Perte_prod_pct[1], "%)\n")
cat("  Secteur avec ↑ prix max       :",
    perte_par_secteur$Secteur[which.max(perte_par_secteur$Hausse_prix_pct)],
    "(+", max(perte_par_secteur$Hausse_prix_pct), "%)\n")
cat("  Contrainte foncière active    :",
    if (contrainte_active) "OUI — terres agricoles limitantes !" else "non\n")
cat("\n")

################################################################################
# SAUVEGARDE INTER-SCRIPTS
################################################################################

cat("=== Sauvegarde des objets persistants (06_esteem) ===\n")

saveRDS(
  list(
    # ── Trajectoires simulées complètes ──────────────────────────────────────
    res_ref              = res_ref,
    res_choc             = res_choc,
    
    # ── Paramètres du modèle ──────────────────────────────────────────────────
    params_base          = params_base,
    params_choc          = params_choc,
    
    # ── Conditions initiales pour le solveur EDO ─────────────────────────────
    # IMPORTANT : state0 est nécessaire dans viz_esteem.R pour les simulations
    # de sensibilité (graphique 8) qui re-résolvent le système EDO.
    state0               = state0,
    
    # ── Fonction EDO du modèle ESTEEM ─────────────────────────────────────────
    # R permet de persister des fonctions dans un .rds. On sauvegarde esteem_ode()
    # pour que viz_esteem.R puisse relancer des simulations de sensibilité sans
    # avoir à sourcer 06_esteem.R (qui charge toutes les dépendances).
    esteem_ode           = esteem_ode,
    
    # ── Paramètres temporels (requis dans viz_esteem.R pour les simulations) ──
    ESTEEM_DT_ANS        = ESTEEM_DT_ANS,
    ESTEEM_HORIZON_ANS   = ESTEEM_HORIZON_ANS,
    ESTEEM_BETA_Y        = ESTEEM_BETA_Y,
    ESTEEM_BETA_P        = ESTEEM_BETA_P,
    
    # ── Chocs issus du transport ──────────────────────────────────────────────
    delta_demande        = delta_demande,
    delta_cout           = delta_cout,
    chocs_df             = chocs_df,
    
    # ── Calibration Rwanda ────────────────────────────────────────────────────
    w_calibre            = w_calibre,
    markup_initial       = markup_initial,
    v_d_calibre          = v_d_calibre,
    q_l                  = q_l,
    q_l_s                = q_l_s,
    x_equilibre          = x_equilibre,
    y0                   = y0,
    
    # ── Trajectoires des ressources biophysiques ──────────────────────────────
    land_ref             = land_ref,
    land_choc            = land_choc,
    contrainte_active    = contrainte_active,
    
    # ── Résultats synthétiques ────────────────────────────────────────────────
    perte_pct            = perte_pct,
    perte_max            = perte_max,
    jour_perte_max       = jour_perte_max,
    perte_cum_pctj       = perte_cum_pctj,
    inflation_surcomp    = inflation_surcomp,
    perte_par_secteur    = perte_par_secteur,
    trajectoires_csv     = trajectoires_csv,
    ario_compare         = ario_compare,
    
    # ── Métadonnées ───────────────────────────────────────────────────────────
    NOM_SCENARIO         = NOM_SCENARIO,
    date_creation        = Sys.time()
  ),
  PERSIST_ESTEEM
)

cat("✓ persist_esteem.rds sauvegardé\n\n")
cat("Lancer viz_esteem.R pour les visualisations.\n")