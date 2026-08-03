################################################################################
# run_sensibilite.R
# RÔLE : Lancer une ANALYSE DE SENSIBILITÉ PAR HYPERCUBE LATIN sur les deux
#        familles de paramètres les plus incertaines du modèle :
#          • BETA_SECTEUR          — élasticités du modèle gravitaire ;
#          • VALEUR_RWF_PAR_TONNE  — conversion valeur → tonnes.
#        Aucune sortie du run de référence (run_all.R) n'est écrasée.
#
#
# PRINCIPE (inchangé pour chaque scénario)
#   1. on fixe SCENARIO_ID / SCENARIO_LIBELLE / SENSIBILITE ;
#   2. on source 00_parametres.R, qui applique les surcharges et redirige les
#      sorties vers outputs/<dossier>/sensibilite/<SCENARIO_ID>/ ;
#   3. on rejoue les modules aval (02→05) puis les visualisations ;
#   4. les figures produites portent le suffixe "_<SCENARIO_ID>" et une mention
#      "TEST DE SENSIBILITÉ".
#
# SYNTHÈSE COMPARATIVE
#   Le plan d'expérience (multiplicateurs tirés par scénario) est écrit dans
#   outputs/exports/sensibilite/plan_lhs.csv. viz_sensibilite.R le relit avec
#   les exports de chaque scénario pour produire les figures qui mettent en
#   évidence les divergences avec la référence (enveloppe d'incertitude, indices
#   de sensibilité, carte de robustesse spatiale).
#
# CE QUI N'EST PAS RECALCULÉ
#   Le module 01 (réseau OSM, pentes, démographie, zones) ne dépend d'aucun
#   paramètre économique : ses résultats sont réutilisés tels quels (copiés
#   depuis le persist de la référence par 00_parametres.R), ce qui économise
#   ~25 min par scénario. ⚠ Passez RELANCER_RESEAU à TRUE seulement si vous
#   testez un paramètre du module 01.
#
# PRÉREQUIS
#   Un run de référence complet (run_all.R) doit avoir été exécuté au moins une
#   fois : les tests de sensibilité repartent de ses fichiers persist et se
#   comparent à ses exports.
################################################################################

# ==============================================================================
# 1. OPTIONS DU RUN
# ==============================================================================

# Relancer le module 01 (réseau/zones) pour chaque scénario.
# FALSE par défaut : la géographie ne dépend pas des paramètres testés (betas et
# valeurs/tonne agissent en aval du réseau). ⚠ TRUE réécrit les caches
# géographiques PARTAGÉS avec la référence.
RELANCER_RESEAU <- FALSE

# Modules de visualisation à rejouer pour chaque scénario.
SENS_VIZ_RESEAU   <- FALSE  # cartes de coûts/pentes : inutiles ici (le réseau et
                            # la valeur du temps ne varient pas dans ce plan)
SENS_VIZ_FRET     <- TRUE   # cartes de trafic, Sankey, compositions sectorielles
SENS_VIZ_VULNERAB <- FALSE  # vulnérabilité : coûteuse et peu affectée par ce plan

# Supprimer les fichiers persist du scénario à la fin de son run.
# Ils pèsent ~100 Mo par scénario et ne servent qu'au chaînage des modules ;
# les cartes, graphiques et exports (dont volumes_fret_par_secteur.csv, lu par
# viz_sensibilite.R) sont conservés.
NETTOYER_PERSIST_SENSIBILITE <- TRUE

# Produire automatiquement la synthèse comparative en fin de run.
LANCER_VIZ_SYNTHESE <- TRUE

# ==============================================================================
# 2. CONSTRUCTION DU PLAN D'EXPÉRIENCE PAR HYPERCUBE LATIN
# ==============================================================================
# On source d'abord 00_parametres.R en mode RÉFÉRENCE pour récupérer, dans un
# état propre :
#   • la liste des secteurs de fret (SECTEURS_FRET) et les noms/ valeurs de
#     référence de BETA_SECTEUR et VALEUR_RWF_PAR_TONNE ;
#   • la configuration du plan (SENS_LHS_N, amplitudes, graine).
# Le plan est ensuite tiré UNE SEULE FOIS, avant la boucle sur les scénarios.
# ==============================================================================

SCENARIO_ID      <<- "reference"
SCENARIO_LIBELLE <<- NULL
SENSIBILITE      <<- list()
source("00_parametres.R", local = FALSE)

# Secteurs effectivement soumis à variation : ceux qui ont à la fois un beta et
# une valeur/tonne FINIE (c.-à-d. les secteurs de fret). On les fige ici pour
# garantir un ordre stable entre les colonnes du plan et les surcharges.
secteurs_sens <- SECTEURS_FRET
n_sec <- length(secteurs_sens)

# Dimensions de l'hypercube : un axe par (secteur × beta) + un axe par
# (secteur × valeur-tonne). Avec 8 secteurs de fret → 16 dimensions.
n_dim <- 2L * n_sec

set.seed(SENS_LHS_GRAINE)

# randomLHS(N, d) renvoie une matrice N×d de valeurs dans (0,1), stratifiée :
# chaque colonne contient exactement une valeur dans chacun des N sous-intervalles
# [0,1/N], [1/N,2/N], … On transforme ensuite ces quantiles uniformes en
# multiplicateurs centrés sur 1 via la borne basse (1 − amplitude) et la plage
# (2 × amplitude) : mult = (1 − amp) + u × (2 × amp).
U <- lhs::randomLHS(SENS_LHS_N, n_dim)

# Colonnes 1..n_sec  → multiplicateurs des betas
# Colonnes (n_sec+1)..2n_sec → multiplicateurs des valeurs/tonne
mult_beta_mat <- (1 - SENS_LHS_AMPLITUDE_BETA) +
                 U[, 1:n_sec, drop = FALSE] * (2 * SENS_LHS_AMPLITUDE_BETA)
mult_val_mat  <- (1 - SENS_LHS_AMPLITUDE_VALEUR_TONNE) +
                 U[, (n_sec + 1):n_dim, drop = FALSE] * (2 * SENS_LHS_AMPLITUDE_VALEUR_TONNE)

colnames(mult_beta_mat) <- secteurs_sens
colnames(mult_val_mat)  <- secteurs_sens

# Identifiants de scénarios : "lhs_01", "lhs_02", … (tri alphabétique = ordre
# des tirages, pratique pour les figures de synthèse).
ids_sens <- sprintf("lhs_%02d", seq_len(SENS_LHS_N))

# ── Écriture du plan d'expérience ───────────────────────────────────────────
# plan_lhs.csv : une ligne par scénario, une colonne par multiplicateur, sous la
# forme beta_<Secteur> / valtonne_<Secteur>. viz_sensibilite.R le relit pour
# relier les variations d'ENTRÉE aux écarts de SORTIE (indices de sensibilité).
plan_lhs <- data.frame(
  id = ids_sens,
  mult_beta_mat,
  mult_val_mat,
  check.names = FALSE,
  stringsAsFactors = FALSE
)
names(plan_lhs) <- c("id",
                     paste0("beta_",     secteurs_sens),
                     paste0("valtonne_", secteurs_sens))

DIR_SENS_EXPORTS <- file.path(DIR_EXPORTS, "sensibilite")   # référence → outputs/exports/sensibilite
dir.create(DIR_SENS_EXPORTS, showWarnings = FALSE, recursive = TRUE)
readr::write_csv(plan_lhs, file.path(DIR_SENS_EXPORTS, "plan_lhs.csv"))

# ── Construction des surcharges d'un scénario ───────────────────────────────
# Renvoie la liste SENSIBILITE attendue par 00_parametres.R : deux fonctions qui
# appliquent, secteur par secteur, les multiplicateurs du tirage. force() fige
# les vecteurs dans la fermeture (sinon, dans la boucle, toutes les fonctions
# partageraient le dernier tirage — piège classique de portée en R).
faire_surcharge <- function(mult_beta, mult_val) {
  force(mult_beta); force(mult_val)
  list(
    BETA_SECTEUR = function(b) {
      b[names(mult_beta)] <- b[names(mult_beta)] * mult_beta[names(mult_beta)]
      b
    },
    VALEUR_RWF_PAR_TONNE = function(v) {
      v[names(mult_val)] <- v[names(mult_val)] * mult_val[names(mult_val)]
      v
    }
  )
}

# Liste finale des scénarios (même structure qu'avant : id / libelle / surcharge)
SCENARIOS <- lapply(seq_len(SENS_LHS_N), function(k) {
  mb <- mult_beta_mat[k, ]
  mv <- mult_val_mat[k, ]
  list(
    id        = ids_sens[k],
    libelle   = sprintf("Tirage LHS %d/%d — betas ×[%.2f;%.2f], valeurs/tonne ×[%.2f;%.2f]",
                        k, SENS_LHS_N, min(mb), max(mb), min(mv), max(mv)),
    surcharge = faire_surcharge(mb, mv)
  )
})

# ==============================================================================
# 3. EXÉCUTION
# ==============================================================================

t_debut_sens <- Sys.time()
cat("\n╔══════════════════════════════════════════════════════╗\n")
cat(  "║  SENSIBILITÉ — HYPERCUBE LATIN :", SENS_LHS_N, "tirages          ║\n")
cat(  "╚══════════════════════════════════════════════════════╝\n")
cat("  Dimensions          :", n_dim, "(", n_sec, "betas +", n_sec, "valeurs/tonne )\n")
cat("  Amplitude betas      : ±", 100 * SENS_LHS_AMPLITUDE_BETA, "%\n")
cat("  Amplitude valeurs    : ±", 100 * SENS_LHS_AMPLITUDE_VALEUR_TONNE, "%\n")
cat("  Plan d'expérience    :", file.path(DIR_SENS_EXPORTS, "plan_lhs.csv"), "\n\n")

for (.sc in SCENARIOS) {

  cat("\n════════════════════════════════════════════════════════\n")
  cat("  SCÉNARIO :", .sc$id, "\n")
  cat("  ", .sc$libelle, "\n")
  cat("════════════════════════════════════════════════════════\n\n")

  t0_sc <- Sys.time()

  # Les trois objets sont posés AVANT de sourcer 00_parametres.R : c'est ce qui
  # bascule tout le run en mode sensibilité (cf. bloc "TESTS DE SENSIBILITÉ" en
  # tête de 00_parametres.R).
  SCENARIO_ID      <<- .sc$id
  SCENARIO_LIBELLE <<- .sc$libelle
  SENSIBILITE      <<- .sc$surcharge

  # tryCatch : un scénario qui échoue (ex. non-convergence du Furness avec des
  # betas extrêmes) ne doit pas interrompre les scénarios suivants.
  tryCatch({

    source("00_parametres.R", local = FALSE)

    if (RELANCER_RESEAU) source("01_reseau.R", local = FALSE)

    source("02_couts.R",         local = FALSE)
    source("03_transport.R",     local = FALSE)
    source("04_affectation.R",   local = FALSE)
    source("05_vulnerabilite.R", local = FALSE)

    if (SENS_VIZ_RESEAU)   source("viz_reseau.R",        local = FALSE)
    if (SENS_VIZ_FRET)     source("viz_fret.R",          local = FALSE)
    if (SENS_VIZ_VULNERAB) source("viz_vulnerabilite.R", local = FALSE)

    cat("\n✓ Scénario", .sc$id, "terminé en",
        round(difftime(Sys.time(), t0_sc, units = "mins"), 1), "min\n")
    cat("  Figures :", DIR_CARTES, "\n")

  }, error = function(e) {
    cat("\n✗ ÉCHEC du scénario", .sc$id, ":\n  ", conditionMessage(e), "\n")
    cat("  On passe au scénario suivant.\n")
  })

  # Nettoyage des objets intermédiaires lourds propres au scénario
  if (NETTOYER_PERSIST_SENSIBILITE && exists("DIR_PERSIST") &&
      grepl("sensibilite", DIR_PERSIST, fixed = TRUE)) {
    unlink(DIR_PERSIST, recursive = TRUE)
    cat("  ✓ Fichiers persist du scénario supprimés (cartes conservées)\n")
  }

  invisible(gc(verbose = FALSE))
}

# ==============================================================================
# 4. RETOUR À LA RÉFÉRENCE
# ==============================================================================
# Les modules ont laissé en mémoire des objets calculés avec les paramètres
# surchargés, et la table DuckDB params_flotte pourrait contenir des valeurs
# non-référence. On resource 00_parametres.R en mode référence pour remettre la
# session dans un état propre — indispensable avant la synthèse et avant tout
# enchaînement avec run_all.R ou des analyses manuelles.
# ==============================================================================

SCENARIO_ID      <<- "reference"
SCENARIO_LIBELLE <<- NULL
SENSIBILITE      <<- list()
source("00_parametres.R", local = FALSE)

cat("\n══════════════════════════════════════════════════════\n")
cat("  Analyse de sensibilité LHS terminée en",
    round(difftime(Sys.time(), t_debut_sens, units = "mins"), 1), "min\n")
cat("  Sorties par scénario : outputs/cartes/sensibilite/<scenario>/\n")
cat("  Session remise en mode RÉFÉRENCE.\n")
cat("══════════════════════════════════════════════════════\n")

# ==============================================================================
# 5. SYNTHÈSE COMPARATIVE
# ==============================================================================
# viz_sensibilite.R relit le plan LHS + les exports de tous les scénarios et
# produit les figures de divergence dans outputs/cartes/sensibilite/_synthese/.
# ==============================================================================

if (LANCER_VIZ_SYNTHESE) {
  cat("\n→ Génération de la synthèse comparative (viz_sensibilite.R)…\n")
  source("viz_sensibilite.R", local = FALSE)
}
