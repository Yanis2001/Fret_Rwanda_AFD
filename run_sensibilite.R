################################################################################
# run_sensibilite.R
# RÔLE : Lancer une série de TESTS DE SENSIBILITÉ sur les paramètres du modèle
#        (betas gravitaires, valeur du temps, conversion valeur→tonnes, etc.)
#        sans jamais écraser les cartes, graphiques et exports du run de
#        référence produit par run_all.R.
#
# PRINCIPE
#   Pour chaque scénario décrit ci-dessous, le script :
#     1. fixe SCENARIO_ID / SCENARIO_LIBELLE / SENSIBILITE ;
#     2. source 00_parametres.R, qui applique les surcharges et redirige toutes
#        les sorties vers outputs/<dossier>/sensibilite/<SCENARIO_ID>/ ;
#     3. rejoue les modules aval (02→05) puis les visualisations ;
#     4. les figures produites portent un suffixe de fichier "_<SCENARIO_ID>"
#        et une mention "TEST DE SENSIBILITÉ" en bas de graphique.
#
# CE QUI N'EST PAS RECALCULÉ
#   Le module 01 (réseau OSM, pentes, démographie, zones) décrit la géographie
#   du Rwanda : il ne dépend d'aucun paramètre économique. Ses résultats sont
#   réutilisés tels quels (copiés depuis le dossier persist de la référence par
#   00_parametres.R), ce qui économise ~25 min par scénario.
#   ⚠ Si vous testez un paramètre du module 01 (zones, Voronoï, RWI, emploi…),
#     passez RELANCER_RESEAU à TRUE ci-dessous.
#
# PRÉREQUIS
#   Un run de référence complet (run_all.R) doit avoir été exécuté au moins une
#   fois : les tests de sensibilité repartent de ses fichiers persist.
################################################################################

# ==============================================================================
# 1. DÉFINITION DES SCÉNARIOS
# ==============================================================================
# Chaque scénario est une liste à trois champs :
#   id       : identifiant technique — sert de nom de sous-dossier ET de suffixe
#              de fichier. Sans espace ni accent.
#   libelle  : phrase lisible affichée en bas de chaque figure.
#   surcharge: liste nommée des paramètres de 00_parametres.R à modifier.
#              Chaque élément est soit une VALEUR de remplacement, soit une
#              FONCTION recevant la valeur de référence et renvoyant la nouvelle
#              (pratique pour les variations en pourcentage).
#
# ⚠ Le nom du paramètre doit exister dans 00_parametres.R : sinon le run
#   s'arrête avec un message explicite (garde-fou anti-faute de frappe).
#
# Pour ne lancer qu'une partie des scénarios, commentez les autres.
# ==============================================================================

SCENARIOS <- list(

  # ── Sensibilité aux élasticités du modèle gravitaire ────────────────────────
  # Beta élevé = commerce plus sensible au coût de transport → flux plus courts,
  # échanges plus locaux. On teste ±20 % sur tous les secteurs simultanément.
  list(
    id        = "beta_plus20",
    libelle   = "Betas gravitaires +20 % (commerce plus sensible à la distance)",
    surcharge = list(BETA_SECTEUR = function(b) b * 1.20)
  ),
  list(
    id        = "beta_moins20",
    libelle   = "Betas gravitaires -20 % (commerce moins sensible à la distance)",
    surcharge = list(BETA_SECTEUR = function(b) b * 0.80)
  ),

  # ── Sensibilité à la valeur du temps ────────────────────────────────────────
  # La valeur du temps entre dans le coût généralisé calculé par 02_couts.R :
  # la doubler renchérit les trajets lents (pentes, routes dégradées) et peut
  # déplacer les itinéraires vers le réseau primaire.
  list(
    id        = "vot_double",
    libelle   = "Valeur du temps x2 pour tous les véhicules",
    surcharge = list(
      params_flotte_df = function(df) dplyr::mutate(df, valeur_temps = valeur_temps * 2)
    )
  ),
  list(
    id        = "vot_moitie",
    libelle   = "Valeur du temps divisée par 2 pour tous les véhicules",
    surcharge = list(
      params_flotte_df = function(df) dplyr::mutate(df, valeur_temps = valeur_temps / 2)
    )
  ),

  # ── Sensibilité à la conversion valeur → tonnes ─────────────────────────────
  # VALEUR_RWF_PAR_TONNE fixe combien de tonnes correspondent à un milliard de
  # RWF échangé. C'est le paramètre le plus incertain du modèle (dire d'expert).
  # Ici : agriculture 30 % plus chère à la tonne → 30 % de tonnage en moins.
  # 00_parametres.R recalcule automatiquement TONNES_PAR_mrd_RWF et SECTEURS_FRET.
  list(
    id        = "valeur_tonne_agri_haute",
    libelle   = "Valeur unitaire agricole +30 % (donc tonnage agricole -23 %)",
    surcharge = list(
      VALEUR_RWF_PAR_TONNE = function(v) {
        v["Agriculture"] <- v["Agriculture"] * 1.30
        v
      }
    )
  )
)

# ==============================================================================
# 2. OPTIONS DU RUN
# ==============================================================================

# Relancer le module 01 (réseau/zones) pour chaque scénario.
# FALSE par défaut : la géographie ne dépend pas des paramètres testés.
# ⚠ TRUE réécrit aussi les caches géographiques PARTAGÉS avec la référence.
RELANCER_RESEAU <- FALSE

# Modules de visualisation à rejouer pour chaque scénario.
SENS_VIZ_RESEAU   <- FALSE  # cartes de coûts/pentes : utile surtout si la valeur
                            # du temps ou les coûts véhicules changent
SENS_VIZ_FRET     <- TRUE   # cartes de trafic, Sankey, compositions sectorielles
SENS_VIZ_VULNERAB <- TRUE   # cartes de vulnérabilité et de détours

# Supprimer les fichiers persist du scénario à la fin de son run.
# Ils pèsent ~100 Mo par scénario et ne servent qu'au chaînage des modules :
# les cartes, graphiques et exports, eux, sont conservés.
NETTOYER_PERSIST_SENSIBILITE <- TRUE

# ==============================================================================
# 3. EXÉCUTION
# ==============================================================================

t_debut_sens <- Sys.time()
cat("\n╔══════════════════════════════════════════════════════╗\n")
cat(  "║  TESTS DE SENSIBILITÉ —", length(SCENARIOS), "scénario(s)                 ║\n")
cat(  "╚══════════════════════════════════════════════════════╝\n\n")

for (.sc in SCENARIOS) {

  cat("\n════════════════════════════════════════════════════════\n")
  cat("  SCÉNARIO :", .sc$id, "\n")
  cat("  ", .sc$libelle, "\n")
  cat("════════════════════════════════════════════════════════\n\n")

  t0_sc <- Sys.time()

  # Les trois objets sont posés AVANT de sourcer 00_parametres.R, qui ne les
  # redéfinit que s'ils n'existent pas (cf. bloc "TESTS DE SENSIBILITÉ" en tête
  # de 00_parametres.R). C'est ce qui bascule tout le run en mode sensibilité.
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
# surchargés, et la table DuckDB params_flotte contient les valeurs du dernier
# scénario. On resource 00_parametres.R en mode référence pour remettre la
# session dans un état propre — indispensable si vous enchaînez avec run_all.R
# ou avec des analyses manuelles dans la console.
# ==============================================================================

SCENARIO_ID      <<- "reference"
SCENARIO_LIBELLE <<- NULL
SENSIBILITE      <<- list()
source("00_parametres.R", local = FALSE)

cat("\n══════════════════════════════════════════════════════\n")
cat("  Tests de sensibilité terminés en",
    round(difftime(Sys.time(), t_debut_sens, units = "mins"), 1), "min\n")
cat("  Sorties : outputs/cartes/sensibilite/<scenario>/\n")
cat("  Session remise en mode RÉFÉRENCE.\n")
cat("══════════════════════════════════════════════════════\n")
