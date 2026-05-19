################################################################################
# run_all.R
# RÔLE : Point d'entrée unique pour un run complet ou partiel.
#        Sourcer ce fichier suffit à enchaîner les modules choisis.
################################################################################

# ==============================================================================
# CONFIGURATION DU RUN
# Mettre TRUE pour les modules à exécuter, FALSE pour les sauter.
# Les modules de visualisation sont indépendants des suivants —
# on peut les désactiver sans bloquer le reste.
# ==============================================================================

RUN_PARAMETRES   <- TRUE   # 00 — toujours TRUE si on part de zéro
RUN_RESEAU       <- TRUE   # 01 — long (~30 min, pentes + Worldpop)
RUN_COUTS        <- TRUE   # 02 — moyen (~5 min)
RUN_TRANSPORT    <- TRUE   # 03 — long (~1h, Dijkstra + gravitaire)
RUN_VULNERAB     <- FALSE   # 04 — long (~2h, criticité)
RUN_ARIO         <- FALSE   # 05 — rapide (~1 min)

RUN_VIZ_RESEAU   <- TRUE   # viz — cartes réseau / coûts / pentes
RUN_VIZ_FRET     <- TRUE   # viz — cartes fret / Sankey
RUN_VIZ_VULNERAB <- FALSE   # viz — cartes vulnérabilité / détours
RUN_VIZ_ARIO     <- FALSE   # viz — trajectoires ARIO

# ==============================================================================
# EXÉCUTION SÉQUENTIELLE
# ==============================================================================

t_debut <- Sys.time()
cat("╔══════════════════════════════════════════╗\n")
cat("║  RUN COMPLET — Réseau Fret Rwanda        ║\n")
cat("╚══════════════════════════════════════════╝\n\n")

executer_module <- function(nom, fichier, actif) {
  if (!actif) {
    cat("── [SKIP]", nom, "\n\n")
    return(invisible(NULL))
  }
  cat("┌──────────────────────────────────────────\n")
  cat("│ START :", nom, "\n")
  cat("│ Heure :", format(Sys.time(), "%H:%M:%S"), "\n")
  cat("└──────────────────────────────────────────\n")
  t0 <- Sys.time()
  source(fichier, local = FALSE)
  duree <- round(difftime(Sys.time(), t0, units = "mins"), 1)
  cat("\n✓", nom, "terminé en", duree, "min\n\n")
}

executer_module("00_parametres",   "00_parametres.R",   RUN_PARAMETRES)
executer_module("01_reseau",       "01_reseau.R",       RUN_RESEAU)
executer_module("02_couts",        "02_couts.R",        RUN_COUTS)
executer_module("03_transport",    "03_transport.R",    RUN_TRANSPORT)
executer_module("04_vulnerabilite","04_vulnerabilite.R",RUN_VULNERAB)
executer_module("05_ario",         "05_ario.R",         RUN_ARIO)
executer_module("viz_reseau",      "viz_reseau.R",      RUN_VIZ_RESEAU)
executer_module("viz_fret",        "viz_fret.R",        RUN_VIZ_FRET)
executer_module("viz_vulnerabilite","viz_vulnerabilite.R",RUN_VIZ_VULNERAB)
executer_module("viz_ario",        "viz_ario.R",        RUN_VIZ_ARIO)

duree_totale <- round(difftime(Sys.time(), t_debut, units = "mins"), 1)
cat("══════════════════════════════════════════\n")
cat("  Run terminé en", duree_totale, "min\n")
cat("══════════════════════════════════════════\n")