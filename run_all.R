################################################################################
# run_all.R
# RÔLE : Point d'entrée unique pour un run complet ou partiel.
#        Sourcer ce fichier suffit à enchaîner les modules choisis.
################################################################################

################################################################################
# ── POUR RETROUVER LE DÉPÔT GITHUB ────────────────────────────────────────────
#  system("git clone https://github.com/Yanis2001/Fret_Rwanda_AFD.git")
################################################################################

# ==============================================================================
# CONNEXION GIT
# Le token est lu depuis la variable d'environnement GITHUB_PAT.
# → Première utilisation : lancez source("setup.R") une seule fois pour
#   enregistrer votre token de façon permanente dans ~/.Renviron.
# → Les utilisateurs qui veulent seulement faire tourner le code sans pusher
#   peuvent ignorer cette étape : le bloc est silencieusement ignoré.
# ==============================================================================

token <- Sys.getenv("GITHUB_PAT")

if (nchar(token) > 0) {
  # Credential helper : transmet le token à Git sans mot de passe interactif
  system("git config --global credential.helper '!f() { echo \"username=token\"; echo \"password=$GITHUB_PAT\"; }; f'")
  # Remotes : dépôt principal + miroir GEMMES-AFD.
  # set-url --push (sans --add) réinitialise la liste des push URLs à une seule
  # entrée, puis --add --push ajoute le miroir. Cela évite l'accumulation de
  # doublons à chaque relance de run_all.R.
  system("git remote set-url origin https://github.com/Yanis2001/Fret_Rwanda_AFD.git")
  system("git remote set-url --push origin https://github.com/Yanis2001/Fret_Rwanda_AFD.git")
  system("git remote set-url --add --push origin https://github.com/GEMMES-AFD/Transport.git")
  system("git remote -v")
} else {
  cat("ℹ GITHUB_PAT non défini — synchronisation Git désactivée.\n")
  cat("  Pour activer le push automatique, lancez source(\"setup.R\") une seule fois.\n\n")
}

# ==============================================================================
# CONFIGURATION DU RUN
# Mettre TRUE pour les modules à exécuter, FALSE pour les sauter.
# Les modules de visualisation sont indépendants des suivants —
# on peut les désactiver sans bloquer le reste.
# ==============================================================================

# Recalcul forcé de tous les caches (réseau, pentes, OD, affectation).
# Remettre à FALSE après un reset pour bénéficier des caches (~30 min gagnés).
RESET_CACHES     <- TRUE  # ← passer à TRUE pour tout recalculer depuis zéro

RUN_PARAMETRES   <- TRUE   # 00 — < 1 min  — packages, DuckDB, palettes
RUN_RESEAU       <- TRUE   # 01 — ~20 min  — réseau OSM, pentes, démographie
RUN_COUTS        <- TRUE   # 02 — < 1 min  — coûts, graphe multi-modal
RUN_TRANSPORT    <- TRUE   # 03 — ~15 min  — OD, gravitaire, affectation
RUN_VULNERAB     <- TRUE   # 04 — ~13 min  — vulnérabilité, criticité

RUN_VIZ_RESEAU   <- TRUE   # viz — ~3 min  — cartes réseau / coûts / pentes
RUN_VIZ_FRET     <- TRUE   # viz — ~2 min  — cartes fret / Sankey
RUN_VIZ_VULNERAB <- TRUE   # viz — ~3 min  — cartes vulnérabilité / détours

# ==============================================================================
# EXÉCUTION SÉQUENTIELLE
# ==============================================================================

t_debut <- Sys.time()
cat("
     ╔══════════════════════════════════════════╗\n
     ║  RUN COMPLET — Réseau de Fret            ║\n
     ╚══════════════════════════════════════════╝\n\n")

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
  # tryCatch intercepte toute erreur survenue pendant l'exécution du module,
  # l'affiche clairement et arrête le run pour éviter les échecs silencieux.
  tryCatch(
    source(fichier, local = FALSE),
    error = function(e) {
      cat("\n✗ ERREUR dans", nom, ":\n  ", conditionMessage(e), "\n")
      cat("  Traceback disponible via traceback() dans la console R.\n\n")
      stop(paste("Échec du module", nom, "—", conditionMessage(e)), call. = FALSE)
    }
  )
  duree <- round(difftime(Sys.time(), t0, units = "mins"), 1)
  cat("\n✓", nom, "terminé en", duree, "min\n")
  # Double gc() après chaque module : le premier passage marque les objets
  # libérables, le second les supprime effectivement de la mémoire physique.
  # Cela évite l'accumulation d'objets entre modules et réduit le risque de crash
  # par pression mémoire en fin de run.
  ram_avant <- sum(gc(verbose = FALSE)[, 2])
  invisible(gc(verbose = FALSE))
  cat("  RAM après nettoyage :", round(ram_avant, 0), "MB\n\n")
}

executer_module("00_parametres",   "00_parametres.R",   RUN_PARAMETRES)

# ==============================================================================
# RESET DES CACHES 
# ==============================================================================

if (!exists("RESET_CACHES")) RESET_CACHES <- FALSE

if (RESET_CACHES) {
  .dir_cache <- if (exists("DIR_CACHE")) DIR_CACHE else file.path("outputs", "cache")
  .caches <- c(
    file.path(.dir_cache, "reseau_corrige_cache.rds"),
    file.path(.dir_cache, "pentes_cache.rds"),
    file.path(.dir_cache, "landuse_cache.rds"),
    file.path(.dir_cache, "od_cache.rds"),
    file.path(.dir_cache, "affectation_cache.rds")
  )
  cat("=== RESET COMPLET DES CACHES ===\n")
  for (.f in .caches) {
    if (file.exists(.f)) {
      file.remove(.f)
      cat("  ✓ Supprimé :", basename(.f), "\n")
    } else {
      cat("  — Absent  :", basename(.f), "\n")
    }
  }
  rm(.dir_cache, .caches, .f)
  cat("\n⚠ RESET_CACHES = TRUE — pensez à le remettre à FALSE\n")
  cat("  Temps de recalcul estimé : ~50 min selon la machine\n\n")
}

executer_module("01_reseau",       "01_reseau.R",       RUN_RESEAU)
executer_module("02_couts",        "02_couts.R",        RUN_COUTS)
executer_module("03_transport",    "03_transport.R",    RUN_TRANSPORT)
executer_module("04_vulnerabilite","04_vulnerabilite.R",RUN_VULNERAB)

executer_module("viz_reseau",      "viz_reseau.R",      RUN_VIZ_RESEAU)
executer_module("viz_fret",        "viz_fret.R",        RUN_VIZ_FRET)
executer_module("viz_vulnerabilite","viz_vulnerabilite.R",RUN_VIZ_VULNERAB)


duree_totale <- round(difftime(Sys.time(), t_debut, units = "mins"), 1)
cat("══════════════════════════════════════════\n")
cat("  Run terminé en", duree_totale, "min\n")
cat("══════════════════════════════════════════\n")

# Fermeture explicite de la connexion DuckDB.
if (exists("con") && tryCatch(DBI::dbIsValid(con), error = function(e) FALSE)) {
  DBI::dbDisconnect(con, shutdown = TRUE)
  cat("✓ Connexion DuckDB fermée proprement.\n")
}