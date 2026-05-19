################################################################################
# setup.R
# RÔLE : Configuration unique du Personal Access Token GitHub.
#        À lancer UNE SEULE FOIS par utilisateur, sur chaque machine.
#        Le token est ensuite disponible automatiquement dans toutes les
#        sessions R futures — plus besoin d'y revenir.
#
# USAGE :
#   source("setup.R")
#   → Suivre les instructions dans la console.
#
# COMPATIBILITÉ : RStudio local, Onyxia (SSP Cloud), tout environnement R.
################################################################################

cat("╔══════════════════════════════════════════════╗\n")
cat("║  Configuration du token GitHub (une seule fois) ║\n")
cat("╚══════════════════════════════════════════════╝\n\n")

# ── Vérification : token déjà présent ? ───────────────────────────────────────
token_existant <- Sys.getenv("GITHUB_PAT")
if (nchar(token_existant) > 0) {
  cat("✓ GITHUB_PAT est déjà défini dans cette session (", nchar(token_existant),
      "caractères).\n")
  cat("  Si vous souhaitez le remplacer, continuez. Sinon, vous pouvez fermer ce script.\n\n")
}

# ── Instructions pour obtenir un token ────────────────────────────────────────
cat("ÉTAPE 1 — Obtenir votre Personal Access Token (PAT) sur GitHub\n")
cat("  1. Allez sur : https://github.com/settings/tokens/new\n")
cat("  2. Note      : 'Fret Rwanda AFD' (ou autre nom explicite)\n")
cat("  3. Expiration: recommandé 'No expiration' pour un usage permanent\n")
cat("  4. Droits    : cochez uniquement 'repo'\n")
cat("  5. Cliquez   : 'Generate token' et copiez la valeur (ghp_xxx...)\n\n")

cat("  ⚠  Le token n'est affiché qu'une seule fois par GitHub — copiez-le avant de continuer.\n\n")

# ── Saisie du token ────────────────────────────────────────────────────────────
pat <- readline("ÉTAPE 2 — Collez votre token ici et appuyez sur Entrée : ")
pat <- trimws(pat)

if (nchar(pat) == 0) {
  stop("Aucun token saisi. Relancez setup.R quand vous avez votre token.")
}

if (!grepl("^ghp_", pat) && !grepl("^github_pat_", pat)) {
  cat("\n⚠  Le token ne commence pas par 'ghp_' ou 'github_pat_'.\n")
  cat("   Assurez-vous d'avoir copié la bonne valeur.\n\n")
}

# ── Enregistrement dans ~/.Renviron ───────────────────────────────────────────
# ~/.Renviron est lu automatiquement à chaque démarrage de session R.
# On l'écrit une fois ; il persistera pour toutes les sessions futures,
# sur cet ordinateur ou cet environnement Onyxia.
renviron_path <- path.expand("~/.Renviron")

# Lire les lignes existantes (si le fichier existe déjà)
lignes <- if (file.exists(renviron_path)) readLines(renviron_path, warn = FALSE) else character(0)

# Retirer une éventuelle ancienne valeur GITHUB_PAT
lignes <- lignes[!grepl("^GITHUB_PAT\\s*=", lignes)]

# Ajouter la nouvelle valeur
lignes <- c(lignes, paste0("GITHUB_PAT=", pat))
writeLines(lignes, renviron_path)

# Recharger immédiatement dans la session courante (sans redémarrer R)
readRenviron(renviron_path)

cat("\n✓ Token enregistré dans :", renviron_path, "\n")
cat("  Il sera chargé automatiquement à chaque démarrage de session R.\n\n")

# ── Vérification ──────────────────────────────────────────────────────────────
verif <- Sys.getenv("GITHUB_PAT")
if (nchar(verif) > 0) {
  cat("✓ Vérification OK — GITHUB_PAT actif dans cette session (", nchar(verif), "caractères)\n")
  cat("  Vous pouvez maintenant lancer run_all.R normalement.\n\n")
} else {
  cat("✗ Problème : GITHUB_PAT toujours vide après enregistrement.\n")
  cat("  Redémarrez votre session R (Session > Restart R) puis relancez run_all.R.\n\n")
}

cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
cat("  setup.R terminé. Ce script n'est plus nécessaire.\n")
cat("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
