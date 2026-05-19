# ================================================
# DOCKERFILE — AIOps API
# Mohamed Aziz Zneti
# ================================================
# Ce fichier construit l'image Docker de l'API.
#
# Étapes de construction :
# 1. Partir d'une image Python officielle
# 2. Installer les dépendances système
# 3. Installer les dépendances Python
# 4. Copier le code source
# 5. Lancer l'API
# ================================================

# ── Étape 1 : Image de base ───────────────────
# Python 3.11 sur Debian Slim
# "slim" = image légère sans outils inutiles
FROM python:3.11-slim

# ── Étape 2 : Métadonnées ─────────────────────
LABEL maintainer="Mohamed Aziz Zneti"
LABEL projet="AIOps — Observabilité Intelligente"
LABEL version="2.0.0"

# ── Étape 3 : Dossier de travail ──────────────
# Tout le code sera dans /app à l'intérieur
# du conteneur
WORKDIR /app

# ── Étape 4 : Variables d'environnement ───────
# Empêche Python de créer des fichiers .pyc
ENV PYTHONDONTWRITEBYTECODE=1
# Affiche les logs Python immédiatement
ENV PYTHONUNBUFFERED=1
# Timezone Tunisia
ENV TZ=Africa/Tunis

# ── Étape 5 : Dépendances système ─────────────
# Nécessaires pour compiler Prophet et NumPy
RUN apt-get update && apt-get install -y \
    build-essential \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get purge -y build-essential \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# ── Étape 6 : Mettre à jour pip ───────────────
RUN pip install --upgrade pip

# ── Étape 7 : Installer les dépendances Python ─
# On copie d'abord requirements.txt SEUL
# Cela permet à Docker de mettre en cache
# cette étape si requirements.txt ne change pas
# → reconstruction plus rapide
COPY requirements.txt .

# Installer toutes les dépendances Python
# --no-cache-dir : ne garde pas le cache pip
#                  pour réduire la taille de l'image
RUN pip install --no-cache-dir -r requirements.txt

# ── Étape 8 : Copier le code source ───────────
# On copie le code APRÈS les dépendances
# pour profiter du cache Docker
COPY . .

# ── Étape 9 : Port exposé ─────────────────────
# L'API écoute sur le port 8000
EXPOSE 8000

# ── Étape 10 : Commande de démarrage ──────────
# uvicorn = serveur ASGI pour FastAPI
# --host 0.0.0.0 = accessible depuis l'extérieur
# --port 8000    = port d'écoute
# --log-level    = niveau de log (info en production)
CMD ["uvicorn", "main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--log-level", "info"]
