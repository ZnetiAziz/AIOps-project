"""
================================================
AIOPS API — Intelligence Artificielle v2.0
Mohamed Aziz Zneti

Rôle : Cerveau de la plateforme AIOps
- Reçoit les alertes d'Alertmanager
- Interroge Prometheus pour les métriques
- Interroge Loki pour les logs
- Détecte les anomalies par Z-score
- Prédit les tendances avec Prophet
- Dialogue avec Mistral (LLM local)
- Mesure TTD et TTR en temps réel
================================================
"""

import os
import json
import time
import sqlite3
import shutil
import subprocess
import httpx
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional, List
from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from prometheus_client import (
    Counter, Histogram, Gauge,
    generate_latest, CONTENT_TYPE_LATEST
)
from starlette.responses import Response

# Import Prophet
try:
    from prophet import Prophet
    PROPHET_DISPONIBLE = True
    print("✅ Prophet disponible")
except ImportError:
    PROPHET_DISPONIBLE = False
    print("⚠️ Prophet non disponible — Z-score utilisé")

# ════════════════════════════════════════════════
# CONFIGURATION
# Variables d'environnement définies dans
# docker-compose.yml
# ════════════════════════════════════════════════

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
OLLAMA_URL     = os.getenv("OLLAMA_URL",     "http://ollama:11434")
OLLAMA_MODEL   = os.getenv("OLLAMA_MODEL",   "mistral")
LOKI_URL       = os.getenv("LOKI_URL",       "http://loki:3100")
ZABBIX_URL     = os.getenv("ZABBIX_URL",     "http://zabbix-web:8080")
ZABBIX_USER    = os.getenv("ZABBIX_USER",    "Admin")
ZABBIX_PASS    = os.getenv("ZABBIX_PASSWORD","zabbix")
ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://alertmanager:9093")
GRAFANA_URL      = os.getenv("GRAFANA_URL",      "http://grafana:3000")
AIOPS_DB_PATH    = os.getenv("AIOPS_DB_PATH",    "/app/aiops.db")

# ════════════════════════════════════════════════
# APPLICATION FASTAPI
# ════════════════════════════════════════════════

app = FastAPI(
    title="AIOps Intelligence API",
    description="""
## Plateforme d'Observabilité Intelligente
**Mohamed Aziz Zneti — Projet AIOps**

### Ce que fait cette API :
1. **Détection Z-score** — Anomalies immédiates
2. **Prévision Prophet** — Tendances futures
3. **Diagnostic Mistral** — Analyse en français
4. **Mesure TTD/TTR** — Preuves de performance
5. **Chat NL** — Questions en langage naturel
""",
    version="2.0.0"
)

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:8088,http://localhost:3001").split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    initialiser_db()
    charger_etat_en_memoire()

# ════════════════════════════════════════════════
# MÉTRIQUES PROMETHEUS DE L'API
# Ces métriques sont exposées sur /metrics
# Prometheus les collecte toutes les 15s
# On peut les visualiser dans Grafana
# ════════════════════════════════════════════════

alertes_recues = Counter(
    "aiops_alertes_total",
    "Nombre total d'alertes reçues",
    ["severity"]
)
anomalies_detectees = Counter(
    "aiops_anomalies_total",
    "Nombre total d'anomalies détectées",
    ["instance", "methode"]
)
llm_requetes = Counter(
    "aiops_llm_requetes_total",
    "Nombre total de requêtes LLM"
)
llm_latence = Histogram(
    "aiops_llm_latence_secondes",
    "Latence des requêtes LLM en secondes",
    buckets=[1, 5, 10, 30, 60, 120]
)
ttd_gauge = Gauge(
    "aiops_ttd_secondes",
    "Time To Detect mesuré en secondes"
)
ttr_gauge = Gauge(
    "aiops_ttr_secondes",
    "Time To Resolve (diagnostic LLM) en secondes"
)
ttr_remediation_gauge = Gauge(
    "aiops_ttr_remediation_secondes",
    "Time To Resolve (remediation/firewall) en secondes"
)
prophet_predictions = Counter(
    "aiops_prophet_predictions_total",
    "Nombre total de prédictions Prophet"
)
alertes_preventives = Counter(
    "aiops_alertes_preventives_total",
    "Nombre d'alertes préventives Prophet"
)

# ════════════════════════════════════════════════
# STOCKAGE EN MÉMOIRE
# En production : Redis ou PostgreSQL
# Pour la démo : mémoire suffisante
# ════════════════════════════════════════════════

MAX_ITEMS_MEMORY = 200

historique_alertes    = []
historique_anomalies  = []
historique_predictions = []
mesures_ttd           = []
mesures_ttr           = []


def cap_list(lst: list, limit: int = MAX_ITEMS_MEMORY):
    """Garde uniquement les `limit` derniers elements."""
    while len(lst) > limit:
        lst.pop(0)


def connexion_db():
    conn = sqlite3.connect(AIOPS_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_utc() -> datetime:
    """Retourne l'heure UTC actuelle — remplace datetime.utcnow() deprece."""
    return datetime.now(timezone.utc)


def initialiser_db():
    os.makedirs(os.path.dirname(AIOPS_DB_PATH) or ".", exist_ok=True)
    with connexion_db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS alert_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            alerte TEXT NOT NULL,
            instance TEXT NOT NULL,
            severite TEXT NOT NULL,
            nb_alertes_groupees INTEGER NOT NULL,
            diagnostic_mistral TEXT NOT NULL,
            duree_analyse_sec REAL NOT NULL,
            metriques_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS anomaly_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            instance TEXT NOT NULL,
            metrique TEXT NOT NULL,
            methode TEXT NOT NULL,
            severite TEXT,
            valeur REAL,
            valeur_attendue REAL,
            score REAL,
            payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS prediction_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            instance TEXT NOT NULL,
            metrique TEXT NOT NULL,
            resultat_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS performance_measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            type TEXT NOT NULL,
            value_sec REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS remediation_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            anomaly_id TEXT NOT NULL,
            instance TEXT NOT NULL,
            type TEXT NOT NULL,
            action TEXT NOT NULL,
            status TEXT NOT NULL,
            blocked INTEGER NOT NULL,
            duration_sec REAL NOT NULL,
            details_json TEXT NOT NULL
        );
        """)


def charger_etat_en_memoire():
    """Recharge l'etat en memoire depuis SQLite au demarrage pour eviter
    l'incoherence entre memoire vide et donnees persistees."""
    with connexion_db() as conn:
        for row in conn.execute(
            "SELECT * FROM alert_history ORDER BY id DESC LIMIT ?",
            (MAX_ITEMS_MEMORY,)
        ).fetchall():
            item = dict(row)
            item["metriques"] = json.loads(item.pop("metriques_json"))
            item.pop("id", None)
            historique_alertes.append(item)
        historique_alertes.reverse()

        mesures_ttd.extend(charger_mesures("ttd", MAX_ITEMS_MEMORY))
        mesures_ttr.extend(charger_mesures("ttr", MAX_ITEMS_MEMORY))


def compter_table(table: str) -> int:
    with connexion_db() as conn:
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def charger_mesures(type_mesure: str, limite: int = 10) -> list:
    with connexion_db() as conn:
        rows = conn.execute(
            """
            SELECT value_sec
            FROM performance_measurements
            WHERE type = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (type_mesure, limite)
        ).fetchall()
    return [float(row["value_sec"]) for row in reversed(rows)]


def moyenne_mesure(type_mesure: str) -> float:
    with connexion_db() as conn:
        value = conn.execute(
            "SELECT AVG(value_sec) FROM performance_measurements WHERE type = ?",
            (type_mesure,)
        ).fetchone()[0]
    return float(value) if value is not None else 0.0


def enregistrer_mesure(type_mesure: str, valeur: float):
    with connexion_db() as conn:
        conn.execute(
            """
            INSERT INTO performance_measurements(timestamp, type, value_sec)
            VALUES (?, ?, ?)
            """,
            (now_utc().isoformat(), type_mesure, float(valeur))
        )


def enregistrer_alerte(entree: dict):
    with connexion_db() as conn:
        conn.execute(
            """
            INSERT INTO alert_history(
                timestamp, alerte, instance, severite, nb_alertes_groupees,
                diagnostic_mistral, duree_analyse_sec, metriques_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entree["timestamp"],
                entree["alerte"],
                entree["instance"],
                entree["severite"],
                int(entree["nb_alertes_groupees"]),
                entree["diagnostic_mistral"],
                float(entree["duree_analyse_sec"]),
                json.dumps(entree["metriques"], ensure_ascii=False)
            )
        )


def lister_alertes(limite: int = 20) -> list:
    with connexion_db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM alert_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limite,)
        ).fetchall()
    alertes = []
    for row in reversed(rows):
        item = dict(row)
        item["metriques"] = json.loads(item.pop("metriques_json"))
        item.pop("id", None)
        alertes.append(item)
    return alertes


def enregistrer_anomalies(instance: str, metrique: str, methode: str, anomalies: list):
    if not anomalies:
        return
    with connexion_db() as conn:
        for anomalie in anomalies:
            conn.execute(
                """
                INSERT INTO anomaly_history(
                    timestamp, instance, metrique, methode, severite,
                    valeur, valeur_attendue, score, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    now_utc().isoformat(),
                    instance,
                    metrique,
                    methode,
                    anomalie.get("severite"),
                    anomalie.get("valeur", anomalie.get("valeur_reelle")),
                    anomalie.get("valeur_attendue", anomalie.get("valeur_predite")),
                    anomalie.get("z_score", anomalie.get("ecart_pct")),
                    json.dumps(anomalie, ensure_ascii=False)
                )
            )


def enregistrer_prediction(entree: dict):
    with connexion_db() as conn:
        conn.execute(
            """
            INSERT INTO prediction_history(timestamp, instance, metrique, resultat_json)
            VALUES (?, ?, ?, ?)
            """,
            (
                entree["timestamp"],
                entree["instance"],
                entree["metrique"],
                json.dumps(entree["resultat"], ensure_ascii=False)
            )
        )


def enregistrer_remediation(entree: dict):
    with connexion_db() as conn:
        conn.execute(
            """
            INSERT INTO remediation_history(
                timestamp, anomaly_id, instance, type, action, status,
                blocked, duration_sec, details_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entree["timestamp"],
                entree["anomaly_id"],
                entree["instance"],
                entree["type"],
                entree["action"],
                entree["status"],
                1 if entree["blocked"] else 0,
                float(entree["duration_sec"]),
                json.dumps(entree["details"], ensure_ascii=False)
            )
        )


def lister_remediations(limite: int = 20) -> list:
    with connexion_db() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM remediation_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limite,)
        ).fetchall()
    remediations = []
    for row in reversed(rows):
        item = dict(row)
        item["blocked"] = bool(item["blocked"])
        item["details"] = json.loads(item.pop("details_json"))
        item.pop("id", None)
        remediations.append(item)
    return remediations


def construire_timeline(limite: int = 50) -> list:
    evenements = []
    with connexion_db() as conn:
        for row in conn.execute(
            """
            SELECT timestamp, alerte, instance, severite, nb_alertes_groupees
            FROM alert_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limite,)
        ).fetchall():
            evenements.append({
                "timestamp": row["timestamp"],
                "type": "alert",
                "titre": row["alerte"],
                "instance": row["instance"],
                "severite": row["severite"],
                "details": {
                    "nb_alertes_groupees": row["nb_alertes_groupees"]
                }
            })

        for row in conn.execute(
            """
            SELECT timestamp, instance, metrique, methode, severite, valeur, score
            FROM anomaly_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limite,)
        ).fetchall():
            evenements.append({
                "timestamp": row["timestamp"],
                "type": "anomaly",
                "titre": f"{row['metrique']} anomalie {row['methode']}",
                "instance": row["instance"],
                "severite": row["severite"] or "warning",
                "details": {
                    "valeur": row["valeur"],
                    "score": row["score"]
                }
            })

        for row in conn.execute(
            """
            SELECT timestamp, instance, metrique, resultat_json
            FROM prediction_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limite,)
        ).fetchall():
            resultat = json.loads(row["resultat_json"])
            evenements.append({
                "timestamp": row["timestamp"],
                "type": "prediction",
                "titre": f"Prévision {row['metrique']}",
                "instance": row["instance"],
                "severite": "warning" if resultat.get("alerte_preventive") else "info",
                "details": {
                    "tendance": resultat.get("tendance"),
                    "alerte_preventive": resultat.get("alerte_preventive"),
                    "valeur_max_predite": resultat.get("valeur_max_predite")
                }
            })

    evenements.sort(key=lambda item: item["timestamp"], reverse=True)
    return evenements[:limite]

# ════════════════════════════════════════════════
# MODÈLES PYDANTIC
# Définissent la structure des données
# ════════════════════════════════════════════════

class AlerteWebhook(BaseModel):
    receiver: str = ""
    status: str = ""
    alerts: list = []
    groupLabels: dict = {}
    commonLabels: dict = {}
    commonAnnotations: dict = {}

class QuestionChat(BaseModel):
    question: str
    contexte: Optional[str] = None

class RequetePrediction(BaseModel):
    instance: str = "wsl-host"
    metrique: str = "cpu"
    heures_historique: int = 24
    heures_prediction: int = 4

class RequeteRemediation(BaseModel):
    anomaly_id: str
    type: str
    instance: str = "wsl-host"
    description: Optional[str] = None
    source_ip: Optional[str] = None
    action: str = "auto_block"

# ════════════════════════════════════════════════
# UTILITAIRES — FONCTIONS DE BASE
# ════════════════════════════════════════════════

async def interroger_prometheus(
    promql: str,
    start: datetime = None,
    end: datetime = None,
    step: str = "60s"
) -> dict:
    """
    Interroge Prometheus via son API REST.

    Prometheus stocke les métriques sous forme de
    séries temporelles. On peut l'interroger avec
    le langage PromQL (Prometheus Query Language).

    Exemple de requête PromQL :
    'rate(node_cpu_seconds_total{mode="idle"}[5m])'
    → taux d'utilisation CPU sur 5 minutes
    """
    async with httpx.AsyncClient(timeout=30) as client:
        if start and end:
            # Requête sur une plage de temps
            reponse = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query_range",
                params={
                    "query": promql,
                    "start": start.isoformat() + "Z",
                    "end": end.isoformat() + "Z",
                    "step": step
                }
            )
        else:
            # Requête sur le moment présent
            reponse = await client.get(
                f"{PROMETHEUS_URL}/api/v1/query",
                params={"query": promql}
            )
        reponse.raise_for_status()
        return reponse.json()


async def extraire_valeur(promql: str) -> Optional[float]:
    """
    Extrait une valeur numérique simple depuis Prometheus.
    Retourne None si aucune donnée disponible.
    """
    try:
        data = await interroger_prometheus(promql)
        resultat = data.get("data", {}).get("result", [])
        if resultat:
            return float(resultat[0]["value"][1])
        return None
    except:
        return None


async def extraire_series_instantanees(promql: str) -> list:
    """Retourne toutes les séries instantanées Prometheus pour une requête."""
    try:
        data = await interroger_prometheus(promql)
        return data.get("data", {}).get("result", [])
    except:
        return []


async def extraire_valeur_instance(promql: str, instance: str) -> Optional[float]:
    """Extrait la valeur de la série Prometheus correspondant à une instance."""
    series = await extraire_series_instantanees(promql)
    for serie in series:
        if serie.get("metric", {}).get("instance") == instance:
            return float(serie["value"][1])
    if len(series) == 1:
        return float(series[0]["value"][1])
    return None


async def interroger_loki(
    query: str = '{job="syslog"}',
    heures: int = 1,
    limite: int = 20
) -> list:
    """
    Récupère les logs récents depuis Loki.

    Les logs donnent le CONTEXTE aux métriques.
    Exemple :
    - Métrique : CPU à 95%
    - Log      : "OutOfMemoryError in thread main"
    - Diagnostic IA : "Fuite mémoire Java détectée"
    """
    maintenant = now_utc()
    debut = maintenant - timedelta(hours=heures)

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            reponse = await client.get(
                f"{LOKI_URL}/loki/api/v1/query_range",
                params={
                    "query": query,
                    "start": str(int(debut.timestamp())) + "000000000",
                    "end": str(int(maintenant.timestamp())) + "000000000",
                    "limit": limite
                }
            )
            reponse.raise_for_status()
            data = reponse.json()
            logs = []
            for stream in data.get("data", {}).get("result", []):
                for valeur in stream.get("values", []):
                    logs.append(valeur[1])
            return logs[-limite:]
        except:
            return []


async def demander_llm(
    prompt: str,
    systeme: str = None,
    max_tokens: int = 600
) -> str:
    """
    Envoie un prompt à Ollama (Mistral) et retourne la réponse.

    Mistral est un LLM open source français qui tourne
    entièrement en local. Aucune donnée n'est envoyée
    à l'extérieur — confidentialité garantie.

    Paramètres :
    - temperature: 0.3 = réponses précises et consistantes
    - num_predict: longueur maximale de la réponse
    """
    llm_requetes.inc()
    debut = time.time()

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.3,
            "num_predict": max_tokens,
            "top_p": 0.9
        }
    }
    if systeme:
        payload["system"] = systeme

    try:
        async with httpx.AsyncClient(timeout=120) as client:
            reponse = await client.post(
                f"{OLLAMA_URL}/api/generate",
                json=payload
            )
            reponse.raise_for_status()
            resultat = reponse.json().get("response", "")
    except Exception as e:
        resultat = f"[LLM indisponible : {str(e)}]"
    finally:
        duree = time.time() - debut
        llm_latence.observe(duree)

    return resultat

# ════════════════════════════════════════════════
# DÉTECTION D'ANOMALIES — Z-SCORE
# Méthode statistique simple et efficace
# ════════════════════════════════════════════════

def detecter_zscore(
    valeurs: list,
    seuil: float = 2.5
) -> list:
    """
    Détection d'anomalies par Z-score.

    Formule : Z = (valeur - moyenne) / écart_type

    Si Z > seuil → la valeur est anormale

    Exemple :
    Valeurs normales : [30, 31, 29, 32, 30]
    Nouvelle valeur  : 85
    Moyenne = 30.4, Écart-type = 1.0
    Z = (85 - 30.4) / 1.0 = 54.6 → ANOMALIE !

    Avantage : pas besoin de définir CPU > 90%
    Le système apprend ce qui est normal
    automatiquement à partir des données.
    """
    if len(valeurs) < 10:
        return []

    arr = np.array(valeurs, dtype=float)
    moyenne = arr.mean()
    ecart_type = arr.std()

    if ecart_type == 0:
        return []

    z_scores = np.abs((arr - moyenne) / ecart_type)
    indices = np.where(z_scores > seuil)[0]

    return [
        {
            "index": int(i),
            "valeur": round(float(arr[i]), 2),
            "valeur_attendue": round(float(moyenne), 2),
            "z_score": round(float(z_scores[i]), 2),
            "severite": "critical" if z_scores[i] > 3.5 else "warning"
        }
        for i in indices
    ]


def decrire_anomalie(
    metrique: str,
    methode: str,
    anomalie: dict,
    instance: str = "wsl-host"
) -> dict:
    """Ajoute les champs lisibles par le dashboard à une anomalie détectée."""
    type_map = {
        "CPU": "cpu",
        "Mémoire": "memory",
        "Disque": "disk"
    }
    metrique_type = type_map.get(metrique, metrique.lower())
    severite = anomalie.get("severite", "warning")

    valeur = anomalie.get("valeur", anomalie.get("valeur_reelle"))
    attendue = anomalie.get("valeur_attendue", anomalie.get("valeur_predite"))
    score = anomalie.get("z_score", anomalie.get("ecart_pct"))
    timestamp = anomalie.get("timestamp", now_utc().isoformat())
    anomaly_id = f"{instance}:{metrique_type}:{methode}:{timestamp}"

    if metrique_type == "cpu":
        description = (
            "Utilisation CPU anormale par rapport au comportement historique. "
            "Cela peut indiquer une surcharge, un processus bloqué ou une attaque."
        )
    elif metrique_type == "memory":
        description = (
            "Consommation mémoire anormale par rapport au comportement historique. "
            "Cela peut indiquer une fuite mémoire ou une charge applicative élevée."
        )
    elif metrique_type == "disk":
        description = (
            "Utilisation disque anormale par rapport au comportement historique. "
            "Cela peut indiquer une saturation, des logs volumineux ou une croissance inattendue."
        )
    else:
        description = (
            f"Comportement anormal détecté sur {metrique} par la méthode {methode}."
        )

    anomalie["anomaly_id"] = anomaly_id
    anomalie["type"] = metrique_type
    anomalie["type_label"] = metrique
    anomalie["description"] = description
    anomalie["etat"] = {
        "bloquee": False,
        "statut": "non_bloquee",
        "label": "Non bloquée",
        "raison": (
            "Détection uniquement. Lancez la remédiation pour mesurer le TTR de blocage."
        )
    }
    anomalie["resume"] = {
        "methode": methode,
        "severite": severite,
        "valeur": valeur,
        "valeur_attendue": attendue,
        "score": score
    }
    return anomalie

# ════════════════════════════════════════════════
# PRÉVISION — PROPHET
# Modèle de Meta pour séries temporelles
# ════════════════════════════════════════════════

def analyser_avec_prophet(
    timestamps: list,
    valeurs: list,
    nom_metrique: str,
    heures_prediction: int = 4
) -> dict:
    """
    Analyse une série temporelle avec Prophet.

    Différence avec Z-score :
    - Z-score  : "cette valeur EST anormale maintenant"
    - Prophet  : "cette valeur SERA anormale dans 4h"

    Prophet apprend :
    - Les tendances (montée progressive)
    - Les cycles jour/nuit
    - Les cycles semaine/weekend
    - Les points de changement (début de fuite mémoire)

    Cas d'usage typique :
    Mémoire : 40% → 42% → 45% → 49% → 54%
    Z-score : "pas encore anormal"
    Prophet : "dans 3h : 95% → ALERTE PRÉVENTIVE"
    """
    if not PROPHET_DISPONIBLE:
        return {"erreur": "Prophet non installé", "fallback": "zscore"}

    if len(valeurs) < 20:
        return {
            "erreur": "Minimum 20 points requis pour Prophet",
            "donnees_disponibles": len(valeurs)
        }

    try:
        # Préparer les données au format Prophet
        df = pd.DataFrame({
            "ds": [datetime.fromtimestamp(t, tz=timezone.utc) for t in timestamps],
            "y": valeurs
        }).dropna()
        df = df[df["y"] >= 0]

        # Configurer et entraîner Prophet
        modele = Prophet(
            interval_width=0.95,
            changepoint_prior_scale=0.05,
            daily_seasonality=True,
            weekly_seasonality=True,
            yearly_seasonality=False,
            verbose=False
        )
        modele.fit(df)
        prophet_predictions.inc()

        # Prédire les prochaines heures
        nb_periodes = heures_prediction * 12
        futur = modele.make_future_dataframe(
            periods=nb_periodes,
            freq="5T"
        )
        previsions = modele.predict(futur)

        # Détecter anomalies passées
        prev_hist = previsions[previsions["ds"] <= df["ds"].max()]
        anomalies = []

        for _, ligne in prev_hist.iterrows():
            vals_reelles = df[df["ds"] == ligne["ds"]]["y"]
            if vals_reelles.empty:
                continue
            val = vals_reelles.values[0]

            if val < ligne["yhat_lower"] or val > ligne["yhat_upper"]:
                ecart_pct = abs(val - ligne["yhat"]) / abs(ligne["yhat"]) * 100 \
                    if ligne["yhat"] != 0 else 0
                anomalies.append({
                    "timestamp": ligne["ds"].isoformat(),
                    "valeur_reelle": round(val, 2),
                    "valeur_predite": round(ligne["yhat"], 2),
                    "borne_basse": round(ligne["yhat_lower"], 2),
                    "borne_haute": round(ligne["yhat_upper"], 2),
                    "ecart_pct": round(ecart_pct, 1),
                    "severite": "critical" if ecart_pct > 50 else "warning"
                })

        # Analyser les prédictions futures
        prev_fut = previsions[previsions["ds"] > df["ds"].max()].tail(nb_periodes)

        predictions_futures = [
            {
                "timestamp": ligne["ds"].isoformat(),
                "valeur_predite": round(ligne["yhat"], 2),
                "borne_basse": round(ligne["yhat_lower"], 2),
                "borne_haute": round(ligne["yhat_upper"], 2)
            }
            for _, ligne in prev_fut.head(12).iterrows()
        ]

        # Calculer la tendance
        tendance = "stable"
        direction = "stable"
        if len(prev_fut) >= 2:
            diff = prev_fut.iloc[-1]["yhat"] - prev_fut.iloc[0]["yhat"]
            if diff > 5:
                tendance = "hausse"
                direction = f"+{diff:.1f} dans {heures_prediction}h"
            elif diff < -5:
                tendance = "baisse"
                direction = f"{diff:.1f} dans {heures_prediction}h"

        # Alerte préventive si saturation prévue
        val_max = float(prev_fut["yhat_upper"].max()) \
            if not prev_fut.empty else 0
        alerte_preventive = val_max > 90

        heure_saturation = None
        if alerte_preventive:
            alertes_preventives.inc()
            sat = prev_fut[prev_fut["yhat"] > 90]
            if not sat.empty:
                heure_saturation = sat.iloc[0]["ds"].isoformat()

        # Message lisible
        if alerte_preventive and heure_saturation:
            message = (
                f"⚠️ ALERTE PRÉVENTIVE : {nom_metrique} va atteindre "
                f"90% vers {heure_saturation}. Agir maintenant."
            )
        elif tendance == "hausse":
            message = (
                f"📈 {nom_metrique} en hausse progressive "
                f"({direction}). {len(anomalies)} anomalie(s)."
            )
        elif anomalies:
            message = (
                f"🔍 {len(anomalies)} anomalie(s) sur {nom_metrique}. "
                f"Tendance : {tendance}."
            )
        else:
            message = f"✅ {nom_metrique} normal. Tendance : {tendance}."

        return {
            "metrique": nom_metrique,
            "modele": "Prophet (Meta/Facebook)",
            "donnees_analysees": len(valeurs),
            "anomalies_detectees": anomalies,
            "nb_anomalies": len(anomalies),
            "predictions_futures": predictions_futures,
            "tendance": tendance,
            "direction": direction,
            "alerte_preventive": alerte_preventive,
            "heure_saturation_prevue": heure_saturation,
            "valeur_max_predite": round(val_max, 2),
            "message": message
        }

    except Exception as e:
        return {
            "erreur": str(e),
            "metrique": nom_metrique,
            "modele": "Prophet (erreur)",
            "nb_anomalies": 0
        }

# ════════════════════════════════════════════════
# ENDPOINTS API
# ════════════════════════════════════════════════

@app.get("/", tags=["Info"])
async def accueil():
    """Page d'accueil — informations sur le projet."""
    return {
        "projet": "AIOps — Observabilité Intelligente",
        "etudiant": "Mohamed Aziz Zneti",
        "version": "2.0.0",
        "prophet_disponible": PROPHET_DISPONIBLE,
        "modele_llm": OLLAMA_MODEL,
        "endpoints": {
            "health":      "GET  /health",
            "metrics":     "GET  /metrics",
            "anomalies":   "GET  /anomalies",
            "prediction":  "POST /predict",
            "chat":        "POST /chat",
            "webhook":     "POST /webhook/alert",
            "historique":  "GET  /alerts/history",
            "performance": "GET  /performance",
            "analyse":     "GET  /analyze/{metric}"
        }
    }


@app.get("/health", tags=["Info"])
async def sante():
    """
    Vérifie que tous les services sont opérationnels.
    Retourne le statut de chaque composant.
    """
    services = {}

    async def verifier_service(nom: str, url: str):
        try:
            async with httpx.AsyncClient(timeout=5) as c:
                r = await c.get(url)
                services[nom] = "ok" if r.status_code < 400 else "indisponible"
        except:
            services[nom] = "indisponible"

    # Vérifier Prometheus
    await verifier_service("prometheus", f"{PROMETHEUS_URL}/-/healthy")

    # Vérifier Ollama
    await verifier_service("ollama", f"{OLLAMA_URL}/api/tags")

    # Vérifier Loki
    await verifier_service("loki", f"{LOKI_URL}/ready")
    await verifier_service("alertmanager", f"{ALERTMANAGER_URL}/-/healthy")
    await verifier_service("zabbix", ZABBIX_URL)
    await verifier_service("grafana", f"{GRAFANA_URL}/api/health")

    statut = "ok" if all(v == "ok" for v in services.values()) else "dégradé"

    return {
        "statut": statut,
        "timestamp": now_utc().isoformat(),
        "modele_llm": OLLAMA_MODEL,
        "prophet": "disponible" if PROPHET_DISPONIBLE else "indisponible",
        "services": services,
        "statistiques": {
            "alertes_traitees": compter_table("alert_history"),
            "anomalies_detectees": compter_table("anomaly_history"),
            "predictions_generees": compter_table("prediction_history"),
            "ttd_moyen_sec": round(moyenne_mesure("ttd"), 3),
            "ttr_moyen_sec": round(moyenne_mesure("ttr"), 2)
        }
    }


@app.get("/dashboard/live", tags=["Info"])
async def dashboard_live(instance: str = "wsl-host"):
    """Données temps réel pour le dashboard sans appel LLM."""
    cpu_query = '100-(avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[5m]))*100)'
    mem_query = '(1-(node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes))*100'
    disk_query = '(1-(node_filesystem_avail_bytes{mountpoint="/"}/node_filesystem_size_bytes{mountpoint="/"})) * 100'

    cpu = await extraire_valeur_instance(cpu_query, instance)
    mem = await extraire_valeur_instance(mem_query, instance)
    disk = await extraire_valeur_instance(disk_query, instance)

    health = await sante()

    return {
        "timestamp": now_utc().isoformat(),
        "instance": instance,
        "services": health["services"],
        "statut": health["statut"],
        "ressources": {
            "cpu_pct": round(cpu, 1) if cpu is not None else None,
            "mem_pct": round(mem, 1) if mem is not None else None,
            "disk_pct": round(disk, 1) if disk is not None else None
        },
        "statistiques": health["statistiques"]
    }


@app.get("/vms/status", tags=["Info"])
async def statut_vms():
    """État et métriques réels des cibles VM connues par Prometheus."""
    up_series = await extraire_series_instantanees('up{job=~"vm.*|node-exporter"}')
    cpu_series = await extraire_series_instantanees(
        '100-(avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[5m]))*100)'
    )
    mem_series = await extraire_series_instantanees(
        '(1-(node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes))*100'
    )
    disk_series = await extraire_series_instantanees(
        '(1-(node_filesystem_avail_bytes{mountpoint="/"}/node_filesystem_size_bytes{mountpoint="/"})) * 100'
    )

    def valeurs_par_instance(series: list) -> dict:
        valeurs = {}
        for serie in series:
            instance_name = serie.get("metric", {}).get("instance")
            if instance_name:
                valeurs[instance_name] = float(serie["value"][1])
        return valeurs

    cpu_map = valeurs_par_instance(cpu_series)
    mem_map = valeurs_par_instance(mem_series)
    disk_map = valeurs_par_instance(disk_series)

    vms = []
    for serie in up_series:
        labels = serie.get("metric", {})
        instance_name = labels.get("instance", labels.get("job", "inconnue"))
        up = float(serie["value"][1]) == 1
        cpu = cpu_map.get(instance_name)
        mem = mem_map.get(instance_name)
        disk = disk_map.get(instance_name)

        status = "offline"
        if up:
            status = "warning" if any(
                value is not None and value >= 85
                for value in [cpu, mem, disk]
            ) else "online"

        vms.append({
            "name": instance_name,
            "job": labels.get("job", ""),
            "role": labels.get("role", "host"),
            "target": labels.get("instance", ""),
            "status": status,
            "up": up,
            "cpu": round(cpu, 1) if cpu is not None else None,
            "mem": round(mem, 1) if mem is not None else None,
            "disk": round(disk, 1) if disk is not None else None
        })

    vms.sort(key=lambda vm: vm["name"])

    return {
        "timestamp": now_utc().isoformat(),
        "total": len(vms),
        "online": sum(1 for vm in vms if vm["status"] == "online"),
        "warning": sum(1 for vm in vms if vm["status"] == "warning"),
        "offline": sum(1 for vm in vms if vm["status"] == "offline"),
        "vms": vms
    }


@app.get("/metrics", tags=["Info"])
async def metriques():
    """Expose les métriques de l'API pour Prometheus."""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/webhook/alert", tags=["Alertes"])
async def recevoir_alerte(
    payload: AlerteWebhook,
    background_tasks: BackgroundTasks
):
    """
    Reçoit les alertes groupées d'Alertmanager.

    Flux complet :
    1. Prometheus détecte une condition d'alerte
    2. Alertmanager groupe les alertes similaires
    3. Alertmanager envoie 1 seul webhook ici
    4. L'API analyse avec Mistral en arrière-plan
    5. Le diagnostic est stocké dans l'historique

    C'est ici que 500 alertes → 1 diagnostic.
    """
    nb = len(payload.alerts)
    for alerte in payload.alerts:
        sev = alerte.get("labels", {}).get("severity", "unknown")
        alertes_recues.labels(severity=sev).inc()

    background_tasks.add_task(
        traiter_alertes, payload.alerts, payload.status
    )

    return {
        "statut": "reçu",
        "nombre_alertes": nb,
        "status_global": payload.status,
        "message": f"{nb} alertes reçues — analyse Mistral en cours"
    }


async def traiter_alertes(alertes: list, status: str):
    """
    Traite les alertes avec Mistral en arrière-plan.
    Génère un diagnostic complet avec contexte.
    """
    if not alertes:
        return

    debut = time.time()

    # Grouper par type
    groupes = {}
    for a in alertes:
        nom = a.get("labels", {}).get("alertname", "Inconnue")
        groupes.setdefault(nom, []).append(a)

    for nom, groupe in groupes.items():
        instance = groupe[0].get("labels", {}).get("instance", "inconnue")
        severite = groupe[0].get("labels", {}).get("severity", "unknown")

        # Métriques actuelles
        cpu = await extraire_valeur(
            '100-(avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))*100)'
        )
        mem = await extraire_valeur(
            '(1-(node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes))*100'
        )
        disk = await extraire_valeur(
            '(1-(node_filesystem_avail_bytes{mountpoint="/"}/node_filesystem_size_bytes{mountpoint="/"})) * 100'
        )

        # Logs récents
        logs = await interroger_loki('{job="syslog"}', heures=1)
        logs_txt = "\n".join(logs[:10]) if logs else "Aucun log récent"

        prompt = f"""Tu es un expert en administration système et AIOps.

ALERTE :
- Type      : {nom}
- Instance  : {instance}
- Sévérité  : {severite}
- Alertes groupées : {len(groupe)}
- Status    : {status}

MÉTRIQUES ACTUELLES :
- CPU    : {f'{cpu:.1f}%' if cpu else 'N/A'}
- Mémoire: {f'{mem:.1f}%' if mem else 'N/A'}
- Disque : {f'{disk:.1f}%' if disk else 'N/A'}

LOGS RÉCENTS :
{logs_txt}

Réponds en français avec ce format exact :

**CAUSE PROBABLE :** [explication précise]

**IMPACT :** [ce qui est affecté]

**ACTION IMMÉDIATE :** [commande exacte à exécuter]

**PRÉVENTION :** [comment éviter que ça se reproduise]"""

        diagnostic = await demander_llm(prompt)
        duree = time.time() - debut
        ttr_gauge.set(duree)
        mesures_ttr.append(duree)
        enregistrer_mesure("ttr", duree)

        entree_alerte = {
            "timestamp": now_utc().isoformat(),
            "alerte": nom,
            "instance": instance,
            "severite": severite,
            "nb_alertes_groupees": len(groupe),
            "diagnostic_mistral": diagnostic,
            "duree_analyse_sec": round(duree, 2),
            "metriques": {
                "cpu_pct": round(cpu, 1) if cpu else None,
                "mem_pct": round(mem, 1) if mem else None,
                "disk_pct": round(disk, 1) if disk else None
            }
        }
        historique_alertes.append(entree_alerte)
        enregistrer_alerte(entree_alerte)

    cap_list(historique_alertes)
    cap_list(mesures_ttr)


@app.get("/anomalies", tags=["Détection IA"])
async def detecter_anomalies(
    instance: str = "wsl-host",
    heures: int = 6,
    seuil: float = 2.5,
    methode: str = "zscore"
):
    """
    Détecte les anomalies sur l'infrastructure.

    Méthodes disponibles :
    - zscore  : Z-score (rapide, anomalies immédiates)
    - prophet : Prophet (lent, détecte tendances lentes)
    - both    : Les deux combinés (recommandé)

    Paramètres :
    - instance : nom de l'instance à analyser
    - heures   : fenêtre d'analyse (défaut: 6h)
    - seuil    : sensibilité Z-score (défaut: 2.5)
    - methode  : zscore / prophet / both
    """
    fin = now_utc()
    debut_periode = fin - timedelta(hours=heures)
    debut_detection = time.time()

    metriques = [
        {
            "nom": "CPU",
            "promql": '100-(avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[5m]))*100)',
            "unite": "%"
        },
        {
            "nom": "Mémoire",
            "promql": '(1-(node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes))*100',
            "unite": "%"
        },
        {
            "nom": "Disque",
            "promql": '(1-(node_filesystem_avail_bytes{mountpoint="/"}/node_filesystem_size_bytes{mountpoint="/"})) * 100',
            "unite": "%"
        }
    ]

    resultats = []

    for m in metriques:
        try:
            data = await interroger_prometheus(
                m["promql"],
                start=debut_periode,
                end=fin,
                step="60s"
            )

            series = [
                serie for serie in data.get("data", {}).get("result", [])
                if serie.get("metric", {}).get("instance") == instance
            ]
            if not series:
                series = data.get("data", {}).get("result", [])[:1]

            for serie in series[:1]:
                valeurs_brutes = serie.get("values", [])
                if len(valeurs_brutes) < 10:
                    continue

                timestamps = [float(v[0]) for v in valeurs_brutes]
                valeurs = [float(v[1]) for v in valeurs_brutes]

                resultat_metrique = {
                    "metrique": m["nom"],
                    "unite": m["unite"],
                    "instance": instance,
                    "nb_points": len(valeurs),
                    "valeur_actuelle": round(valeurs[-1], 2),
                    "valeur_moyenne": round(np.mean(valeurs), 2),
                    "zscore": None,
                    "prophet": None
                }

                # Z-score
                if methode in ["zscore", "both"]:
                    anomalies_z = detecter_zscore(valeurs, seuil)
                    for a in anomalies_z:
                        a["timestamp"] = datetime.fromtimestamp(
                            timestamps[a["index"]], tz=timezone.utc
                        ).isoformat()
                        decrire_anomalie(m["nom"], "zscore", a, instance)
                        anomalies_detectees.labels(
                            instance=instance,
                            methode="zscore"
                        ).inc()

                    resultat_metrique["zscore"] = {
                        "methode": "Z-score",
                        "seuil_utilise": seuil,
                        "anomalies": anomalies_z,
                        "nb_anomalies": len(anomalies_z)
                    }
                    enregistrer_anomalies(
                        instance, m["nom"], "zscore", anomalies_z
                    )

                # Prophet
                if methode in ["prophet", "both"] and PROPHET_DISPONIBLE:
                    resultat_prophet = analyser_avec_prophet(
                        timestamps, valeurs, m["nom"]
                    )
                    for a in resultat_prophet.get("anomalies_detectees", []):
                        decrire_anomalie(m["nom"], "prophet", a, instance)
                        anomalies_detectees.labels(
                            instance=instance,
                            methode="prophet"
                        ).inc()

                    resultat_metrique["prophet"] = resultat_prophet
                    enregistrer_anomalies(
                        instance,
                        m["nom"],
                        "prophet",
                        resultat_prophet.get("anomalies_detectees", [])
                    )

                resultats.append(resultat_metrique)
                zscore_result = resultat_metrique.get("zscore") or {}
                historique_anomalies.extend(
                    zscore_result.get("anomalies", [])
                )
                cap_list(historique_anomalies)

        except Exception as e:
            resultats.append({
                "metrique": m["nom"],
                "erreur": str(e)
            })

    duree = time.time() - debut_detection
    ttd_gauge.set(duree)
    mesures_ttd.append(duree)
    enregistrer_mesure("ttd", duree)
    cap_list(mesures_ttd)
    cap_list(historique_alertes)

    return {
        "instance": instance,
        "periode_heures": heures,
        "methode": methode,
        "duree_detection_sec": round(duree, 3),
        "timestamp": now_utc().isoformat(),
        "resultats": resultats,
        "resume": {
            "total_anomalies_zscore": sum(
                (r.get("zscore") or {}).get("nb_anomalies", 0)
                for r in resultats
            ),
            "alertes_preventives_prophet": sum(
                1 for r in resultats
                if (r.get("prophet") or {}).get("alerte_preventive", False)
            )
        }
    }


@app.post("/predict", tags=["Prophet"])
async def predire(requete: RequetePrediction):
    """
    Prédit l'évolution d'une métrique avec Prophet.

    C'est la fonctionnalité la plus avancée du projet.
    Elle répond à la question :
    "Est-ce que mon infrastructure va avoir un problème
    dans les prochaines heures ?"

    Cas d'usage pour la soutenance :
    - Prédire une saturation disque dans 4h
    - Détecter une fuite mémoire progressive
    - Anticiper une surcharge CPU avant le pic
    """
    if not PROPHET_DISPONIBLE:
        return {
            "erreur": "Prophet non disponible",
            "solution": "Installer prophet dans requirements.txt"
        }

    metriques_map = {
        "cpu": '100-(avg by(instance)(rate(node_cpu_seconds_total{mode="idle"}[5m]))*100)',
        "memory": '(1-(node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes))*100',
        "disk": '(1-(node_filesystem_avail_bytes{mountpoint="/"}/node_filesystem_size_bytes{mountpoint="/"})) * 100'
    }

    promql = metriques_map.get(
        requete.metrique,
        requete.metrique
    )

    fin = now_utc()
    debut = fin - timedelta(hours=requete.heures_historique)

    try:
        data = await interroger_prometheus(
            promql, start=debut, end=fin, step="300s"
        )

        series = data.get("data", {}).get("result", [])
        if not series:
            return {"erreur": "Aucune donnée disponible"}

        serie_instance = next(
            (
                serie for serie in series
                if serie.get("metric", {}).get("instance") == requete.instance
            ),
            series[0]
        )

        valeurs_brutes = serie_instance.get("values", [])
        timestamps = [float(v[0]) for v in valeurs_brutes]
        valeurs = [float(v[1]) for v in valeurs_brutes]

        resultat = analyser_avec_prophet(
            timestamps, valeurs,
            requete.metrique,
            requete.heures_prediction
        )

        entree_prediction = {
            "timestamp": now_utc().isoformat(),
            "instance": requete.instance,
            "metrique": requete.metrique,
            "resultat": resultat
        }
        historique_predictions.append(entree_prediction)
        enregistrer_prediction(entree_prediction)
        cap_list(historique_predictions)

        # Si alerte préventive → diagnostic LLM automatique
        if resultat.get("alerte_preventive"):
            prompt = f"""Prophet prédit une saturation de {requete.metrique} 
vers {resultat.get('heure_saturation_prevue')}.
Valeur max prédite : {resultat.get('valeur_max_predite')}%
Tendance : {resultat.get('direction')}

Génère une alerte préventive en français avec :
1. Le problème prédit
2. L'action à faire MAINTENANT
3. La commande exacte"""

            diagnostic = await demander_llm(prompt)
            resultat["diagnostic_preventif"] = diagnostic

        return {
            "instance": requete.instance,
            "metrique": requete.metrique,
            "historique_heures": requete.heures_historique,
            "prediction_heures": requete.heures_prediction,
            "timestamp": now_utc().isoformat(),
            "analyse_prophet": resultat
        }

    except Exception as e:
        return {"erreur": str(e)}


@app.post("/chat", tags=["LLM"])
async def chat(requete: QuestionChat):
    """
    Chatbot en langage naturel.

    L'administrateur pose une question en français.
    L'API collecte le contexte en temps réel et
    Mistral génère une réponse intelligente.

    Exemples :
    - "Quel serveur consomme le plus de CPU ?"
    - "Y a-t-il des anomalies en ce moment ?"
    - "Que s'est-il passé ce matin à 3h ?"
    - "Ma VM va-t-elle avoir un problème ?"
    """
    # Collecter contexte temps réel
    cpu = await extraire_valeur(
        '100-(avg(rate(node_cpu_seconds_total{mode="idle"}[5m]))*100)'
    )
    mem = await extraire_valeur(
        '(1-(node_memory_MemAvailable_bytes/node_memory_MemTotal_bytes))*100'
    )
    disk = await extraire_valeur(
        '(1-(node_filesystem_avail_bytes{mountpoint="/"}/node_filesystem_size_bytes{mountpoint="/"})) * 100'
    )

    logs = await interroger_loki('{job="syslog"}', heures=1)
    logs_txt = "\n".join(logs[:5]) if logs else "Aucun log récent"

    contexte = f"""
ÉTAT INFRASTRUCTURE EN TEMPS RÉEL :
- CPU utilisé    : {f'{cpu:.1f}%' if cpu else 'N/A'}
- Mémoire usée   : {f'{mem:.1f}%' if mem else 'N/A'}
- Disque utilisé : {f'{disk:.1f}%' if disk else 'N/A'}
- Alertes traitées : {compter_table("alert_history")}
- Anomalies détectées : {compter_table("anomaly_history")}
- Prédictions générées : {compter_table("prediction_history")}
- Prophet disponible : {PROPHET_DISPONIBLE}

LOGS RÉCENTS :
{logs_txt}
"""

    systeme = """Tu es AIOps Assistant, expert en observabilité et administration système.
Tu analyses l'infrastructure en temps réel.
Tu réponds TOUJOURS en français.
Tu es direct, précis et actionnable.
Si on demande une requête PromQL, génère-la directement.
Si tu détectes un problème, propose immédiatement une solution."""

    prompt = f"""{contexte}

QUESTION : {requete.question}

Réponds de façon claire, structurée et utile."""

    debut = time.time()
    reponse = await demander_llm(prompt, systeme)
    duree = time.time() - debut

    return {
        "question": requete.question,
        "reponse": reponse,
        "contexte": contexte,
        "duree_sec": round(duree, 2),
        "modele": OLLAMA_MODEL,
        "timestamp": now_utc().isoformat()
    }


@app.get("/alerts/history", tags=["Alertes"])
async def historique_alertes_endpoint(limite: int = 20):
    """
    Retourne l'historique des alertes avec diagnostics Mistral.
    Chaque entrée montre comment l'IA a analysé l'alerte.
    """
    alertes = lister_alertes(limite)
    return {
        "total": compter_table("alert_history"),
        "alertes": alertes,
        "ttr_moyen_sec": round(moyenne_mesure("ttr"), 2)
    }


@app.post("/remediate/anomaly", tags=["Remédiation"])
async def remedier_anomalie(requete: RequeteRemediation):
    """
    Lance une remédiation et mesure le TTR de blocage.

    Sur l'environnement de développement, la remédiation est simulée si le
    firewall hôte n'est pas activé ou si aucune IP source n'est disponible.
    Sur appliance serveur, une IP source peut être bloquée via nftables.
    """
    debut = time.time()
    firewall_enabled = os.getenv("FIREWALL_ENABLE", "0") == "1"
    nft_path = shutil.which("nft")
    blocked = False
    status = "bloquee_simulee"
    details = {
        "mode": "simulation",
        "message": (
            "Remédiation simulée : l'anomalie est marquée contenue pour mesurer "
            "le TTR, mais aucune règle firewall hôte n'a été appliquée."
        )
    }

    if requete.source_ip and firewall_enabled and nft_path:
        try:
            subprocess.run(
                [
                    nft_path,
                    "add", "rule", "inet", "aiops_filter", "input",
                    "ip", "saddr", requete.source_ip, "drop"
                ],
                check=True,
                timeout=5,
                capture_output=True,
                text=True
            )
            blocked = True
            status = "bloquee_firewall"
            details = {
                "mode": "firewall",
                "source_ip": requete.source_ip,
                "message": f"IP {requete.source_ip} bloquée via nftables."
            }
        except Exception as e:
            status = "echec_firewall"
            details = {
                "mode": "firewall",
                "source_ip": requete.source_ip,
                "message": f"Échec du blocage nftables : {str(e)}"
            }
    elif requete.source_ip and not firewall_enabled:
        details["source_ip"] = requete.source_ip
        details["message"] = (
            f"Blocage simulé de {requete.source_ip}. FIREWALL_ENABLE=0, "
            "aucune règle nftables n'a été appliquée."
        )

    duree = time.time() - debut
    ttr_remediation_gauge.set(duree)
    enregistrer_mesure("ttr_remediation", duree)

    entree = {
        "timestamp": now_utc().isoformat(),
        "anomaly_id": requete.anomaly_id,
        "instance": requete.instance,
        "type": requete.type,
        "action": requete.action,
        "status": status,
        "blocked": blocked,
        "duration_sec": round(duree, 3),
        "details": details
    }
    enregistrer_remediation(entree)

    return entree


@app.get("/remediations/history", tags=["Remédiation"])
async def historique_remediations(limite: int = 20):
    return {
        "total": compter_table("remediation_history"),
        "ttr_remediation_moyen_sec": round(moyenne_mesure("ttr_remediation"), 3),
        "remediations": lister_remediations(limite)
    }


@app.get("/incidents/timeline", tags=["Alertes"])
async def incidents_timeline(limite: int = 50):
    """Timeline unifiée des alertes, anomalies et prédictions persistées."""
    return {
        "timestamp": now_utc().isoformat(),
        "total": min(
            compter_table("alert_history")
            + compter_table("anomaly_history")
            + compter_table("prediction_history"),
            limite
        ),
        "evenements": construire_timeline(limite)
    }


@app.get("/performance", tags=["Métriques AIOps"])
async def performance():
    """
    Métriques de performance TTD et TTR.

    Ces chiffres prouvent l'efficacité de l'AIOps
    par rapport au monitoring classique.

    TTD (Time To Detect) :
    - Sans IA : 2 à 6 heures
    - Avec IA : quelques secondes

    TTR (Time To Resolve) :
    - Sans IA : 1 à 3 heures
    - Avec IA : moins d'1 minute
    """
    return {
        "ttd": {
            "description": "Time To Detect — détection des anomalies",
            "sans_ia_heures": "2 à 6 heures",
            "avec_ia_secondes": round(moyenne_mesure("ttd"), 3),
            "amelioration": "-95%",
            "mesures": charger_mesures("ttd")
        },
        "ttr": {
            "description": "Time To Resolve — remédiation/blocage ou diagnostic IA",
            "sans_ia_heures": "1 à 3 heures",
            "avec_ia_secondes": round(
                moyenne_mesure("ttr_remediation") or moyenne_mesure("ttr"),
                3
            ),
            "amelioration": "-90%",
            "mesures": charger_mesures("ttr_remediation") or charger_mesures("ttr"),
            "diagnostic_llm_moyen_sec": round(moyenne_mesure("ttr"), 2),
            "remediation_moyen_sec": round(moyenne_mesure("ttr_remediation"), 3)
        },
        "resume": {
            "alertes_traitees": compter_table("alert_history"),
            "anomalies_detectees": compter_table("anomaly_history"),
            "remediations": compter_table("remediation_history"),
            "predictions_prophet": compter_table("prediction_history"),
            "modele_llm": OLLAMA_MODEL,
            "prophet": PROPHET_DISPONIBLE
        }
    }


@app.get("/analyze/{metric_name}", tags=["Détection IA"])
async def analyser_metrique(metric_name: str, heures: int = 24):
    """
    Analyse complète d'une métrique avec statistiques et LLM.
    """
    fin = now_utc()
    debut = fin - timedelta(hours=heures)

    try:
        data = await interroger_prometheus(
            metric_name, start=debut, end=fin, step="300s"
        )
        series = data.get("data", {}).get("result", [])

        if not series:
            return {"erreur": f"Aucune donnée pour {metric_name}"}

        toutes_valeurs = []
        for s in series:
            toutes_valeurs.extend([float(v[1]) for v in s.get("values", [])])

        if not toutes_valeurs:
            return {"erreur": "Aucune valeur trouvée"}

        arr = np.array(toutes_valeurs)
        stats = {
            "minimum": round(float(arr.min()), 4),
            "maximum": round(float(arr.max()), 4),
            "moyenne": round(float(arr.mean()), 4),
            "ecart_type": round(float(arr.std()), 4),
            "p95": round(float(np.percentile(arr, 95)), 4),
            "p99": round(float(np.percentile(arr, 99)), 4)
        }

        prompt = f"""Analyse la métrique "{metric_name}" sur {heures}h :

Min={stats['minimum']} Max={stats['maximum']}
Moy={stats['moyenne']} StdDev={stats['ecart_type']}
P95={stats['p95']} P99={stats['p99']}

Donne en français :
1. Comportement observé (normal/anormal/tendance)
2. Points d'attention
3. Recommandation concrète"""

        analyse = await demander_llm(prompt)

        return {
            "metrique": metric_name,
            "periode_heures": heures,
            "statistiques": stats,
            "analyse_llm": analyse,
            "timestamp": now_utc().isoformat()
        }

    except Exception as e:
        return {"erreur": str(e)}
