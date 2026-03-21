import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')

import discord
from discord.ext import commands
from datetime import datetime, timedelta
import sqlite3
import time
import os
import asyncio
import pytz
from dotenv import load_dotenv

# Chargement des variables d'environnement depuis le fichier .env (en local)
# Sur Railway, ces variables sont définies directement dans l'interface web
load_dotenv()

# Fuseau horaire utilisé pour convertir les dates des messages (stockées en UTC par Discord)
paris_tz = pytz.timezone("Europe/Paris")

# Nombre de messages accumulés en mémoire avant chaque écriture en base de données
# Plus la valeur est grande, moins il y a d'écritures disque, mais plus la mémoire est sollicitée
BUFFER_SIZE = 1000

# Chemin vers le fichier SQLite, configurable via variable d'environnement
# Par défaut : messages.db dans le répertoire courant
DB_PATH = os.getenv("DB_PATH", "messages.db")

# Configuration des intents Discord : détermine quels événements le bot peut recevoir
# Intents.all() est nécessaire pour accéder à l'historique des messages et aux fils archivés
intents = discord.Intents.all()
bot = commands.Bot(command_prefix="/botchan ", description="Bot-chan", intents=intents)


@bot.event
async def on_ready() -> None:
    """Événement déclenché quand le bot est connecté et prêt à recevoir des commandes."""
    print(f"Bot-chan est opérationnelle. Connectée en tant que {bot.user}")


async def _collect_history(
    source,
    name: str,
    label: str,
    buffer: list,
    conn: sqlite3.Connection,
    begin_date: datetime,
    end_date: datetime
) -> int:
    """
    Parcourt l'historique d'un salon ou fil et alimente le buffer de messages.

    Factorisation de la logique commune à tous les types de salons (textuels,
    fils actifs, fils archivés, salons vocaux) : récupération, mise en buffer,
    flush vers SQLite et comptage.

    Args:
        source: Le salon ou fil Discord dont on parcourt l'historique.
        name: Le nom du salon ou fil, utilisé pour l'insertion en base.
        label: Libellé affiché dans les logs (ex. "salon", "fils actif").
        buffer: La liste partagée dans laquelle les messages sont accumulés.
        conn: La connexion SQLite active.
        begin_date: Date de début de la période (incluse).
        end_date: Date de fin de la période (exclue).

    Returns:
        Le nombre de messages collectés depuis cette source.
    """
    count = 0
    async for message in source.history(limit=None, after=begin_date, before=end_date):
        buffer.append(_build_row(message, name))
        if len(buffer) >= BUFFER_SIZE:
            _store_buffer(conn, buffer)
            buffer.clear()
        count += 1
        print(f"{count} messages comptés ({label}).")
    return count


def _create_daily_stats_table(conn: sqlite3.Connection) -> None:
    """
    Crée la table daily_stats si elle n'existe pas encore.

    Cette table agrège le nombre de messages par jour,
    calculé à partir des données de la table messages.
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_stats (
            date TEXT PRIMARY KEY,
            message_count INTEGER
        )
    """)
    conn.commit()


def _populate_daily_stats(conn: sqlite3.Connection) -> None:
    """
    Remplit la table daily_stats en agrégeant les messages par jour.

    Utilise INSERT OR REPLACE pour mettre à jour les comptages
    si la commande count est relancée sur une période déjà traitée.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO daily_stats (date, message_count)
        SELECT date(date), COUNT(*)
        FROM messages
        GROUP BY date(date)
    """)
    conn.commit()


@bot.command()
async def count(ctx: commands.Context) -> None:
    """
    Commande principale : parcourt tous les salons du serveur sur une période donnée,
    compte les messages et les stocke dans une base de données SQLite.

    Couvre :
    - Les salons textuels
    - Les fils actifs et archivés de chaque salon
    - Les salons vocaux avec chat textuel

    Envoie un message de confirmation au début, puis un résumé à la fin
    avec le nombre de messages comptés et le temps d'exécution.
    """
    start = time.perf_counter_ns()

    buffer: list[tuple] = []

    # Période de comptage — à modifier selon vos besoins
    begin_date = datetime(2026, 2, 1)
    end_date = datetime(2026, 3, 1)
    previous_day = end_date - timedelta(days=1)

    msg = (
        f"Bot-chan commence le comptage, l'opération prendra un peu de temps. <:botchan:1062835070903271476>\n"
        f"Début du comptage : {begin_date.strftime('%d/%m/%Y')}\n"
        f"Fin du comptage : {previous_day.strftime('%d/%m/%Y')}"
    )
    await ctx.send(msg)

    server = ctx.message.guild
    count_messages: int = 0

    # Ouverture d'une seule connexion SQLite pour toute la durée du comptage
    # (plus efficace que d'ouvrir/fermer la connexion à chaque insertion)
    conn = sqlite3.connect(DB_PATH)

    # --- Salons textuels ---
    for channel in server.text_channels:
        count_messages += await _collect_history(channel, channel.name, "salon", buffer, conn, begin_date, end_date)

        # Fils actifs rattachés au salon
        for thread in channel.threads:
            count_messages += await _collect_history(thread, thread.name, "fils actif", buffer, conn, begin_date, end_date)

        # Fils archivés rattachés au salon
        async for thread in channel.archived_threads(limit=None):
            count_messages += await _collect_history(thread, thread.name, "fils archivé", buffer, conn, begin_date, end_date)

    # --- Salons vocaux avec chat textuel ---
    for voice_channel in server.voice_channels:
        # last_message_id est None si le salon vocal n'a jamais reçu de message texte
        if voice_channel.last_message_id:
            count_messages += await _collect_history(voice_channel, voice_channel.name, "salon vocal", buffer, conn, begin_date, end_date)

    # Insertion du reste du buffer (inférieur à BUFFER_SIZE)
    if buffer:
        _store_buffer(conn, buffer)

    # Mise à jour des statistiques quotidiennes
    _create_daily_stats_table(conn)
    _populate_daily_stats(conn)

    conn.close()

    # Calcul du temps d'exécution total
    elapsed = (time.perf_counter_ns() - start) / 1_000_000_000
    time_str = (
        f"{elapsed:.2f} sec" if elapsed < 60
        else f"{int(elapsed // 60)} min {int(elapsed % 60)} sec"
    )

    await ctx.send(f"Nombre de messages comptés : {count_messages}\nTemps d'exécution : {time_str}")


def _build_row(message: discord.Message, channel_name: str) -> tuple:
    """
    Construit un tuple représentant une ligne à insérer dans la table SQLite.

    Convertit la date du message de UTC vers le fuseau horaire de Paris,
    et sérialise les réactions sous forme de liste de dictionnaires.

    Args:
        message: L'objet message Discord à sérialiser.
        channel_name: Le nom du salon ou fil dans lequel le message se trouve.

    Returns:
        Un tuple (id, auteur, date, contenu, salon, réactions).
    """
    reactions = str([
        {"emoji": str(r.emoji), "count": r.count}
        for r in message.reactions
    ])
    return (
        message.id,
        message.author.name,
        message.created_at.astimezone(paris_tz).strftime('%Y-%m-%d %H:%M:%S'),
        message.content,
        channel_name,
        reactions
    )


def _store_buffer(conn: sqlite3.Connection, messages_buffer: list[tuple]) -> None:
    """
    Crée la table SQLite si elle n'existe pas, puis insère un lot de messages.

    Utilise INSERT OR IGNORE pour éviter les doublons en cas de relance du comptage
    sur une période déjà traitée (la clé primaire étant l'ID du message).

    Args:
        conn: La connexion SQLite active.
        messages_buffer: La liste de tuples à insérer.
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            author TEXT,
            date TEXT,
            content TEXT,
            channel TEXT,
            reactions TEXT
        )
    """)
    try:
        print(f"Insertion de {len(messages_buffer)} messages dans la table messages.")
        cur.executemany(
            "INSERT OR IGNORE INTO messages (id, author, date, content, channel, reactions) VALUES (?, ?, ?, ?, ?, ?)",
            messages_buffer
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur SQL: {e}")


async def main() -> None:
    """
    Point d'entrée du bot.

    Lit le token Discord depuis les variables d'environnement et démarre le bot.
    Lève une erreur explicite si le token est absent plutôt que d'échouer silencieusement.
    """
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise ValueError("DISCORD_TOKEN manquant dans les variables d'environnement.")
    async with bot:
        await bot.start(token)


asyncio.run(main())
