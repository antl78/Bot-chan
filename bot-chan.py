import asyncio
import os
import sqlite3
import time
from datetime import datetime, timedelta
from functools import wraps
import logging

import discord
import pytz
from discord.ext import commands
from dotenv import load_dotenv



logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
)



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

# Liste des IDs Discord autorisés à lancer la commande count.
# Format dans .env : ALLOWED_USERS=123456789,987654321
# Si la variable est absente ou vide, personne ne peut lancer la commande.
_raw_allowed = os.getenv("ALLOWED_USERS", "")
ALLOWED_USER_IDS: set[int] = {
    int(uid.strip()) for uid in _raw_allowed.split(",") if uid.strip().isdigit()
}

# Configuration des intents Discord : détermine quels événements le bot peut recevoir
# Intents.all() est nécessaire pour accéder à l'historique des messages et aux fils archivés
intents = discord.Intents.all()
bot = commands.Bot(command_prefix="/botchan ", description="Bot-chan", intents=intents)


def allowed_users_only():
    """
    Decorator de vérification : n'autorise l'exécution de la commande que si
    l'ID de l'auteur figure dans ALLOWED_USER_IDS.

    En cas de refus, envoie un message d'erreur discret dans le salon
    et lève une exception pour bloquer l'exécution de la commande.
    """
    async def predicate(ctx: commands.Context) -> bool:
        if ctx.author.id not in ALLOWED_USER_IDS:
            await ctx.send(
                f"⛔ Désolée {ctx.author.mention}, tu n'es pas autorisé(e) à utiliser cette commande."
            )
            return False
        return True
    return commands.check(predicate)


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
    end_date: datetime,
    total_so_far: int = 0,
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
        total_so_far: Nombre de messages déjà comptés sur le serveur,
                      pour afficher un compteur global continu dans les logs.

    Returns:
        Le nombre de messages collectés depuis cette source.
    """
    message_count = 0
    async for message in source.history(limit=None, after=begin_date, before=end_date):
        buffer.append(_build_row(message, name))
        if len(buffer) >= BUFFER_SIZE:
            _store_buffer(conn, buffer)
            buffer.clear()
        message_count += 1
        print(f"{total_so_far + message_count} messages comptés ({label}).")
    return message_count


@bot.command()
@allowed_users_only()
async def count(ctx: commands.Context, begin: str, end: str) -> None:
    """
    Commande principale : parcourt tous les salons du serveur sur une période donnée,
    compte les messages et les stocke dans une base de données SQLite.

    Usage:
        /botchan count YYYY-MM-DD YYYY-MM-DD

    Exemples:
        /botchan count 2026-03-01 2026-03-31   → tout le mois de mars 2026
        /botchan count 2026-01-01 2026-01-31   → tout le mois de janvier 2026

    Args:
        begin: Date de début au format YYYY-MM-DD (incluse).
        end:   Date de fin au format YYYY-MM-DD (incluse).

    Couvre :
    - Les salons textuels
    - Les fils actifs et archivés de chaque salon
    - Les salons vocaux avec chat textuel

    Envoie un message de confirmation au début, puis un résumé à la fin
    avec le nombre de messages comptés et le temps d'exécution.
    """
    # --- Validation et parsing des dates ---
    try:
        begin_date = paris_tz.localize(datetime.strptime(begin, "%Y-%m-%d"))
        # On ajoute un jour à end_date pour que la date saisie soit incluse dans le comptage
        # (discord.py traite `before` comme exclusif)
        end_date = paris_tz.localize(datetime.strptime(end, "%Y-%m-%d")) + timedelta(days=1)
    except ValueError:
        await ctx.send(
            "❌ Format de date invalide. Utilise : `/botchan count YYYY-MM-DD YYYY-MM-DD`\n"
            "Exemple : `/botchan count 2026-03-01 2026-03-31`"
        )
        return

    if end_date <= begin_date:
        await ctx.send("❌ La date de fin doit être strictement après la date de début.")
        return

    start = time.perf_counter_ns()

    buffer: list[tuple] = []

    # end_date a été décalée d'un jour en interne pour rendre la borne inclusive ;
    # on soustrait ce jour pour afficher la date telle que l'utilisateur l'a saisie.
    display_end = end_date - timedelta(days=1)
    msg = (
        f"Bot-chan commence le comptage, l'opération prendra un peu de temps. <:botchan:1062835070903271476>\n"
        f"Début du comptage : {begin_date.strftime('%d/%m/%Y')}\n"
        f"Fin du comptage : {display_end.strftime('%d/%m/%Y')}"
    )
    await ctx.send(msg)

    server = ctx.message.guild
    count_messages: int = 0

    # Ouverture d'une seule connexion SQLite pour toute la durée du comptage
    # (plus efficace que d'ouvrir/fermer la connexion à chaque insertion)
    conn = sqlite3.connect(DB_PATH)

    # --- Salons textuels ---
    for channel in server.text_channels:
        count_messages += await _collect_history(
            channel,
            channel.name,
            "salon",
            buffer,
            conn,
            begin_date,
            end_date,
            count_messages,
        )

        # Fils actifs rattachés au salon
        for thread in channel.threads:
            count_messages += await _collect_history(
                thread,
                thread.name,
                "fils actif",
                buffer,
                conn,
                begin_date,
                end_date,
                count_messages,
            )

        # Fils archivés rattachés au salon
        async for thread in channel.archived_threads(limit=None):
            count_messages += await _collect_history(
                thread,
                thread.name,
                "fils archivé",
                buffer,
                conn,
                begin_date,
                end_date,
                count_messages,
            )

    # --- Salons vocaux avec chat textuel ---
    for voice_channel in server.voice_channels:
        # last_message_id est None si le salon vocal n'a jamais reçu de message texte
        if voice_channel.last_message_id:
            count_messages += await _collect_history(
                voice_channel,
                voice_channel.name,
                "salon vocal",
                buffer,
                conn,
                begin_date,
                end_date,
                count_messages,
            )

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
        f"{elapsed:.2f} sec"
        if elapsed < 60
        else f"{int(elapsed // 60)} min {int(elapsed % 60)} sec"
    )

    await ctx.send(
        f"Nombre de messages comptés : {count_messages}\nTemps d'exécution : {time_str}"
    )

@count.error
async def count_error(ctx: commands.Context, error: commands.CommandError) -> None:
    """Gestionnaire d'erreurs pour la commande count."""
    if isinstance(error, (commands.MissingRequiredArgument, commands.TooManyArguments)):
        await ctx.send(
            "❌ Paramètres invalides. Utilise : `/botchan count YYYY-MM-DD YYYY-MM-DD`\n"
            "Exemple : `/botchan count 2026-03-01 2026-03-31`"
        )
    elif isinstance(error, commands.CheckFailure):
        pass  # Déjà géré dans allowed_users_only()
    else:
        await ctx.send(f"❌ Une erreur inattendue s'est produite : {error}")


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
    reactions = str(
        [{"emoji": str(r.emoji), "count": r.count} for r in message.reactions]
    )
    return (
        message.id,
        message.author.name,
        message.created_at.astimezone(paris_tz).strftime("%Y-%m-%d %H:%M:%S"),
        message.content,
        channel_name,
        reactions,
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
            messages_buffer,
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"Erreur SQL: {e}")


def _create_daily_stats_table(conn: sqlite3.Connection) -> None:
    """
    Crée la table daily_stats si elle n'existe pas encore.

    Cette table agrège le nombre de messages par jour,
    calculé à partir des données de la table messages.

    Args:
        conn: La connexion SQLite active.
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

    Args:
        conn: La connexion SQLite active.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT OR REPLACE INTO daily_stats (date, message_count)
        SELECT date(date), COUNT(*)
        FROM messages
        GROUP BY date(date)
    """)
    conn.commit()


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
