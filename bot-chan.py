import asyncio
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta
import logging
import random
from pathlib import Path

# Configuration des logs console.
# Les consoles comme celle de PyCharm affichent en rouge tout ce qui passe par stderr :
# on envoie donc les messages normaux (INFO, WARNING) sur stdout et seulement les
# erreurs (ERROR, CRITICAL) sur stderr, pour qu'elles ressortent en rouge.
# Cette configuration doit être faite AVANT d'importer discord : discord.py émet des
# avertissements dès son import (PyNaCl absent…), qui partiraient sinon sur stderr.
_log_format = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

_stdout_handler = logging.StreamHandler(sys.stdout)
_stdout_handler.setFormatter(_log_format)
_stdout_handler.addFilter(lambda record: record.levelno < logging.ERROR)

_stderr_handler = logging.StreamHandler(sys.stderr)
_stderr_handler.setFormatter(_log_format)
_stderr_handler.setLevel(logging.ERROR)

logging.basicConfig(level=logging.INFO, handlers=[_stdout_handler, _stderr_handler])
logger = logging.getLogger("bot-chan")

import aiohttp
import discord
import pytz
from discord.ext import commands
from dotenv import load_dotenv


# Chargement des variables d'environnement depuis le fichier .env (en local)
# Sur Railway, ces variables sont définies directement dans l'interface web
load_dotenv()

# Fuseau horaire utilisé pour convertir les dates des messages (stockées en UTC par Discord)
paris_tz = pytz.timezone("Europe/Paris")

# Nombre de messages accumulés en mémoire avant chaque écriture en base de données
# Plus la valeur est grande, moins il y a d'écritures disque, mais plus la mémoire est sollicitée
BUFFER_SIZE = 1000

# Fréquence d'affichage de la progression dans la console (tous les N messages).
# Afficher une ligne par message ralentit fortement le comptage sous Windows.
PROGRESS_EVERY = 1000

# Erreurs considérées comme temporaires : on réessaie au lieu d'abandonner le comptage.
# - DiscordServerError : erreurs 5xx renvoyées par Discord (500, 502, 503, 504)
# - aiohttp.ClientError : connexion coupée, serveur déconnecté, DNS indisponible…
# - asyncio.TimeoutError / OSError : délai dépassé, réseau injoignable
TRANSIENT_ERRORS = (
    discord.DiscordServerError,
    aiohttp.ClientError,
    asyncio.TimeoutError,
    OSError,
)

# Nombre maximal de tentatives consécutives avant d'abandonner,
# et délai d'attente maximal entre deux tentatives (en secondes).
MAX_RETRIES = 8
MAX_RETRY_DELAY = 120

# Chemin vers le fichier SQLite, configurable via variable d'environnement
# Par défaut : messages.db dans le répertoire courant
DB_PATH = os.getenv("DB_PATH", "messages.db")

# Chemin vers le dossier contenant les images de dédicaces
# Par défaut : sous-dossier "dedis" dans le répertoire courant
DEDIS_PATH = Path(os.getenv("DEDIS_PATH", "dedis"))


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
bot = commands.Bot(
    command_prefix="/botchan ",
    description="Bot-chan",
    intents=intents,
    help_command=None,  # désactive le help par défaut
)


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
    # on_ready peut aussi se déclencher après une reconnexion à Discord
    logger.info("Bot-chan est opérationnelle. Connectée en tant que %s", bot.user)


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

    En cas d'erreur temporaire (Discord indisponible, coupure réseau…), le parcours
    reprend après le dernier message déjà traité, après une attente croissante
    (2 s, 4 s, 8 s… plafonnée à MAX_RETRY_DELAY). Aucun message n'est compté deux fois.
    Si le bot n'a pas accès à la source (403), elle est ignorée avec un avertissement.
    """
    message_count = 0
    # Point de reprise : on repart après le dernier message traité.
    # Avec `after`, discord.py parcourt l'historique du plus ancien au plus récent.
    resume_after: datetime | discord.Message = begin_date
    attempt = 0

    while True:
        try:
            async for message in source.history(
                limit=None, after=resume_after, before=end_date
            ):
                buffer.append(_build_row(message, name))
                if len(buffer) >= BUFFER_SIZE:
                    _store_buffer(conn, buffer)
                    buffer.clear()
                message_count += 1
                resume_after = message
                attempt = 0  # le parcours avance de nouveau : on remet le compteur à zéro
                if (total_so_far + message_count) % PROGRESS_EVERY == 0:
                    logger.info(
                        "%d messages comptés (%s : %s).",
                        total_so_far + message_count,
                        label,
                        name,
                    )
            return message_count

        except discord.Forbidden:
            logger.warning("Accès refusé au %s « %s » : ignoré.", label, name)
            return message_count

        except TRANSIENT_ERRORS as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                logger.error(
                    "Abandon sur le %s « %s » après %d tentatives.",
                    label,
                    name,
                    MAX_RETRIES,
                )
                raise
            delay = _retry_delay(attempt)
            logger.warning(
                "Erreur temporaire sur le %s « %s » (%s). "
                "Reprise dans %d s (tentative %d/%d).",
                label,
                name,
                e,
                delay,
                attempt,
                MAX_RETRIES,
            )
            await asyncio.sleep(delay)


def _retry_delay(attempt: int) -> int:
    """Délai avant la tentative n° `attempt` : 2, 4, 8… plafonné à MAX_RETRY_DELAY."""
    return min(2**attempt, MAX_RETRY_DELAY)


async def _list_archived_threads(
    channel: discord.TextChannel,
) -> list[discord.Thread]:
    """
    Récupère la liste des fils archivés publics d'un salon.

    La liste est récupérée en entier avant le parcours des messages, ce qui permet
    de la redemander depuis le début en cas d'erreur temporaire sans effet de bord.

    Args:
        channel: Le salon textuel dont on veut les fils archivés.

    Returns:
        La liste des fils archivés (vide si le bot n'y a pas accès).
    """
    attempt = 0
    while True:
        try:
            return [thread async for thread in channel.archived_threads(limit=None)]
        except discord.Forbidden:
            logger.warning(
                "Accès refusé aux fils archivés du salon « %s » : ignorés.",
                channel.name,
            )
            return []
        except TRANSIENT_ERRORS as e:
            attempt += 1
            if attempt > MAX_RETRIES:
                raise
            delay = _retry_delay(attempt)
            logger.warning(
                "Erreur temporaire sur les fils archivés de « %s » (%s). "
                "Reprise dans %d s (tentative %d/%d).",
                channel.name,
                e,
                delay,
                attempt,
                MAX_RETRIES,
            )
            await asyncio.sleep(delay)


@bot.command()
@allowed_users_only()
# Un seul comptage à la fois : deux comptages simultanés écriraient dans la même base
@commands.max_concurrency(1, per=commands.BucketType.default, wait=False)
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
        end_date = paris_tz.localize(datetime.strptime(end, "%Y-%m-%d")) + timedelta(
            days=1
        )
    except ValueError:
        await ctx.send(
            "❌ Format de date invalide. Utilise : `/botchan count YYYY-MM-DD YYYY-MM-DD`\n"
            "Exemple : `/botchan count 2026-03-01 2026-03-31`"
        )
        return

    if end_date <= begin_date:
        await ctx.send(
            "❌ La date de fin doit être strictement après la date de début."
        )
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
    _create_messages_table(conn)

    try:
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
                    "fil actif",
                    buffer,
                    conn,
                    begin_date,
                    end_date,
                    count_messages,
                )

            # Fils archivés rattachés au salon
            for thread in await _list_archived_threads(channel):
                count_messages += await _collect_history(
                    thread,
                    thread.name,
                    "fil archivé",
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
    finally:
        # Insertion du reste du buffer (inférieur à BUFFER_SIZE), y compris en cas
        # d'erreur : les messages déjà récupérés ne sont pas perdus.
        if buffer:
            _store_buffer(conn, buffer)
            buffer.clear()
        conn.close()

    # Mise à jour des statistiques quotidiennes.
    # L'agrégation parcourt toute la table messages (plusieurs millions de lignes) :
    # elle tourne dans un thread séparé pour ne pas bloquer le bot pendant ce temps
    # (sinon Discord ne reçoit plus les heartbeats et finit par couper la connexion).
    await asyncio.to_thread(_refresh_daily_stats, DB_PATH)

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
    elif isinstance(error, commands.MaxConcurrencyReached):
        await ctx.send("⏳ Un comptage est déjà en cours, attends qu'il se termine !")
    elif isinstance(error, commands.CheckFailure):
        pass  # Déjà géré dans allowed_users_only()
    else:
        # CommandInvokeError enveloppe l'exception d'origine : on l'extrait pour un
        # message plus lisible, et on garde la trace complète dans la console.
        original = getattr(error, "original", error)
        logger.error(
            "Erreur pendant la commande count",
            exc_info=(type(original), original, original.__traceback__),
        )
        await ctx.send(
            f"❌ Une erreur inattendue s'est produite : {type(original).__name__}: {original}\n"
            "Les messages récupérés avant l'erreur sont enregistrés : "
            "tu peux relancer la même commande sans risque de doublons."
        )


@bot.command(name="help")
async def help_command(ctx: commands.Context) -> None:
    """
    Commande d'aide : affiche la liste des commandes disponibles avec leur syntaxe.
    """
    embed = discord.Embed(
        title="📖 Aide — Bot-chan",
        description="Voici la liste des commandes disponibles.",
        color=discord.Color.blurple(),
    )

    embed.add_field(
        name="`/botchan count <début> <fin>`",
        value=(
            "Parcourt tous les salons du serveur sur la période indiquée, "
            "compte les messages et les stocke dans la base de données SQLite.\n"
            "• `<début>` et `<fin>` au format `YYYY-MM-DD` (les deux bornes sont incluses)\n"
            "📌 Exemple : `/botchan count 2026-03-01 2026-03-31`"
        ),
        inline=False,
    )

    embed.add_field(
        name="`/botchan dedicace`",
        value="Affiche une image de dédicace qui a marqué la commu kj.",
        inline=False,
    )

    embed.add_field(
        name="`/botchan help`",
        value="Affiche ce message d'aide.",
        inline=False,
    )

    embed.set_footer(
        text="Bot-chan • Seuls les admins/modos peuvent utiliser la commande count."
    )

    await ctx.send(embed=embed)


@bot.command(name="dedicace")
async def dedicace_command(ctx: commands.Context) -> None:
    """
    Commande publique : envoie une image de dédicace tirée au hasard
    depuis le dossier DEDIS_PATH.

    Usage:
        /botchan dedicace

    Les formats supportés sont : JPG, JPEG, PNG, GIF, WEBP.
    Si le dossier est introuvable ou vide, envoie un message d'erreur discret.
    """
    extensions = {".jpg", ".jpeg", ".png", ".gif", ".webp"}

    if not DEDIS_PATH.is_dir():
        await ctx.send("❌ Le dossier de dédicaces est introuvable. Contacte un admin !")
        return

    images = [f for f in DEDIS_PATH.iterdir() if f.suffix.lower() in extensions]

    if not images:
        await ctx.send("📭 Aucune dédicace disponible pour le moment...")
        return

    chosen = random.choice(images)
    await ctx.send(file=discord.File(chosen))


def _build_row(message: discord.Message, channel_name: str) -> tuple:
    """
    Construit un tuple représentant une ligne à insérer dans la table SQLite.

    Convertit la date du message de UTC vers le fuseau horaire de Paris,
    et sérialise les réactions sous forme de liste de dictionnaires.

    Args:
        message: L'objet message Discord à sérialiser.
        channel_name: Le nom du salon ou fil dans lequel le message se trouve.

    Returns:
        Un tuple (id, auteur, date, contenu, salon, réactions, reply_to_message_id).
    """
    reactions = str(
        [{"emoji": str(r.emoji), "count": r.count} for r in message.reactions]
    )
    reply_to_message_id = (
        message.reference.message_id if message.reference is not None else None
    )
    return (
        message.id,
        message.author.name,
        message.created_at.astimezone(paris_tz).strftime("%Y-%m-%d %H:%M:%S"),
        message.content,
        channel_name,
        reactions,
        reply_to_message_id,
    )


def _create_messages_table(conn: sqlite3.Connection) -> None:
    """
    Crée la table messages si elle n'existe pas encore.

    Args:
        conn: La connexion SQLite active.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            author TEXT,
            date TEXT,
            content TEXT,
            channel TEXT,
            reactions TEXT,
            reply_to_message_id INTEGER
        )
    """)
    conn.commit()


def _store_buffer(conn: sqlite3.Connection, messages_buffer: list[tuple]) -> None:
    """
    Insère un lot de messages dans la table messages.

    Utilise INSERT OR IGNORE pour éviter les doublons en cas de relance du comptage
    sur une période déjà traitée (la clé primaire étant l'ID du message).

    Une erreur SQL n'est pas avalée : elle interrompt le comptage et remonte jusqu'au
    gestionnaire d'erreurs, plutôt que d'annoncer un comptage réussi alors que
    des messages n'ont pas été enregistrés.

    Args:
        conn: La connexion SQLite active.
        messages_buffer: La liste de tuples à insérer.
    """
    logger.info("Insertion de %d messages dans la table messages.", len(messages_buffer))
    conn.executemany(
        "INSERT OR IGNORE INTO messages "
        "(id, author, date, content, channel, reactions, reply_to_message_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        messages_buffer,
    )
    conn.commit()


def _refresh_daily_stats(db_path: str) -> None:
    """
    Crée si besoin puis remplit la table daily_stats en agrégeant les messages par jour.

    Utilise INSERT OR REPLACE pour mettre à jour les comptages
    si la commande count est relancée sur une période déjà traitée.

    Ouvre sa propre connexion : la fonction est appelée via asyncio.to_thread(),
    et une connexion SQLite ne peut pas être partagée entre threads.

    Args:
        db_path: Chemin vers le fichier SQLite.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY,
                message_count INTEGER
            )
        """)
        conn.execute("""
            INSERT OR REPLACE INTO daily_stats (date, message_count)
            SELECT date(date), COUNT(*)
            FROM messages
            GROUP BY date(date)
        """)
        conn.commit()
    finally:
        conn.close()


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


if __name__ == "__main__":
    asyncio.run(main())
