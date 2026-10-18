"""
Diagnostic : liste les fils privés archivés du serveur et indique combien de leurs
messages manquent dans la base SQLite.

Le comptage (/botchan count) ne parcourt que les fils archivés publics : ce script
permet de mesurer ce qui a été raté avant de décider de les inclure.

Usage (depuis le dossier du projet, avec le venv activé) :
    python check_private_threads.py

Le script se connecte avec le même token que Bot-chan (.env), affiche son rapport
dans la console, puis se déconnecte. Il n'écrit rien : la base est ouverte en
lecture seule.
"""

import asyncio
import logging
import os
import sqlite3

import discord
import pytz
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")

DB_PATH = os.getenv("DB_PATH", "messages.db")
paris_tz = pytz.timezone("Europe/Paris")

# Taille des lots pour la requête SQL "id IN (...)" (limite de variables SQLite)
SQL_CHUNK = 500


def count_ids_in_db(conn: sqlite3.Connection, ids: list[int]) -> int:
    """Renvoie combien des IDs de messages donnés sont déjà présents dans la table messages."""
    found = 0
    for i in range(0, len(ids), SQL_CHUNK):
        chunk = ids[i : i + SQL_CHUNK]
        placeholders = ",".join("?" * len(chunk))
        found += conn.execute(
            f"SELECT COUNT(*) FROM messages WHERE id IN ({placeholders})", chunk
        ).fetchone()[0]
    return found


async def list_private_archived(
    channel: discord.TextChannel,
) -> tuple[list[discord.Thread], str]:
    """
    Récupère les fils privés archivés d'un salon.

    Essaie d'abord tous les fils privés (permission « Gérer les fils » requise),
    puis à défaut seulement ceux dont le bot est membre.

    Returns:
        (liste des fils, mode utilisé : "tous", "rejoints" ou "aucun accès")
    """
    try:
        threads = [t async for t in channel.archived_threads(limit=None, private=True)]
        return threads, "tous"
    except discord.Forbidden:
        pass
    try:
        threads = [
            t
            async for t in channel.archived_threads(
                limit=None, private=True, joined=True
            )
        ]
        return threads, "rejoints"
    except discord.Forbidden:
        return [], "aucun accès"


class Checker(discord.Client):
    async def on_ready(self) -> None:
        try:
            await self.run_check()
        finally:
            await self.close()

    async def run_check(self) -> None:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        total_threads = total_msgs = total_missing = 0
        limited_channels: list[str] = []

        for guild in self.guilds:
            print(f"\n=== Serveur : {guild.name} ===")
            for channel in guild.text_channels:
                threads, mode = await list_private_archived(channel)
                if mode != "tous":
                    limited_channels.append(f"#{channel.name} ({mode})")
                for thread in threads:
                    ids: list[int] = []
                    last_date = None
                    try:
                        async for msg in thread.history(limit=None):
                            ids.append(msg.id)
                            if last_date is None:  # history() part du plus récent
                                last_date = msg.created_at
                    except discord.Forbidden:
                        print(f"  #{channel.name} › {thread.name} : historique inaccessible")
                        continue

                    in_db = count_ids_in_db(conn, ids)
                    missing = len(ids) - in_db
                    total_threads += 1
                    total_msgs += len(ids)
                    total_missing += missing
                    last = (
                        last_date.astimezone(paris_tz).strftime("%d/%m/%Y")
                        if last_date
                        else "vide"
                    )
                    print(
                        f"  #{channel.name} › {thread.name} : {len(ids)} messages, "
                        f"{missing} absents de la base (dernier message : {last})"
                    )

        conn.close()
        print("\n=== Bilan ===")
        print(f"Fils privés archivés trouvés : {total_threads}")
        print(f"Messages dans ces fils       : {total_msgs}")
        print(f"Messages absents de la base  : {total_missing}")
        if limited_channels:
            print(
                "\n⚠ Sans la permission « Gérer les fils », le bot ne voit que les fils "
                "privés dont il est membre. Salons concernés :"
            )
            for c in limited_channels:
                print(f"  - {c}")


def main() -> None:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise ValueError("DISCORD_TOKEN manquant dans les variables d'environnement.")
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Base introuvable : {DB_PATH}")
    Checker(intents=discord.Intents.all()).run(token, log_handler=None)


if __name__ == "__main__":
    main()
