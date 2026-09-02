"""
word_counts.py

Génère word_counts.csv (author;word;count) à partir de messages.csv,
pour alimenter un nouvel onglet "Mots les plus utilisés par utilisateur"
dans le modèle Power BI Bot-chan.

Pourquoi ce script plutôt qu'une table calculée DAX / Power Query ?
Avec ~9M messages, découper chaque "content" en mots directement dans le
modèle (DAX ou M) ferait exploser la table intermédiaire (potentiellement
50-100M lignes) et rendrait le refresh très lourd. Un pré-calcul en Python,
qui streame le CSV ligne par ligne sans tout charger en mémoire, est
largement plus rapide et produit une table déjà agrégée (quelques centaines
de milliers de lignes au pire), facile à charger dans Power BI.

Usage :
    python word_counts.py

Pense à ajuster les constantes ci-dessous si besoin (chemins, comptes à
exclure, salons à exclure, longueur mini des mots...).
"""

import re
import csv
from collections import defaultdict

# ---- Configuration ----------------------------------------------------

INPUT_CSV = r"C:\Antoine\Programmation\Python\Bot-chan\messages.csv"
OUTPUT_CSV = r"C:\Antoine\Programmation\Python\Bot-chan\word_counts.csv"

# Comptes de bots à exclure (en minuscules)
BOT_ACCOUNTS = {"mudae", "bot-chan"}

# Salon(s) à exclure de l'analyse de vocabulaire (ex: le salon des commandes
# bot). Laisser vide si tu ne veux rien exclure. Les noms doivent matcher
# exactement la colonne "channel" du CSV (avec l'emoji préfixe s'il y en a).
EXCLUDED_CHANNELS: set[str] = set()

MIN_WORD_LENGTH = 3

# Liste de stopwords français (déterminants, pronoms, verbes être/avoir
# conjugués, interjections courantes de chat...). Pas de dépendance externe
# nécessaire (pas besoin de nltk/spacy).
STOPWORDS = {
    "alors", "au", "aucun", "aucune", "aussi", "autre", "autres", "avant",
    "avec", "avoir", "bon", "car", "ce", "cela", "ces", "cet", "cette",
    "ceux", "chaque", "comme", "comment", "dans", "de", "des", "du",
    "dedans", "dehors", "depuis", "devrait", "doit", "donc", "elle",
    "elles", "en", "encore", "est", "et", "eu", "eux", "fait", "faites",
    "fois", "font", "hors", "ici", "il", "ils", "je", "juste", "la", "le",
    "les", "leur", "leurs", "là", "ma", "maintenant", "mais", "mes",
    "moins", "mon", "même", "ni", "notre", "nos", "nous", "ou", "où",
    "par", "parce", "pas", "peut", "peu", "plupart", "pour", "pourquoi",
    "quand", "que", "quel", "quelle", "quelles", "quels", "qui", "sa",
    "sans", "se", "ses", "seulement", "si", "sien", "son", "sont", "sous",
    "sur", "ta", "tandis", "tellement", "tels", "tes", "toi", "ton",
    "tous", "tout", "toute", "toutes", "trop", "très", "tu", "un", "une",
    "vos", "votre", "vous", "vu", "ça", "étaient", "état", "été", "être",
    "ai", "as", "avez", "ont", "suis", "es", "sommes", "êtes", "aie",
    "aies", "ait", "ayons", "ayez", "aient", "serai", "seras", "sera",
    "serons", "serez", "seront", "serais", "serait", "serions", "seriez",
    "seraient", "étais", "était", "étions", "étiez", "lui", "ceci",
    "moi", "nan", "non", "oui", "ok", "mdr", "ptdr", "genre", "truc",
    "chose", "voilà", "enfin", "bref", "quoi", "hein", "ah", "oh", "eh",
    "euh", "bah", "ben", "jsp", "jpp", "wtf", "lol", "the", "and",
    # Formes d'élision de plus de 2 lettres, qui peuvent survivre au split
    # (les formes d'une ou deux lettres comme "c", "j", "qu", "on" sont de
    # toute façon éliminées par MIN_WORD_LENGTH)
    "jusqu", "lorsqu", "puisqu", "quoiqu",
    # Tournures interrogatives/négatives avec tiret interne : le tiret est
    # préservé (pour ne pas casser "peut-être"), donc après split de
    # l'élision ("n'est-ce" -> "n" + "est-ce") le second morceau doit être
    # filtré explicitement.
    "est-ce", "est-à-dire",
}

URL_RE = re.compile(r"https?://\S+")
MENTION_RE = re.compile(r"<@!?[0-9]+>|<#[0-9]+>|<@&[0-9]+>")
CUSTOM_EMOJI_RE = re.compile(r"<a?:\w+:[0-9]+>|:\w+:")
NON_WORD_RE = re.compile(r"[^a-zàâäçéèêëîïôöùûüÿœæ'\-]+")

# Préfixes d'élision française courants : c'est, j'ai, l'humanité, d'un,
# n'est, s'il, t'inquiète, qu'il, jusqu'à, lorsqu'on, puisqu'il, quoiqu'il...
# On sépare le préfixe (toujours filtré ensuite, trop court ou stopword) du
# reste du mot, qui lui peut être un mot plein (ex: "j'adore" -> "adore").
ELISION_RE = re.compile(
    r"^(c|d|j|l|m|n|s|t|qu|jusqu|lorsqu|puisqu|quoiqu)'(.+)$"
)


def clean_author(author: str) -> str:
    """Réplique la correction déjà faite dans Power Query (kcnoxious -> Noxious)."""
    if author.strip().lower() == "kcnoxious":
        return "Noxious"
    return author


def split_elisions(word: str) -> list[str]:
    """Coupe "c'est" -> ["c", "est"], "j'adore" -> ["j", "adore"], etc.
    Ne touche pas aux mots comme "aujourd'hui" ou "quelqu'un", dont le
    préfixe avant l'apostrophe ne correspond à aucune élision connue."""
    match = ELISION_RE.match(word)
    if match:
        return [match.group(1), match.group(2)]
    return [word]


def tokenize(content: str) -> list[str]:
    if not content:
        return []
    text = content.lower()
    text = URL_RE.sub(" ", text)
    text = MENTION_RE.sub(" ", text)
    text = CUSTOM_EMOJI_RE.sub(" ", text)
    text = NON_WORD_RE.sub(" ", text)

    words = []
    for raw in text.split():
        raw = raw.strip("-")
        for part in split_elisions(raw):
            part = part.strip("'-")
            if part:
                words.append(part)

    return [
        w for w in words
        if len(w) >= MIN_WORD_LENGTH and w not in STOPWORDS and not w.isdigit()
    ]


def main() -> None:
    counts: dict[tuple[str, str], int] = defaultdict(int)
    total_rows = 0
    skipped_bots = 0
    skipped_channels = 0

    with open(INPUT_CSV, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            author = clean_author((row.get("author") or "").strip())
            if not author or author.lower() in BOT_ACCOUNTS:
                skipped_bots += 1
                continue
            channel = row.get("channel") or ""
            if channel in EXCLUDED_CHANNELS:
                skipped_channels += 1
                continue

            for word in tokenize(row.get("content") or ""):
                counts[(author, word)] += 1

            total_rows += 1
            if total_rows % 500_000 == 0:
                print(f"{total_rows:,} messages traités...")

    print(
        f"Terminé : {total_rows:,} messages traités "
        f"({skipped_bots:,} messages de bots ignorés, "
        f"{skipped_channels:,} messages de salons exclus ignorés), "
        f"{len(counts):,} paires (auteur, mot) uniques."
    )

    with open(OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["author", "word", "count"])
        for (author, word), count in sorted(counts.items(), key=lambda x: -x[1]):
            writer.writerow([author, word, count])

    print(f"Fichier écrit : {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
