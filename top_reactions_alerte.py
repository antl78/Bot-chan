"""
Top 3 membres et top 3 posts ayant reçu le plus de réactions "alerte" (et dérivés) ou "atome".

Règle : sur un même post, on retient uniquement le COUNT le plus élevé parmi
les emojis correspondants (pas d'addition intra-post).
Les counts sont ensuite additionnés entre les différents posts d'un même auteur.
"""

import ast
import re
import csv
from collections import defaultdict

# --- Fichier source ---
CSV_FILE = "csv/2026_07.csv"

# --- Critère de matching ---
PATTERN = re.compile(r"alerte|atome", re.IGNORECASE)


def parse_reactions(raw: str) -> list[dict]:
    """Convertit la chaîne de réactions (format liste Python) en liste de dicts."""
    raw = raw.strip()
    if not raw or raw == "[]":
        return []
    try:
        return ast.literal_eval(raw)
    except Exception:
        return []


def max_matching_count(reactions: list[dict]) -> int:
    """
    Retourne le count le plus élevé parmi les emojis correspondant au pattern.
    Retourne 0 si aucun emoji ne correspond.
    """
    counts = []
    for r in reactions:
        emoji_str = str(r.get("emoji", ""))
        if PATTERN.search(emoji_str):
            counts.append(int(r.get("count", 0)))
    return max(counts) if counts else 0


def main():
    # Scores cumulés par auteur
    author_scores: dict[str, int] = defaultdict(int)

    # Liste des posts avec leur score (pour le top posts)
    post_scores: list[dict] = []

    with open(CSV_FILE, encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for i, row in enumerate(reader, start=2):  # start=2 car ligne 1 = header
            reactions = parse_reactions(row.get("reactions", ""))
            score = max_matching_count(reactions)

            if score > 0:
                author = row.get("author", "inconnu")
                author_scores[author] += score

                post_scores.append({
                    "line":    i,
                    "author":  author,
                    "date":    row.get("date", ""),
                    "channel": row.get("channel", ""),
                    "content": row.get("content", "")[:80],  # aperçu tronqué
                    "score":   score,
                })

    # --- Top 3 membres ---
    top_members = sorted(author_scores.items(), key=lambda x: x[1], reverse=True)[:3]

    print("=" * 60)
    print("TOP 3 MEMBRES — réactions alerte / atome (et dérivés)")
    print("=" * 60)
    for rank, (author, total) in enumerate(top_members, 1):
        print(f"  #{rank}  {author:<25} {total} réactions")

    # --- Top 3 posts ---
    top_posts = sorted(post_scores, key=lambda x: x["score"], reverse=True)[:3]

    print()
    print("=" * 60)
    print("TOP 3 POSTS — réactions alerte / atome (et dérivés)")
    print("=" * 60)
    for rank, post in enumerate(top_posts, 1):
        print(f"  #{rank}  Score : {post['score']}")
        print(f"      Auteur  : {post['author']}")
        print(f"      Date    : {post['date']}")
        print(f"      Canal   : {post['channel']}")
        print(f"      Contenu : {post['content']}...")
        print(f"      (ligne CSV : {post['line']})")
        print()


if __name__ == "__main__":
    main()
