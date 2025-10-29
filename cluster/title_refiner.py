from __future__ import annotations

import math
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence

TOKEN_RE = re.compile(r"\w+", re.UNICODE)
STOP_WORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "from",
    "your",
    "their",
    "into",
    "after",
    "before",
    "over",
    "under",
    "about",
    "against",
    "across",
    "around",
    "through",
    "into",
    "onto",
    "between",
    "without",
    "within",
    "while",
    "whose",
    "where",
    "when",
    "дело",
    "дня",
    "новости",
    "новость",
    "матч",
    "матча",
    "матче",
    "сезона",
    "сезон",
    "игра",
    "игры",
    "игре",
    "игрок",
    "игроки",
    "тур",
    "туре",
    "турнир",
    "турнира",
    "турнире",
    "команда",
    "команды",
    "команде",
    "клуб",
    "клуба",
    "клубе",
    "год",
    "года",
    "году",
    "что",
    "как",
    "где",
    "когда",
    "после",
    "перед",
    "при",
    "под",
    "над",
    "между",
    "если",
    "почему",
    "из",
    "на",
    "по",
    "в",
    "во",
    "к",
    "ко",
    "о",
    "об",
    "обо",
    "за",
    "до",
    "без",
    "со",
    "от",
    "то",
    "так",
    "же",
    "ли",
    "не",
    "да",
    "но",
    "или",
    "бы",
}

MONTH_NAMES = [
    "января",
    "февраля",
    "марта",
    "апреля",
    "мая",
    "июня",
    "июля",
    "августа",
    "сентября",
    "октября",
    "ноября",
    "декабря",
]


def _truncate_plain(text: str, limit: int) -> str:
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(limit - 1, 0)] + "…"


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    text = text.replace(" ", "T")
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    for candidate in (text, text + "+00:00"):
        try:
            dt = datetime.fromisoformat(candidate)
        except ValueError:
            continue
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    return None


def build_article_payload(conn, news_ids: Sequence[int]) -> List[dict]:
    if not news_ids:
        return []
    placeholders = ",".join("?" for _ in news_ids)
    rows = conn.execute(
        f"""
        SELECT id, title, COALESCE(published, published_at, created_at) AS published_at
        FROM news
        WHERE id IN ({placeholders})
        """,
        tuple(news_ids),
    ).fetchall()
    news_map = {row["id"]: row for row in rows}

    tag_rows = conn.execute(
        f"""
        SELECT nat.news_id, t.type, t.name
        FROM news_article_tags nat
        JOIN tags t ON t.id = nat.tag_id
        WHERE nat.news_id IN ({placeholders})
          AND t.type IN ('sport', 'tournament', 'team', 'player')
        """,
        tuple(news_ids),
    ).fetchall()
    tag_map: Dict[int, Dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for row in tag_rows:
        tag_map[row["news_id"]][row["type"]].add(row["name"])

    articles: List[dict] = []
    for news_id in news_ids:
        row = news_map.get(news_id)
        if not row:
            continue
        tags = tag_map.get(news_id, {})
        articles.append(
            {
                "id": news_id,
                "title": row["title"] or "",
                "published": _parse_timestamp(row["published_at"]),
                "sports": sorted(tags.get("sport", [])),
                "tournaments": sorted(tags.get("tournament", [])),
                "teams": sorted(tags.get("team", [])),
                "players": sorted(tags.get("player", [])),
            }
        )
    return articles


def _extract_tokens(title: str) -> List[str]:
    tokens = []
    for match in TOKEN_RE.finditer(title or ""):
        word = match.group(0)
        normalized = word.lower()
        if len(normalized) <= 1:
            continue
        if normalized in STOP_WORDS:
            continue
        tokens.append((normalized, word))
    return tokens


def _select_primary_entity(articles: Sequence[dict]) -> Optional[str]:
    priority = ("tournaments", "teams", "players", "sports")
    total = len(articles)
    required = max(1, math.ceil(0.5 * total))
    for field in priority:
        counter: Counter = Counter()
        for article in articles:
            for name in article.get(field, []):
                counter[name] += 1
        eligible = [name for name, count in counter.items() if count >= required]
        if eligible:
            eligible.sort(key=lambda v: (-len(v), v))
            return eligible[0]
    return None


def _select_representative_title(token_info: Sequence[dict]) -> str:
    if not token_info:
        return ""
    if len(token_info) == 1:
        return token_info[0]["article"]["title"]
    best_score = -1.0
    best_title = token_info[0]["article"]["title"]
    for idx, info in enumerate(token_info):
        tokens_a = info["tokens"]
        score = 0.0
        comparisons = 0
        for jdx, other in enumerate(token_info):
            if idx == jdx:
                continue
            tokens_b = other["tokens"]
            union = len(tokens_a | tokens_b)
            intersection = len(tokens_a & tokens_b)
            comparisons += 1
            score += (intersection / union) if union else 0.0
        if comparisons:
            avg = score / comparisons
        else:
            avg = 0.0
        if avg > best_score:
            best_score = avg
            best_title = info["article"]["title"]
    return best_title


def compute_story_title(articles: Sequence[dict]) -> str:
    if not articles:
        return "Сводка дня"

    token_counter: Counter = Counter()
    token_info: List[dict] = []
    for article in articles:
        tokens = _extract_tokens(article.get("title", ""))
        token_set = {normalized for normalized, _ in tokens}
        token_counter.update(token_set)
        token_info.append({"tokens": token_set, "ordered": tokens, "article": article})

    required = max(1, math.ceil(0.6 * len(articles)))
    common_tokens = {token for token, count in token_counter.items() if count >= required}

    entity_name = _select_primary_entity(articles)

    topic = ""
    if common_tokens:
        best_info = max(token_info, key=lambda info: len(info["tokens"] & common_tokens))
        used = set()
        topic_words: List[str] = []
        for normalized, original in best_info["ordered"]:
            if normalized in common_tokens and normalized not in used:
                topic_words.append(original)
                used.add(normalized)
        topic = " ".join(topic_words).strip()
        if topic and topic[0].islower():
            topic = topic[0].upper() + topic[1:]

    if not topic:
        representative = _select_representative_title(token_info)
        if entity_name:
            return _truncate_plain(f"Сводка: {entity_name}", 140)
        return _truncate_plain(representative or "Сводка дня", 140)

    if entity_name and topic:
        topic_lower = topic.lower()
        entity_lower = entity_name.lower()
        if topic_lower.startswith(entity_lower):
            trimmed = topic[len(entity_name):].lstrip(" —:-–")
            if trimmed:
                if trimmed[0].islower():
                    trimmed = trimmed[0].upper() + trimmed[1:]
                topic = trimmed
            else:
                topic = ""
    if entity_name:
        if topic:
            base_title = f"{entity_name} — {topic}"
        else:
            base_title = f"Сводка: {entity_name}"
    else:
        base_title = topic

    dates = {
        article["published"].date()
        for article in articles
        if isinstance(article.get("published"), datetime)
    }
    if dates and len(dates) == 1:
        date_value = next(iter(dates))
        month_index = date_value.month - 1
        if 0 <= month_index < len(MONTH_NAMES):
            suffix = f" на {date_value.day} {MONTH_NAMES[month_index]}"
            if len(base_title) + len(suffix) <= 140:
                base_title = f"{base_title}{suffix}"

    return _truncate_plain(base_title, 140)
