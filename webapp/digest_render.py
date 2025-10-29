from __future__ import annotations

import html
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

from cluster.antidup import filter_near_duplicates
from cluster.fingerprints import compute_signatures
from db.utils import get_conn

MAX_ARTICLES_PER_ITEM = 3
ARTICLE_TITLE_LIMIT = 120
DIGEST_TITLE_LIMIT = 120
MD_SPECIAL = set(r"_*[]()~`>#+-=|{}.!")


def _escape_markdown(text: str) -> str:
    return "".join(f"\\{c}" if c in MD_SPECIAL else c for c in text)


def _truncate(text: str, limit: int) -> str:
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)] + "…"


def _json_datetime(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat(timespec="seconds")


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _compute_story_score(
    article_count: int,
    max_published: Optional[datetime],
    has_tournament: bool,
    team_count: int,
    has_player: bool,
) -> float:
    size_factor = min(10.0, article_count / 3.0)
    freshness = 0.0
    if max_published:
        age_hours = (datetime.now(timezone.utc) - max_published).total_seconds() / 3600
        if age_hours <= 6:
            freshness = 3.0
        elif age_hours <= 24:
            freshness = 2.0
        elif age_hours <= 72:
            freshness = 1.0
    entity_weight = 0.0
    if has_tournament or team_count >= 2:
        entity_weight += 2.0
    if has_player:
        entity_weight += 1.0
    return size_factor + freshness + entity_weight


def _fetch_story_rows(conn, since_iso: str, until_iso: str, limit: int) -> List[sqlite3.Row]:
    query = """
        SELECT
            s.id,
            s.title,
            s.updated_at,
            COUNT(DISTINCT sa.news_id) AS article_count,
            MAX(COALESCE(n.published, n.published_at, n.created_at)) AS max_published,
            SUM(CASE WHEN nat_sport.sport_id IS NOT NULL THEN 1 ELSE 0 END) > 0 AS has_sport,
            SUM(CASE WHEN nat_tournament.tournament_id IS NOT NULL THEN 1 ELSE 0 END) > 0 AS has_tournament,
            SUM(CASE WHEN nat_team.team_id IS NOT NULL THEN 1 ELSE 0 END) AS team_count,
            SUM(CASE WHEN nat_player.player_id IS NOT NULL THEN 1 ELSE 0 END) > 0 AS has_player
        FROM stories s
        JOIN story_articles sa ON sa.story_id = s.id
        JOIN news n ON n.id = sa.news_id
        LEFT JOIN news_articles nat_sport ON nat_sport.news_id = n.id AND nat_sport.sport_id IS NOT NULL
        LEFT JOIN news_articles nat_tournament ON nat_tournament.news_id = n.id AND nat_tournament.tournament_id IS NOT NULL
        LEFT JOIN news_articles nat_team ON nat_team.news_id = n.id AND nat_team.team_id IS NOT NULL
        LEFT JOIN news_articles nat_player ON nat_player.news_id = n.id AND nat_player.player_id IS NOT NULL
        WHERE s.updated_at >= ?
          AND s.updated_at < ?
        GROUP BY s.id
        ORDER BY s.updated_at DESC
        LIMIT ?
    """
    return conn.execute(query, (since_iso, until_iso, limit * 2)).fetchall()


def _fetch_story_rows_by_ids(conn, story_ids: Sequence[int]) -> List[sqlite3.Row]:
    if not story_ids:
        return []
    placeholders = ",".join("?" for _ in story_ids)
    query = f"""
        SELECT
            s.id,
            s.title,
            s.updated_at,
            COUNT(DISTINCT sa.news_id) AS article_count,
            MAX(COALESCE(n.published, n.published_at, n.created_at)) AS max_published,
            SUM(CASE WHEN nat_sport.sport_id IS NOT NULL THEN 1 ELSE 0 END) > 0 AS has_sport,
            SUM(CASE WHEN nat_tournament.tournament_id IS NOT NULL THEN 1 ELSE 0 END) > 0 AS has_tournament,
            SUM(CASE WHEN nat_team.team_id IS NOT NULL THEN 1 ELSE 0 END) AS team_count,
            SUM(CASE WHEN nat_player.player_id IS NOT NULL THEN 1 ELSE 0 END) > 0 AS has_player
        FROM stories s
        JOIN story_articles sa ON sa.story_id = s.id
        JOIN news n ON n.id = sa.news_id
        LEFT JOIN news_articles nat_sport ON nat_sport.news_id = n.id AND nat_sport.sport_id IS NOT NULL
        LEFT JOIN news_articles nat_tournament ON nat_tournament.news_id = n.id AND nat_tournament.tournament_id IS NOT NULL
        LEFT JOIN news_articles nat_team ON nat_team.news_id = n.id AND nat_team.team_id IS NOT NULL
        LEFT JOIN news_articles nat_player ON nat_player.news_id = n.id AND nat_player.player_id IS NOT NULL
        WHERE s.id IN ({placeholders})
        GROUP BY s.id
    """
    rows = conn.execute(query, tuple(story_ids)).fetchall()
    row_map = {row["id"]: row for row in rows}
    return [row_map[sid] for sid in story_ids if sid in row_map]


def _fetch_story_articles(conn, story_id: int, limit: int = MAX_ARTICLES_PER_ITEM) -> List[dict]:
    rows = conn.execute(
        """
        SELECT
            n.id,
            n.title,
            n.url,
            COALESCE(n.published, n.published_at, n.created_at) AS published_iso,
            cf.title_sig,
            cf.entity_sig
        FROM story_articles sa
        JOIN news n ON n.id = sa.news_id
        LEFT JOIN content_fingerprints cf ON cf.news_id = n.id
        WHERE sa.story_id = ?
        ORDER BY COALESCE(n.published, n.published_at, n.created_at) DESC
        LIMIT ?
        """,
        (story_id, limit * 3),
    ).fetchall()
    prepared = []
    for row in rows:
        title = row["title"] or "Без названия"
        title_sig = row["title_sig"]
        entity_sig = row["entity_sig"]
        if not title_sig:
            title_sig, computed_entity = compute_signatures(
                title,
                {"sport": None, "tournament": None, "team": None, "player": None},
            )
            if not entity_sig:
                entity_sig = computed_entity
        prepared.append(
            (
                row["id"],
                title_sig or "",
                entity_sig,
                {
                    "id": row["id"],
                    "title": title,
                    "url": row["url"],
                    "published": row["published_iso"],
                },
            )
        )
    visible, _ = filter_near_duplicates(prepared)
    return visible[:limit]


def _fetch_article_tags(conn, news_id: int) -> List[Tuple[str, str]]:
    tag_rows = conn.execute(
        """
        SELECT t.type, t.name
        FROM news_article_tags nat
        JOIN tags t ON t.id = nat.tag_id
        WHERE nat.news_id = ?
          AND t.type IN ('sport','tournament','team','player')
        ORDER BY t.type
        """,
        (news_id,),
    ).fetchall()
    return [(row["type"], row["name"]) for row in tag_rows]


def _badge_for_type(tag_type: str) -> str:
    return {
        "sport": "🏅",
        "tournament": "🏆",
        "team": "🏟️",
        "player": "👤",
    }.get(tag_type, "🏷️")


def _format_story_badges(item: dict) -> str:
    badges = []
    if item.get("sport_count"):
        badges.append(f"🏅 {item['sport_count']}")
    if item.get("tournament_count"):
        badges.append(f"🏆 {item['tournament_count']}")
    if item.get("team_count"):
        badges.append(f"🏟️ {item['team_count']}")
    if item.get("player_count"):
        badges.append(f"👤 {item['player_count']}")
    return " · ".join(badges)


def build_digest_dataset(
    period: str,
    since: datetime,
    until: datetime,
    limit: int = 25,
    story_ids: Optional[Sequence[int]] = None,
) -> dict:
    if period not in {"daily", "weekly"}:
        raise ValueError("period must be 'daily' or 'weekly'")
    since = _normalize_datetime(since)
    until = _normalize_datetime(until)
    since_iso = _json_datetime(since)
    until_iso = _json_datetime(until)

    conn = get_conn()
    try:
        if story_ids:
            story_rows = _fetch_story_rows_by_ids(conn, story_ids)
        else:
            story_rows = _fetch_story_rows(conn, since_iso, until_iso, limit)
        items: List[dict] = []
        for row in story_rows:
            article_count = int(row["article_count"] or 0)
            if article_count == 0:
                continue
            articles = _fetch_story_articles(conn, row["id"], limit=MAX_ARTICLES_PER_ITEM)
            if not articles:
                continue

            max_pub_raw = row["max_published"]
            max_pub = None
            if max_pub_raw:
                try:
                    max_pub = datetime.fromisoformat(max_pub_raw)
                    if max_pub.tzinfo is None:
                        max_pub = max_pub.replace(tzinfo=timezone.utc)
                except ValueError:
                    max_pub = None

            updated_dt = None
            updated_raw = row["updated_at"]
            if updated_raw:
                try:
                    updated_dt = datetime.fromisoformat(updated_raw)
                    if updated_dt.tzinfo is None:
                        updated_dt = updated_dt.replace(tzinfo=timezone.utc)
                except ValueError:
                    updated_dt = None
            if updated_dt is None:
                updated_dt = datetime.min.replace(tzinfo=timezone.utc)

            team_count = int(row["team_count"] or 0)
            has_tournament = bool(row["has_tournament"])
            has_player = bool(row["has_player"])
            score = _compute_story_score(
                article_count=article_count,
                max_published=max_pub,
                has_tournament=has_tournament,
                team_count=team_count,
                has_player=has_player,
            )

            article_payload = []
            for article in articles:
                tags = _fetch_article_tags(conn, article["id"])
                article_payload.append(
                    {
                        "title": _truncate(article.get("title") or "Без названия", ARTICLE_TITLE_LIMIT),
                        "url": article.get("url"),
                        "published": article.get("published"),
                        "tags": [
                            {"type": tag_type, "name": tag_name, "icon": _badge_for_type(tag_type)}
                            for tag_type, tag_name in tags[:4]
                        ],
                    }
                )

            items.append(
                {
                    "story_id": row["id"],
                    "title": _truncate(row["title"] or "История без названия", ARTICLE_TITLE_LIMIT),
                    "updated_at": row["updated_at"],
                    "article_count": article_count,
                    "score": score,
                    "articles": article_payload,
                    "has_tournament": has_tournament,
                    "team_count": team_count,
                    "has_player": has_player,
                    "sport_count": 1 if row["has_sport"] else 0,
                    "tournament_count": 1 if has_tournament else 0,
                    "player_count": 1 if has_player else 0,
                    "_updated_dt": updated_dt,
                }
            )

        if story_ids:
            ordering = {sid: idx for idx, sid in enumerate(story_ids)}
            items.sort(key=lambda item: ordering.get(item["story_id"], len(story_ids)))
        else:
            items.sort(key=lambda item: (item["score"], item["_updated_dt"]), reverse=True)
            items = items[:limit]
        for item in items:
            item.pop("_updated_dt", None)

        if period == "daily":
            title = f"Дайджест за {since.strftime('%d %B %Y')}"
        else:
            title = f"Дайджест за неделю {since.strftime('%d %b')} – {until.strftime('%d %b %Y')}"
        title = _truncate(title, DIGEST_TITLE_LIMIT)

        return {
            "period": period,
            "since": since_iso,
            "until": until_iso,
            "title": title,
            "items": items,
            "count": len(items),
        }
    finally:
        conn.close()


def render_markdown(dataset: dict) -> str:
    lines: List[str] = []
    title = dataset.get("title") or "Дайджест"
    lines.append(f"# {_escape_markdown(title)}")
    lines.append(f"> Период: {_escape_markdown(dataset.get('since', ''))} — {_escape_markdown(dataset.get('until', ''))}")
    lines.append("")
    for idx, item in enumerate(dataset.get("items", []), start=1):
        heading = f"## {idx}. {_escape_markdown(item['title'])}"
        lines.append(heading)
        badge_summary = _format_story_badges(item)
        if badge_summary:
            lines.append(f"*{_escape_markdown(badge_summary)}*")
        lines.append("")
        for article in item.get("articles", []):
            title = _escape_markdown(article["title"])
            url = article.get("url")
            tags = " · ".join(f"{tag['icon']} {_escape_markdown(tag['name'])}" for tag in article.get("tags", [])[:3])
            bullet = f"- [{title}]({url})" if url else f"- {title}"
            if tags:
                bullet += f" — {tags}"
            lines.append(bullet)
        lines.append("")
    return "\n".join(lines).strip() + "\n"


def render_html(dataset: dict) -> str:
    title = dataset.get("title") or "Дайджест"
    parts = [
        "<!DOCTYPE html>",
        '<html lang="ru">',
        "<head>",
        '<meta charset="utf-8">',
        f"<title>{html.escape(title)}</title>",
        "<style>body{font-family:Arial,sans-serif;line-height:1.5;}h1{margin-bottom:0.5em;}h2{margin-top:1.5em;}ul{padding-left:1.2em;}li{margin-bottom:0.4em;}small.badges{color:#555;}</style>",
        "</head>",
        "<body>",
        f"<h1>{html.escape(title)}</h1>",
        "<p><small>Период: "
        f"{html.escape(dataset.get('since', ''))} — {html.escape(dataset.get('until', ''))}</small></p>",
    ]
    for idx, item in enumerate(dataset.get("items", []), start=1):
        heading = f"<h2>{idx}. {html.escape(item['title'])}</h2>"
        parts.append(heading)
        badge_summary = _format_story_badges(item)
        if badge_summary:
            parts.append(f'<p><small class="badges">{html.escape(badge_summary)}</small></p>')
        parts.append("<ul>")
        for article in item.get("articles", []):
            title = html.escape(article["title"])
            url = article.get("url")
            tags = " · ".join(f"{tag['icon']} {html.escape(tag['name'])}" for tag in article.get("tags", [])[:3])
            if url:
                line = f'<a href="{html.escape(url)}">{title}</a>'
            else:
                line = title
            if tags:
                line += f" — {html.escape(tags)}"
            parts.append(f"<li>{line}</li>")
        parts.append("</ul>")
    parts.append("</body></html>")
    return "\n".join(parts)
