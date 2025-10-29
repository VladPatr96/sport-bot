from __future__ import annotations

import html
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from cluster.antidup import filter_near_duplicates
from cluster.fingerprints import compute_signatures
from db.utils import get_conn

LOGGER = logging.getLogger(__name__)
TELEGRAM_LIMIT = 4096
MAX_STORY_ITEMS = 5
MIN_STORY_ITEMS = 3
UPDATE_SHORT_LIMIT = 2
UPDATE_FULL_LIMIT = 5

ICON_MAP = {
    "sport": "🏅",
    "tournament": "🏆",
    "team": "🏟️",
    "player": "👤",
}

INDEX_EMOJI = ["1️⃣", "2️⃣", "3️⃣", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣"]


@dataclass
class RenderedMessage:
    text: str
    entities: List[str]
    links: List[str]
    meta: Optional[Dict] = None


def _normalize_icon(tag_type: str) -> str:
    return ICON_MAP.get(tag_type, "🏷️")


def _truncate(text: str, limit: int) -> str:
    if limit <= 0:
        return ""
    if len(text) <= limit:
        return text
    return text[: max(limit - 1, 0)] + "…"


def _limit_lines(lines: Iterable[str], max_length: int = TELEGRAM_LIMIT) -> str:
    text = ""
    for line in lines:
        if not line.endswith("\n"):
            line += "\n"
        if len(text) + len(line) > max_length:
            LOGGER.warning("Message truncated to respect Telegram limit (%s chars)", TELEGRAM_LIMIT)
            break
        text += line
    return text.rstrip()


def _fetch_article_tags(conn, news_id: int) -> List[Tuple[str, str]]:
    rows = conn.execute(
        """
        SELECT t.type, t.name
        FROM news_article_tags nat
        JOIN tags t ON t.id = nat.tag_id
        WHERE nat.news_id = ?
          AND t.type IN ('sport', 'tournament', 'team', 'player')
        ORDER BY t.type
        """,
        (news_id,),
    ).fetchall()
    return [(row["type"], row["name"]) for row in rows]


def _render_tag_line(tags: Sequence[Tuple[str, str]]) -> str:
    parts = []
    for tag_type, tag_name in tags:
        icon = _normalize_icon(tag_type)
        parts.append(f"{icon} {tag_name}")
    return " · ".join(parts)


def _build_article_meta(news_id: int, title: str, url: str, tags: List[Tuple[str, str]]) -> Dict:
    return {
        "type": "article",
        "id": news_id,
        "title": title,
        "url": url,
        "tags": [
            {"type": tag_type, "name": tag_name, "icon": _normalize_icon(tag_type)}
            for tag_type, tag_name in tags
        ],
    }


def _article_preview_lines(title: str, tag_line: str, url: str) -> List[str]:
    title_line = _truncate(title, 1024)
    tag_line = _truncate(tag_line, 1024) if tag_line else ""
    url_line = _truncate(url, 1024) if url else ""
    lines = [title_line]
    if tag_line:
        lines.append(tag_line)
    if url_line:
        lines.append(url_line)
    return lines


def render_article_message(news_id: int, conn=None) -> RenderedMessage:
    close_conn = False
    if conn is None:
        conn = get_conn()
        close_conn = True
    try:
        row = conn.execute(
            """
            SELECT id, title, COALESCE(published, published_at) AS published_at, url
            FROM news
            WHERE id = ?
            """,
            (news_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"Article not found: {news_id}")

        title = row["title"] or "Без названия"
        url = row["url"] or ""
        tags = _fetch_article_tags(conn, news_id)
        tag_line = _render_tag_line(tags[:3])

        lines = _article_preview_lines(title, tag_line, url)
        text = _limit_lines(lines)
        meta = _build_article_meta(news_id, title, url, tags)
        return RenderedMessage(
            text=text,
            entities=[tag_name for _, tag_name in tags],
            links=[url] if url else [],
            meta=meta,
        )
    finally:
        if close_conn:
            conn.close()


def _fetch_story_articles(conn, story_id: int, limit: int = 10) -> List[dict]:
    rows = conn.execute(
        """
        SELECT
            n.id,
            n.title,
            n.url,
            COALESCE(n.published, n.published_at, n.created_at) AS published_at,
            cf.title_sig,
            cf.entity_sig
        FROM story_articles sa
        JOIN news n ON n.id = sa.news_id
        LEFT JOIN content_fingerprints cf ON cf.news_id = n.id
        WHERE sa.story_id = ?
        ORDER BY COALESCE(n.published, n.published_at, n.created_at) DESC
        LIMIT ?
        """,
        (story_id, limit),
    ).fetchall()
    articles: List[dict] = []
    for row in rows:
        articles.append(
            {
                "id": row["id"],
                "title": row["title"],
                "url": row["url"],
                "published": row["published_at"],
                "title_sig": row["title_sig"],
                "entity_sig": row["entity_sig"],
            }
        )
    return articles


def render_story_message(story_id: int, conn=None) -> RenderedMessage:
    close_conn = False
    if conn is None:
        conn = get_conn()
        close_conn = True
    try:
        story_row = conn.execute(
            "SELECT id, title FROM stories WHERE id = ?",
            (story_id,),
        ).fetchone()
        if not story_row:
            raise ValueError(f"Story not found: {story_id}")

        story_title = story_row["title"] or "История без названия"
        articles = _fetch_story_articles(conn, story_id)
        if not articles:
            raise ValueError(f"Story {story_id} has no articles")

        prepared = []
        for art in articles:
            title = art.get("title", "")
            title_sig = art.get("title_sig")
            entity_sig = art.get("entity_sig")
            if not title_sig:
                title_sig, computed_entity = compute_signatures(
                    title,
                    {"sport": None, "tournament": None, "team": None, "player": None},
                )
                if not entity_sig:
                    entity_sig = computed_entity
            prepared.append(
                (
                    art["id"],
                    title_sig or "",
                    entity_sig,
                    {
                        "id": art["id"],
                        "title": title,
                        "url": art.get("url", ""),
                        "published": art.get("published"),
                    },
                )
            )

        visible_payloads, hidden_payloads = filter_near_duplicates(prepared)
        visible_payloads = visible_payloads[:MAX_STORY_ITEMS]
        visible_ids = {item["id"] for item in visible_payloads}
        hidden_payloads = [item for item in hidden_payloads if item["id"] not in visible_ids]

        if len(visible_payloads) < MIN_STORY_ITEMS:
            for art in articles:
                if art["id"] in visible_ids:
                    continue
                visible_payloads.append(
                    {
                        "id": art["id"],
                        "title": art.get("title", ""),
                        "url": art.get("url", ""),
                        "published": art.get("published"),
                    }
                )
                visible_ids.add(art["id"])
                if len(visible_payloads) >= MIN_STORY_ITEMS:
                    break

        lines: List[str] = [story_title]
        entities: List[str] = []
        links: List[str] = []
        story_meta_articles: List[Dict] = []

        for index, payload in enumerate(visible_payloads, start=1):
            try:
                article_message = render_article_message(payload["id"], conn=conn)
            except Exception as exc:
                LOGGER.warning("Failed to render article %s in story %s: %s", payload["id"], story_id, exc)
                continue

            article_lines = article_message.text.splitlines()
            snippet = article_lines[0] if article_lines else payload["title"]
            index_icon = INDEX_EMOJI[index - 1] if index - 1 < len(INDEX_EMOJI) else f"{index}."
            lines.append(f"{index_icon} {snippet}")
            if len(article_lines) > 1:
                lines.append("   " + " ".join(article_lines[1:]))
            entities.extend(article_message.entities)
            links.extend(article_message.links)
            story_meta_articles.append(
                {
                    "id": payload["id"],
                    "title": (article_message.meta or {}).get("title", payload["title"]),
                    "url": (article_message.meta or {}).get("url", payload.get("url", "")),
                    "tags": (article_message.meta or {}).get("tags", []),
                }
            )

        remainder = max(len(articles) - len(visible_payloads), 0)
        if remainder:
            lines.append(f"… и ещё {remainder} материалов в этой истории.")

        text_output = _limit_lines(lines)
        meta = {
            "type": "story",
            "id": story_id,
            "title": story_title,
            "articles": story_meta_articles,
            "hidden_articles": hidden_payloads,
            "total_articles": len(articles),
        }
        return RenderedMessage(text=text_output, entities=entities, links=links, meta=meta)
    finally:
        if close_conn:
            conn.close()


def _escape_markdown(text: str) -> str:
    return "".join(f"\\{c}" if c in MD_V2_SPECIAL else c for c in text)


def _format_timestamp(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        return value
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M")


def render_story_update(
    story_id: int,
    *,
    kind: str = "short",
    mode: str = "html",
    conn=None,
) -> str:
    if kind not in {"short", "full"}:
        raise ValueError("kind must be 'short' or 'full'")
    if mode not in {"html", "markdown"}:
        raise ValueError("mode must be 'html' or 'markdown'")

    limit = UPDATE_SHORT_LIMIT if kind == "short" else UPDATE_FULL_LIMIT

    close_conn = False
    if conn is None:
        conn = get_conn()
        close_conn = True
    try:
        story_row = conn.execute(
            "SELECT id, title FROM stories WHERE id = ?",
            (story_id,),
        ).fetchone()
        if not story_row:
            raise ValueError(f"Story not found: {story_id}")

        story_title = story_row["title"] or "История без названия"
        articles = _fetch_story_articles(conn, story_id, limit=limit)
        if not articles:
            raise ValueError(f"Story {story_id} has no articles")

        lines: List[str] = []
        if mode == "markdown":
            lines.append(f"*Обновление:* {_escape_markdown(story_title)}")
        else:
            lines.append(f"<b>Обновление:</b> {html.escape(story_title)}")
        lines.append("")

        for article in articles[:limit]:
            title = article.get("title") or "Без названия"
            url = article.get("url") or ""
            published = _format_timestamp(article.get("published"))

            if mode == "markdown":
                bullet = f"- {_escape_markdown(title)}"
                if published:
                    bullet += f" ({_escape_markdown(published)})"
                lines.append(bullet)
                if url:
                    lines.append(f"  {_escape_markdown(url)}")
            else:
                safe_title = html.escape(title)
                if url:
                    bullet = f'• <a href="{html.escape(url)}">{safe_title}</a>'
                else:
                    bullet = f"• {safe_title}"
                if published:
                    bullet += f" ({html.escape(published)})"
                lines.append(bullet)

        return "\n".join(lines).strip()
    finally:
        if close_conn:
            conn.close()
