from __future__ import annotations

import math

import argparse
import itertools
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

from cluster.antidup import is_near_duplicate
from cluster.title_refiner import build_article_payload, compute_story_title
from db.utils import get_conn
from scripts.db_migrate import apply_migrations

LOGGER = logging.getLogger(__name__)
JACCARD_THRESHOLD = 0.6
TIME_DELTA_LIMIT = timedelta(hours=6)
TOKEN_CLEAN_RE = re.compile(r"[^\w]+", flags=re.UNICODE)


@dataclass
class NewsItem:
    news_id: int
    title: str
    tokens: Set[str]
    published: Optional[datetime]
    entities: Set[int]
    sports: Set[int]
    tournaments: Set[int]



def _load_fingerprint(conn, news_id: int) -> Optional[Tuple[str, Optional[str]]]:
    row = conn.execute("SELECT title_sig, entity_sig FROM content_fingerprints WHERE news_id = ?", (news_id,)).fetchone()
    if not row:
        return None
    return row[0], row[1]


def _find_near_duplicate_story(conn, news_id: int, lookback_hours: int = 72) -> Optional[Tuple[int, int, float, bool]]:
    fingerprint = _load_fingerprint(conn, news_id)
    if not fingerprint:
        return None
    title_sig, entity_sig = fingerprint
    if not title_sig:
        return None

    window_start = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    window_iso = window_start.replace(microsecond=0).isoformat(timespec="seconds")

    rows = conn.execute(
        """
        SELECT sa.story_id, cf.news_id, cf.title_sig, cf.entity_sig
        FROM story_articles sa
        JOIN stories s ON s.id = sa.story_id
        JOIN content_fingerprints cf ON cf.news_id = sa.news_id
        WHERE cf.news_id != ?
          AND s.updated_at >= ?
        """,
        (news_id, window_iso),
    ).fetchall()

    candidates = []
    story_map = {}
    for story_id, cand_news_id, cand_title_sig, cand_entity_sig in rows:
        if not cand_title_sig:
            continue
        story_map[cand_news_id] = story_id
        candidates.append((cand_news_id, cand_title_sig, cand_entity_sig))

    duplicate = is_near_duplicate(title_sig, entity_sig, candidates)
    if duplicate is None:
        return None
    duplicate_news_id, score, entity_match = duplicate
    story_id = story_map.get(duplicate_news_id)
    if story_id is None:
        return None
    return story_id, duplicate_news_id, score, entity_match

class UnionFind:
    def __init__(self, nodes: Iterable[int]) -> None:
        nodes_list = list(nodes)
        self.parent: Dict[int, int] = {node: node for node in nodes_list}
        self.rank: Dict[int, int] = {node: 0 for node in nodes_list}

    def find(self, node: int) -> int:
        if self.parent[node] != node:
            self.parent[node] = self.find(self.parent[node])
        return self.parent[node]

    def union(self, a: int, b: int) -> None:
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a == root_b:
            return
        rank_a = self.rank[root_a]
        rank_b = self.rank[root_b]
        if rank_a < rank_b:
            self.parent[root_a] = root_b
        elif rank_a > rank_b:
            self.parent[root_b] = root_a
        else:
            self.parent[root_b] = root_a
            self.rank[root_a] += 1


def parse_timestamp(value: Optional[str]) -> Optional[datetime]:
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


def tokenize_title(title: str) -> Set[str]:
    normalized = TOKEN_CLEAN_RE.sub(" ", (title or "").lower())
    return {token for token in normalized.split() if token}


def jaccard_similarity(tokens_a: Set[str], tokens_b: Set[str]) -> float:
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = len(tokens_a & tokens_b)
    union = len(tokens_a | tokens_b)
    if union == 0:
        return 0.0
    return intersection / union


def fetch_news(conn, since_days: int, limit: int) -> List[NewsItem]:
    threshold = datetime.now(timezone.utc) - timedelta(days=since_days)
    threshold_str = threshold.replace(microsecond=0).isoformat(timespec="seconds")

    rows = conn.execute(
        """
        SELECT
            id,
            title,
            COALESCE(published, published_at, created_at) AS published_at
        FROM news
        WHERE COALESCE(published, published_at, created_at) >= ?
        ORDER BY COALESCE(published, published_at, created_at) DESC
        LIMIT ?
        """,
        (threshold_str, limit),
    ).fetchall()

    if not rows:
        return []

    news_ids = [row["id"] for row in rows]
    placeholders = ",".join("?" for _ in news_ids)
    tag_rows = conn.execute(
        f"""
        SELECT
            nat.news_id,
            t.id AS tag_id,
            t.type
        FROM news_article_tags nat
        JOIN tags t ON t.id = nat.tag_id
        WHERE nat.news_id IN ({placeholders})
          AND t.type IN ('sport', 'tournament', 'team', 'player')
        """,
        news_ids,
    ).fetchall()

    entities_map: Dict[int, Set[int]] = defaultdict(set)
    sports_map: Dict[int, Set[int]] = defaultdict(set)
    tournaments_map: Dict[int, Set[int]] = defaultdict(set)

    for tag_row in tag_rows:
        news_id = tag_row["news_id"]
        tag_id = tag_row["tag_id"]
        tag_type = tag_row["type"]
        entities_map[news_id].add(tag_id)
        if tag_type == "sport":
            sports_map[news_id].add(tag_id)
        elif tag_type == "tournament":
            tournaments_map[news_id].add(tag_id)

    items: List[NewsItem] = []
    for row in rows:
        news_id = row["id"]
        title = row["title"] or ""
        published = parse_timestamp(row["published_at"])
        items.append(
            NewsItem(
                news_id=news_id,
                title=title,
                tokens=tokenize_title(title),
                published=published,
                entities=entities_map.get(news_id, set()),
                sports=sports_map.get(news_id, set()),
                tournaments=tournaments_map.get(news_id, set()),
            )
        )
    return items


def evaluate_pair(a: NewsItem, b: NewsItem) -> Tuple[bool, Dict[str, float | int | bool]]:
    score_details: Dict[str, float | int | bool] = {}

    title_score = jaccard_similarity(a.tokens, b.tokens)
    score_details["title_jaccard"] = title_score
    cond_title = title_score >= JACCARD_THRESHOLD

    entities_overlap = bool(a.entities & b.entities)
    score_details["entities_overlap"] = int(entities_overlap)

    if a.published and b.published:
        delta = abs(a.published - b.published)
        cond_time = delta <= TIME_DELTA_LIMIT
        score_details["time_diff_hours"] = round(delta.total_seconds() / 3600, 3)
    else:
        cond_time = False
        score_details["time_diff_hours"] = math.inf

    conditions_met = sum([cond_title, entities_overlap, cond_time])
    score_details["conditions_met"] = conditions_met
    return conditions_met >= 2, score_details


def union_clusters(items: Sequence[NewsItem]) -> Tuple[UnionFind, int]:
    union_find = UnionFind(node.news_id for node in items)
    groups: Dict[int | None, List[int]] = defaultdict(list)
    id_to_item = {item.news_id: item for item in items}

    for item in items:
        keys = list(item.sports | item.tournaments)
        if not keys:
            groups[None].append(item.news_id)
        else:
            for key in keys:
                groups[key].append(item.news_id)

    seen_pairs: Set[Tuple[int, int]] = set()
    pairs_evaluated = 0

    for news_ids in groups.values():
        if len(news_ids) < 2:
            continue
        for left, right in itertools.combinations(news_ids, 2):
            pair = tuple(sorted((left, right)))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            pairs_evaluated += 1
            item_a = id_to_item[left]
            item_b = id_to_item[right]
            matches, details = evaluate_pair(item_a, item_b)
            LOGGER.debug(
                "Pair %s-%s evaluated: %s",
                left,
                right,
                details,
            )
            if matches:
                union_find.union(left, right)

    return union_find, pairs_evaluated


def build_clusters(
    items: Sequence[NewsItem],
    union_find: UnionFind,
) -> List[List[NewsItem]]:
    components: Dict[int, List[NewsItem]] = defaultdict(list)
    id_to_item = {item.news_id: item for item in items}
    for item in items:
        root = union_find.find(item.news_id)
        components[root].append(item)
    clusters = [
        sorted(cluster, key=lambda it: (it.published or datetime.min.replace(tzinfo=timezone.utc)), reverse=True)
        for cluster in components.values()
        if len(cluster) >= 2
    ]
    clusters.sort(key=len, reverse=True)
    return clusters


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def load_existing_assignments(conn, news_ids: Sequence[int]) -> Dict[int, Set[int]]:
    if not news_ids:
        return {}
    placeholders = ",".join("?" for _ in news_ids)
    rows = conn.execute(
        f"""
        SELECT story_id, news_id
        FROM story_articles
        WHERE news_id IN ({placeholders})
        """,
        news_ids,
    ).fetchall()
    mapping: Dict[int, Set[int]] = defaultdict(set)
    for row in rows:
        mapping[row["news_id"]].add(row["story_id"])
    return mapping


def ensure_story_exists(
    conn,
    cluster: List[NewsItem],
    assignments: Dict[int, Set[int]],
    dry_run: bool,
) -> Tuple[Optional[int], bool]:
    if cluster:
        near_dup = _find_near_duplicate_story(conn, cluster[0].news_id)
        if near_dup:
            story_id_dup, duplicate_news_id, score, entity_match = near_dup
            LOGGER.info(
                "Joined existing story_id=%s by near-dup of news_id=%s (jaccard=%.3f, entity_match=%s)",
                story_id_dup,
                duplicate_news_id,
                score,
                entity_match,
            )
            return story_id_dup, False
    shared_story_ids: Set[int] | None = None
    for item in cluster:
        story_ids = assignments.get(item.news_id, set())
        if shared_story_ids is None:
            shared_story_ids = set(story_ids)
        else:
            shared_story_ids &= story_ids
        if shared_story_ids:
            # continue searching potential common story
            continue
    if shared_story_ids:
        existing_story_id = sorted(shared_story_ids)[0]
        LOGGER.debug(
            "Cluster already fully linked to story_id=%s; skipping creation",
            existing_story_id,
        )
        return existing_story_id, False

    first_story_candidate = None
    for item in cluster:
        existing_ids = sorted(assignments.get(item.news_id, set()))
        if existing_ids:
            first_story_candidate = existing_ids[0]
            break

    if first_story_candidate is not None:
        LOGGER.debug(
            "Using existing story_id=%s for cluster expansion",
            first_story_candidate,
        )
        return first_story_candidate, False

    article_ids = [item.news_id for item in cluster]
    articles_payload = build_article_payload(conn, article_ids)
    title = compute_story_title(articles_payload) if articles_payload else (cluster[0].title if cluster else "Story")
    if not title:
        title = cluster[0].title if cluster else "Story"
    timestamp = now_iso()
    if dry_run:
        LOGGER.info("DRY-RUN: would create story with title=%s", title)
        return None, True

    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO stories (title, created_at, updated_at) VALUES (?, ?, ?)",
        (title, timestamp, timestamp),
    )
    story_id = cursor.lastrowid
    LOGGER.info("Created story_id=%s title=%s", story_id, title)
    return story_id, True


def link_cluster(
    conn,
    cluster: List[NewsItem],
    story_id: Optional[int],
    assignments: Dict[int, Set[int]],
    dry_run: bool,
) -> Tuple[int, int]:
    links_created = 0
    links_skipped = 0

    if story_id is None:
        # Assign a synthetic ID for dry-run bookkeeping
        story_id = -abs(hash(tuple(item.news_id for item in cluster)))  # deterministic for the run

    cursor = conn.cursor()

    for item in cluster:
        existing_ids = assignments.get(item.news_id, set())
        if story_id in existing_ids:
            LOGGER.debug(
                "Story link already exists: story_id=%s news_id=%s",
                story_id,
                item.news_id,
            )
            links_skipped += 1
            continue

        if existing_ids:
            LOGGER.warning(
                "news_id=%s already linked to stories=%s; attaching to story_id=%s",
                item.news_id,
                sorted(existing_ids),
                story_id,
            )

        if dry_run:
            LOGGER.info("DRY-RUN: would link story_id=%s news_id=%s", story_id, item.news_id)
            links_created += 1
            assignments.setdefault(item.news_id, set()).add(story_id)
            continue

        cursor.execute(
            "INSERT OR IGNORE INTO story_articles (story_id, news_id) VALUES (?, ?)",
            (story_id, item.news_id),
        )
        if cursor.rowcount:
            links_created += 1
            assignments.setdefault(item.news_id, set()).add(story_id)
            cursor.execute(
                "UPDATE stories SET updated_at = ? WHERE id = ?",
                (now_iso(), story_id),
            )
        else:
            links_skipped += 1

    return links_created, links_skipped


def process_clusters(
    conn,
    clusters: List[List[NewsItem]],
    assignments: Dict[int, Set[int]],
    dry_run: bool,
) -> Tuple[int, int, int]:
    new_stories = 0
    links_created_total = 0
    links_skipped_total = 0

    for cluster in clusters:
        story_id, created = ensure_story_exists(conn, cluster, assignments, dry_run)
        if created:
            new_stories += 1
        created_links, skipped_links = link_cluster(conn, cluster, story_id, assignments, dry_run)
        links_created_total += created_links
        links_skipped_total += skipped_links

    return new_stories, links_created_total, links_skipped_total


def run(
    since_days: int,
    limit: int,
    dry_run: bool,
    verbose: bool,
) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    apply_migrations()

    conn = get_conn()
    try:
        items = fetch_news(conn, since_days=since_days, limit=limit)
        if not items:
            LOGGER.info("No news items found for window since_days=%s", since_days)
            return

        LOGGER.info("Fetched %s news items for clustering window", len(items))
        union_find, pairs_evaluated = union_clusters(items)
        clusters = build_clusters(items, union_find)
        if not clusters:
            LOGGER.info(
                "processed=%s pairs_eval=%s new_stories=0 links_created=0 links_skipped=0",
                len(items),
                pairs_evaluated,
            )
            return

        assignments = load_existing_assignments(conn, [item.news_id for item in items])
        new_stories, links_created, links_skipped = process_clusters(conn, clusters, assignments, dry_run)

        if not dry_run and (links_created or new_stories):
            conn.commit()

        LOGGER.info(
            "processed=%s pairs_eval=%s new_stories=%s links_created=%s links_skipped=%s",
            len(items),
            pairs_evaluated,
            new_stories,
            links_created,
            links_skipped,
        )
    finally:
        conn.close()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cluster related news into stories")
    parser.add_argument("--since-days", type=int, default=2, help="Look back window in days")
    parser.add_argument("--max", type=int, default=100, help="Maximum news items to consider")
    parser.add_argument("--dry-run", action="store_true", help="Do not modify the database")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    run(
        since_days=args.since_days,
        limit=args.max,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
