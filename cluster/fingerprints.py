from __future__ import annotations

import re
from collections import Counter
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

_RU_STOP = {
    "и",
    "в",
    "на",
    "к",
    "по",
    "о",
    "от",
    "за",
    "для",
    "с",
    "во",
    "как",
    "или",
    "но",
    "а",
    "не",
    "это",
    "что",
    "из",
    "со",
    "же",
    "бы",
    "ли",
    "до",
    "об",
    "обо",
    "над",
    "между",
    "при",
    "под",
    "у",
    "про",
    "же",
    "ещё",
}

_EN_STOP = {
    "and",
    "or",
    "the",
    "a",
    "an",
    "of",
    "in",
    "on",
    "to",
    "for",
    "by",
    "with",
    "as",
    "at",
    "from",
    "is",
    "are",
    "was",
    "were",
    "be",
    "this",
    "that",
    "these",
    "those",
    "it",
    "its",
    "their",
    "your",
    "our",
    "his",
    "her",
}

_WORD_RE = re.compile(r"[A-Za-zА-Яа-я0-9\-]+", re.UNICODE)


def tokenize(text: str) -> List[str]:
    if not text:
        return []
    tokens = [match.group(0).lower() for match in _WORD_RE.finditer(text)]
    return [tok for tok in tokens if tok not in _RU_STOP and tok not in _EN_STOP]


def title_signature(tokens: Iterable[str], top: int = 8) -> str:
    counter = Counter(tokens)
    top_tokens = [word for word, _ in counter.most_common(top)]
    return "|".join(sorted(top_tokens))


def entity_signature(
    sport: Optional[str],
    tournament: Optional[str],
    team: Optional[str],
    player: Optional[str],
) -> Optional[str]:
    parts: List[str] = []
    if tournament:
        parts.append(f"t:{tournament.strip().lower()}")
    if team:
        parts.append(f"team:{team.strip().lower()}")
    if player:
        parts.append(f"p:{player.strip().lower()}")
    if sport:
        parts.append(f"s:{sport.strip().lower()}")
    return "|".join(parts) or None


def signature_tokens(signature: str) -> List[str]:
    if not signature:
        return []
    return [piece for piece in signature.split("|") if piece]


def jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    set_a = set(a)
    set_b = set(b)
    if not set_a and not set_b:
        return 1.0
    if not set_a or not set_b:
        return 0.0
    return len(set_a & set_b) / len(set_a | set_b)


def compute_signatures(
    title: str,
    entities: Dict[str, Optional[str]],
) -> Tuple[str, Optional[str]]:
    tokens = tokenize(title)
    title_sig = title_signature(tokens)
    entity_sig = entity_signature(
        sport=entities.get("sport"),
        tournament=entities.get("tournament"),
        team=entities.get("team"),
        player=entities.get("player"),
    )
    return title_sig, entity_sig
