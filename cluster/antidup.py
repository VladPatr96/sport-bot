from __future__ import annotations

from typing import Iterable, List, Optional, Sequence, Tuple

from .fingerprints import jaccard, signature_tokens

JACCARD_STRICT = 0.90
JACCARD_ENTITY = 0.80


def is_near_duplicate(
    ref_title_sig: str,
    ref_entity_sig: Optional[str],
    candidates: Sequence[Tuple[int, str, Optional[str]]],
) -> Optional[Tuple[int, float, bool]]:
    ref_tokens = signature_tokens(ref_title_sig)
    for candidate_id, cand_title_sig, cand_entity_sig in candidates:
        cand_tokens = signature_tokens(cand_title_sig)
        score = jaccard(ref_tokens, cand_tokens)
        entity_match = bool(ref_entity_sig and cand_entity_sig and cand_entity_sig == ref_entity_sig)
        if entity_match and score >= JACCARD_ENTITY:
            return candidate_id, score, True
        if score >= JACCARD_STRICT:
            return candidate_id, score, False
    return None


def filter_near_duplicates(
    articles: Sequence[Tuple[int, str, Optional[str], dict]],
) -> Tuple[List[dict], List[dict]]:
    kept: List[Tuple[int, str, Optional[str], dict]] = []
    hidden: List[dict] = []
    for article in articles:
        article_id, title_sig, entity_sig, payload = article
        candidates = [(aid, sig, ent) for aid, sig, ent, _ in kept]
        duplicate = is_near_duplicate(title_sig, entity_sig, candidates)
        if duplicate is not None:
            duplicate_id, score, entity_match = duplicate
            hidden.append(
                {
                    **payload,
                    "duplicate_of": duplicate_id,
                    "jaccard": round(score, 3),
                    "entity_match": entity_match,
                }
            )
            continue
        kept.append(article)
    return [payload for _, _, _, payload in kept], hidden
