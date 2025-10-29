from __future__ import annotations

import re
from typing import Literal, Optional
from urllib.parse import urlparse

TagType = Literal['team', 'player', 'unknown']
_KNOWN_TYPES = {'sport', 'tournament', 'team', 'player'}

# Frequent Russian football/hockey club prefixes.
_TEAM_PREFIXES: tuple[str, ...] = (
    'фк ',
    'фк-',
    'fc ',
    'fc-',
    'fk ',
    'fk-',
    'ск ',
    'ск-',
    'hc ',
    'hc-',
    'bc ',
    'bc-',
    'хк ',
    'хк-',
    'бк ',
    'бк-',
    'пфк ',
    'сборная ',
    'сборная-',
    'лос анджелес',
    'лос-анджелес',
    'цска',
    'ак барс',
    'зенит',
    'спартак',
    'динамо',
    'локомотив',
    'ростов',
    'сочи',
    'крылья',
    'ахмат',
    'урал',
    'краснодар',
    'рубин',
    'амкар',
    'амур',
    'авангард',
    'салават',
    'витязь',
    'торпедо',
    'северсталь',
    'автомобилист',
    'адмирал',
    'нефтьехимик',
    'трактор',
    'химки',
    'оренбург',
)

_TEAM_ABBREVIATION_RE = re.compile(r'\b(FC|CF|SC|HC|B|BC)\b', re.IGNORECASE)
_TEAM_URL_HINTS = (
    '/team/',
    '/teams/',
    '/club/',
    '/klub/',
    '/komanda/',
    '/squad/',
    '/roster/',
)

_TEAM_SINGLE_NAMES = {
    'крылья',
    'спартак',
    'нефтьехимик',
    'салават',
    'сочи',
    'витязь',
    'адмирал',
    'рубин',
    'северсталь',
    'локомотив',
    'автомобилист',
    'урал',
    'амкар',
    'динамо',
    'авангард',
    'амур',
    'краснодар',
    'ростов',
    'зенит',
    'ахмат',
    'торпедо',
    'цска',
}

_TEAM_CITY_PATTERNS = (
    'москва',
    'санкт петербург',
    'петербург',
    'питер',
    'минск',
    'казань',
    'самара',
    'тольятти',
    'екатеринбург',
    'нижний новгород',
    'новосибирск',
    'ростов',
    'сочи',
    'уфа',
    'омск',
    'ярославль',
    'череповец',
    'нижнекамск',
    'владивосток',
    'хабаровск',
    'красноярск',
)

_PLAYER_NAME_RE = re.compile(
    r"^[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё\-\'’]+(?:\s+[A-ZА-ЯЁ][A-Za-zА-Яа-яЁё\-\'’]+){1,2}$"
)
_PLAYER_URL_HINTS = (
    '/player/',
    '/players/',
    '/igrok/',
    '/igroki/',
    '/hockeyplayer/',
    '/hockeyplayers/',
    '/footballplayer/',
    '/footballplayers/',
)

_PLAYER_ROLE_MARKERS = (
    'нападающий',
    'защитник',
    'форвард',
    'полузащитник',
    'вратарь',
    'голкипер',
    'капитан',
    'снайпер',
    'striker',
    'defender',
    'forward',
    'winger',
    'goalie',
    'goalkeeper',
    'center',
    'centre',
    'center-back',
    'centre-back',
    'midfielder',
)
_PLAYER_MARKER_WINDOW = 40


def _clean_text(value: Optional[str]) -> str:
    if not value:
        return ''
    return value.strip()


def _to_matchable(value: str) -> str:
    return value.lower()


def _split_words(value: str) -> list[str]:
    return [token for token in re.split(r'[\s\-]+', value) if token]


def _smart_title(name: str) -> str:
    name = _clean_text(name)
    if not name:
        return ''

    def _capitalize_token(token: str) -> str:
        if not token:
            return token
        return token[0].upper() + token[1:].lower()

    tokens = []
    for part in re.split(r'\s+', name):
        subparts = part.split('-')
        subparts = [_capitalize_token(sub) for sub in subparts]
        tokens.append('-'.join(subparts))
    return ' '.join(token for token in tokens if token)


def _is_person_name(name: str) -> bool:
    candidate = _clean_text(name)
    if not candidate:
        return False
    if _PLAYER_NAME_RE.match(candidate):
        return True
    candidate_title = _smart_title(candidate)
    if candidate_title and _PLAYER_NAME_RE.match(candidate_title):
        return True
    return False


def _extract_slug_fragment(url: Optional[str]) -> str:
    if not url:
        return ''
    try:
        parsed = urlparse(url)
    except ValueError:
        return ''
    path = (parsed.path or '').rstrip('/')
    if not path:
        return ''
    slug = path.rsplit('/', 1)[-1]
    slug = slug.lstrip('0123456789-_')
    slug = slug.replace('-', ' ')
    return slug.strip()


def _matches_team_prefix(text: str) -> bool:
    for prefix in _TEAM_PREFIXES:
        if text.startswith(prefix):
            return True
    return False


def _match_team_one_word_city(words: list[str]) -> bool:
    if len(words) not in {2, 3}:
        return False
    base = words[0]
    rest = ' '.join(words[1:])
    if base not in _TEAM_SINGLE_NAMES:
        return False
    for pattern in _TEAM_CITY_PATTERNS:
        if rest.startswith(pattern):
            return True
    return False


def _guess_team(name_matchable: str, name_words: list[str], url: Optional[str]) -> bool:
    if _matches_team_prefix(name_matchable):
        return True

    slug = _extract_slug_fragment(url)
    if slug:
        slug_matchable = slug.lower()
        if _matches_team_prefix(slug_matchable):
            return True
        if _TEAM_ABBREVIATION_RE.search(slug):
            return True

    if _TEAM_ABBREVIATION_RE.search(name_matchable):
        return True

    if url:
        lowered_url = url.lower()
        if any(hint in lowered_url for hint in _TEAM_URL_HINTS):
            return True
        if _TEAM_ABBREVIATION_RE.search(url):
            return True

    if _match_team_one_word_city(name_words):
        return True

    return False


def _has_player_marker_near(name: str, context: str, window: int = _PLAYER_MARKER_WINDOW) -> bool:
    context_lower = context.lower()
    name_words = _split_words(name.lower())
    if not name_words:
        return False

    pattern = r'\b' + r'(?:[\s\-]+)'.join(re.escape(word) for word in name_words) + r'\b'
    for match in re.finditer(pattern, context_lower):
        start = max(0, match.start() - window)
        end = min(len(context_lower), match.end() + window)
        snippet = context_lower[start:end]
        if any(marker in snippet for marker in _PLAYER_ROLE_MARKERS):
            return True

    # Fallback: check each word individually (surname only tags).
    for word in name_words:
        for match in re.finditer(r'\b' + re.escape(word) + r'\b', context_lower):
            start = max(0, match.start() - window)
            end = min(len(context_lower), match.end() + window)
            snippet = context_lower[start:end]
            if any(marker in snippet for marker in _PLAYER_ROLE_MARKERS):
                return True

    return False


def _guess_player(name_original: str, url: Optional[str], context: Optional[str]) -> bool:
    candidate = _clean_text(name_original)
    if not candidate:
        return False

    if _is_person_name(candidate):
        return True

    if context and _has_player_marker_near(candidate, context):
        return True

    if url:
        lowered_url = url.lower()
        if any(hint in lowered_url for hint in _PLAYER_URL_HINTS):
            words = _split_words(candidate.lower())
            if len(words) >= 2:
                return True
            if context and _has_player_marker_near(candidate, context):
                return True

    return False


def guess_tag_type_with_context(
    name: Optional[str],
    url: Optional[str],
    context: Optional[str],
) -> TagType:
    clean_name = _clean_text(name)
    clean_url = _clean_text(url)
    clean_context = _clean_text(context)

    if clean_name:
        matchable_name = _to_matchable(clean_name)
        name_words = _split_words(matchable_name)
        if _guess_team(matchable_name, name_words, clean_url):
            return 'team'
        if _guess_player(clean_name, clean_url, clean_context):
            return 'player'

    if clean_url:
        slug = _extract_slug_fragment(clean_url)
        if slug:
            slug_matchable = slug.lower()
            slug_words = _split_words(slug_matchable)
            if _guess_team(slug_matchable, slug_words, clean_url):
                return 'team'
            if not clean_name:
                slug_title = _smart_title(slug)
                if _guess_player(slug_title, clean_url, clean_context):
                    return 'player'

    if clean_url:
        lowered_url = clean_url.lower()
        if any(hint in lowered_url for hint in _TEAM_URL_HINTS):
            return 'team'
        if any(hint in lowered_url for hint in _PLAYER_URL_HINTS):
            if clean_name:
                if len(_split_words(clean_name.lower())) >= 2:
                    return 'player'
            else:
                slug = _extract_slug_fragment(clean_url)
                if slug and _is_person_name(_smart_title(slug)):
                    return 'player'

    return 'unknown'


def guess_tag_type(name: Optional[str], url: Optional[str]) -> TagType:
    return guess_tag_type_with_context(name, url, None)


def enrich_tag_type(
    raw_type: Optional[str],
    name: Optional[str],
    url: Optional[str],
    context: Optional[str] = None,
) -> str:
    raw_clean = (_clean_text(raw_type).lower()) or 'unknown'
    if raw_clean in _KNOWN_TYPES:
        return raw_clean

    guess = guess_tag_type_with_context(name, url, context)
    return guess if guess != 'unknown' else raw_clean
