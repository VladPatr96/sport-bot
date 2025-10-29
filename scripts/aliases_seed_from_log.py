
from __future__ import annotations

import argparse
import logging
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List

import yaml

from categorizer.normalize import normalize_token

LOGGER = logging.getLogger(__name__)
BASE_DIR = Path(__file__).resolve().parents[1]
LOG_DIR = BASE_DIR / "database" / "logs"
SEED_PATH = BASE_DIR / "mappings" / "aliases_seed.yml"
ALLOWED_TYPES = ("sport", "tournament", "team", "player")


def parse_unknown_log(path: Path) -> Dict[str, Dict[str, int]]:
    counters: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            parts = line.split("	")
            if len(parts) < 4:
                continue
            _news_id, alias, alias_type, _tag_id = parts[:4]
            alias_type = alias_type.strip()
            if alias_type not in ALLOWED_TYPES:
                continue
            normalized = normalize_token(alias)
            if not normalized:
                continue
            counters[alias_type][alias.strip()] += 1
    return counters


def build_seed_data(counters: Dict[str, Dict[str, int]], top: int) -> Dict[str, List[Dict[str, object]]]:
    seed: Dict[str, List[Dict[str, object]]] = {t: [] for t in ALLOWED_TYPES}
    for alias_type in ALLOWED_TYPES:
        aliases = counters.get(alias_type, {})
        sorted_aliases = sorted(aliases.items(), key=lambda item: item[1], reverse=True)[:top]
        for alias, _count in sorted_aliases:
            seed[alias_type].append(
                {
                    "canonical": alias,
                    "entity_id": None,
                    "aliases": [alias],
                }
            )
    return seed


def write_seed(seed: Dict[str, List[Dict[str, object]]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "aliases": seed,
    }
    with path.open('w', encoding='utf-8') as fh:
        yaml.safe_dump(
            data,
            fh,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
        )


def find_latest_log() -> Path:
    candidates = sorted(LOG_DIR.glob('unknown_aliases_*.log'))
    if not candidates:
        raise FileNotFoundError('No unknown_aliases_*.log found in database/logs/')
    return candidates[-1]


def main() -> None:
    parser = argparse.ArgumentParser(description='Generate alias seed YAML from the latest unknown alias log')
    parser.add_argument('--top', type=int, default=50)
    parser.add_argument('--log', type=Path)
    parser.add_argument('--output', type=Path, default=SEED_PATH)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    log_path = args.log if args.log else find_latest_log()
    LOGGER.info('Parsing unknown-alias log: %s', log_path)
    counters = parse_unknown_log(log_path)
    seed_data = build_seed_data(counters, args.top)
    write_seed(seed_data, args.output)
    LOGGER.info('Seed written to %s', args.output)


if __name__ == '__main__':
    main()
