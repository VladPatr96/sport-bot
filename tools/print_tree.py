#!/usr/bin/env python3
from __future__ import annotations
import argparse, os, sys
from pathlib import Path

DEFAULT_EXCLUDES = {
    ".venv", ".git", "__pycache__", "node_modules",
    ".mypy_cache", ".ruff_cache", "dist", "build", ".idea", ".pytest_cache"
}

def is_excluded(path: Path, excludes: set[str]) -> bool:
    parts = set(p.name for p in path.parts and path.parents)  # parents set
    # Быстро: если любое имя из пути в excludes — пропускаем
    for part in path.parts:
        if part in excludes:
            return True
    return False

def tree(root: Path, max_depth: int, excludes: set[str]) -> list[str]:
    lines: list[str] = []
    root = root.resolve()

    def walk(dir_path: Path, prefix: str = "", depth: int = 0):
        if depth > max_depth:
            return
        try:
            entries = sorted(dir_path.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
        except PermissionError:
            return
        entries = [e for e in entries if not is_excluded(e.relative_to(root), excludes)]
        for i, e in enumerate(entries):
            connector = "└── " if i == len(entries) - 1 else "├── "
            lines.append(f"{prefix}{connector}{e.name}")
            if e.is_dir():
                ext_prefix = f"{prefix}{'    ' if i == len(entries) - 1 else '│   '}"
                walk(e, ext_prefix, depth + 1)

    lines.append(root.name)
    walk(root, "", 1 if max_depth == 0 else 0)
    return lines

def main():
    ap = argparse.ArgumentParser(description="Печатает дерево проекта без .venv и прочего мусора.")
    ap.add_argument("root", nargs="?", default=".", help="Корень проекта (по умолчанию текущая папка)")
    ap.add_argument("--max-depth", type=int, default=6, help="Максимальная глубина дерева (по умолчанию 6)")
    ap.add_argument("--exclude", "-E", action="append", default=[], help="Исключить имя каталога/файла (можно несколько)")
    ap.add_argument("--output", "-o", type=str, help="Сохранить в файл (например, docs/PROJECT_TREE.md)")
    args = ap.parse_args()

    excludes = set(DEFAULT_EXCLUDES) | set(args.exclude or [])
    root = Path(args.root).resolve()

    lines = tree(root, args.max_depth, excludes)
    output = "```\n" + "\n".join(lines) + "\n```"

    if args.output:
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(output, encoding="utf-8")
        print(f"✅ Сохранено в {out_path}")
    else:
        print(output)

if __name__ == "__main__":
    main()
