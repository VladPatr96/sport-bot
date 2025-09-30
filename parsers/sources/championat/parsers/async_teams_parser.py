import os
import asyncio
import aiosqlite
import yaml
from urllib.parse import urljoin

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

def build_paths():
    # __file__ is at .../parsers/sources/championat/parsers/async_teams_parser.py
    # need to go up 5 levels to reach project root
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../../../.."))
    config_path = os.path.join(project_root, "parsers", "sources", "championat", "config", "sources_config.yml")
    db_path = os.path.join(project_root, "database", "prosport.db")
    return project_root, config_path, db_path

def load_config(config_path):
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    with open(config_path, encoding="utf-8") as f:
        all_cfg = yaml.safe_load(f)
    return all_cfg["championat"]

PRAGMAS = [
    "PRAGMA journal_mode=WAL",
    "PRAGMA synchronous=NORMAL",
    "PRAGMA temp_store=MEMORY",
    "PRAGMA cache_size=-32000",
]
CREATE_INDEX_SQL = "CREATE UNIQUE INDEX IF NOT EXISTS idx_teams_url ON teams(url)"

async def open_db(db_path):
    db = await aiosqlite.connect(db_path)
    db.row_factory = aiosqlite.Row
    for p in PRAGMAS:
        await db.execute(p)
    await db.execute(CREATE_INDEX_SQL)
    await db.commit()
    return db

async def select_tournaments(db):
    cur = await db.execute("SELECT id, name, tournaments_url, type FROM tournaments WHERE tournaments_url IS NOT NULL")
    rows = await cur.fetchall()
    await cur.close()
    return rows

async def upsert_team(db, *, name, url, tag_url, tournament_id):
    cur = await db.execute("SELECT id, name, tag_url, tournament_id FROM teams WHERE url = ?", (url,))
    row = await cur.fetchone()
    await cur.close()
    if row is None:
        await db.execute(
            "INSERT INTO teams (name, url, tag_url, tournament_id) VALUES (?, ?, ?, ?)",
            (name, url, tag_url, tournament_id)
        )
        return True
    else:
        new_tag = tag_url if (tag_url and tag_url.strip()) else row["tag_url"]
        if row["name"] != name or row["tag_url"] != new_tag or row["tournament_id"] != tournament_id:
            await db.execute(
                "UPDATE teams SET name = ?, tag_url = ?, tournament_id = ? WHERE id = ?",
                (name, new_tag, tournament_id, row["id"])
            )
            return True
    return False

ROUTE_BLOCK_TYPES = {"image", "stylesheet", "font"}

async def make_browser(playwright, headless=True):
    browser = await playwright.chromium.launch(headless=headless, args=[
        "--disable-gpu",
        "--disable-dev-shm-usage",
        "--no-sandbox",
    ])
    ctx = await browser.new_context(java_script_enabled=True)
    await ctx.route("**/*", lambda route: asyncio.create_task(route.abort()) if route.request.resource_type in ROUTE_BLOCK_TYPES else asyncio.create_task(route.continue_()))
    return browser, ctx

async def goto_safe(page, url):
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=15000)
        return True
    except PlaywrightTimeout:
        return False

class AsyncTeamsParser:
    def __init__(self, config, db, ctx, verbose=False, concurrency=6):
        self.cfg = config
        self.db = db
        self.ctx = ctx
        self.verbose = verbose
        self.sem = asyncio.Semaphore(concurrency)

        sel = self.cfg.get("selectors", {})
        self.sel_teams_page_link = sel.get("teams_page_link", "a[href*='/teams/']")
        self.sel_teams_cards = sel.get("teams_cards", ".table a.table-item, .m-page__content a.table-item, .mc-sport a.table-item")
        self.sel_teams_card_name = sel.get("teams_card_name", "span.table-item__name, .table-item__name")
        self.sel_results_team_link = sel.get("results_team_link", "a[href*='/teams/'], a[href*='/team/']")
        self.sel_team_tag_link = sel.get("team_tag_link", "a[href$='.html'], a[href*='/tag/']")

    async def _collect_teams_on_listing(self, page):
        teams = []
        try:
            await page.wait_for_selector(self.sel_teams_cards, timeout=8000)
            cards = await page.query_selector_all(self.sel_teams_cards)
            for a in cards:
                href = await a.get_attribute("href") or ""
                if not href:
                    continue
                url_abs = urljoin(page.url, href)
                name = ""
                name_el = await a.query_selector(self.sel_teams_card_name)
                if name_el:
                    name = (await name_el.text_content() or "").strip()
                if not name:
                    name = (await a.text_content() or "").strip()
                if not name:
                    continue
                teams.append((name, url_abs))
        except Exception:
            pass
        return teams

    async def _collect_teams_from_results(self, page):
        teams = []
        try:
            links = await page.query_selector_all(self.sel_results_team_link)
            for a in links:
                href = await a.get_attribute("href") or ""
                if not href:
                    continue
                url_abs = urljoin(page.url, href)
                name = (await a.text_content() or "").strip()
                if not name:
                    continue
                teams.append((name, url_abs))
        except Exception:
            pass
        return teams

    async def _extract_tag_url(self, team_page_url):
        page = await self.ctx.new_page()
        ok = await goto_safe(page, team_page_url)
        if not ok:
            await page.close()
            return None
        tag = None
        try:
            links = await page.query_selector_all(self.sel_team_tag_link)
            for a in links:
                href = await a.get_attribute("href") or ""
                if href.endswith(".html") or "/tag/" in href:
                    tag = href
                    break
        except Exception:
            tag = None
        await page.close()
        return tag

    async def process_tournament(self, tournament):
        t_id, t_name, t_url = tournament["id"], tournament["name"], tournament["tournaments_url"]
        if self.verbose:
            print(f"\n--- Турнир: {t_name} (ID: {t_id}) ---")

        page = await self.ctx.new_page()
        ok = await goto_safe(page, t_url)
        if not ok:
            await page.close()
            if self.verbose:
                print(f"   ⚠ не открылся: {t_url}")
            return

        teams_page_href = None
        try:
            link_el = await page.query_selector(self.sel_teams_page_link)
            if link_el:
                teams_page_href = await link_el.get_attribute("href")
        except Exception:
            teams_page_href = None

        teams = []
        if teams_page_href:
            ok2 = await goto_safe(page, teams_page_href)
            if ok2:
                teams = await self._collect_teams_on_listing(page)

        if not teams:
            await goto_safe(page, t_url)
            teams = await self._collect_teams_from_results(page)

        await page.close()

        uniq = []
        seen = set()
        for name, url in teams:
            if url and url not in seen:
                seen.add(url)
                uniq.append((name, url))

        await self.db.execute("BEGIN")
        changed = 0

        async def _handle_team(name, url):
            nonlocal changed
            async with self.sem:
                tag_url = await self._extract_tag_url(url)
                updated = await upsert_team(self.db, name=name, url=url, tag_url=tag_url, tournament_id=t_id)
                if updated:
                    changed += 1

        await asyncio.gather(*[_handle_team(n, u) for n, u in uniq])
        await self.db.commit()

        if self.verbose:
            print(f"   команд обработано: {len(uniq)}, изменено в БД: {changed}")

    async def run(self):
        tournaments = await select_tournaments(self.db)
        sem = asyncio.Semaphore(4)

        async def _wrap(t):
            async with sem:
                await self.process_tournament(t)

        await asyncio.gather(*[_wrap(t) for t in tournaments])

async def main():
    project_root, config_path, db_path = build_paths()
    print(f"Загрузка конфигурации из: {config_path}")
    print(f"Подключение к базе данных: {db_path}")

    cfg = load_config(config_path)
    db = await open_db(db_path)

    async with async_playwright() as pw:
        browser, ctx = await make_browser(pw, headless=True)
        try:
            parser = AsyncTeamsParser(cfg, db, ctx, verbose=False, concurrency=8)
            await parser.run()
        finally:
            await ctx.close()
            await browser.close()
            await db.close()

if __name__ == "__main__":
    asyncio.run(main())
