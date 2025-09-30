import asyncio
import os
import sqlite3
import yaml
from dataclasses import dataclass
from typing import Dict, List, Optional, Set
from urllib.parse import urljoin, urlparse

import aiohttp
from aiohttp import ClientSession, CookieJar
from bs4 import BeautifulSoup
import subprocess
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
)
from yarl import URL


TABLE_SELECTOR = "table.table-row-hover.mc-sport-tournament__drop-block"
VERBOSE_EVERY_LINK = os.environ.get("LOG_EVERY_LINK", "0") == "1"
LOG_DEBUG = os.environ.get("LOG_DEBUG", "1") == "1"


@dataclass
class FetchResult:
    url: str
    html: Optional[str] = None
    error: Optional[str] = None
    status: Optional[int] = None
    snippet: Optional[str] = None


def debug(msg: str) -> None:
    if LOG_DEBUG:
        print(f"[DEBUG] {msg}")


def _log_row_full(cursor: sqlite3.Cursor, row_id: int, tag: str) -> None:
    """Log full tournament row state: id, name, url, sport_id, season, tournaments_url, type."""
    try:
        row = cursor.execute("SELECT * FROM tournaments WHERE id=?", (row_id,)).fetchone()
    except sqlite3.Error as e:
        print(f"[DB][{tag}][error] id={row_id} select-failed reason={e}")
        return
    if not row:
        print(f"[DB][{tag}][warn] id={row_id} not found")
        return
    try:
        type_val = row["type"]
    except Exception:
        type_val = None
    print(
        f"[DB][{tag}] id={row['id']} name={row['name']} url={row['url']} sport_id={row['sport_id']} "
        f"season={row['season']} tournaments_url={row['tournaments_url']} type={type_val}"
    )


def insert_tournament(
    cursor: sqlite3.Cursor,
    name: str,
    url: str,
    sport_id: int,
    season: Optional[str] = None,
    tournaments_url: Optional[str] = None,
) -> Optional[int]:
    try:
        cursor.execute(
            """
            INSERT OR IGNORE INTO tournaments (name, url, sport_id, season, tournaments_url)
            VALUES (?, ?, ?, ?, ?)
            """,
            (name, url, sport_id, season, tournaments_url),
        )
        if cursor.lastrowid:
            _log_row_full(cursor, cursor.lastrowid, "insert")
            return cursor.lastrowid

        cursor.execute(
            "SELECT id, name, url, season FROM tournaments WHERE tournaments_url = ?",
            (tournaments_url,),
        )
        row = cursor.fetchone()
        if not row:
            print(f"[DB][warn] lookup-miss name={name} tournaments_url={tournaments_url}")
            return None

        existing_id, existing_name, existing_url, existing_season = (
            row["id"],
            row["name"],
            row["url"],
            row["season"],
        )
        needs_update = (
            existing_name != name or existing_url != url or existing_season != season
        )
        if needs_update:
            cursor.execute(
                "UPDATE tournaments SET name = ?, url = ?, season = ? WHERE id = ?",
                (name, url, season, existing_id),
            )
            _log_row_full(cursor, existing_id, "update")
        else:
            _log_row_full(cursor, existing_id, "keep")
        return existing_id
    except sqlite3.Error as e:
        print(f"[DB][error] name={name} tournaments_url={tournaments_url} reason={e}")
        return None


class ChampionatTournamentsParser:
    def __init__(self, config_path: str, db_path: str):
        self.config = self._load_config(config_path)

        self.driver = self._init_driver()
        self.wait = WebDriverWait(self.driver, 15)

        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")

        try:
            self.cursor.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_tournaments_url ON tournaments(tournaments_url);"
            )
        except sqlite3.Error:
            pass

        selectors_section = (
            self.config.get("selectors")
            or self.config.get("parser")
            or {}
        )
        self.stat_slug_overrides = selectors_section.get("stat_slug_overrides", {}) or {}

        self.async_timeout = 25
        self.async_concurrency = 6

    def _load_config(self, path: str) -> Dict:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Config not found: {path}")
        with open(path, encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
        return cfg["championat"]

    def _init_driver(self) -> webdriver.Chrome:
        opts = webdriver.ChromeOptions()
        opts.add_argument("--headless=new")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-webgl")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--enable-unsafe-swiftshader")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--ignore-certificate-errors")
        opts.add_argument("--allow-insecure-localhost")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        # Reduce noisy Chrome/Driver logs
        opts.add_argument("--log-level=3")
        opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
        )
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2,
            "profile.managed_default_content_settings.fonts": 2,
        }
        opts.add_experimental_option("prefs", prefs)
        opts.page_load_strategy = "eager"
        from selenium.webdriver.chrome.service import Service
        service = Service(log_output=subprocess.DEVNULL)
        return webdriver.Chrome(service=service, options=opts)

    def _sports_from_db(self):
        self.cursor.execute("SELECT id, name, url, slug FROM sports")
        return self.cursor.fetchall()

    def parse_tournaments(self):
        seen_tournament_urls: Set[str] = {
            row[0]
            for row in self.cursor.execute(
                "SELECT tournaments_url FROM tournaments WHERE tournaments_url IS NOT NULL"
            )
        }

        for sport in self._sports_from_db():
            sport_id = sport["id"]
            sport_name = sport["name"]
            sport_url = sport["url"]
            slug = sport["slug"]
            effective_slug = self.stat_slug_overrides.get(slug, slug)

            print(
                f"\n--- Обработка вида спорта: {sport_name} "
                f"(ID {sport_id}, URL: {sport_url}, slug: {slug}, stat slug: {effective_slug}) ---"
            )
            stat_url = f"https://www.championat.com/stat/{effective_slug}/"
            print(f"  Переходим на страницу статистики/турниров: {stat_url}")

            try:
                self.driver.get(stat_url)
                tournament_links = self._extract_category_links(stat_url)
                async_results = self._fetch_pages_async(tournament_links)

                for result in async_results:
                    if result.html:
                        debug(
                            f"Async success {result.url}: status={result.status}, "
                            f"len={len(result.html)}"
                        )
                        self._process_html_result(
                            result.url, result.html, sport_id, seen_tournament_urls
                        )
                    else:
                        debug(
                            f"Async failure {result.url}: status={result.status}, "
                            f"error={result.error}, snippet={result.snippet}"
                        )
                        print(
                            f"    ⚠️ Async fetch failed for {result.url}: {result.error}. "
                            "Пробуем Selenium."
                        )
                        self._process_with_selenium(
                            result.url, sport_id, seen_tournament_urls
                        )

            except TimeoutException as e:
                print(f"  ✖ Timeout на {stat_url}: {e}")
            except Exception as e:
                print(f"  ✖ Непредвиденная ошибка на {stat_url}: {e}")

    def _extract_category_links(self, stat_url: str) -> List[str]:
        links: List[str] = []
        try:
            li_elements = self.wait.until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.block__title"))
            )
            debug(f"Нашли {len(li_elements)} блоков категорий для {stat_url}")
            for li in li_elements:
                for a in li.find_elements(By.CSS_SELECTOR, "a.block__more"):
                    href = a.get_attribute("href")
                    if href:
                        links.append(href)
        except TimeoutException:
            print("  ⚠️ Не нашли блоки категорий, используем основную страницу.")
        except Exception as e:
            print(f"  ⚠️ Ошибка при сборе ссылок категорий: {e}")

        if not links:
            links = [stat_url]
        else:
            dedup: List[str] = []
            seen = set()
            for link in links:
                if link not in seen:
                    seen.add(link)
                    dedup.append(link)
            links = dedup
        debug(f"Ссылки для обработки ({len(links)}): {links}")
        if VERBOSE_EVERY_LINK and links:
            for i, l in enumerate(links, 1):
                print(f"[LINK][categories][{i}] {l}")
        return links

    def _fetch_pages_async(self, urls: List[str]) -> List[FetchResult]:
        if not urls:
            return []

        cookies = {cookie["name"]: cookie["value"] for cookie in self.driver.get_cookies()}
        debug(f"Cookies из Selenium: {cookies}")

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru,en;q=0.9",
            "Referer": "https://www.championat.com/",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "same-origin",
            "Upgrade-Insecure-Requests": "1",
        }

        async def _fetch_one(
            session: ClientSession,
            url: str,
            semaphore: asyncio.Semaphore,
        ) -> FetchResult:
            async with semaphore:
                try:
                    async with session.get(url, timeout=self.async_timeout, cookies=cookies) as resp:
                        status = resp.status
                        text = await resp.text()
                        snippet = text[:200].replace("\n", " ")
                        debug(f"GET {url}: status={status}, len={len(text)}, snippet={snippet}")
                        if status != 200:
                            return FetchResult(url=url, status=status, snippet=snippet, error=f"HTTP {status}")
                        soup = BeautifulSoup(text, "html.parser")
                        if not soup.select(TABLE_SELECTOR):
                            return FetchResult(
                                url=url,
                                status=status,
                                snippet=snippet,
                                error="tables not found",
                            )
                        return FetchResult(url=url, html=text, status=status, snippet=snippet)
                except Exception as exc:
                    return FetchResult(url=url, error=str(exc), snippet=str(exc))

        async def _run(urls_batch: List[str]) -> List[FetchResult]:
            semaphore = asyncio.Semaphore(self.async_concurrency)
            jar = CookieJar(unsafe=True)
            jar.update_cookies(cookies, response_url=URL("https://www.championat.com/"))
            async with aiohttp.ClientSession(headers=headers, cookie_jar=jar) as session:
                tasks = [_fetch_one(session, url, semaphore) for url in urls_batch]
                return await asyncio.gather(*tasks)

        try:
            return asyncio.run(_run(urls))
        except RuntimeError:
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)
                return loop.run_until_complete(_run(urls))
            finally:
                loop.close()
                asyncio.set_event_loop(None)
        except Exception as exc:
            print(f"  ⚠️ Async fetch failed entirely: {exc}")
            return [FetchResult(url=url, error=str(exc)) for url in urls]

    def _process_html_result(
        self,
        url: str,
        html: str,
        sport_id: int,
        seen_urls: Set[str],
    ):
        soup = BeautifulSoup(html, "html.parser")
        tables = soup.select(TABLE_SELECTOR)
        debug(f"HTML {url}: найдено таблиц {len(tables)} (async)")
        if not tables:
            print(f"    ⚠️ В HTML страницы {url} отсутствуют таблицы турниров (async).")
            return

        for idx, table in enumerate(tables, 1):
            rows = table.select("tr.fav-item") or table.select("tr")
            debug(f"  Таблица {idx}: строк {len(rows)}")
            for row in rows:
                link_el = row.select_one("a.table-item")
                name_el = row.select_one("span.table-item__name") or row.select_one("a.table-item")
                if not link_el or not name_el:
                    print(f"[SKIP][async][no-elements] page={url}")
                    continue

                href_tournament_data = link_el.get("href") or ""
                if not href_tournament_data:
                    print(f"[SKIP][async][no-href] page={url}")
                    continue
                href_tournament_data = urljoin(url, href_tournament_data)

                if href_tournament_data in seen_urls:
                    print(f"[SKIP][dup][async] {href_tournament_data}")
                    continue
                seen_urls.add(href_tournament_data)

                name = name_el.get_text(strip=True)
                if not name:
                    print(f"[SKIP][async][no-name] tournaments_url={href_tournament_data}")
                    continue

                debug(f"    → {name}: {href_tournament_data}")
                news_tag_url = self._derive_news_tag_url(href_tournament_data)
                if VERBOSE_EVERY_LINK:
                    print(f"[LINK][async] name={name} tournaments_url={href_tournament_data} news_tag_url={news_tag_url}")
                insert_tournament(
                    cursor=self.cursor,
                    name=name,
                    url=news_tag_url,
                    sport_id=sport_id,
                    season=None,
                    tournaments_url=href_tournament_data,
                )

    def _process_with_selenium(
        self,
        url: str,
        sport_id: int,
        seen_urls: Set[str],
    ):
        debug(f"Selenium fallback для {url}")
        try:
            self.driver.get(url)
            containers = self.wait.until(
                EC.presence_of_all_elements_located((By.CLASS_NAME, "mc-sport"))
            )
            debug(f"Selenium: найдено контейнеров {len(containers)}")
        except TimeoutException:
            print(f"    ⚠️ Selenium: на странице {url} не нашли блоков mc-sport.")
            return
        except Exception as e:
            print(f"    ✖ Selenium: ошибка при загрузке {url}: {e}")
            return

        for container_idx, container in enumerate(containers, 1):
            anchors = container.find_elements(By.CSS_SELECTOR, "a.table-item")
            debug(f"  Контейнер {container_idx}: ссылок {len(anchors)}")
            for a in anchors:
                name = ""
                href_tournament_data = ""

                raw_href = a.get_attribute("href")
                if raw_href:
                    href_tournament_data = urljoin(self.driver.current_url, raw_href)
                else:
                    try:
                        js_href = self.driver.execute_script(
                            "return arguments[0].getAttribute('href');", a
                        )
                        if js_href:
                            href_tournament_data = urljoin(self.driver.current_url, js_href)
                    except Exception as js_href_e:
                        print(
                            f"    ? Ошибка JS при получении href: {js_href_e}. "
                            f"HTML: {a.get_attribute('outerHTML')[:200]}..."
                        )
                        href_tournament_data = ""

                try:
                    name_span_element = WebDriverWait(a, 5).until(
                        EC.visibility_of_element_located(
                            (By.CSS_SELECTOR, "span.table-item__name")
                        )
                    )
                    name = name_span_element.text.strip()
                except (NoSuchElementException, TimeoutException):
                    pass

                if not name:
                    name = a.text.strip()

                if not name:
                    try:
                        name = (
                            self.driver.execute_script(
                                "return arguments[0].textContent;", a
                            )
                            .strip()
                        )
                    except Exception as js_e:
                        print(
                            f"    ? Ошибка JS при получении textContent для {href_tournament_data}: {js_e}"
                        )
                        name = ""

                if not name:
                    print(f"[SKIP][selenium][no-name] tournaments_url={href_tournament_data}")
                    continue

                if not href_tournament_data:
                    print(f"[SKIP][selenium][no-href] name={name}")
                    continue

                if href_tournament_data in seen_urls:
                    print(f"[SKIP][dup][selenium] {href_tournament_data}")
                    continue
                seen_urls.add(href_tournament_data)

                debug(f"    → {name}: {href_tournament_data} (Selenium)")
                news_tag_url = self._derive_news_tag_url(href_tournament_data)
                if VERBOSE_EVERY_LINK:
                    print(f"[LINK][selenium] name={name} tournaments_url={href_tournament_data} news_tag_url={news_tag_url}")
                insert_tournament(
                    cursor=self.cursor,
                    name=name,
                    url=news_tag_url,
                    sport_id=sport_id,
                    season=None,
                    tournaments_url=href_tournament_data,
                )

    @staticmethod
    def _derive_news_tag_url(tournament_href: str) -> str:
        parsed = urlparse(tournament_href)
        base = f"{parsed.scheme}://{parsed.netloc}"
        segments = [seg for seg in parsed.path.split("/") if seg]

        if "tournament" in segments:
            idx = segments.index("tournament")
            path = "/" + "/".join(segments[:idx]) + ".html"
            return urljoin(base, path)

        if len(segments) >= 2:
            path = "/" + "/".join(segments[:2]) + ".html"
            return urljoin(base, path)

        if segments:
            return urljoin(base, f"/{segments[0]}.html")

        return tournament_href

    def close(self):
        if self.conn:
            self.conn.commit()
            print("\n✔ Все изменения сохранены в базе.")
            self.conn.close()
            print("✔ Подключение к базе данных закрыто.")
        if self.driver:
            self.driver.quit()
            print("✔ WebDriver завершён.")


if __name__ == "__main__":
    config_path = os.path.join(
        os.path.dirname(os.getcwd()),
        "sport-news-bot",
        "parsers",
        "sources",
        "championat",
        "config",
        "sources_config.yml",
    )
    db_path = os.path.join(
        os.path.dirname(os.getcwd()),
        "sport-news-bot",
        "database",
        "prosport.db",
    )

    print(f"Читаем конфигурацию из: {config_path}")
    print(f"Подключаемся к базе данных: {db_path}")

    parser = None
    try:
        parser = ChampionatTournamentsParser(config_path, db_path)
        parser.parse_tournaments()
    except FileNotFoundError as e:
        print(f"Критическая ошибка: {e}")
    except Exception as e:
        print(f"Непредвиденная ошибка: {e}")
    finally:
        if parser:
            parser.close()
