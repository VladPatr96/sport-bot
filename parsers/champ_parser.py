# parsers/champ_parser.py

import time, requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

class ChampParser:
    def __init__(self, config):
        self.base_url = config["url"]
        rate, _ = config["rate_limit"].split("/")
        self.delay = 1.0 / float(rate)
        self.headers = {
            "User-Agent": "Mozilla/5.0"
        }
        self.cfg = config["parser"]

    def _sleep(self):
        time.sleep(self.delay)

    def fetch_list(self):
        resp = requests.get(self.base_url, headers=self.headers)
        resp.raise_for_status(); self._sleep()
        soup = BeautifulSoup(resp.text, "html.parser")
        items = []
        for date_block in soup.select(self.cfg["date_group"]):
            date_txt = date_block.get_text(strip=True)
            sib = date_block.find_next_sibling()
            while sib and "news-item" in sib.get("class", []):
                time_el = sib.select_one(self.cfg["time"])
                time_txt = time_el.get_text(strip=True) if time_el else ""
                link_el = sib.select_one(self.cfg["article_link"])
                if link_el:
                    url = urljoin(self.base_url, link_el["href"])
                    items.append({
                        "url": url,
                        "published": f"{date_txt} {time_txt}",
                        "tag": sib.select_one(self.cfg["tag"]).get_text(strip=True) if sib.select_one(self.cfg["tag"]) else "",
                        "comments": None
                    })
                sib = sib.find_next_sibling()
        return items

    def fetch_article(self, meta):
        resp = requests.get(meta["url"], headers=self.headers)
        resp.raise_for_status(); self._sleep()
        soup = BeautifulSoup(resp.text, "html.parser")

        # title
        title_el = soup.select_one(self.cfg["article_title"])
        title = title_el.get_text(strip=True) if title_el else ""
        print(f"[DEBUG] Заголовок: {title}")

        # summary = first <p> inside body_container
        body_container = soup.select_one(self.cfg["article_body_container"])
        summary = ""
        if body_container:
            p = body_container.find("p")
            summary = p.get_text(" ", strip=True) if p else ""

        # body_html (до "Материалы по теме")
        body_html = ""
        if body_container:
            # remove everything after heading
            for el in body_container.find_all():
                text = el.get_text(strip=True)
                if any(phrase in text for phrase in ["Материалы по теме", "Сейчас читают", "Источник", "Читайте также"]):
                    el.decompose(); break
            body_html = body_container.decode_contents()

        # tags
        tags = [t.get_text(strip=True) for t in soup.select(self.cfg["article_tags"])]

        # images
        images = []
        for img in soup.select(self.cfg["article_images"]):
            src = img.get("data-src") or img.get("src")
            if src: images.append(urljoin(meta["url"], src))

        # videos
        videos = []
        for v in soup.select(self.cfg["article_videos"]):
            src = v.get("src") or v.get("data-src")
            if src: videos.append(src)

        return {
            "url":      meta["url"],
            "title":    title,
            "published":meta["published"],
            "tags":     tags,
            "summary":  summary,
            "body":     body_html,
            "images":   images,
            "videos":   videos
        }
