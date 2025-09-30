# utils/render.py

import re
from bs4 import BeautifulSoup

def clean_html_for_telegram(raw_html: str) -> str:
    # remove <p> tags → single newline, strip after-marker
    html = raw_html.replace("</p>", "\n").replace("<p>", "")
    # strip all tags except allowed
    soup = BeautifulSoup(html, "html.parser")
    allowed = {"b","strong","i","em","u","s","a","code","pre","blockquote"}
    for tag in soup.find_all(True):
        if tag.name not in allowed:
            tag.unwrap()
    text = ''.join(str(el) for el in soup.contents)
    # collapse multiple newlines
    return re.sub(r'\n{2,}', '\n\n', text).strip()

def render_preview(article):
    """
    Короткий preview для Telegram: заголовок + саммари + первая картинка
    """
    parts = []
    if article["images"]:
        parts.append(f'<a href="{article["images"][0]}">&#8205;</a>')
    parts.append(f"<b>{article['title']}</b>")
    if article["summary"]:
        parts.append(f"{article['summary']}")
    return "\n\n".join(parts)

def render_full(article):
    """
    Полный HTML для Web App — используется в шаблоне Jinja.
    """
    return {
        "title": article["title"],
        "published": article["published"],
        "tags": article["tags"],
        "images": article["images"],
        "summary": article["summary"],
        "body": article["body"],
        "videos": article["videos"]
    }
