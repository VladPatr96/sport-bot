import os
import shutil
import yaml
import hashlib
import requests
from jinja2 import Environment, FileSystemLoader
from parsers.champ_parser import ChampParser

# Загрузка конфигурации и парсера
cfg    = yaml.safe_load(open("sources_config.yml"))["championat"]
parser = ChampParser(cfg)

# Подготовка папки pages
if os.path.exists("pages"):
    shutil.rmtree("pages")
os.makedirs("pages/images", exist_ok=True)
shutil.copy("assets/logo.png", "pages/logo.png")

# Шаблон для статей
env = Environment(loader=FileSystemLoader("templates"))
tpl = env.get_template("article.html")

def download_image(url: str) -> str | None:
    """Скачать картинку и вернуть относительный путь в pages/images."""
    try:
        ext = os.path.splitext(url)[1].split("?")[0] or ".jpg"
        name = hashlib.md5(url.encode()).hexdigest() + ext
        out = os.path.join("pages/images", name)
        if not os.path.exists(out):
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            with open(out, "wb") as f:
                f.write(resp.content)
        return f"images/{name}"
    except Exception as e:
        print(f"[WARN] Не удалось скачать {url}: {e}")
        return None

# Генерация страниц
metas = parser.fetch_list()
print(f"[INFO] Найдено статей: {len(metas)}")

for meta in metas:
    art = parser.fetch_article(meta)

    # Нормализуем имя файла
    raw  = art["url"].rstrip("/").split("/")[-1]
    slug = raw.removesuffix(".html") + ".html"

    # Кэширование главного изображения
    img_url = art["images"][0] if art["images"] else None
    cached  = download_image(img_url) if img_url else None

    # Рендеринг HTML
    html = tpl.render(
        title     = art["title"],
        published = art["published"],
        body      = art["body"],
        image     = cached,
        summary   = art["summary"]
    )

    # Запись файла
    out_path = os.path.join("pages", slug)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] Сохранено: {out_path}")

print("[DONE] Статические страницы сгенерированы.")
