import os, shutil, yaml, hashlib, requests
from jinja2 import Environment, FileSystemLoader
from parsers.champ_parser import ChampParser

print("[START] Генерация статических страниц...")

cfg = yaml.safe_load(open("sources_config.yml"))["championat"]
parser = ChampParser(cfg)

# Очистка и подготовка папки pages
if os.path.exists("pages"):
    shutil.rmtree("pages")
os.makedirs("pages/images", exist_ok=True)
shutil.copy("assets/logo.svg", "pages/logo.svg")
print("[INFO] Папка pages очищена и логотип скопирован")

# Подготовка шаблона
env = Environment(loader=FileSystemLoader("templates"))
tpl = env.get_template("article.html")
print("[INFO] Шаблон article.html загружен")

articles = parser.fetch_list()
print(f"[INFO] Найдено статей: {len(articles)}")

def download_image(url):
    try:
        ext = os.path.splitext(url)[-1].split("?")[0] or ".jpg"
        name = hashlib.md5(url.encode()).hexdigest() + ext
        path = os.path.join("pages/images", name)
        if not os.path.exists(path):
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            with open(path, "wb") as f:
                f.write(resp.content)
        return f"images/{name}"
    except Exception as e:
        print(f"[WARN] Не удалось скачать {url}: {e}")
        return None

for meta in articles:
    art = parser.fetch_article(meta)
    main_image = art["images"][0] if art["images"] else None
    cached_image = download_image(main_image) if main_image else None

    html = tpl.render(
        title=art["title"],
        published=art["published"],
        body=art["body"],
        image=cached_image,
        summary=art["summary"]
    )

    fname = art["url"].rstrip("/").split("/")[-1].removesuffix(".html") + ".html"

    path = f"pages/{fname}"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] Сохранено: {path}")

print("[DONE] Генерация завершена.")
