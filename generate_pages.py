import os, shutil, yaml
from jinja2 import Environment, FileSystemLoader
from parsers.champ_parser import ChampParser

print("[START] Генерация статических страниц...")

cfg = yaml.safe_load(open("sources_config.yml"))["championat"]
parser = ChampParser(cfg)

# Очистить и пересоздать папку pages
if os.path.exists("pages"):
    shutil.rmtree("pages")
os.makedirs("pages")
print("[INFO] Папка pages очищена и создана заново")

# Копируем логотип
shutil.copy("assets/logo.svg", "pages/logo.svg")

# Подготовка шаблона
env = Environment(loader=FileSystemLoader("templates"))
tpl = env.get_template("article.html")
print("[INFO] Шаблон article.html загружен")

articles = parser.fetch_list()
print(f"[INFO] Найдено статей: {len(articles)}")

for meta in articles:
    art = parser.fetch_article(meta)
    main_image = art["images"][0] if art["images"] else None

    html = tpl.render(
        title=art["title"],
        published=art["published"],
        body=art["body"],
        image=main_image,
        summary=art["summary"]
    )

    fname = art["url"].rstrip("/").split("/")[-1]
    if not fname.endswith(".html"):
        fname += ".html"

    path = f"pages/{fname}"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] Сохранено: {path}")

print("[DONE] Генерация завершена.")
