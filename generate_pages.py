# generate_pages.py
import os, shutil, yaml
from jinja2 import Environment, FileSystemLoader
from parsers.champ_parser import ChampParser

print("[START] Генерация статических страниц...")

# 1. Загрузить конфиг
cfg = yaml.safe_load(open("sources_config.yml"))["championat"]
parser = ChampParser(cfg)

# 2. Очистить папку pages
if os.path.exists("pages"):
    shutil.rmtree("pages")
os.makedirs("pages")
print("[INFO] Папка pages очищена и создана заново")

# 3. Парсим список статей
articles = parser.fetch_list()
print(f"[INFO] Найдено статей: {len(articles)}")

# 4. Подготовка шаблона
env = Environment(loader=FileSystemLoader("templates"))
tpl = env.get_template("article.html")
print("[INFO] Шаблон article.html загружен")

# 5. Генерация HTML-файлов
for meta in articles:
    art = parser.fetch_article(meta)
    html = tpl.render(
        title=art["title"],
        published=art["published"],
        body=art["body"]
    )
    fname = art["url"].rstrip("/").split("/")[-1]
    if not fname.endswith(".html"):
        fname += ".html"
    path = f"pages/{fname}"
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] Сохранено: {path}")

print("[DONE] Генерация завершена.")

