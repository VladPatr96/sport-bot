# generate_pages.py
import os, shutil, yaml
from jinja2 import Environment, FileSystemLoader
from parsers.champ_parser import ChampParser

# 1. Загрузить конфиг
cfg = yaml.safe_load(open("sources_config.yml"))["championat"]
parser = ChampParser(cfg)

# 2. Очистить папку pages
if os.path.exists("pages"):
    shutil.rmtree("pages")
os.makedirs("pages")

# 3. Парсим список и каждую статью пишем в HTML
env = Environment(loader=FileSystemLoader("templates"))
tpl = env.get_template("article.html")

for meta in parser.fetch_list():
    art = parser.fetch_article(meta)
    html = tpl.render(
        title=art["title"],
        published=art["published"],
        body=art["body"]
    )
    # имя файла (slugify можно добавить)
    fname = art["url"].rstrip("/").split("/")[-1] + ".html"
    with open(f"pages/{fname}", "w", encoding="utf-8") as f:
        f.write(html)
