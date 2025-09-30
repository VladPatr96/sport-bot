# webapp/app.py

from flask import Flask, request, render_template
from parsers.champ_parser import ChampParser
import yaml
from pyngrok import ngrok

app = Flask(__name__)

public_url = ngrok.connect(5000, bind_tls=True)
print("Public URL:", public_url)

cfg = yaml.safe_load(open("sources_config.yml", encoding="utf-8"))["championat"]
parser = ChampParser(cfg)

@app.route("/article")
def article():
    url = request.args.get("url")
    if not url:
        return "Missing url", 400
    art = parser.fetch_article({"url": url, "published": "", "tag": "", "comments": ""})
    data = render_template("article.html", article=art)
    return data

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
