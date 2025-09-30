```
sport-news-bot
├── .github
│   ├── workflows
│   │   ├── full_deploy.yml
│   │   └── release-drafter.yml
│   └── release-drafter.yml
├── .vscode
│   └── launch.json
├── assets
│   └── logo.svg
├── config
│   └── app_config.yml
├── database
│   ├── migrations
│   │   └── 2025-09-09_m2m_tournaments.sql
│   ├── __init__.py
│   ├── prosport.db
│   └── prosport_db.py
├── db
│   ├── gen_manifest.py
│   └── manifest.json
├── docs
│   ├── DB_SCHEMA.md
│   ├── PROJECT_TREE.md
│   ├── PROMPT_AI.md
│   ├── RELEASE_NOTES.md
│   ├── ROADMAP.md
│   └── TASKS_AI.md
├── drivers
│   ├── chromedriver.exe
│   └── README.md
├── logos
│   ├── ChatGPT Image 20 июл. 2025 г., 08_49_36.png
│   ├── ChatGPT Image 20 июл. 2025 г., 08_49_54.png
│   └── photo_2025-07-14_20-11-19.jpg
├── notebooks
│   ├── athletes.ipynb
│   ├── champ.ipynb
│   ├── champ_parser.ipynb
│   ├── teams.ipynb
│   └── tournaments.ipynb
├── parsers
│   ├── sources
│   │   ├── championat
│   │   │   ├── config
│   │   │   │   └── sources_config.yml
│   │   │   ├── parsers
│   │   │   │   ├── __init__.py
│   │   │   │   ├── async_teams_parser.py
│   │   │   │   ├── athlete_parser_async.py
│   │   │   │   ├── athlete_tag_url_filler_async.py
│   │   │   │   ├── athletes_parser.py
│   │   │   │   ├── champ_parser.py
│   │   │   │   ├── championat_data_loader.py
│   │   │   │   ├── sports_parser.py
│   │   │   │   ├── teams_parser.py
│   │   │   │   └── tournaments_parser.py
│   │   │   ├── __init__.py
│   │   │   ├── main_structural_data_loader.py
│   │   │   ├── retry_failed_athletes_parser.py
│   │   │   └── utils.py
│   │   ├── espn
│   │   └── __init__.py
│   ├── utils
│   │   └── webdriver_setup.py
│   ├── __init__.py
│   └── main_parser_runner.py
├── release
├── scheduler
│   └── main_scheduler.py
├── scripts
│   ├── hooks
│   │   ├── enforce_change_scope.sh
│   │   └── sql_guard.py
│   ├── backfill_m2m_and_tags.py
│   ├── backfill_tag_lineage.py
│   ├── categorize_articles.py
│   ├── database_utils.py
│   ├── fetch_entity_news_on_demand.py
│   ├── insert_test_data.py
│   ├── migrate_m2m.py
│   └── universal_entity_news.py
├── src
│   └── prosport
│       └── __init__.py
├── telegram_bot
│   ├── bot
│   │   ├── __init__.py
│   │   ├── __main__.py
│   │   └── main.py
│   ├── utils
│   │   ├── __init__.py
│   │   └── render.py
│   ├── webapp
│   │   ├── templates
│   │   │   └── article.html
│   │   ├── __init__.py
│   │   ├── __main__.py
│   │   └── app.py
│   └── generate_pages.py
├── tests
│   ├── __init__.py
│   ├── test_categorization.py
│   ├── test_champ_parser.py
│   ├── test_champ_to_json.py
│   ├── test_champ_to_json_sync.py
│   └── test_parser.py
├── tools
│   ├── generate_prompt.py
│   ├── generate_tasks.py
│   ├── print_tree.py
│   └── prosport_cli.py
├── work
│   └── categorization.py
├── .change_scope
├── .gitignore
├── pyproject.toml
├── requirements.txt
└── sync_champ_news.py
```