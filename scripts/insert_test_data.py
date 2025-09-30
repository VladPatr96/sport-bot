import sqlite3

# 🔹 Подключаемся к базе
conn = sqlite3.connect('prosport.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# 🔹 Данные тестовой новости
news_data = {
    "title": "Леброн Джеймс установил новый рекорд",
    "url": "https://example.com/lebron-record",
    "content": "Леброн Джеймс стал самым результативным игроком в истории NBA.",
    "source": "test_source",
    "published_at": "2025-07-16T12:00:00",
    "lang": "ru"
}

# 🔹 Проверим, есть ли уже такая новость
cursor.execute("SELECT id FROM news WHERE url = ?", (news_data["url"],))
existing = cursor.fetchone()

if existing:
    print(f"❗ Новость уже существует в базе (id = {existing['id']})")
    news_id = existing["id"]
else:
    cursor.execute('''
        INSERT INTO news (title, url, content, source, published_at, lang)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        news_data["title"],
        news_data["url"],
        news_data["content"],
        news_data["source"],
        news_data["published_at"],
        news_data["lang"]
    ))
    conn.commit()
    news_id = cursor.lastrowid
    print(f"✅ Новость добавлена в базу (id = {news_id})")

# 🔹 Добавление сущности с псевдонимом
def insert_entity(name, type_, lang="ru"):
    cursor.execute(
        "SELECT id FROM entities WHERE name = ? AND type = ?", (name, type_)
    )
    existing = cursor.fetchone()
    if existing:
        print(f"! Сущность '{name}' уже существует (id = {existing['id']})")
        return existing["id"]

    cursor.execute(
        "INSERT INTO entities (name, type, lang) VALUES (?, ?, ?)",
        (name, type_, lang)
    )
    conn.commit()
    entity_id = cursor.lastrowid
    print(f"✅ Добавлена сущность: {name} (id = {entity_id})")

    add_alias(entity_id, name, lang)
    return entity_id

# 🔹 Добавление alias (если ещё не существует)
def add_alias(entity_id, alias, lang="ru"):
    alias = alias.strip().lower()
    cursor.execute('''
        SELECT 1 FROM entity_aliases 
        WHERE entity_id = ? AND LOWER(alias) = ?
    ''', (entity_id, alias))
    
    if cursor.fetchone():
        print(f"🔗 Псевдоним уже существует: {alias}")
        return
    
    cursor.execute('''
        INSERT INTO entity_aliases (entity_id, alias, lang)
        VALUES (?, ?, ?)
    ''', (entity_id, alias, lang))
    conn.commit()
    print(f"✅ Добавлен псевдоним '{alias}' для entity_id = {entity_id}")

# 🔹 Вставляем сущности
sport_id = insert_entity("Баскетбол", "sport")
tournament_id = insert_entity("NBA", "tournament")
team_id = insert_entity("Лос-Анджелес Лейкерс", "team")
player_id = insert_entity("Леброн Джеймс", "player")

# 🔹 Связи между сущностями
def link_entities(parent_id, child_id, relation="member_of"):
    cursor.execute(
        "SELECT id FROM entity_relations WHERE parent_id = ? AND child_id = ?",
        (parent_id, child_id)
    )
    existing = cursor.fetchone()
    if existing:
        print(f"🔗 Связь уже существует (id = {existing['id']})")
        return existing['id']

    cursor.execute('''
        INSERT INTO entity_relations (parent_id, child_id, relation)
        VALUES (?, ?, ?)
    ''', (parent_id, child_id, relation))
    conn.commit()
    relation_id = cursor.lastrowid
    print(f"✅ Добавлена связь: {parent_id} → {child_id} (id = {relation_id})")
    return relation_id

link_entities(sport_id, tournament_id)
link_entities(tournament_id, team_id)
link_entities(team_id, player_id)

# 🔹 Привязка новости к сущности
def link_news_to_entity(news_id, entity_id, confidence=1.0):
    cursor.execute(
        "SELECT id FROM news_entities WHERE news_id = ? AND entity_id = ?",
        (news_id, entity_id)
    )
    if cursor.fetchone():
        print(f"📰 Новость уже привязана к сущности (id = {entity_id})")
        return

    cursor.execute('''
        INSERT INTO news_entities (news_id, entity_id, confidence)
        VALUES (?, ?, ?)
    ''', (news_id, entity_id, confidence))
    conn.commit()
    print(f"✅ Привязана новость (id = {news_id}) к сущности (id = {entity_id})")

link_news_to_entity(news_id, player_id)

# 🔍 Поиск сущности по имени или alias
def find_entity_by_name(name):
    name = name.strip().lower()

    cursor.execute('''
        SELECT e.id, e.name, e.type
        FROM entities e
        LEFT JOIN entity_aliases a ON e.id = a.entity_id
        WHERE LOWER(e.name) LIKE ? OR LOWER(a.alias) LIKE ?
        LIMIT 1
    ''', (f"%{name}%", f"%{name}%"))

    row = cursor.fetchone()
    if row:
        print(f"✅ Найдена сущность: {row['name']} (id = {row['id']})")
    else:
        print(f"❌ Сущность '{name}' не найдена.")
    return row

# 🔎 Получение новостей по сущности с учётом иерархии
def get_news_by_entity(entity_id):
    query = '''
    WITH RECURSIVE descendants(id) AS (
        SELECT id FROM entities WHERE id = ?
        UNION
        SELECT er.child_id
        FROM entity_relations er
        JOIN descendants d ON er.parent_id = d.id
    )
    SELECT DISTINCT n.*
    FROM news n
    JOIN news_entities ne ON ne.news_id = n.id
    WHERE ne.entity_id IN (SELECT id FROM descendants)
    ORDER BY n.published_at DESC
    '''
    cursor.execute(query, (entity_id,))
    results = cursor.fetchall()
    print(f"\n🔎 Найдено {len(results)} новостей:")
    for row in results:
        print(f"- [{row['published_at']}] {row['title']} (id={row['id']})")

def categorize_news(news_id):
    # Получаем текст новости
    cursor.execute("SELECT title, content FROM news WHERE id = ?", (news_id,))
    news = cursor.fetchone()
    if not news:
        print(f"❌ Новость с id={news_id} не найдена.")
        return

    text = (news["title"] or "") + " " + (news["content"] or "")
    text = text.lower()

    # Загружаем все alias
    cursor.execute('''
        SELECT e.id AS entity_id, LOWER(COALESCE(a.alias, e.name)) AS alias
        FROM entities e
        LEFT JOIN entity_aliases a ON e.id = a.entity_id
    ''')
    aliases = cursor.fetchall()

    found_ids = set()
    for row in aliases:
        alias = row["alias"]
        if alias and alias in text:
            found_ids.add(row["entity_id"])

    for entity_id in found_ids:
        cursor.execute('''
            SELECT 1 FROM news_entities WHERE news_id = ? AND entity_id = ?
        ''', (news_id, entity_id))
        if not cursor.fetchone():
            cursor.execute('''
                INSERT INTO news_entities (news_id, entity_id, confidence)
                VALUES (?, ?, ?)
            ''', (news_id, entity_id, 0.9))
    
    conn.commit()
    print(f"✅ Привязано {len(found_ids)} сущностей к новости id={news_id}")

def categorize_all_uncategorized_news():
    cursor.execute('''
        SELECT n.id
        FROM news n
        LEFT JOIN news_entities ne ON n.id = ne.news_id
        WHERE ne.news_id IS NULL
    ''')
    rows = cursor.fetchall()

    print(f"🔎 Найдено {len(rows)} новостей без категорий")
    for row in rows:
        categorize_news(row["id"])
        

print(f"✅ Новость добавлена в базу (id = {news_id})")
categorize_news(news_id)  # <-- Автоматическая привязка сущностей


# 🔍 Тест поиска и новостей
entity = find_entity_by_name("Леброн Джеймс")
if entity:
    get_news_by_entity(entity["id"])

# ✅ Закрываем соединение
conn.close()

