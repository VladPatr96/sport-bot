# parsers/sources/championat/utils.py

import aiohttp

async def fetch_page(session, url):
    """
    Извлекает HTML-содержимое страницы по заданному URL.
    Использует aiohttp.ClientSession.
    """
    try:
        async with session.get(url) as resp:
            resp.raise_for_status()  # Вызовет исключение для статусов 4xx/5xx
            return await resp.text()
    except aiohttp.ClientError as e:
        print(f"❌ Ошибка HTTP при запросе {url}: {e}")
        return None
    except Exception as e:
        print(f"❌ Непредвиденная ошибка при запросе {url}: {e}")
        return None