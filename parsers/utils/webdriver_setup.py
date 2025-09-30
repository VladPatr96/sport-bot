#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
webdriver_setup.py — отдельный скрипт для инициализации Chrome WebDriver

Как скачать новый ChromeDriver:
  Варианты:
    1) Автоматически через Selenium Manager (selenium>=4.13)
       • Установи/обнови selenium: pip install --upgrade selenium
       • Service() без пути сам подтянет нужный драйвер под Chrome 140.

    2) Автоматически через chromedriver-autoinstaller
       • Установи пакет: pip install chromedriver-autoinstaller
       • Вызов chromedriver_autoinstaller.install() скачает и положит
         подходящий chromedriver (140.x) в локальную папку.

    3) Вручную с сайта Chrome for Testing
       • Определи версию Chrome: "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe" --version
       • Скачай chromedriver той же версии: https://googlechromelabs.github.io/chrome-for-testing/
       • Распакуй chromedriver.exe в проект и укажи путь в Service(...).

По умолчанию этот скрипт пробует сперва Selenium Manager, затем chromedriver-autoinstaller.
"""

import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import WebDriverException

try:
    import chromedriver_autoinstaller
except ImportError:
    chromedriver_autoinstaller = None


def _chrome_options(headless: bool) -> Options:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--use-gl=swiftshader")
    opts.add_argument("--disable-features=VizDisplayCompositor")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    return opts


def build_driver(headless: bool = True) -> webdriver.Chrome:
    opts = _chrome_options(headless)

    # Попробуем через Selenium Manager (selenium>=4.13)
    try:
        service = Service()
        driver = webdriver.Chrome(service=service, options=opts)
        driver.set_page_load_timeout(60)
        return driver
    except WebDriverException as e:
        print(f"⚠️ Selenium Manager не сработал: {e}")
        if not chromedriver_autoinstaller:
            raise

    # Фолбэк: chromedriver-autoinstaller
    driver_path = chromedriver_autoinstaller.install()
    service = Service(driver_path)
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(60)
    return driver


def main():
    try:
        driver = build_driver(headless=True)
        print("✅ WebDriver инициализирован успешно")
        driver.get("https://www.google.com")
        print("🌐 Открыта страница:", driver.title)
    except Exception as e:
        print(f"❌ Ошибка инициализации WebDriver: {e}")
    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    main()
