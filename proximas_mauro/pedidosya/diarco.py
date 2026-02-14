#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
diarco.py
---------
1) Abre https://www.pedidosya.com/ (navegador visible)
2) Click en "Argentina"
3) Espera el buscador
4) Escribe "diarco" y Enter
5) Busca un checkbox (<input type="checkbox">) y lo marca
6) Pausa para pruebas manuales con Playwright Inspector

Requisitos:
  pip install playwright
  playwright install
"""

import re
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError

URL = "https://www.pedidosya.com/"


def safe_click(locator, timeout=8000) -> bool:
    try:
        locator.wait_for(state="visible", timeout=timeout)
        locator.click(timeout=timeout)
        return True
    except PWTimeoutError:
        return False


def try_close_popups(page):
    """
    Cierra popups comunes (cookies / modales) si aparecen.
    No rompe si no existen.
    """
    candidates = [
        page.get_by_role("button", name=re.compile(r"aceptar|acepto|entendido|ok", re.I)),
        page.get_by_role("button", name=re.compile(r"cerrar|close", re.I)),
        page.get_by_role("button", name=re.compile(r"continuar", re.I)),
    ]
    for loc in candidates:
        try:
            if loc.count() > 0 and loc.first.is_visible():
                loc.first.click(timeout=1200)
        except Exception:
            pass


def click_argentina(page) -> None:
    """Selecciona 'Argentina' en el selector de país."""
    page.wait_for_load_state("domcontentloaded")
    try_close_popups(page)

    # 1) Por role + name (ideal)
    if safe_click(page.get_by_role("link", name="Argentina")):
        return

    # 2) Por <a> que contenga texto
    if safe_click(page.locator("a", has_text="Argentina").first):
        return

    # 3) Fallback por texto exacto
    try:
        t = page.get_by_text("Argentina", exact=True)
        t.wait_for(timeout=10000)
        try:
            t.click(timeout=2000)
            return
        except Exception:
            page.locator("a", has=t).first.click(timeout=8000)
            return
    except PWTimeoutError as e:
        raise RuntimeError("No pude encontrar/clickear 'Argentina'.") from e


def wait_until_arg_site(page):
    """
    No usamos networkidle (puede NO cumplirse nunca).
    Esperamos si cambia a .com.ar, y garantizamos DOM listo.
    """
    try:
        page.wait_for_url(re.compile(r"pedidosya\.com\.ar"), timeout=15000)
    except PWTimeoutError:
        pass

    try:
        page.wait_for_load_state("domcontentloaded", timeout=15000)
    except PWTimeoutError:
        pass

    try_close_popups(page)


def get_search_input(page):
    """Devuelve locator del input de búsqueda, evitando clases dinámicas."""
    # Preferido: placeholder estable
    loc = page.locator('input[placeholder="Buscar locales"]').first
    try:
        loc.wait_for(state="visible", timeout=20000)
        return loc
    except PWTimeoutError:
        pass

    # Fallback: role searchbox
    loc = page.locator('input[role="searchbox"]').first
    loc.wait_for(state="visible", timeout=20000)
    return loc


def search_text(page, text: str) -> None:
    """Escribe en el buscador y presiona Enter."""
    wait_until_arg_site(page)

    search = get_search_input(page)

    # A veces overlays bloquean el click
    try:
        search.click(timeout=5000)
    except PWTimeoutError:
        try_close_popups(page)
        search.click(timeout=5000)

    search.fill(text)
    search.press("Enter")


def check_first_checkbox(page, timeout=15000) -> bool:
    """
    Busca un checkbox (<input type="checkbox">) y lo marca.
    Estrategias:
      1) role=checkbox
      2) input visible
      3) label que contiene el input
      4) forzar check en el primer input[type=checkbox]
    """
    # 1) Por role
    try:
        cb = page.get_by_role("checkbox").first
        cb.wait_for(state="visible", timeout=timeout)
        cb.check(timeout=timeout)
        return True
    except Exception:
        pass

    # 2) Input visible
    try:
        cb = page.locator('input[type="checkbox"]:visible').first
        cb.wait_for(state="visible", timeout=timeout)
        cb.check(timeout=timeout)
        return True
    except Exception:
        pass

    # 3) Checkbox custom: click al label
    try:
        lab = page.locator('label:has(input[type="checkbox"])').first
        lab.wait_for(state="visible", timeout=timeout)
        lab.click(timeout=timeout)
        return True
    except Exception:
        pass

    # 4) Último recurso: forzar
    try:
        cb = page.locator('input[type="checkbox"]').first
        cb.wait_for(timeout=timeout)
        cb.check(force=True, timeout=timeout)
        return True
    except Exception:
        return False


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=60)
        context = browser.new_context(
            viewport={"width": 1280, "height": 800},
            locale="es-AR",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        )

        page = context.new_page()

        print("[INFO] Abriendo:", URL)
        page.goto(URL, wait_until="domcontentloaded")
        print("[INFO] URL:", page.url)

        print("[INFO] Click en Argentina...")
        click_argentina(page)
        wait_until_arg_site(page)
        print("[INFO] URL después de Argentina:", page.url)

        print("[INFO] Buscando 'diarco'...")
        search_text(page, "diarco")

        # Espera breve a que se renderice la vista de resultados (sin networkidle)
        try:
            page.wait_for_timeout(1200)
        except Exception:
            pass

        print("[INFO] Marcando primer checkbox encontrado...")
        ok = check_first_checkbox(page)
        print("[INFO] Checkbox marcado:", ok)

        print("[INFO] Pausa para pruebas manuales (Inspector).")
        page.pause()

        context.close()
        browser.close()


if __name__ == "__main__":
    main()
