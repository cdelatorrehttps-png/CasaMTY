"""
(Opcional) Toma capturas de pantalla del dashboard con Playwright y reporta errores de JavaScript.
    pip install playwright && playwright install chromium
    python src/captura_dashboard.py
Genera docs/img/dashboard_*.png (útiles para pegar en el reporte o la presentación).
"""
import sys

from playwright.sync_api import sync_playwright

import config as C


def main():
    errores = []
    with sync_playwright() as p:
        b = p.chromium.launch()
        pg = b.new_page(viewport={"width": 1280, "height": 900})
        pg.on("pageerror", lambda e: errores.append(str(e)))
        pg.on("console", lambda m: errores.append(m.text) if m.type == "error" else None)
        pg.goto((C.DASHBOARD / "index.html").as_uri())
        pg.wait_for_timeout(500)
        pg.screenshot(path=str(C.IMG / "dashboard_01_mercado.png"), full_page=False)
        pg.select_option("#fMuni", "1")   # San Pedro Garza García
        pg.wait_for_timeout(300)
        pg.screenshot(path=str(C.IMG / "dashboard_02_san_pedro.png"), full_page=True)
        pg.locator("#calidad").scroll_into_view_if_needed()
        pg.locator("#calidad").screenshot(path=str(C.IMG / "dashboard_03_calidad.png"))
        pg.locator("#modelo").screenshot(path=str(C.IMG / "dashboard_04_modelo.png"))
        b.close()
    print("errores JS:", errores or "ninguno")
    sys.exit(1 if errores else 0)


if __name__ == "__main__":
    main()
