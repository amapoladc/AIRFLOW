# /opt/airflow/plugins/utils/selenium_service.py
from __future__ import annotations

import glob
import os
import time
from typing import Optional, List, Set
from datetime import datetime
from urllib.parse import urljoin

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    ElementClickInterceptedException,
)

# =============================
# Utilidades básicas
# =============================

def _wdwait(driver, seconds: int = 60) -> WebDriverWait:
    return WebDriverWait(
        driver,
        seconds,
        poll_frequency=0.25,
        ignored_exceptions=(NoSuchElementException, StaleElementReferenceException),
    )

def _wait_ready(driver, seconds: int = 90) -> None:
    _wdwait(driver, seconds).until(
        lambda d: d.execute_script("return document.readyState") == "complete"
    )

def _wait_network_quiet(driver, idle_ms: int = 1200, timeout: int = 90) -> None:
    """
    Espera a que no haya actividad de red (XHR + fetch) por al menos idle_ms.
    """
    driver.execute_script("""
        if (!window._xhrMonitored) {
            (function(open, send) {
                let active = 0, last = Date.now();
                XMLHttpRequest.prototype.open = function() { open.apply(this, arguments); };
                XMLHttpRequest.prototype.send = function() {
                    active++; last = Date.now();
                    this.addEventListener('loadend', function(){ active--; last = Date.now(); });
                    send.apply(this, arguments);
                };
                window._netmon = { get active(){ return active; }, get last(){ return last; } };
                window._xhrMonitored = true;
            })(XMLHttpRequest.prototype.open, XMLHttpRequest.prototype.send);
        }
        if (!window._fetchMonitored) {
            (function(origFetch){
                let activeF = 0, last = Date.now();
                window.fetch = function() {
                    activeF++; last = Date.now();
                    return origFetch.apply(this, arguments).finally(function(){ activeF--; last = Date.now(); });
                };
                window._fetmon = { get active(){ return activeF; }, get last(){ return last; } };
                window._fetchMonitored = true;
            })(window.fetch);
        }
    """)
    start = time.time()
    while time.time() - start < timeout:
        active_x = driver.execute_script("return window._netmon ? window._netmon.active : 0;")
        active_f = driver.execute_script("return window._fetmon ? window._fetmon.active : 0;")
        last_x   = driver.execute_script("return window._netmon ? window._netmon.last : Date.now();")
        last_f   = driver.execute_script("return window._fetmon ? window._fetmon.last : Date.now();")
        now_ms = int(time.time() * 1000)
        last_any = max(int(last_x), int(last_f))
        if (active_x + active_f) == 0 and (now_ms - last_any) > idle_ms:
            return
        time.sleep(0.25)
    raise TimeoutException("La red no quedó ociosa a tiempo")

def _switch_into_iframe_containing(driver, by: By, selector: str, seconds: int = 45) -> None:
    """
    Cambia al primer iframe (o root) que contenga el elemento indicado.
    """
    driver.switch_to.default_content()
    try:
        WebDriverWait(driver, 2).until(EC.presence_of_element_located((by, selector)))
        return
    except Exception:
        pass

    frames = driver.find_elements(By.TAG_NAME, "iframe")
    for fr in frames:
        driver.switch_to.default_content()
        driver.switch_to.frame(fr)
        try:
            WebDriverWait(driver, 2).until(EC.presence_of_element_located((by, selector)))
            return
        except Exception:
            continue
    driver.switch_to.default_content()
    raise TimeoutException(f"No se encontró iframe que contenga: {by}={selector}")

def _refind_in_iframe(driver, locator_by, locator, seconds: int = 45):
    """
    Reentra a un iframe que contenga el locator y devuelve SIEMPRE un elemento fresco.
    """
    driver.switch_to.default_content()
    try:
        return WebDriverWait(driver, 2).until(EC.presence_of_element_located((locator_by, locator)))
    except Exception:
        pass

    for fr in driver.find_elements(By.TAG_NAME, "iframe"):
        try:
            driver.switch_to.default_content()
            driver.switch_to.frame(fr)
            el = WebDriverWait(driver, 2).until(EC.presence_of_element_located((locator_by, locator)))
            return el
        except Exception:
            continue

    driver.switch_to.default_content()
    raise TimeoutException(f"No se encontró {locator_by}={locator} dentro de iframes")

def _dump_debug(driver, tag: str = "debug") -> None:
    """
    Guarda screenshot y HTML para diagnóstico en /opt/airflow/logs/selenium_debug.
    """
    import pathlib
    ts = time.strftime("%Y%m%d-%H%M%S")
    base = pathlib.Path("/opt/airflow/logs/selenium_debug")
    base.mkdir(parents=True, exist_ok=True)
    try:
        driver.save_screenshot(str(base / f"{tag}_{ts}.png"))
    except Exception:
        pass
    try:
        with open(base / f"{tag}_{ts}.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
    except Exception:
        pass

def _center_click(driver, element) -> None:
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
        driver.execute_script("arguments[0].click();", element)
    except Exception:
        try:
            element.click()
        except Exception:
            pass

# =============================
# Seteo de fechas (jQuery UI)
# =============================

def _jq_set_date(driver, element, date_str: str):
    """
    Usa jQuery UI datepicker('setDate', ...) y dispara eventos.
    """
    driver.execute_script("""
        const el = arguments[0], val = arguments[1];
        if (window.jQuery && jQuery.fn && jQuery(el).datepicker) {
            jQuery(el).datepicker('setDate', val);
            jQuery(el).trigger('input').trigger('change').trigger('blur');
        } else {
            el.value = val;
            ['input','change','blur'].forEach(ev => el.dispatchEvent(new Event(ev, {bubbles:true})));
        }
    """, element, date_str)

def _wait_input_value(driver, element, expected: str, timeout: int = 15):
    end = time.time() + timeout
    while time.time() < end:
        val = (element.get_attribute("value") or "").strip()
        if val == expected.strip():
            return
        time.sleep(0.2)
    raise TimeoutException(f"El input no tomó el valor esperado: '{expected}'")

# =============================
# Clase principal
# =============================

class SeleniumDownloader:
    """
    Context manager para Chrome + utilidades de navegación/descarga.
    """

    def __init__(self, base_url: str, download_dir: str, driver_path: str | None = None):
        self.base_url = base_url
        self.download_dir = os.path.abspath(download_dir)
        # Ignorado si usas Selenium Manager; se mantiene por compatibilidad
        self.driver_path = driver_path
        os.makedirs(self.download_dir, exist_ok=True)
        self.driver: Optional[webdriver.Chrome] = None

    # ---------- Chrome Options ----------

    def _options(self) -> Options:
        o = Options()
        o.add_argument('--headless=new')
        o.add_argument('--no-sandbox')
        o.add_argument('--disable-dev-shm-usage')
        o.add_argument('--window-size=1920,1080')
        o.add_argument('--ignore-certificate-errors')
        o.set_capability("acceptInsecureCerts", True)
        o.page_load_strategy = "eager"  # no esperar recursos diferidos

        chrome_bin = (
            os.environ.get("GOOGLE_CHROME_SHIM")
            or os.environ.get("CHROME_BIN")
            or "/usr/bin/google-chrome"  # ubicación típica en la imagen con google-chrome-stable
        )
        if chrome_bin:
            o.binary_location = chrome_bin

        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "profile.default_content_setting_values.automatic_downloads": 1,
            "download.restrictions": 0,
            "profile.default_content_settings.popups": 0,
        }
        o.add_experimental_option("prefs", prefs)
        return o

    def __enter__(self) -> "SeleniumDownloader":
        # Usar Selenium Manager: no pasar executable_path ni ruta
        # (descarga y gestiona chromedriver automáticamente)
        self.driver = webdriver.Chrome(service=Service(), options=self._options())
        self.driver.set_script_timeout(180)
        self.driver.set_page_load_timeout(120)
        self.driver.implicitly_wait(0)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            if self.driver:
                self.driver.quit()
        except Exception:
            pass

    # ---------- Autenticación ----------

    def login(self, user: str, password: str) -> None:
        d = self.driver
        assert d is not None

        try:
            d.execute_cdp_cmd("Network.clearBrowserCache", {})
            d.execute_cdp_cmd("Network.clearBrowserCookies", {})
        except Exception:
            pass

        d.get(self.base_url)
        _wait_ready(d, 90)
        _wait_network_quiet(d, idle_ms=1200, timeout=60)

        _wdwait(d, 30).until(EC.presence_of_element_located((By.ID, "input_user"))).send_keys(user)
        d.find_element(By.NAME, "input_pass").send_keys(password)
        d.find_element(By.NAME, "submit_login").click()

        _wait_ready(d, 90)
        if d.find_elements(By.CSS_SELECTOR, ".form-login-error"):
            raise RuntimeError("Login failed: Invalid credentials")

    # ---------- Helpers DOM ----------

    def _find_in_frames_and_click(self, xpath: str) -> bool:
        d = self.driver
        assert d is not None
        try:
            el = WebDriverWait(d, 30).until(EC.element_to_be_clickable((By.XPATH, xpath)))
            d.execute_script("arguments[0].click();", el)
            d.switch_to.default_content()
            return True
        except Exception:
            pass

        d.switch_to.default_content()
        frames = d.find_elements(By.TAG_NAME, 'iframe')
        for fr in frames:
            try:
                d.switch_to.default_content()
                d.switch_to.frame(fr)
                els = d.find_elements(By.XPATH, xpath)
                if els:
                    d.execute_script("arguments[0].scrollIntoView(true);", els[0])
                    d.execute_script("arguments[0].click();", els[0])
                    d.switch_to.default_content()
                    return True
            except Exception:
                continue

        d.switch_to.default_content()
        return False

    def _wait_new_csv(self, before: Optional[Set[str]] = None, timeout: int = 150) -> str:
        """
        Espera a que aparezca un CSV nuevo y a que termine completamente la descarga.
        """
        before = before or set(glob.glob(os.path.join(self.download_dir, '*.csv')))
        start = time.time()
        while time.time() - start < timeout:
            time.sleep(2)
            after = set(glob.glob(os.path.join(self.download_dir, '*.csv')))
            new = list(after - before)
            if new:
                path = max(new, key=os.path.getctime)
                # Espera a que desaparezca .crdownload
                for _ in range(60):
                    if not os.path.exists(path + ".crdownload"):
                        break
                    time.sleep(1)
                # Tamaño estable > 0
                size = -1
                for _ in range(30):
                    cur = os.path.getsize(path)
                    if cur == size and cur > 0:
                        return path
                    size = cur
                    time.sleep(2)
        raise RuntimeError('CSV not detected within timeout')

    # ---------- Setters de fecha ----------

    def _set_date_dynamic(self, el, value: str):
        d = self.driver
        assert d is not None
        last_exc = None
        name = None
        id_ = None
        try:
            name = el.get_attribute("name")
            id_ = el.get_attribute("id")
        except Exception:
            pass

        for attempt in range(1, 6):
            try:
                _wdwait(d, 10).until(EC.visibility_of(el))
                _center_click(d, el)
                _jq_set_date(d, el, value)
                _wait_input_value(d, el, value, timeout=10)
                return
            except StaleElementReferenceException as e:
                last_exc = e
                try:
                    if name:
                        el = _refind_in_iframe(d, By.NAME, name, seconds=10)
                    elif id_:
                        el = _refind_in_iframe(d, By.ID, id_, seconds=10)
                except Exception as ee:
                    last_exc = ee
                time.sleep(0.3 * attempt)
                continue
            except Exception as e:
                last_exc = e
                time.sleep(0.3 * attempt)
                continue
        raise TimeoutException(f"No se pudo setear la fecha; último error: {last_exc}")

    # ---------- Flujos de descarga ----------

    def download_calls_detail(self, date_str: str) -> str:
        """
        Descarga el reporte "Calls Detail" para una fecha dada (formato 'dd M yyyy', ej: '08 Oct 2025').
        """
        d = self.driver
        assert d is not None

        try:
            calls_detail_url = f"{self.base_url.rstrip('/')}/index.php?menu=calls_detail"
            d.get(calls_detail_url)
            _wait_ready(d, 90)
            _wait_network_quiet(d, 1500, 90)

            _switch_into_iframe_containing(d, By.ID, "neo-table-filter-button-arrow", seconds=45)
            btn = _wdwait(d, 45).until(EC.element_to_be_clickable((By.ID, "neo-table-filter-button-arrow")))
            d.execute_script("arguments[0].click();", btn)

            _switch_into_iframe_containing(d, By.NAME, "date_start", seconds=45)

            start_el = None
            for by_, sel in [
                (By.NAME, "date_start"),
                (By.CSS_SELECTOR, "input[name*='date_start']"),
                (By.CSS_SELECTOR, "input.hasDatepicker"),
            ]:
                try:
                    start_el = _refind_in_iframe(d, by_, sel, seconds=6)
                    if start_el:
                        break
                except Exception:
                    pass
            if not start_el:
                _dump_debug(d, "no_date_start")
                raise RuntimeError("No se encontró input date_start")

            end_el = None
            for by_, sel in [
                (By.NAME, "date_end"),
                (By.CSS_SELECTOR, "input[name*='date_end']"),
                (By.CSS_SELECTOR, "input.hasDatepicker"),
            ]:
                try:
                    candidate = _refind_in_iframe(d, by_, sel, seconds=6)
                    if candidate and candidate._id != start_el._id:
                        end_el = candidate
                        break
                except Exception:
                    pass
            if not end_el:
                _dump_debug(d, "no_date_end")
                raise RuntimeError("No se encontró input date_end")

            self._set_date_dynamic(start_el, date_str)
            self._set_date_dynamic(end_el,   date_str)

            filter_btn = _wdwait(d, 15).until(EC.element_to_be_clickable((By.NAME, "filter")))
            d.execute_script("arguments[0].click();", filter_btn)
            _wait_network_quiet(d, 1500, 90)

            _switch_into_iframe_containing(d, By.ID, "neo-table-button-download-right", seconds=45)
            _wdwait(d, 30).until(EC.element_to_be_clickable((By.ID, "neo-table-button-download-right"))).click()

            try:
                csv = WebDriverWait(d, 15).until(EC.presence_of_element_located((By.ID, 'CSV')))
                href = csv.find_element(By.XPATH, "./parent::a").get_attribute('href')
                if href:
                    d.get(href)
                else:
                    d.execute_script("arguments[0].click();", csv)
            except Exception:
                links = d.find_elements(By.XPATH, "//a[contains(., 'CSV') or contains(@href, '.csv')]")
                if not links:
                    raise RuntimeError('CSV link not found')
                href = links[0].get_attribute('href')
                if href:
                    d.get(href)
                else:
                    d.execute_script("arguments[0].click();", links[0])

            return self._wait_new_csv()

        except Exception:
            _dump_debug(d, "download_calls_detail_timeout")
            raise
        finally:
            try:
                d.switch_to.default_content()
            except Exception:
                pass

    def download_campaigns(self, names: List[str]) -> List[str]:
        """
        Descarga CSVs de campañas por objetivos (p. ej., 'generali alt' y 'generali'),
        buscando el nombre en la 2da celda (td[2]//a), tolerando 'generali' vs 'generalli'.
        Pagina por objetivo; resuelve href relativo con urljoin y navega directo.
        """
        d = self.driver
        assert d is not None

        # --- Normaliza y prioriza objetivos más largos ---
        raw_targets = [n for n in (names or []) if n and n.strip()]
        norm = []
        for n in raw_targets:
            t = " ".join(n.strip().lower().split())
            t = t.replace("generalli", "generali")  # typo común
            norm.append(t)
        targets = sorted(set(norm), key=len, reverse=True)  # ej: "generali alt", luego "generali"
        if not targets:
            raise RuntimeError("No campaign names provided")

        # --- Helpers tabla/paginación ---

        def _in_campaigns_table_iframe():
            probes = [
                (By.XPATH, "//a[normalize-space(text())='[CSV Data]']"),
                (By.XPATH, "//a[contains(@href,'action=csv_data')]"),
                (By.XPATH, "//table//tr[td]"),
            ]
            for by_, sel in probes:
                try:
                    _switch_into_iframe_containing(d, by_, sel, seconds=5)
                    return True
                except Exception:
                    continue
            d.switch_to.default_content()
            return False

        def _visible_campaign_rows():
            """
            Devuelve (row, name) usando el texto del enlace en la 2da celda (td[2]//a),
            que contiene el nombre de la campaña (p. ej., 'Generali', 'Generalli', 'Generali alt').
            """
            rows = d.find_elements(By.XPATH, "//table//tr[td] | //tr[td]")
            out = []
            for r in rows:
                try:
                    name_el = r.find_element(By.XPATH, ".//td[2]//a")
                    name = (name_el.text or "").strip().lower()
                    if name:
                        name = " ".join(name.split())
                        out.append((r, name))
                except Exception:
                    continue
            return out

        def _download_csv_from_row(row) -> str | bool:
            link = None
            for xp in [
                ".//a[normalize-space(text())='[CSV Data]']",
                ".//a[contains(@href,'action=csv_data')]",
                ".//a[contains(., 'CSV')]",
            ]:
                try:
                    link = row.find_element(By.XPATH, xp)
                    if link:
                        break
                except Exception:
                    continue
            if not link:
                return False

            href_rel = link.get_attribute("href") or ""
            if not href_rel:
                return False

            abs_url = urljoin(self.base_url, href_rel)
            before = set(glob.glob(os.path.join(self.download_dir, '*.csv')))
            try:
                d.get(abs_url)
                return self._wait_new_csv(before)
            except Exception:
                return False

        def _goto_first_page_if_possible():
            for xp in [
                "//a[normalize-space(text())='First']",
                "//a[normalize-space(text())='Inicio']",
                "//a[contains(., '«') or contains(., '<<')]",
            ]:
                try:
                    nav = d.find_element(By.XPATH, xp)
                    d.execute_script("arguments[0].click();", nav)
                    _wait_ready(d, 60)
                    _wait_network_quiet(d, 1200, 60)
                    break
                except Exception:
                    continue

        def _next_page() -> bool:
            for xp in [
                "//a[normalize-space(text())='Next' or normalize-space(text())='Siguiente' or normalize-space(text())='Próximo']",
                "//a[contains(., '»') or contains(., '>')][not(contains(., '<<')) and not(contains(., '<'))]",
                "//button[contains(., 'Next') or contains(., 'Siguiente') or contains(., 'Próximo')]",
                "//li[@class='next']/a",
            ]:
                try:
                    nav = d.find_element(By.XPATH, xp)
                    d.execute_script("arguments[0].scrollIntoView({block:'center'});", nav)
                    d.execute_script("arguments[0].click();", nav)
                    _wait_ready(d, 60)
                    _wait_network_quiet(d, 1200, 60)
                    return True
                except Exception:
                    continue
            return False

        def _find_and_download_for_target(target_label: str, max_pages: int = 20) -> Optional[str]:
            """
            Busca por nombre (td[2]//a), tolera 'generali' vs 'generalli'.
            Para 'generali' base, excluye 'alt'.
            """
            t = " ".join(target_label.strip().lower().split())
            t = t.replace("generalli", "generali")

            def _is_match(name: str) -> bool:
                n_norm = name.replace("generalli", "generali")
                if t == "generali":
                    return ("generali" in n_norm) and ("alt" not in n_norm)
                else:
                    return t in n_norm

            for _ in range(max_pages):
                rows = _visible_campaign_rows()
                candidates = [r for (r, name) in rows if _is_match(name)]
                if candidates:
                    for r in candidates:
                        p = _download_csv_from_row(r)
                        if p:
                            return p
                if not _next_page():
                    break
            return None

        def _download_targets_on_menu(menu_key: str, wanted: List[str]) -> List[str]:
            """Navega a ?menu=<menu_key> y descarga para cada target faltante."""
            d.get(f"{self.base_url.rstrip('/')}/index.php?menu={menu_key}")
            _wait_ready(d, 90); _wait_network_quiet(d, 1200, 60)
            _in_campaigns_table_iframe()
            _goto_first_page_if_possible()

            out: List[str] = []
            for t in wanted:
                p = _find_and_download_for_target(t, max_pages=20)
                if p:
                    out.append(p)
            return out

        try:
            downloads: List[str] = []
            # 1) Ingoing campaigns
            downloads += _download_targets_on_menu("campaign_in", targets)

            # 2) Si faltan objetivos, intenta en outgoing
            missing = []
            for t in targets:
                # si el nombre del archivo no permite inferir target, igual reintenta
                if not any("generali" in os.path.basename(p).lower() for p in downloads):
                    missing.append(t)
                else:
                    # criterio más fino: si al menos uno contiene 'alt' y t == 'generali', fuerza missing para la base
                    if t == "generali" and not any(("alt" not in os.path.basename(p).lower()) for p in downloads):
                        missing.append(t)
                    if t == "generali alt" and not any(("alt" in os.path.basename(p).lower()) for p in downloads):
                        missing.append(t)

            if missing:
                downloads += _download_targets_on_menu("campaign_out", missing)

            if downloads:
                return downloads

            # Diagnóstico
            print("[campaigns] Current URL:", d.current_url)
            rows_now = _visible_campaign_rows()
            visible = [nm for (_, nm) in rows_now][:30]
            print("[campaigns] Visible rows (sample):\n", "\n".join(visible))
            _dump_debug(d, "download_campaigns_no_matches")
            raise RuntimeError('No campaign reports downloaded')

        except Exception:
            _dump_debug(d, "download_campaigns_timeout")
            raise
        finally:
            try:
                d.switch_to.default_content()
            except Exception:
                pass

# =============================
# Utilidad de formato + exports
# =============================

def fmt_date_en_dd_M_yyyy(dt: datetime) -> str:
    """
    Devuelve fecha como 'dd M yyyy', ej: '08 Oct 2025' (compat con jQuery UI 'dd M yy').
    """
    import calendar
    m = calendar.month_abbr[dt.month]  # Jan, Feb, ...
    return f"{dt.day:02d} {m} {dt.year}"

__all__ = ["SeleniumDownloader", "fmt_date_en_dd_M_yyyy"]
