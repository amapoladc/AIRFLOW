# ────────────────────────────────────────────────────────────
#  Nombre: docker/Dockerfile
#  Propósito: Imagen base de Airflow con Chrome, Selenium y dbt
# ────────────────────────────────────────────────────────────
FROM apache/airflow:3.0.2

# ---------- 1) Paquetes del sistema + Google Chrome ----------
USER root
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        wget curl gnupg unzip ca-certificates \
        # dependencias gráficas de Chrome
        libglib2.0-0 libnss3 libgconf-2-4 libfontconfig1 libx11-6 xvfb \
        libatk-bridge2.0-0 libxcomposite1 libxdamage1 libxrandr2 libgbm1 \
        libasound2 && \
    rm -rf /var/lib/apt/lists/*

# Repo de Google Chrome y paquete estable
RUN mkdir -p /etc/apt/keyrings && \
    curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /etc/apt/keyrings/google-linux.gpg && \
    echo "deb [arch=amd64 signed-by=/etc/apt/keyrings/google-linux.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
      > /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      google-chrome-stable && \
    rm -rf /var/lib/apt/lists/*

# Nota: NO instalamos chromium-driver. Selenium 4+ usa Selenium Manager
# y resuelve automáticamente el driver compatible con Chrome.

# ---------- 2) Docker CLI (para usar DockerOperator con /var/run/docker.sock) ----------
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends docker.io && \
    usermod -aG docker airflow && \
    rm -rf /var/lib/apt/lists/*

# ---------- 3) Dependencias Python desde requirements.txt ----------
USER airflow
# dentro de docker/Dockerfile
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
