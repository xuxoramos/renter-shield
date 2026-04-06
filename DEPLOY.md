# Deploying to Hetzner VPS

Target: Hetzner **CX22** (2 vCPU, 4 GB RAM, 40 GB disk — €4.35/mo).
Location: **Falkenstein** or **Nuremberg** (Germany).

## 1. Provision the server

1. Create a Hetzner Cloud project and add an SSH key.
2. Spin up a **CX22** running **Ubuntu 24.04**.
3. Note the public IP (`$SERVER_IP`).

## 2. Initial server setup

```bash
ssh root@$SERVER_IP

# Updates
apt update && apt upgrade -y

# Docker
curl -fsSL https://get.docker.com | sh
apt install -y docker-compose-plugin

# Firewall
ufw allow OpenSSH
ufw allow 80/tcp
ufw allow 443/tcp
ufw enable
```

## 3. Clone and configure

```bash
git clone https://github.com/<your-org>/renter-shield.git /opt/renter-shield
cd /opt/renter-shield

# Copy scored output (download from Zenodo or scp from your machine)
mkdir -p output
# Option A: from Zenodo
# curl -L https://zenodo.org/records/19418744/files/output.tar.gz | tar xz -C output/
# Option B: from your machine
# scp output/all_landlords_harm_scores.parquet root@$SERVER_IP:/opt/renter-shield/output/

# Set API key
export LI_API_KEY=$(openssl rand -hex 32)
echo "LI_API_KEY=$LI_API_KEY" >> .env
echo "Save this API key: $LI_API_KEY"
```

## 4. Launch

```bash
cd deploy
docker compose up -d --build
```

Verify:
```bash
curl http://localhost/healthz          # nginx → "ok"
curl http://localhost/                  # Streamlit renter HTML
curl http://localhost/investigator/    # Streamlit investigator HTML
curl -H "X-API-Key: $LI_API_KEY" http://localhost/api/investigator/jurisdictions  # API
```

## 5. TLS with Let's Encrypt

```bash
apt install -y certbot

# Get certificate (stop nginx briefly)
docker compose stop nginx
certbot certonly --standalone -d your-domain.example.com
docker compose start nginx
```

Then in `deploy/nginx.conf`:
1. Uncomment the HTTPS server block.
2. Replace `your-domain.example.com` with your actual domain.
3. Uncomment the HTTP → HTTPS redirect.
4. Uncomment the certbot volume mount in `deploy/docker-compose.yml`.
5. Restart: `docker compose restart nginx`.

Auto-renew:
```bash
echo "0 3 * * * certbot renew --pre-hook 'docker compose -f /opt/renter-shield/deploy/docker-compose.yml stop nginx' --post-hook 'docker compose -f /opt/renter-shield/deploy/docker-compose.yml start nginx'" | crontab -
```

## 6. Updating data

When new scores are generated:

```bash
cd /opt/renter-shield
scp user@local:output/all_landlords_harm_scores.parquet output/
docker compose -f deploy/docker-compose.yml restart app
```

## Architecture

```
Internet → Hetzner VPS (Germany)
  ├─ :80/:443  nginx (rate-limited reverse proxy)
  │    ├─ /              → Streamlit (:8501)  — renter public dashboard
  │    ├─ /investigator/ → Streamlit (:8502)  — investigator dashboard
  │    └─ /api/*         → FastAPI  (:8000)   — API key required
  └─ Data:  output/all_landlords_harm_scores.parquet (mounted read-only)
```

**Investigator Streamlit** is started with `--server.baseUrlPath investigator` so
that it serves at `/investigator/`.  The nginx `proxy_pass http://investigator;`
(no trailing slash) forwards the **full** `/investigator/` path unchanged to the
Streamlit process, which is the correct form for Streamlit's baseUrlPath feature.

**Jurisdictional separation**: the VPS runs in Germany (EU), data archived
on Zenodo (Switzerland), source code on GitHub (US). No single jurisdiction
controls all three.
