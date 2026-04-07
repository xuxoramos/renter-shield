# Deploying to Hetzner VPS

Target: Hetzner **CX32** (4 vCPU, 8 GB RAM, 80 GB disk — ~€7.50/mo).
Location: **Falkenstein** or **Nuremberg** (Germany).

**Current server IP:** `204.168.232.213`

## 1. Provision the server

1. Create a Hetzner Cloud project and add an SSH key.
2. Spin up a **CX32** running **Ubuntu 24.04**.
3. Note the public IP. The current deployed IP is `204.168.232.213`.

## 2. Configure DNS

Before anything else, point `rentershield.org` at the server IP. Without
this step, HTTPS certificate issuance will fail and the site will be
unreachable.

At your domain registrar create the following DNS records (and optionally
AAAA if the VPS has IPv6):

| Type  | Host | Value                | TTL  |
|-------|------|---------------------|------|
| A     | @    | `204.168.232.213`   | 300  |
| A     | www  | `204.168.232.213`   | 300  |
| CNAME | ftp  | `rentershield.org.` | 3600 |
| TXT   | @    | `"v=spf1 -all"`     | 3600 |

> The `ftp` CNAME resolves to the same IP as the root domain so any
> requests to `ftp.rentershield.org` are handled and redirected to HTTPS
> by the same nginx instance.  The SPF record `v=spf1 -all` explicitly
> disallows all mail senders for this domain.

> **Cloudflare users:** set DNS records to **DNS only** (grey cloud) — not
> **Proxied** (orange cloud). Cloudflare's proxy rewrites headers and can
> interfere with Let's Encrypt ACME challenges and WebSocket connections
> used by Streamlit.

Wait for propagation (typically 1–5 minutes for low TTL, up to 48 hours
for some registrars), then verify:

```bash
# Should return 204.168.232.213
dig +short rentershield.org
dig +short www.rentershield.org
# Should also resolve (via CNAME) to 204.168.232.213
dig +short ftp.rentershield.org

# Quick connectivity check from your local machine
curl -sI http://204.168.232.213    # expect "connection refused" (nginx not up yet) or a response
```

If `dig` returns nothing or a wrong IP, DNS hasn't propagated yet — do **not**
proceed to TLS setup until both records resolve correctly.

A diagnostic script is included at `deploy/dns-check.sh` — see §7.

## 3. Initial server setup

```bash
ssh root@$SERVER_IP

# Updates
apt update && apt upgrade -y

# Docker
curl -fsSL https://get.docker.com | sh
apt install -y docker-compose-plugin

# Swap — the CX32 has 8 GB RAM and no swap by default.
# A 2 GB swapfile gives the OOM killer more headroom and prevents
# Streamlit processes from being killed during memory spikes.
fallocate -l 2G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab

# Firewall
ufw allow OpenSSH
ufw allow 80/tcp
ufw allow 443/tcp
ufw enable
```

## 4. Clone and configure

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

## 5. Launch

```bash
cd deploy
docker compose up -d --build
```

Verify (after TLS is configured in the next section):
```bash
curl -I https://rentershield.org/healthz          # 200 "ok"
curl -I https://rentershield.org/                  # 301 → /about
curl -I https://rentershield.org/about             # landing page HTML
curl -I https://rentershield.org/investigator/     # Streamlit investigator HTML
curl -H "X-API-Key: $LI_API_KEY" https://rentershield.org/api/investigator/jurisdictions  # API
```

## 6. TLS with Let's Encrypt

The nginx config is already set up for `rentershield.org`. Just obtain the
certificate and restart:

```bash
apt install -y certbot

# Get certificate (stop nginx briefly so certbot can bind :80)
docker compose stop nginx
certbot certonly --standalone \
  -d rentershield.org \
  -d www.rentershield.org \
  -d ftp.rentershield.org
docker compose start nginx
```

Auto-renew:
```bash
echo "0 3 * * * certbot renew --pre-hook 'docker compose -f /opt/renter-shield/deploy/docker-compose.yml stop nginx' --post-hook 'docker compose -f /opt/renter-shield/deploy/docker-compose.yml start nginx'" | crontab -
```

## 7. Troubleshooting DNS and connectivity

A diagnostic script is included at `deploy/dns-check.sh`. Run it from
any machine with `dig` and `curl` installed:

```bash
bash deploy/dns-check.sh
```

It checks:
1. DNS resolution for `rentershield.org`, `www.rentershield.org`, and `ftp.rentershield.org`
2. TCP connectivity on ports 80 and 443
3. HTTP response from the server
4. TLS certificate validity (if HTTPS is configured)

### Common problems

| Symptom | Cause | Fix |
|---------|-------|-----|
| `dig` returns no IP | A record missing or not propagated | Add A record at registrar; wait and retry |
| `dig` returns wrong IP | Stale DNS / wrong record | Update the A record to `204.168.232.213` |
| IP is correct but port 80 unreachable | Firewall blocking traffic | Run `ufw allow 80/tcp && ufw allow 443/tcp` |
| HTTP works but HTTPS fails | Certs not issued yet | Run certbot per §6 |
| Cloudflare 522/525 errors | Proxied mode conflicts with origin | Set DNS records to "DNS only" (grey cloud) |
| `ERR_TOO_MANY_REDIRECTS` | Cloudflare SSL set to "Flexible" | Set Cloudflare SSL mode to "Full (strict)" or disable proxy |

## 8. Updating data

When new scores are generated:

```bash
cd /opt/renter-shield
scp user@local:output/all_landlords_harm_scores.parquet output/
docker compose -f deploy/docker-compose.yml restart app
```

## Architecture

```
Internet → https://rentershield.org → Hetzner VPS (Germany)
  ├─ :80           nginx — redirects HTTP → HTTPS
  ├─ :443          nginx (TLS, rate-limited reverse proxy)
  │    ├─ /              → 301 redirect to /about (landing page)
  │    ├─ /about         → static landing page
  │    ├─ /* (fallback)  → Streamlit (:8501)  — renter public dashboard
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
