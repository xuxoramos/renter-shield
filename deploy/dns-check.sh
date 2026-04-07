#!/usr/bin/env bash
# deploy/dns-check.sh — Diagnose DNS, connectivity, and TLS for rentershield.org
# Usage: bash deploy/dns-check.sh [EXPECTED_IP]
#
# If EXPECTED_IP is provided, the script also verifies the DNS A record
# matches that IP.  Otherwise it just reports what DNS returns.
set -euo pipefail

DOMAIN="rentershield.org"
WWW_DOMAIN="www.rentershield.org"
FTP_DOMAIN="ftp.rentershield.org"
EXPECTED_IP="${1:-}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() { printf "${GREEN}✓ %s${NC}\n" "$1"; }
warn() { printf "${YELLOW}⚠ %s${NC}\n" "$1"; }
fail() { printf "${RED}✗ %s${NC}\n" "$1"; }

# curl returns "000" when the connection is refused or times out.
CURL_CONN_FAIL="000"
errors=0

# ── 1. DNS resolution ───────────────────────────────────────────
echo ""
echo "=== DNS Resolution ==="

root_domain_ip=""
for host in "$DOMAIN" "$WWW_DOMAIN"; do
    ip=$(dig +short "$host" A 2>/dev/null | head -1)
    if [ -z "$ip" ]; then
        fail "$host — no A record found"
        echo "  Fix: Add an A record for '$host' pointing to your server IP at your registrar."
        errors=$((errors + 1))
    else
        if [ -n "$EXPECTED_IP" ] && [ "$ip" != "$EXPECTED_IP" ]; then
            fail "$host → $ip (expected $EXPECTED_IP)"
            echo "  Fix: Update the A record to point to $EXPECTED_IP."
            errors=$((errors + 1))
        else
            pass "$host → $ip"
        fi
        # Capture the root domain IP for reuse in subsequent checks
        [ "$host" = "$DOMAIN" ] && root_domain_ip="$ip"
    fi
done

# Verify the ftp CNAME resolves to the same server IP
ftp_resolved=$(dig +short "$FTP_DOMAIN" A 2>/dev/null | head -1)
ftp_cname=$(dig +short "$FTP_DOMAIN" CNAME 2>/dev/null | head -1)
if [ -z "$ftp_resolved" ]; then
    fail "$FTP_DOMAIN — CNAME does not resolve to an IP"
    echo "  Fix: Add a CNAME record for 'ftp' pointing to 'rentershield.org.' at your registrar."
    errors=$((errors + 1))
else
    if [ -n "$root_domain_ip" ] && [ "$ftp_resolved" != "$root_domain_ip" ]; then
        warn "$FTP_DOMAIN → $ftp_resolved (differs from $DOMAIN → $root_domain_ip)"
    else
        if [ -n "$ftp_cname" ]; then
            pass "$FTP_DOMAIN CNAME → $ftp_cname → $ftp_resolved"
        else
            pass "$FTP_DOMAIN → $ftp_resolved"
        fi
    fi
fi

# ── 2. Port connectivity ────────────────────────────────────────
echo ""
echo "=== Port Connectivity ==="

# Determine target IP for connectivity checks (reuse captured root IP if available)
target_ip="$root_domain_ip"
if [ -z "$target_ip" ]; then
    if [ -n "$EXPECTED_IP" ]; then
        target_ip="$EXPECTED_IP"
        warn "DNS not resolved; testing connectivity against provided IP ($target_ip)"
    else
        warn "Skipping port checks — DNS does not resolve and no IP provided."
        target_ip=""
    fi
fi

if [ -n "$target_ip" ]; then
    for port in 80 443; do
        if timeout 5 bash -c "echo >/dev/tcp/$target_ip/$port" 2>/dev/null; then
            pass "Port $port on $target_ip is reachable"
        else
            fail "Port $port on $target_ip is NOT reachable"
            echo "  Fix: Ensure the firewall allows TCP/$port (ufw allow $port/tcp) and the service is running."
            errors=$((errors + 1))
        fi
    done
fi

# ── 3. HTTP response ────────────────────────────────────────────
echo ""
echo "=== HTTP Response ==="

http_status=$(curl -sI -o /dev/null -w "%{http_code}" --max-time 10 "http://$DOMAIN/" 2>/dev/null || echo "$CURL_CONN_FAIL")
if [ "$http_status" = "$CURL_CONN_FAIL" ]; then
    fail "http://$DOMAIN/ — no response (connection refused or timed out)"
    errors=$((errors + 1))
elif [ "$http_status" = "301" ] || [ "$http_status" = "302" ]; then
    pass "http://$DOMAIN/ → HTTP $http_status (redirect — expected)"
else
    warn "http://$DOMAIN/ → HTTP $http_status (expected 301 redirect to HTTPS)"
fi

# ── 4. HTTPS / TLS ──────────────────────────────────────────────
echo ""
echo "=== HTTPS / TLS ==="

https_status=$(curl -sI -o /dev/null -w "%{http_code}" --max-time 10 "https://$DOMAIN/" 2>/dev/null || echo "$CURL_CONN_FAIL")
if [ "$https_status" = "$CURL_CONN_FAIL" ]; then
    fail "https://$DOMAIN/ — no response (TLS handshake failed or port 443 unreachable)"
    echo "  Fix: Ensure certbot has issued certificates (see DEPLOY.md §6)."
    errors=$((errors + 1))
elif [ "$https_status" = "301" ]; then
    pass "https://$DOMAIN/ → HTTP $https_status (redirect to /about — expected)"
elif [ "$https_status" = "200" ]; then
    pass "https://$DOMAIN/ → HTTP $https_status"
else
    warn "https://$DOMAIN/ → HTTP $https_status"
fi

# Check certificate expiry
if command -v openssl >/dev/null 2>&1; then
    cert_expiry=$(echo | openssl s_client -servername "$DOMAIN" -connect "$DOMAIN:443" 2>/dev/null \
        | openssl x509 -noout -enddate 2>/dev/null | cut -d= -f2)
    if [ -n "$cert_expiry" ]; then
        pass "TLS certificate expires: $cert_expiry"
    else
        warn "Could not retrieve TLS certificate — HTTPS may not be configured yet."
    fi
fi

# ── Summary ──────────────────────────────────────────────────────
echo ""
if [ "$errors" -eq 0 ]; then
    echo "=== All checks passed ==="
else
    echo "=== $errors issue(s) found — see above for fixes ==="
    exit 1
fi
