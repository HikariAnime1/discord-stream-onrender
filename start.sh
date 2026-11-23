#!/bin/sh
set -e

# Priority 1: Static external proxy explicitly provided in DISCORD_PROXY (e.g. Residential VN Proxy)
if [ -n "$DISCORD_PROXY" ]; then
  echo "[net-guard] Mode=EXTERNAL_PROXY (using DISCORD_PROXY from environment)"
  export REQUIRE_PROXY="${REQUIRE_PROXY:-1}"
  exec python3 -u stream.py
fi

# Priority 2: Fixed WireGuard tunnel (e.g. WireGuard VN or private WireGuard server)
if [ -n "$WG_PRIVATE_KEY" ] && [ -n "$WG_PUBLIC_KEY" ] && [ -n "$WG_ENDPOINT" ]; then
  echo "[net-guard] Mode=WIREGUARD_PINNED (endpoint=${WG_ENDPOINT})"
  mkdir -p /tmp/wg
  cat > /tmp/wg/wireproxy.conf <<EOF
[Interface]
PrivateKey = ${WG_PRIVATE_KEY}
Address = ${WG_ADDRESS:-10.0.0.2/32}
DNS = 1.1.1.1
MTU = 1280

[Peer]
PublicKey = ${WG_PUBLIC_KEY}
Endpoint = ${WG_ENDPOINT}
KeepAlive = 25
EOF
  if [ -n "$WG_PRESHARED_KEY" ]; then
    echo "PresharedKey = ${WG_PRESHARED_KEY}" >> /tmp/wg/wireproxy.conf
  fi
  cat >> /tmp/wg/wireproxy.conf <<EOF

[http]
BindAddress = 127.0.0.1:1055
EOF

  wireproxy -c /tmp/wg/wireproxy.conf &
  sleep 2
  export DISCORD_PROXY="http://127.0.0.1:1055"
  export REQUIRE_PROXY="1"
  exec python3 -u stream.py
fi

# Priority 3: Cloudflare WARP via wireproxy (pin WARP_PRIVATE_KEY + WARP_ENDPOINT to keep identity fixed)
if [ "${ENABLE_WARP:-1}" = "1" ]; then
  echo "[net-guard] Mode=CLOUDFLARE_WARP"
  mkdir -p /tmp/warp

  if [ -n "$WARP_PRIVATE_KEY" ]; then
    PRIV_KEY="$WARP_PRIVATE_KEY"
    PUB_KEY="${WARP_PUBLIC_KEY:-bmXOC+F1FxEMF9dyiK2H5/1SUtzH0JuVo51h2wPfgyo=}"
    ADDR_V4="${WARP_ADDR_V4:-172.16.0.2/32}"
    echo "[warp] Using pinned WARP_PRIVATE_KEY from environment"
  else
    echo "[warp] Registering free Cloudflare WARP profile via wgcf..."
    (
      cd /tmp/warp
      wgcf register --accept-tos >/dev/null
      wgcf generate >/dev/null
    )
    PRIV_KEY=$(grep -m1 '^PrivateKey' /tmp/warp/wgcf-profile.conf | cut -d '=' -f 2- | tr -d ' \r')
    PUB_KEY=$(grep -m1 '^PublicKey' /tmp/warp/wgcf-profile.conf | cut -d '=' -f 2- | tr -d ' \r')
    ADDR_V4=$(grep -m1 '^Address *= *[0-9]' /tmp/warp/wgcf-profile.conf | cut -d '=' -f 2- | tr -d ' \r' | cut -d ',' -f 1)
    ADDR_V4="${ADDR_V4:-172.16.0.2/32}"
    if [ "${PRINT_WARP_KEY:-0}" = "1" ]; then
      echo "[warp] Copy into Render Env to pin key: WARP_PRIVATE_KEY=${PRIV_KEY}"
    fi
  fi

  ENDPOINT="${WARP_ENDPOINT:-162.159.192.1:2408}"

  cat > /tmp/warp/wireproxy.conf <<EOF
[Interface]
PrivateKey = ${PRIV_KEY}
Address = ${ADDR_V4}
DNS = 1.1.1.1
MTU = 1280

[Peer]
PublicKey = ${PUB_KEY}
Endpoint = ${ENDPOINT}
KeepAlive = 25

[http]
BindAddress = 127.0.0.1:1055
EOF

  wireproxy -c /tmp/warp/wireproxy.conf &

  WARP_OK=0
  for i in 1 2 3 4 5; do
    sleep 2
    TRACE=$(curl -s --max-time 6 -x http://127.0.0.1:1055 https://www.cloudflare.com/cdn-cgi/trace || true)
    WARP_STATE=$(echo "$TRACE" | grep '^warp=' | cut -d '=' -f 2 | tr -d '\r')
    if [ "$WARP_STATE" = "on" ] || [ "$WARP_STATE" = "plus" ]; then
      WARP_OK=1
      break
    fi
    echo "[warp] Waiting for handshake (${i}/5)..."
  done

  if [ "$WARP_OK" != "1" ]; then
    echo "[warp] Handshake failed; stream.py IP Guard will halt cleanly without leaking Render IP."
  fi

  export DISCORD_PROXY="http://127.0.0.1:1055"
  export REQUIRE_PROXY="1"
fi

exec python3 -u stream.py
