#!/usr/bin/env python3
"""
Discord Go-Live Stealth Streamer v7.1 — Audited & Patched Human Desktop Parity Edition
Fixes & Refinements in v7.1:
  - [FIX] Gateway Loopback + Proxy Conflict: skips wsClientOpts.agent when wsBridgeActive=true
    so ws://127.0.0.1:<port>/gw/<secret> connects locally while *.discord.media still uses proxy.
  - [FIX] Startup Crash with DISCORD_DEVICE_COOKIES: seeds cookies AFTER _curl_sess init.
  - [FIX] Cookie Isolation: DISCORD_DEVICE_COOKIES applies ONLY to Main; ALT_DEVICE_COOKIES
    applies ONLY to Alt (eliminates cross-account device cookie leak).
  - [FIX] Thread-safe curl_cffi WS handshake lock in LocalChromeWSBridge._open_upstream().
  - [FIX] Propagates captcha/verification lock detection (400 captcha_key, 403 40002) to
    ensure_analytics_token() and post_science() Circuit Breaker.
  - [OPT] Decoupled running_game_heartbeat cadence (180-300s) from video_stream_heartbeat (~60s).
  - [OPT] Adaptive exponential backoff (15-60s) for Alt Viewer OP 20 polling when stream is absent.
New in v7:
  - Fixed v6 watchdog method placement into StreamRunnerStealth (guaranteed runtime IP guard).
  - Secret & Token Redaction in Logger: automatically scrubs Discord tokens, proxy credentials,
    and auth secrets from stdout/Render logs so nothing sensitive is ever exposed.
  - Persistent Device Cookies (__dcfduid, __sdcfduid) support via env/state to avoid looking
    like a brand-new stolen-token login on every container restart.
  - Humanized Daily Schedule Jitter (ACTIVE_HOURS_JITTER_MIN): shifts start/stop times by
    random ±minutes each calendar day so activity never looks like a rigid cron job.
  - Authentic Desktop Event Order: sends launch_game + initial running_game_heartbeat BEFORE
    Go-Live stream_started (matching real Discord Desktop game detection -> Go Live flow).
  - Humanized Alt Viewer Lifecycle: Alt joins 10-28s after stream starts and leaves 8-25s
    after streamer stops for breaks/rest instead of idling robotically in an empty VC.
  - Pre-Flight Anti-Detection Audit + Extended Auth/Captcha/Verification Circuit Breaker.
New in v6:
  - LocalChromeWSBridge (RFC 6455 loopback relay -> curl_cffi Chrome JA3/JA4 WebSocket):
    Tunnels Node Gateway WS (wss://gateway.discord.gg) through Python curl_cffi Chrome TLS
    with synced cookies, headers & proxy (auto-fallback if curl_cffi lacks ws_connect).
  - Runtime Egress IP Drift Watchdog (LOCK_INITIAL_IP + IP_WATCHDOG_MIN):
    Continuously verifies egress IP/city/country mid-session; halts immediately on silent IP drift.
  - Deep Fake Device v6: coherent GPU encoder (NVENC/QuickSync/VideoToolbox), power state,
    simulated OS uptime, new win10_i3_desktop tier, and FAKE_* hardware env overrides.
  - Deep Fake Desktop App v6: full Desktop Gateway OP 2 client_state & bot-field scrubbing,
    expanded read-only cold-boot REST flow (settings-proto/1, settings-proto/2, country-code),
    session identity rotation after ACTIVE_HOURS rest, realistic WebRTC telemetry jitter.
  - Anti-Pattern Behavior: GAME_ROTATION + SESSION_BREAK_MIN + stealth_ambient video preset.
New in v5:
  - City/Region/ISP GeoIP Pinning & Fail-Closed Guard (EXPECTED_CITY, EXPECTED_REGION, EXPECTED_ISP)
    NOTE: Login location (e.g. Da Nang, Da Nang, Vietnam) is determined by Discord/Cloudflare
    strictly from the real egress IP, never from spoofed HTTP headers. This guard verifies
    the proxy egress IP resolves to your target city/region BEFORE sending any token.
  - Full Chromium/Electron High-Entropy Client Hints (sec-ch-ua-platform-version, arch, bitness,
    full-version-list) strictly synchronized with DeviceProfile OS & Chrome version.
  - Realistic Discord Desktop Cold-Boot REST sequence (/app -> /users/@me -> /settings-proto/1 -> app_opened).
  - SDP WebRTC local container IP scrubbing in Node worker.
Upgrades over v3:
  1. Deep Fake Device Engine (DeviceProfile + Smooth Telemetry Drift)
     - Deterministic, persistent virtual PC hardware profile bound to DEVICE_SEED (and
       separate ALT_DEVICE_SEED so Main and Alt look like two genuinely distinct PCs).
     - Coherent Windows 10/11 or macOS hardware matrix: CPU brand/cores, total system RAM,
       GPU vendor/model, display resolution/scale/count, audio I/O devices, OS SDK/build.
     - Smooth random-walk CPU & memory telemetry drift around realistic game-streaming baselines
       instead of erratic random spikes.
  2. Deep Fake Discord Desktop App (ClientBuildResolver + Electron/Gateway Normalization)
     - Live Discord Desktop Stable metadata discovery: CordAPI primary + official Discord
       updater API fallback (/api/updates/stable) so client_version stays current.
     - Exact Electron Desktop REST headers: synchronized x-super-properties, User-Agent,
       Sec-CH-UA*, Sec-Fetch-*, Accept-Language, dynamic channel Referer, and complete
       scrubbing of discord.js/undici/Node headers at LocalChromeTLSBridge.
     - Gateway OP 2 Identify normalization: aligns properties, modern Desktop capabilities
       bitmask, and client_state strictly with official Discord Desktop v9.
  3. Authentic Desktop App Lifecycle & Behavioral Engine
     - Organic cold-boot flow: /app cookie warm-up -> /users/@me -> app_opened telemetry ->
       join voice (natural self_mute/self_deaf) -> launch game presence -> Go-Live stream.
     - FocusSimulator (state-duration Markov model): stays background for 4-15 min while
       "playing", occasionally Alt-Tabs to focused for 1-3 min, then returns to background.
     - Authentic Desktop shutdown sequence: stream end -> final game heartbeat -> clear
       Presence (OP 3) -> stop stream (OP 19) -> leave voice (OP 4).
  4. Inherited v3 Network Protections
     - Fail-Closed Egress IP & Country Guard (REQUIRE_PROXY, EXPECTED_IP, ALLOWED_COUNTRIES).
     - Local Chrome-TLS REST Bridge (127.0.0.1 + curl_cffi) + Node https.request WS hook.
     - Auth-Lock Circuit Breaker (halts cleanly on 401/403/4004).

Usage:
  Setup (Docker build): python3 -u stream.py --setup-only
  Run:                  python3 -u stream.py
"""
from __future__ import annotations

import atexit
import base64
import datetime
import gzip
import hashlib
import http.client
import http.server
import itertools
import json
import os
import random
import re
import secrets
import shutil
import signal
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None  # type: ignore

try:
    from curl_cffi import requests as curl_requests  # type: ignore
    HAS_CURL_CFFI = True
except Exception:
    curl_requests = None
    HAS_CURL_CFFI = False

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except (AttributeError, OSError):
        pass

ROOT = Path(__file__).resolve().parent
WEBRTC_DIR = ROOT / ".webrtc_engine"
MEDIA_CACHE_DIR = ROOT / ".media_cache"
GAMES_CACHE_FILE = ROOT / "games_compact.json"
STATE_FILE = ROOT / ".stealth_state.json"

os.environ["PATH"] = str(ROOT) + os.pathsep + os.environ.get("PATH", "")

DISCORD_HOST = "discord.com"
PROPERTIES_URL_WIN = "https://cordapi.dolfi.es/api/v2/properties/windows"
PROPERTIES_URL_MAC = "https://cordapi.dolfi.es/api/v2/properties/macos"
UPDATES_URL_WIN = "https://discord.com/api/updates/stable?platform=win"
UPDATES_URL_MAC = "https://discord.com/api/updates/stable?platform=osx"
GAMES_CDN_URL = "https://cdn.discordapp.com/detectables/games.json"
CF_TRACE_URL = "https://www.cloudflare.com/cdn-cgi/trace"
GEOIP_PRIMARY_URL = "https://ipwho.is/"
GEOIP_FALLBACK_URL = "https://ipinfo.io/json"

AUTH_MAX_AGE = 12 * 3600
FALLBACK_CLIENT_VERSION = "1.0.9253"
FALLBACK_BUILD_NUMBER = 594031
FALLBACK_NATIVE_BUILD = 88414
FALLBACK_ELECTRON_VERSION = "36.3.2"
FALLBACK_CHROME_VERSION = "136.0.7103.114"
FALLBACK_DESKTOP_CAPABILITIES = 30717

_PACK_FP_STRUCT = struct.Struct("<BQII32s32s32s32s")

# Realistic hardware tiers for deterministic Fake Device profiles
_WIN_HARDWARE_TIERS = (
    {
        "tier": "win11_mid_desktop",
        "os_version": "10.0.26100",
        "os_sdk_version": "26100",
        "cpu_brand": "Intel(R) Core(TM) i5-12400F",
        "cpu_cores": 12,
        "ram_total_mb": 16384,
        "gpu_vendor": "NVIDIA",
        "gpu_model": "NVIDIA GeForce RTX 3060",
        "encoder_name": "NVENC",
        "is_laptop": False,
        "display_w": 1920,
        "display_h": 1080,
        "display_scale": 1.0,
        "display_count": 1,
        "audio_in": "Microphone (Realtek(R) Audio)",
        "audio_out": "Speakers (Realtek(R) Audio)",
    },
    {
        "tier": "win11_laptop_igpu",
        "os_version": "10.0.22631",
        "os_sdk_version": "22631",
        "cpu_brand": "Intel(R) Core(TM) i5-1135G7",
        "cpu_cores": 8,
        "ram_total_mb": 16384,
        "gpu_vendor": "Intel",
        "gpu_model": "Intel(R) Iris(R) Xe Graphics",
        "encoder_name": "QuickSync",
        "is_laptop": True,
        "display_w": 1920,
        "display_h": 1080,
        "display_scale": 1.25,
        "display_count": 1,
        "audio_in": "Microphone Array (Intel Smart Sound Technology)",
        "audio_out": "Speakers (Realtek High Definition Audio)",
    },
    {
        "tier": "win10_i3_desktop",
        "os_version": "10.0.19045",
        "os_sdk_version": "19045",
        "cpu_brand": "Intel(R) Core(TM) i3-7100 CPU @ 3.90GHz",
        "cpu_cores": 4,
        "ram_total_mb": 16384,
        "gpu_vendor": "Intel",
        "gpu_model": "Intel(R) HD Graphics 630",
        "encoder_name": "QuickSync",
        "is_laptop": False,
        "display_w": 1920,
        "display_h": 1080,
        "display_scale": 1.0,
        "display_count": 1,
        "audio_in": "Microphone (Realtek High Definition Audio)",
        "audio_out": "Speakers (Realtek High Definition Audio)",
    },
    {
        "tier": "win10_office_desktop",
        "os_version": "10.0.19045",
        "os_sdk_version": "19045",
        "cpu_brand": "Intel(R) Core(TM) i3-10105",
        "cpu_cores": 8,
        "ram_total_mb": 16384,
        "gpu_vendor": "Intel",
        "gpu_model": "Intel(R) UHD Graphics 630",
        "encoder_name": "QuickSync",
        "is_laptop": False,
        "display_w": 1920,
        "display_h": 1080,
        "display_scale": 1.0,
        "display_count": 1,
        "audio_in": "Microphone (USB Audio Device)",
        "audio_out": "Headphones (High Definition Audio Device)",
    },
    {
        "tier": "win11_ryzen_gaming",
        "os_version": "10.0.26200",
        "os_sdk_version": "26200",
        "cpu_brand": "AMD Ryzen 5 5600X 6-Core Processor",
        "cpu_cores": 12,
        "ram_total_mb": 32768,
        "gpu_vendor": "NVIDIA",
        "gpu_model": "NVIDIA GeForce RTX 4060",
        "encoder_name": "NVENC",
        "is_laptop": False,
        "display_w": 2560,
        "display_h": 1440,
        "display_scale": 1.0,
        "display_count": 2,
        "audio_in": "Microphone (HyperX SoloCast)",
        "audio_out": "Headset Earphone (Realtek USB Audio)",
    },
)

_MAC_HARDWARE_TIERS = (
    {
        "tier": "macos_sonoma_intel",
        "os_version": "23.6.0",
        "os_sdk_version": "23",
        "cpu_brand": "Intel(R) Core(TM) i5-1038NG7",
        "cpu_cores": 8,
        "ram_total_mb": 16384,
        "gpu_vendor": "Intel",
        "gpu_model": "Intel Iris Plus Graphics",
        "encoder_name": "VideoToolbox",
        "is_laptop": True,
        "display_w": 2560,
        "display_h": 1600,
        "display_scale": 2.0,
        "display_count": 1,
        "audio_in": "MacBook Pro Microphone",
        "audio_out": "MacBook Pro Speakers",
    },
    {
        "tier": "macos_sequoia_arm",
        "os_version": "24.3.0",
        "os_sdk_version": "24",
        "cpu_brand": "Apple M2",
        "cpu_cores": 8,
        "ram_total_mb": 16384,
        "gpu_vendor": "Apple",
        "gpu_model": "Apple M2",
        "encoder_name": "VideoToolbox",
        "is_laptop": True,
        "display_w": 2560,
        "display_h": 1664,
        "display_scale": 2.0,
        "display_count": 1,
        "audio_in": "MacBook Air Microphone",
        "audio_out": "MacBook Air Speakers",
    },
)

QUALITY_PRESETS = {
    "ultra_lite": {
        "width": 256,
        "height": 144,
        "fps": 5,
        "kbps": 50,
        "max_kbps": 100,
        "include_audio": False,
        "video_mode": "black",
    },
    "stealth_ambient": {
        "width": 320,
        "height": 180,
        "fps": 6,
        "kbps": 65,
        "max_kbps": 110,
        "include_audio": False,
        "video_mode": "ambient_hud",
    },
    "stealth_lite": {
        "width": 320,
        "height": 180,
        "fps": 8,
        "kbps": 90,
        "max_kbps": 150,
        "include_audio": False,
        "video_mode": "dark_motion",
    },
    "render_lite": {
        "width": 480,
        "height": 270,
        "fps": 8,
        "kbps": 150,
        "max_kbps": 250,
        "include_audio": True,
        "video_mode": "dark_motion",
    },
    "farm10fps": {
        "width": 640,
        "height": 360,
        "fps": 10,
        "kbps": 250,
        "max_kbps": 400,
        "include_audio": True,
        "video_mode": "testsrc",
    },
    "720p30": {
        "width": 1280,
        "height": 720,
        "fps": 30,
        "kbps": 2200,
        "max_kbps": 3500,
        "include_audio": True,
        "video_mode": "testsrc",
    },
}

_NSFW_PATTERN = re.compile(
    r"(hentai|sex|porn|nsfw|nude|erotic|waifu|oppai|succubus|lewd|futa|brothel|strip|ecchi|harem|lust|fetish|milf|boob|panties|nekopara|koikatsu|honey\s*select)",
    re.IGNORECASE,
)
_AAA_KEYWORDS = (
    "counter-strike", "valorant", "league of legends", "dota 2", "grand theft auto",
    "elden ring", "cyberpunk 2077", "minecraft", "apex legends", "genshin impact",
    "red dead redemption", "overwatch", "call of duty", "baldur's gate", "fortnite",
    "rocket league", "rainbow six", "pubg", "helldivers", "monster hunter",
)

# Native C++ WebRTC Sidecar Worker v4:
#   - Routes 100% of Discord REST through Python's LocalChromeTLSBridge (Chrome JA3/JA4 + cookies)
#   - Hooks Node https.request so ALL WebSockets (Gateway + Voice + Stream SFU) carry Desktop
#     Origin/User-Agent/Accept-Language headers and pinned Proxy Agent
#   - Normalizes Gateway OP 2 Identify (Desktop properties + capabilities bitmask + client_state)
#   - Enforces realistic Desktop voice state (self_mute / self_deaf) and clean shutdown order
_WEBRTC_WORKER_MJS = r"""
import { Client } from "discord.js-selfbot-v13";
import { Streamer, prepareStream, playStream, Utils, Encoders } from "@dank074/discord-video-stream";
import crypto from "crypto";
import fs from "fs";
import path from "path";
import { createRequire } from "module";

const cjsRequire = createRequire(import.meta.url);
const httpsMod = cjsRequire("https");

function loadModuleFromPkg(pkgName, pkgDir = process.cwd()) {
  try {
    const req = createRequire(path.join(pkgDir, "package.json"));
    return req(pkgName);
  } catch (_) {
    return null;
  }
}

const cfgPath = process.argv[2];
let cfg = JSON.parse(fs.readFileSync(cfgPath, "utf8"));
let currentCommand = null;
let isShuttingDown = false;
let authFatalTriggered = false;

const nmDir = path.join(process.cwd(), "node_modules");
const searchDirs = [
  process.cwd(),
  path.join(nmDir, "@dank074", "discord-video-stream"),
  path.join(nmDir, "discord.js-selfbot-v13"),
];

const wsCandidates = [];
for (const dir of searchDirs) {
  const mod = loadModuleFromPkg("ws", dir);
  const WsCls = mod?.WebSocket || mod?.default || mod;
  if (WsCls && WsCls.prototype && !wsCandidates.includes(WsCls)) {
    wsCandidates.push(WsCls);
  }
}
const WebSocket = wsCandidates[0] || globalThis.WebSocket;

let nodeDataChannel = null;
for (const dir of searchDirs) {
  nodeDataChannel = loadModuleFromPkg("node-datachannel", dir);
  if (nodeDataChannel) break;
}

// Optional Proxy Agent for ALL Discord WebSockets (Gateway + Voice + Stream SFU)
let wsProxyAgent = null;
if (cfg.proxy) {
  try {
    const isSocks = String(cfg.proxy).toLowerCase().startsWith("socks");
    if (isSocks) {
      const mod = loadModuleFromPkg("socks-proxy-agent", process.cwd());
      const AgentCls = mod?.SocksProxyAgent || mod?.default || mod;
      if (AgentCls) wsProxyAgent = new AgentCls(cfg.proxy);
    } else {
      const mod = loadModuleFromPkg("https-proxy-agent", process.cwd());
      const AgentCls = mod?.HttpsProxyAgent || mod?.default || mod;
      if (AgentCls) wsProxyAgent = new AgentCls(cfg.proxy);
    }
  } catch (e) {
    console.error(`[WEBRTC-ERROR] Proxy agent init failed: ${e?.message || e}`);
  }
}

// Harden Node https.request for WebSockets to discord.gg / discord.media / discord.com
if (!httpsMod.__stealthHooked) {
  const origHttpsRequest = httpsMod.request;
  httpsMod.request = function patchedHttpsRequest(urlOrOpts, maybeOpts, maybeCb) {
    try {
      let opts = null;
      let host = "";
      if (typeof urlOrOpts === "string" || urlOrOpts instanceof URL) {
        const u = new URL(String(urlOrOpts));
        host = u.hostname;
        if (maybeOpts && typeof maybeOpts === "object" && typeof maybeOpts !== "function") {
          opts = maybeOpts;
        }
      } else if (urlOrOpts && typeof urlOrOpts === "object") {
        opts = urlOrOpts;
        host = String(opts.hostname || opts.host || "").split(":")[0];
      }

      if (opts && /(?:^|\.)(?:discord\.gg|discord\.media|discord\.com)$/i.test(host)) {
        opts.headers = { ...(opts.headers || {}) };
        if (cfg.userAgent) opts.headers["User-Agent"] = cfg.userAgent;
        if (cfg.acceptLanguage) opts.headers["Accept-Language"] = cfg.acceptLanguage;
        const isWsUpgrade =
          String(opts.headers["Upgrade"] || opts.headers["upgrade"] || "").toLowerCase() === "websocket" ||
          /discord\.gg$|discord\.media$/i.test(host);
        if (isWsUpgrade) {
          opts.headers["Origin"] = "https://discord.com";
          opts.headers["Pragma"] = "no-cache";
          opts.headers["Cache-Control"] = "no-cache";
        }
        if (wsProxyAgent && !opts.agent) {
          opts.agent = wsProxyAgent;
        }
      }
    } catch (_) {}
    return origHttpsRequest.apply(this, arguments);
  };
  httpsMod.__stealthHooked = true;
}

// Fallback fetch interceptor: rewrites any direct discord.com/api fetch into the local Chrome-TLS bridge
if (typeof globalThis.fetch === "function" && !globalThis.fetch.__stealthWrapped) {
  const origFetch = globalThis.fetch.bind(globalThis);
  globalThis.fetch = function (input, init = {}) {
    try {
      const urlStr = typeof input === "string" ? input : String(input?.url || "");
      if (cfg.restBridgeBase && /^https:\/\/(?:canary\.|ptb\.)?discord\.com\/api(\/.*)?$/i.test(urlStr)) {
        const parsed = new URL(urlStr);
        const rewritten = `${cfg.restBridgeBase}${parsed.pathname}${parsed.search}`;
        if (typeof input === "string") {
          input = rewritten;
        } else {
          input = new Request(rewritten, input);
        }
      } else if (urlStr.includes("discord.com/api")) {
        const hdrs = new Headers(init?.headers || (input instanceof Request ? input.headers : undefined));
        if (cfg.userAgent) hdrs.set("user-agent", cfg.userAgent);
        if (cfg.superPropsB64) hdrs.set("x-super-properties", cfg.superPropsB64);
        if (cfg.locale) hdrs.set("x-discord-locale", cfg.locale);
        if (cfg.timezone) hdrs.set("x-discord-timezone", cfg.timezone);
        if (cfg.acceptLanguage) hdrs.set("accept-language", cfg.acceptLanguage);
        init = { ...init, headers: hdrs };
      }
    } catch (_) {}
    return origFetch(input, init);
  };
  globalThis.fetch.__stealthWrapped = true;
}

const REGION_AIRPORT_MAP = {
  sin: "singapore", hkg: "hongkong", tyo: "japan", syd: "sydney",
  icn: "south-korea", bom: "india", maa: "india", ams: "rotterdam",
  fra: "frankfurt", lhr: "london", iad: "us-east", ewr: "us-east",
  atl: "us-east", ord: "us-central", dfw: "us-central", lax: "us-west",
  sjc: "us-west", sea: "us-west", gru: "brazil",
};

function inferRegionFromHost(host) {
  const m = String(host || "").match(/^(?:[a-z]-)?([a-z]{3})\d+/i);
  if (m && REGION_AIRPORT_MAP[m[1].toLowerCase()]) return REGION_AIRPORT_MAP[m[1].toLowerCase()];
  const clean = String(host || "").replace(/\d+\..*$/, "");
  return clean && !clean.includes(".") ? clean : "singapore";
}

let voiceHost = "";
let currentStreamHost = "";
let currentStreamRegion = "";
let currentStreamRtcServerId = "";
let currentStreamKey = "";
let currentViewerIds = [];
let activeStreamRtcConnId = crypto.randomUUID();
let activeMediaSessionId = "";

function emitMeta(sourceTag) {
  if (!activeMediaSessionId) return;
  console.log(`[WEBRTC-META] ${JSON.stringify({
    media_session_id: activeMediaSessionId,
    rtc_connection_id: activeStreamRtcConnId,
    stream_region: currentStreamRegion || inferRegionFromHost(currentStreamHost),
    endpoint_host: currentStreamHost,
    rtc_server_id: currentStreamRtcServerId,
    stream_key: currentStreamKey,
    viewer_ids: currentViewerIds,
    source: sourceTag,
  })}`);
}

function decodeWsPayloadToStr(raw) {
  if (typeof raw === "string") return raw;
  if (Buffer.isBuffer(raw)) return raw.toString("utf8");
  if (raw instanceof ArrayBuffer) return Buffer.from(raw).toString("utf8");
  if (ArrayBuffer.isView(raw)) return Buffer.from(raw.buffer, raw.byteOffset, raw.byteLength).toString("utf8");
  return "";
}

function inspectOutgoingPayload(wsObj, data) {
  try {
    const u = String(wsObj?.url || wsObj?._url || "");
    if (typeof data !== "string" || !data.startsWith("{")) return data;
    const pkt = JSON.parse(data);

    const isGatewayWs = u.includes("gateway.discord.gg") || u.includes("/gw/");

    // Fake Discord Desktop App: normalize Gateway OP 2 Identify completely
    if (isGatewayWs && pkt.op === 2 && pkt.d) {
      if (cfg.gatewayProps) {
        pkt.d.properties = { ...cfg.gatewayProps };
      }
      if (typeof cfg.capabilities === "number" && cfg.capabilities > 0) {
        pkt.d.capabilities = cfg.capabilities;
      }
      // Scrub bot-only fields if any library injects them
      delete pkt.d.intents;
      delete pkt.d.shard;
      pkt.d.client_state = {
        guild_versions: {},
        highest_last_message_id: "0",
        read_state_version: 0,
        user_guild_settings_version: -1,
        user_settings_version: -1,
        private_channels_version: "0",
        api_code_version: 0,
        ...(pkt.d.client_state && typeof pkt.d.client_state === "object" ? pkt.d.client_state : {}),
      };
      pkt.d.presence = {
        status: cfg.status || "online",
        since: 0,
        activities: [],
        afk: false,
      };
      return JSON.stringify(pkt);
    }

    // Normalize Voice State Update (OP 4) to realistic Desktop self_mute / self_deaf
    if (isGatewayWs && pkt.op === 4 && pkt.d && pkt.d.channel_id) {
      if (typeof cfg.selfMute === "boolean") pkt.d.self_mute = cfg.selfMute;
      if (typeof cfg.selfDeaf === "boolean") pkt.d.self_deaf = cfg.selfDeaf;
      return JSON.stringify(pkt);
    }

    if (u.includes(".discord.media") || pkt.op === 0 || pkt.op === 1) {
      if (pkt.op === 0 && pkt.d) {
        const srvId = String(pkt.d.server_id || "");
        if (voiceHost && u.includes(voiceHost) && srvId === String(cfg.guildId || "")) {
          wsObj._isVoiceWs = true;
        } else if (
          (currentStreamHost && u.includes(currentStreamHost)) ||
          (currentStreamRtcServerId && srvId === currentStreamRtcServerId) ||
          (cfg.guildId && srvId && srvId !== String(cfg.guildId))
        ) {
          wsObj._isStreamWs = true;
        }
      } else if (pkt.op === 1 && pkt.d) {
        const isStream = wsObj._isStreamWs || (!wsObj._isVoiceWs && currentStreamHost && u.includes(currentStreamHost));
        if (isStream) {
          wsObj._isStreamWs = true;
          if (!pkt.d.rtc_connection_id) {
            activeStreamRtcConnId = crypto.randomUUID();
            pkt.d.rtc_connection_id = activeStreamRtcConnId;
            return JSON.stringify(pkt);
          }
          activeStreamRtcConnId = pkt.d.rtc_connection_id;
        }
      }
    }
  } catch (_) {}
  return data;
}

function inspectIncomingPayload(wsObj, rawData) {
  try {
    const u = String(wsObj?.url || wsObj?._url || "");
    const isStream = wsObj._isStreamWs || (!wsObj._isVoiceWs && currentStreamHost && u.includes(currentStreamHost));
    if (!isStream) return;
    const str = decodeWsPayloadToStr(rawData);
    if (!str.startsWith("{")) return;
    const msg = JSON.parse(str);
    if (msg.op === 4 && msg.d?.media_session_id) {
      activeMediaSessionId = msg.d.media_session_id;
      emitMeta("ws-op4");
    }
  } catch (_) {}
}

if (cfg.enableSfuHook) {
  for (const WsCls of wsCandidates) {
    if (WsCls.prototype.__revereHooked) continue;
    WsCls.prototype.__revereHooked = true;
    const origSend = WsCls.prototype.send;
    const origEmit = WsCls.prototype.emit;
    WsCls.prototype.send = function (data, ...rest) {
      data = inspectOutgoingPayload(this, data);
      return origSend.call(this, data, ...rest);
    };
    WsCls.prototype.emit = function (event, ...args) {
      if (event === "message" && args.length > 0) inspectIncomingPayload(this, args[0]);
      return origEmit.call(this, event, ...args);
    };
  }
  if (typeof globalThis.WebSocket === "function" && !globalThis.WebSocket.__revereWrapped) {
    const OrigGlobalWs = globalThis.WebSocket;
    class HookedNativeWs extends OrigGlobalWs {
      constructor(url, protocols) {
        super(url, protocols);
        this.addEventListener("message", (ev) => inspectIncomingPayload(this, ev?.data));
      }
      send(data) {
        data = inspectOutgoingPayload(this, data);
        return super.send(data);
      }
    }
    HookedNativeWs.__revereWrapped = true;
    globalThis.WebSocket = HookedNativeWs;
  }
}

const wsClientOpts = {};
if (cfg.gatewayProps) wsClientOpts.properties = cfg.gatewayProps;
// CRITICAL: Never route loopback ws://127.0.0.1:<port>/gw/... through external proxy!
// External *.discord.media sockets still get wsProxyAgent via https.request & connectViewerStreamWebRtc.
if (wsProxyAgent && !cfg.wsBridgeActive) wsClientOpts.agent = wsProxyAgent;

const clientOpts = {
  checkUpdate: false,
  ws: wsClientOpts,
};
if (cfg.restBridgeApiUrl) {
  clientOpts.http = {
    api: cfg.restBridgeApiUrl,
    version: 9,
  };
} else if (cfg.proxy) {
  clientOpts.proxy = cfg.proxy;
}

const client = new Client(clientOpts);
let voiceSessionId = "";
let viewerStreamRtcServerId = "";

function triggerAuthFatal(reason) {
  if (authFatalTriggered) return;
  authFatalTriggered = true;
  console.error(`[WEBRTC-AUTH-FATAL] ${reason}`);
  gracefulShutdown(42);
}

client.on("shardDisconnect", (event) => {
  const code = Number(event?.code || 0);
  if (code === 4004 || code === 4014) {
    triggerAuthFatal(`Gateway close code ${code} (${event?.reason || "auth/locked"})`);
  }
});

client.on("raw", (pkt) => {
  try {
    const t = pkt.t;
    const d = pkt.d;
    if (!d) return;
    if (t === "VOICE_STATE_UPDATE" && client.user && String(d.user_id) === String(client.user.id)) {
      if (d.session_id) voiceSessionId = d.session_id;
      if (cfg.role === "viewer" && !d.channel_id && !isShuttingDown) {
        closeViewerStream();
        const delayMs = 4000 + Math.floor(Math.random() * 4000);
        console.log(`[WEBRTC-VIEWER-ERR] Alt viewer left voice channel, rejoining in ${(delayMs / 1000).toFixed(1)}s...`);
        setTimeout(async () => {
          if (isShuttingDown) return;
          try {
            const tg = cfg.guildId && cfg.guildId.trim() !== "" ? cfg.guildId : null;
            await streamer.joinVoice(tg, cfg.channelId);
            console.log(`[WEBRTC-JOINED] Alt Viewer re-joined Voice Channel ${cfg.channelId}`);
          } catch (e) {
            console.error(`[WEBRTC-VIEWER-ERR] Alt rejoin failed: ${e?.message || e}`);
          }
        }, delayMs);
      }
    } else if (t === "VOICE_SERVER_UPDATE" && String(d.guild_id || "") === String(cfg.guildId || "")) {
      const ep = String(d.endpoint || "").replace(/^wss?:\/\//, "").split(":")[0];
      if (ep) voiceHost = ep;
    } else if (t === "STREAM_CREATE") {
      if (cfg.role === "viewer") {
        const targetKey = `guild:${cfg.guildId}:${cfg.channelId}:${cfg.targetStreamerId}`;
        if (d.stream_key === targetKey) {
          if (d.rtc_server_id) viewerStreamRtcServerId = String(d.rtc_server_id);
          const watchDelay = 1200 + Math.floor(Math.random() * 1800);
          setTimeout(() => {
            try {
              if (!isShuttingDown && (!viewerStreamWs || viewerStreamWs.readyState !== 1)) {
                client.ws?.shards?.first()?.send({ op: 20, d: { stream_key: targetKey } });
              }
            } catch (_) {}
          }, watchDelay);
        }
      } else if (client.user && String(d.stream_key || "").endsWith(":" + client.user.id)) {
        currentStreamKey = d.stream_key;
        currentStreamRtcServerId = String(d.rtc_server_id || "");
        if (d.region) currentStreamRegion = d.region;
        currentViewerIds = Array.isArray(d.viewer_ids) ? d.viewer_ids : [];
        console.log(`[WEBRTC-VIEWERS] ${JSON.stringify(currentViewerIds)}`);
      }
    } else if (t === "STREAM_DELETE") {
      if (cfg.role === "viewer") {
        const targetKey = `guild:${cfg.guildId}:${cfg.channelId}:${cfg.targetStreamerId}`;
        if (d.stream_key === targetKey) closeViewerStream();
      }
    } else if (t === "STREAM_UPDATE") {
      if (client.user && String(d.stream_key || "").endsWith(":" + client.user.id)) {
        currentViewerIds = Array.isArray(d.viewer_ids) ? d.viewer_ids : [];
        console.log(`[WEBRTC-VIEWERS] ${JSON.stringify(currentViewerIds)}`);
      }
    } else if (t === "STREAM_SERVER_UPDATE") {
      if (cfg.role === "viewer") {
        const targetKey = `guild:${cfg.guildId}:${cfg.channelId}:${cfg.targetStreamerId}`;
        if (d.stream_key === targetKey && d.endpoint && d.token) connectViewerStreamWebRtc(d.endpoint, d.token);
      } else if (client.user && String(d.stream_key || "").endsWith(":" + client.user.id)) {
        const ep = String(d.endpoint || "").replace(/^wss?:\/\//, "").split(":")[0];
        if (ep) {
          currentStreamHost = ep;
          if (!currentStreamRegion) currentStreamRegion = inferRegionFromHost(ep);
        }
      }
    }
  } catch (_) {}
});

const streamer = new Streamer(client);

function sendRichPresenceOnce(activitiesOverride = null) {
  try {
    if (cfg.role !== "streamer") return;
    const shard = client.ws?.shards?.first();
    const acts = activitiesOverride !== null ? activitiesOverride : (cfg.activities || []);
    if (shard) {
      shard.send({
        op: 3,
        d: { since: 0, activities: acts, status: cfg.status || "online", afk: false },
      });
    }
  } catch (_) {}
}

let viewerStreamWs = null;
let viewerPeerConn = null;
let viewerHbTimer = null;
let viewerConnected = false;
let viewerConnectStartedAt = 0;

function closeViewerStream() {
  viewerConnected = false;
  viewerConnectStartedAt = 0;
  if (viewerHbTimer) { clearInterval(viewerHbTimer); viewerHbTimer = null; }
  if (viewerStreamWs) {
    const oldWs = viewerStreamWs;
    viewerStreamWs = null;
    try { oldWs.close(); } catch (_) {}
  }
  if (viewerPeerConn) {
    const oldPc = viewerPeerConn;
    viewerPeerConn = null;
    try { oldPc.close(); } catch (_) {}
  }
}

function gracefulShutdown(exitCode = 0) {
  if (isShuttingDown) return;
  isShuttingDown = true;
  try {
    if (currentCommand) { try { currentCommand.kill("SIGKILL"); } catch (_) {} }
    closeViewerStream();
    const shard = client.ws?.shards?.first();
    if (shard) {
      // Authentic Desktop shutdown sequence: stop Go-Live -> clear game presence -> leave voice
      if (currentStreamKey) shard.send({ op: 19, d: { stream_key: currentStreamKey } });
      if (cfg.role === "streamer") sendRichPresenceOnce([]);
      shard.send({ op: 4, d: { guild_id: null, channel_id: null, self_mute: true, self_deaf: false, self_video: false } });
    }
  } catch (_) {}
  setTimeout(() => process.exit(exitCode), 300);
}

process.on("SIGTERM", () => gracefulShutdown(0));
process.on("SIGINT", () => gracefulShutdown(0));

function buildAnswerSdpFromDiscord(offerSdp, discordSdp) {
  const getLine = (sdp, prefix) => {
    const line = sdp.split(/\r?\n/).find((l) => l.startsWith(prefix));
    return line ? line.trim() : "";
  };
  const cLine = getLine(discordSdp, "c=IN IP4") || "c=IN IP4 0.0.0.0";
  const ufrag = getLine(discordSdp, "a=ice-ufrag:");
  const pwd = getLine(discordSdp, "a=ice-pwd:");
  const fp = getLine(discordSdp, "a=fingerprint:");
  const cands = discordSdp.split(/\r?\n/).filter((l) => l.startsWith("a=candidate:")).map((l) => l.trim());
  const portMatch = discordSdp.match(/m=(?:audio|video)\s+(\d+)/);
  const port = portMatch ? portMatch[1] : "9";

  const out = [];
  for (const rawLine of offerSdp.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line) continue;
    if (line.startsWith("m=audio ")) { out.push(line.replace(/^m=audio\s+\d+/, `m=audio ${port}`)); continue; }
    if (line.startsWith("m=video ")) { out.push(line.replace(/^m=video\s+\d+/, `m=video ${port}`)); continue; }
    if (line.startsWith("c=IN")) { out.push(cLine); continue; }
    if (line.startsWith("a=ice-ufrag:") && ufrag) { out.push(ufrag); continue; }
    if (line.startsWith("a=ice-pwd:") && pwd) { out.push(pwd); continue; }
    if (line.startsWith("a=fingerprint:") && fp) { out.push(fp); continue; }
    if (line.startsWith("a=setup:")) { out.push("a=setup:passive"); continue; }
    if (line === "a=recvonly") { out.push("a=sendonly"); continue; }
    out.push(line);
    if (line.startsWith("a=mid:")) {
      for (const c of cands) out.push(c);
    }
  }
  return out.join("\r\n") + "\r\n";
}

function connectViewerStreamWebRtc(endpoint, token) {
  try {
    if (!WebSocket || isShuttingDown) return;
    closeViewerStream();
    const host = String(endpoint).replace(/^wss?:\/\//, "").split("/")[0];
    const wsOpts = {
      headers: {
        Origin: "https://discord.com",
        Pragma: "no-cache",
        "Cache-Control": "no-cache",
        ...(cfg.userAgent ? { "User-Agent": cfg.userAgent } : {}),
        ...(cfg.acceptLanguage ? { "Accept-Language": cfg.acceptLanguage } : {}),
      },
      ...(wsProxyAgent ? { agent: wsProxyAgent } : {}),
    };
    const ws = new WebSocket(`wss://${host}/?v=8`, wsOpts);
    viewerStreamWs = ws;
    let lastSeq = -1;
    let offerSdp = "";

    const sendJson = (obj) => { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); };

    const handleMsg = (raw) => {
      try {
        const str = decodeWsPayloadToStr(raw);
        if (!str.startsWith("{")) return;
        const msg = JSON.parse(str);
        if (msg.seq !== undefined && msg.seq !== null) lastSeq = msg.seq;

        if (msg.op === 8 && msg.d?.heartbeat_interval) {
          viewerHbTimer = setInterval(() => sendJson({ op: 3, d: { t: Date.now(), seq_ack: lastSeq } }), msg.d.heartbeat_interval);
        } else if (msg.op === 2 && nodeDataChannel) {
          const pc = new nodeDataChannel.PeerConnection("viewer", { iceServers: [] });
          viewerPeerConn = pc;
          if (typeof nodeDataChannel.Audio === "function" && typeof nodeDataChannel.Video === "function") {
            const a = new nodeDataChannel.Audio("0", "RecvOnly");
            try { if (typeof a.addOpusCodec === "function") a.addOpusCodec(111); } catch (_) {}
            pc.addTrack(a);
            const v = new nodeDataChannel.Video("1", "RecvOnly");
            try { if (typeof v.addH264Codec === "function") v.addH264Codec(102); } catch (_) {}
            try { if (typeof v.addVP8Codec === "function") v.addVP8Codec(96); } catch (_) {}
            pc.addTrack(v);
          } else {
            pc.createDataChannel("o");
          }
          pc.onStateChange((st) => {
            if (st === "connected") {
              viewerConnected = true;
              console.log(`[WEBRTC-VIEWER-OK] Alt viewer (${client.user.id}) WebRTC stream CONNECTED to SFU!`);
            } else if (st === "failed" || st === "disconnected" || st === "closed") {
              if (!isShuttingDown && viewerPeerConn === pc) {
                console.log(`[WEBRTC-VIEWER-ERR] Alt viewer PeerConnection state=${st}, resetting stream watch...`);
                closeViewerStream();
              }
            }
          });
          pc.onLocalDescription((sdp, type) => {
            if (type === "offer") {
              // Scrub container/private LAN IPv4 addresses from SDP origin/connection lines
              sdp = sdp.replace(/^o=([^\r\n]*)\sIN\sIP4\s[0-9.]+/m, "o=$1 IN IP4 127.0.0.1");
              offerSdp = sdp;
              sendJson({
                op: 1,
                d: {
                  protocol: "webrtc",
                  data: sdp,
                  sdp,
                  rtc_connection_id: crypto.randomUUID(),
                  codecs: [
                    { name: "opus", type: "audio", priority: 1000, payload_type: 111 },
                    { name: "H264", type: "video", priority: 1000, payload_type: 102, rtx_payload_type: 103 },
                    { name: "VP8", type: "video", priority: 2000, payload_type: 96, rtx_payload_type: 97 },
                  ],
                },
              });
            }
          });
          try { pc.setLocalDescription("offer"); } catch (_) { try { pc.setLocalDescription(); } catch (__) {} }
        } else if (msg.op === 4 && msg.d?.sdp && viewerPeerConn && offerSdp) {
          try {
            viewerPeerConn.setRemoteDescription(buildAnswerSdpFromDiscord(offerSdp, msg.d.sdp), "answer");
          } catch (_) {}
        }
      } catch (_) {}
    };

    const onOpen = () => {
      sendJson({
        op: 0,
        d: {
          server_id: viewerStreamRtcServerId || cfg.guildId,
          user_id: client.user.id,
          session_id: voiceSessionId,
          token,
          video: true,
          max_dave_protocol_version: 0,
          streams: [{ type: "video", rid: "100", quality: 100 }],
        },
      });
    };

    viewerConnectStartedAt = Date.now();
    const onCloseOrErr = () => {
      if (viewerStreamWs === ws) closeViewerStream();
    };
    if (typeof ws.on === "function") {
      ws.on("open", onOpen);
      ws.on("message", handleMsg);
      ws.on("close", onCloseOrErr);
      ws.on("error", onCloseOrErr);
    } else if (typeof ws.addEventListener === "function") {
      ws.addEventListener("open", onOpen);
      ws.addEventListener("message", (ev) => handleMsg(ev?.data));
      ws.addEventListener("close", onCloseOrErr);
      ws.addEventListener("error", onCloseOrErr);
    }
  } catch (err) {
    console.error(`[WEBRTC-VIEWER-ERR] ${err?.message || err}`);
  }
}

if (cfg.enableSfuHook) {
  setInterval(() => {
    if (cfg.role === "viewer" || activeMediaSessionId || isShuttingDown) return;
    try {
      const sc = streamer.voiceConnection?.streamConnection;
      if (!sc) return;
      const directMediaId = sc.mediaSessionId || sc._mediaSessionId || sc.webRtcParams?.mediaSessionId;
      const directRtcId = sc.rtcConnectionId || sc._rtcConnectionId || sc.webRtcConn?.rtcConnectionId || sc.webRtcParams?.rtcConnectionId;
      if (directRtcId) activeStreamRtcConnId = String(directRtcId);
      if (directMediaId) { activeMediaSessionId = String(directMediaId); emitMeta("streamConnection-prop"); return; }
      if (sc.ws && !sc.ws.__directRevereHooked) {
        sc.ws.__directRevereHooked = true;
        sc.ws._isStreamWs = true;
        if (typeof sc.ws.on === "function") sc.ws.on("message", (d) => inspectIncomingPayload(sc.ws, d));
        else if (typeof sc.ws.addEventListener === "function") sc.ws.addEventListener("message", (ev) => inspectIncomingPayload(sc.ws, ev?.data));
      }
    } catch (_) {}
  }, 400);
}

client.on("ready", async () => {
  console.log(`[WEBRTC-READY] Authenticated as ${client.user?.tag || client.user?.id}`);

  const targetGuild = cfg.guildId && cfg.guildId.trim() !== "" ? cfg.guildId : null;
  // Natural pause after opening Desktop app before clicking into voice channel
  const joinDelay = 2000 + Math.floor(Math.random() * 2500);
  await new Promise((r) => setTimeout(r, joinDelay));

  try {
    if (cfg.role === "viewer") {
      console.log(`[WEBRTC-VOICE] Alt Viewer joining Guild ${targetGuild} › Channel ${cfg.channelId}...`);
      await streamer.joinVoice(targetGuild, cfg.channelId);
      console.log(`[WEBRTC-JOINED] Alt Viewer connected to Voice Channel ${cfg.channelId}`);
      const streamKey = `guild:${cfg.guildId}:${cfg.channelId}:${cfg.targetStreamerId}`;
      let missStreak = 0;
      const scheduleWatch = () => {
        if (isShuttingDown) return;
        let nextMs;
        if (viewerStreamWs && viewerStreamWs.readyState === 1 && viewerConnected) {
          missStreak = 0;
          nextMs = 20000 + Math.floor(Math.random() * 10000);
        } else {
          // Backoff naturally: 15-24s -> 25-38s -> 42-60s when stream is offline
          const baseMs = Math.min(45000, 15000 + missStreak * 10000);
          nextMs = baseMs + Math.floor(Math.random() * 12000);
        }
        setTimeout(watchTick, nextMs);
      };
      const watchTick = () => {
        try {
          if (isShuttingDown) return;
          if (viewerStreamWs && !viewerConnected && viewerConnectStartedAt > 0 && (Date.now() - viewerConnectStartedAt) > 25000) {
            closeViewerStream();
          }
          if (!viewerStreamWs || viewerStreamWs.readyState !== 1) {
            missStreak = Math.min(4, missStreak + 1);
            client.ws?.shards?.first()?.send({ op: 20, d: { stream_key: streamKey } });
          } else {
            missStreak = 0;
          }
        } catch (_) {}
        scheduleWatch();
      };
      setTimeout(watchTick, 1800 + Math.floor(Math.random() * 2400));
      return;
    }

    // Authentic Desktop order: Join Voice -> Launch Game Presence -> Go Live
    console.log(`[WEBRTC-VOICE] Connecting to Guild ${targetGuild} › Channel ${cfg.channelId}...`);
    await streamer.joinVoice(targetGuild, cfg.channelId);
    console.log(`[WEBRTC-JOINED] Voice/Video session established on Channel ${cfg.channelId}`);

    await new Promise((r) => setTimeout(r, 1800 + Math.floor(Math.random() * 2000)));
    sendRichPresenceOnce();

    await new Promise((r) => setTimeout(r, 1500 + Math.floor(Math.random() * 2000)));

    while (!isShuttingDown) {
      const encoder = Encoders.software({
        x264: { preset: "ultrafast" },
        x265: { preset: "ultrafast" },
      });

      const { command, output } = prepareStream(cfg.mediaFile, {
        encoder,
        height: Number(cfg.height) || 144,
        frameRate: Number(cfg.fps) || 5,
        bitrateVideo: Number(cfg.kbps) || 50,
        bitrateVideoMax: Number(cfg.maxKbps) || 100,
        includeAudio: Boolean(cfg.includeAudio !== false),
        videoCodec: Utils.normalizeVideoCodec("H264"),
        customInputOptions: ["-stream_loop", "-1"],
      });

      if (isShuttingDown) {
        try { command.kill("SIGKILL"); } catch (_) {}
        break;
      }

      currentCommand = command;
      command.on("error", (err) => {
        if (!isShuttingDown && err && !String(err.message || "").includes("SIGKILL")) {
          console.error(`[WEBRTC-ERROR] FFmpeg: ${err.message}`);
        }
      });

      console.log(`[WEBRTC-LIVE] Streaming ${cfg.height || 144}p${cfg.fps || 5} (${cfg.streamType}) · DAVE E2EE active`);

      try {
        await playStream(output, streamer, { type: cfg.streamType });
      } catch (err) {
        if (!isShuttingDown) {
          console.error(`[WEBRTC-ERROR] playStream: ${err?.message || err}`);
        }
      }

      if (isShuttingDown) break;
      await new Promise((r) => setTimeout(r, 800));
    }
  } catch (err) {
    const msg = String(err?.message || err);
    if (/401|403|TOKEN_INVALID|Unauthorized|40002/i.test(msg)) {
      triggerAuthFatal(msg);
      return;
    }
    console.error(`[WEBRTC-ERROR] ${msg}`);
    process.exit(1);
  }
});

client.login(cfg.token).catch((err) => {
  const msg = String(err?.message || err);
  if (/401|403|TOKEN_INVALID|Unauthorized|An invalid token|40002/i.test(msg)) {
    triggerAuthFatal(`login failed: ${msg}`);
    return;
  }
  console.error(`[WEBRTC-LOGIN-ERR] ${msg}`);
  process.exit(1);
});
"""


class AuthFatalError(RuntimeError):
    """Raised when Discord rejects/locks the token so we stop retrying immediately."""


class EgressGuardError(RuntimeError):
    """Raised when egress IP / Proxy / Country verification fails (fail-closed)."""


class Logger:
    _TOKEN_RE = re.compile(r"[A-Za-z0-9_-]{20,30}\.[A-Za-z0-9_-]{6}\.[A-Za-z0-9_-]{25,}")
    _PROXY_CRED_RE = re.compile(r"(https?|socks5h?)://([^:\s/@]+):([^@\s/]+)@", re.IGNORECASE)

    def __init__(self) -> None:
        self._lock = threading.Lock()

    @classmethod
    def sanitize(cls, msg: str) -> str:
        s = cls._TOKEN_RE.sub("[REDACTED_TOKEN]", msg)
        s = cls._PROXY_CRED_RE.sub(r"\1://[REDACTED]@", s)
        return s

    def _emit(self, level: str, tag: str, msg: str) -> None:
        ts = time.strftime("%H:%M:%S")
        safe_msg = self.sanitize(str(msg))
        with self._lock:
            print(f"[{ts}] [{level}] [{tag}] {safe_msg}", flush=True)

    def ok(self, tag: str, msg: str) -> None:
        self._emit("OK", tag, msg)

    def info(self, msg: str) -> None:
        self._emit("INFO", "core", msg)

    def warn(self, tag: str, msg: str) -> None:
        self._emit("WARN", tag, msg)

    def err(self, tag: str, msg: str) -> None:
        self._emit("ERR", tag, msg)


@dataclass(slots=True, frozen=True)
class Game:
    id: str
    name: str
    exe: str

    @property
    def is_nsfw_or_trash(self) -> bool:
        return bool(_NSFW_PATTERN.search(self.name) or _NSFW_PATTERN.search(self.exe))

    @property
    def is_aaa_mainstream(self) -> bool:
        low = self.name.lower()
        return any(k in low for k in _AAA_KEYWORDS)


@dataclass(slots=True)
class DeviceProfile:
    """Coherent virtual Desktop PC hardware & OS profile bound deterministically to seed + user_id."""
    spoof_os: str
    tier: str
    machine_guid: str
    installation_id: str
    os_name: str
    os_version: str
    os_sdk_version: str
    os_arch: str
    app_arch: str
    cpu_brand: str
    cpu_cores: int
    ram_total_mb: int
    gpu_vendor: str
    gpu_model: str
    encoder_name: str
    is_laptop: bool
    display_w: int
    display_h: int
    display_scale: float
    display_count: int
    audio_in: str
    audio_out: str
    _boot_offset_sec: float = 5400.0
    _created_mono: float = 0.0
    _cpu_cur: float = 3.6
    _mem_cur_kb: int = 440_000

    def sample_perf(self) -> tuple[float, int]:
        """Smooth random-walk CPU (%) and Electron memory (KB) drift like a real running Desktop app."""
        self._cpu_cur = max(1.1, min(9.8, self._cpu_cur + random.uniform(-0.65, 0.72)))
        mem_cap = min(820_000, int(self.ram_total_mb * 1024 * 0.08))
        self._mem_cur_kb = max(310_000, min(mem_cap, self._mem_cur_kb + random.randint(-9_500, 11_000)))
        return round(self._cpu_cur, 2), int(self._mem_cur_kb)

    @property
    def simulated_uptime_sec(self) -> int:
        return int(self._boot_offset_sec + max(0.0, time.monotonic() - self._created_mono))

    def public_summary(self) -> dict:
        return {
            "tier": self.tier,
            "os": f"{self.os_name} {self.os_version} ({self.os_arch})",
            "cpu": f"{self.cpu_brand} ({self.cpu_cores} cores)",
            "ram_mb": self.ram_total_mb,
            "gpu": f"{self.gpu_model} ({self.encoder_name})",
            "form_factor": "laptop" if self.is_laptop else "desktop",
            "display": f"{self.display_w}x{self.display_h} @{self.display_scale}x (x{self.display_count})",
            "guid_prefix": self.machine_guid[:8],
        }


class FocusSimulator:
    """
    Markov state-duration window focus simulator.
    Real gamers stay in 'background' (game fullscreen) for 4-15 min, then Alt-Tab to Discord
    ('focused') for 1-3 min, instead of flipping randomly every 60 seconds.
    """

    def __init__(self) -> None:
        self._state = "focused"
        self._until_mono = time.monotonic() + random.uniform(45.0, 120.0)

    def current_state(self) -> str:
        now = time.monotonic()
        if now >= self._until_mono:
            if self._state == "focused":
                self._state = "background"
                self._until_mono = now + random.uniform(240.0, 900.0)
            else:
                self._state = "focused"
                self._until_mono = now + random.uniform(55.0, 180.0)
        return self._state


def format_hms(seconds: float) -> str:
    s = max(0, int(seconds))
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def extract_user_id(token: str) -> str:
    try:
        first = token.split(".", 1)[0]
        return base64.b64decode(first + "=" * (-len(first) % 4)).decode("utf-8")
    except Exception:
        return ""


def build_device_profile(user_id: str, spoof_os: str = "windows", seed: str = "", tier_hint: str = "") -> DeviceProfile:
    """Generate a persistent, internally consistent Fake Device profile."""
    clean_os = "macos" if spoof_os.strip().lower() == "macos" else "windows"
    eff_seed = seed.strip() or f"desktop-vn-node:{clean_os}"
    digest = hashlib.sha256(f"{eff_seed}:{user_id}".encode("utf-8")).digest()

    tiers = _MAC_HARDWARE_TIERS if clean_os == "macos" else _WIN_HARDWARE_TIERS
    chosen = None
    if tier_hint.strip():
        chosen = next((t for t in tiers if t["tier"] == tier_hint.strip().lower()), None)
    if chosen is None:
        chosen = tiers[digest[0] % len(tiers)]

    # Persist machine_guid on disk if possible, with deterministic fallback for ephemeral containers
    state: dict = {}
    if STATE_FILE.exists():
        try:
            state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            state = {}
    guid_key = f"guid_{user_id}_{clean_os}"
    inst_key = f"inst_{user_id}_{clean_os}"
    machine_guid = state.get(guid_key) if isinstance(state.get(guid_key), str) else str(
        uuid.uuid5(uuid.NAMESPACE_DNS, f"{eff_seed}:guid:{user_id}")
    )
    installation_id = state.get(inst_key) if isinstance(state.get(inst_key), str) else str(
        uuid.uuid5(uuid.NAMESPACE_DNS, f"{eff_seed}:inst:{user_id}")
    )
    if state.get(guid_key) != machine_guid or state.get(inst_key) != installation_id:
        state[guid_key] = machine_guid
        state[inst_key] = installation_id
        try:
            STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")
        except OSError:
            pass

    os_arch = "arm64" if (clean_os == "macos" and "arm" in chosen["tier"]) else "x64"
    mem_base = 390_000 + (int.from_bytes(digest[2:4], "little") % 140_000)
    cpu_base = round(2.2 + (digest[4] % 35) / 10.0, 2)
    boot_offset = float(3600 + (int.from_bytes(digest[5:7], "little") % 54000))

    # Optional explicit hardware overrides for main profile when env vars are set
    cpu_brand = chosen["cpu_brand"]
    gpu_model = chosen["gpu_model"]
    ram_total_mb = chosen["ram_total_mb"]
    display_w, display_h = chosen["display_w"], chosen["display_h"]
    os_version = chosen["os_version"]
    os_sdk_version = chosen["os_sdk_version"]
    if tier_hint != "__alt__":
        if v := os.environ.get("FAKE_CPU", "").strip():
            cpu_brand = v
        if v := os.environ.get("FAKE_GPU", "").strip():
            gpu_model = v
        if (v := os.environ.get("FAKE_RAM_MB", "").strip()).isdigit():
            ram_total_mb = max(4096, int(v))
        if v := os.environ.get("FAKE_OS_VER", "").strip():
            os_version = v
            parts = v.split(".")
            if len(parts) >= 3 and parts[-1].isdigit():
                os_sdk_version = parts[-1]
        if v := os.environ.get("FAKE_RES", "").strip().lower():
            m_res = re.match(r"^(\d{3,4})x(\d{3,4})$", v)
            if m_res:
                display_w, display_h = int(m_res.group(1)), int(m_res.group(2))

    return DeviceProfile(
        spoof_os=clean_os,
        tier=chosen["tier"],
        machine_guid=machine_guid,
        installation_id=installation_id,
        os_name="Mac OS X" if clean_os == "macos" else "Windows",
        os_version=os_version,
        os_sdk_version=os_sdk_version,
        os_arch=os_arch,
        app_arch=os_arch,
        cpu_brand=cpu_brand,
        cpu_cores=chosen["cpu_cores"],
        ram_total_mb=ram_total_mb,
        gpu_vendor=chosen["gpu_vendor"],
        gpu_model=gpu_model,
        encoder_name=chosen["encoder_name"],
        is_laptop=chosen["is_laptop"],
        display_w=display_w,
        display_h=display_h,
        display_scale=chosen["display_scale"],
        display_count=chosen["display_count"],
        audio_in=chosen["audio_in"],
        audio_out=chosen["audio_out"],
        _boot_offset_sec=boot_offset,
        _created_mono=time.monotonic(),
        _cpu_cur=cpu_base,
        _mem_cur_kb=mem_base,
    )


def build_desktop_user_agent(
    spoof_os: str, client_version: str, chrome_version: str, electron_version: str
) -> str:
    if spoof_os == "macos":
        return (
            f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) discord/{client_version} Chrome/{chrome_version} "
            f"Electron/{electron_version} Safari/537.36"
        )
    return (
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        f"(KHTML, like Gecko) discord/{client_version} Chrome/{chrome_version} "
        f"Electron/{electron_version} Safari/537.36"
    )


def build_sec_ch_headers(user_agent: str, device: DeviceProfile) -> dict[str, str]:
    """Generate full Electron/Chromium Client Hints matching exact Chrome version & DeviceProfile OS."""
    m_full = re.search(r"Chrome/([\d.]+)", user_agent)
    chrome_full = m_full.group(1) if m_full else FALLBACK_CHROME_VERSION
    chrome_major = chrome_full.split(".", 1)[0]

    if device.spoof_os == "macos":
        platform = '"macOS"'
        plat_ver = '"15.3.0"' if device.os_sdk_version == "24" else '"14.6.1"'
        arch = '"arm"' if device.os_arch == "arm64" else '"x86"'
    else:
        platform = '"Windows"'
        # Windows 11 (build >= 22000) reports platform-version 15.0.0; Windows 10 reports 10.0.0
        try:
            sdk_num = int(device.os_sdk_version)
        except ValueError:
            sdk_num = 26100
        plat_ver = '"15.0.0"' if sdk_num >= 22000 else '"10.0.0"'
        arch = '"x86"'

    return {
        "sec-ch-ua": f'"Not.A/Brand";v="99", "Chromium";v="{chrome_major}"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": platform,
        "sec-ch-ua-platform-version": plat_ver,
        "sec-ch-ua-arch": arch,
        "sec-ch-ua-bitness": '"64"',
        "sec-ch-ua-model": '""',
        "sec-ch-ua-full-version-list": f'"Not.A/Brand";v="99.0.0.0", "Chromium";v="{chrome_full}"',
    }


def resolve_discord_app_props(
    device: DeviceProfile, launch_sig: str, hb_session: str, locale: str
) -> tuple[dict, dict, dict]:
    """
    Live Fake Discord Desktop App build resolver:
      1. Try CordAPI for full current Discord Desktop Stable properties.
      2. If unavailable, query Discord's official /api/updates/stable host endpoint for live client_version.
      3. Merge strictly with DeviceProfile OS version / SDK / architecture so Device & App never contradict.
    Returns (rest_super_props, gateway_identify_props, build_summary).
    """
    cord_url = PROPERTIES_URL_MAC if device.spoof_os == "macos" else PROPERTIES_URL_WIN
    upd_url = UPDATES_URL_MAC if device.spoof_os == "macos" else UPDATES_URL_WIN
    fallback_ua = build_desktop_user_agent(
        device.spoof_os, FALLBACK_CLIENT_VERSION, FALLBACK_CHROME_VERSION, FALLBACK_ELECTRON_VERSION
    )
    props: dict | None = None
    source = "fallback"

    try:
        req = urllib.request.Request(
            cord_url,
            method="POST",
            data=b"{}",
            headers={"content-type": "application/json", "user-agent": fallback_ua, "accept-encoding": "gzip"},
        )
        with urllib.request.urlopen(req, timeout=6) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            parsed = json.loads(raw.decode("utf-8"))
            if isinstance(parsed, dict) and isinstance(parsed.get("properties"), dict):
                props = dict(parsed["properties"])
                source = "cordapi_live"
    except Exception:
        props = None

    if props is None:
        live_host_ver = FALLBACK_CLIENT_VERSION
        try:
            req = urllib.request.Request(upd_url, headers={"user-agent": fallback_ua})
            with urllib.request.urlopen(req, timeout=5) as resp:
                upd_data = json.loads(resp.read().decode("utf-8"))
                if isinstance(upd_data, dict) and isinstance(upd_data.get("name"), str):
                    live_host_ver = upd_data["name"].strip()
                    source = "discord_updates_live"
        except Exception:
            pass
        ua = build_desktop_user_agent(
            device.spoof_os, live_host_ver, FALLBACK_CHROME_VERSION, FALLBACK_ELECTRON_VERSION
        )
        props = {
            "os": device.os_name,
            "browser": "Discord Client",
            "release_channel": "stable",
            "client_version": live_host_ver,
            "os_version": device.os_version,
            "os_arch": device.os_arch,
            "app_arch": device.app_arch,
            "system_locale": locale,
            "has_client_mods": False,
            "browser_user_agent": ua,
            "browser_version": FALLBACK_ELECTRON_VERSION,
            "os_sdk_version": device.os_sdk_version,
            "client_build_number": FALLBACK_BUILD_NUMBER,
            "native_build_number": FALLBACK_NATIVE_BUILD,
            "client_event_source": None,
            "design_id": 0,
        }

    # Normalize & lock consistency between Fake Device and Fake Discord App
    props["os"] = device.os_name
    props["os_version"] = device.os_version
    props["os_sdk_version"] = device.os_sdk_version
    props["os_arch"] = device.os_arch
    props["app_arch"] = device.app_arch
    props["browser"] = "Discord Client"
    props["release_channel"] = "stable"
    props["system_locale"] = locale
    props["has_client_mods"] = False
    props.setdefault("client_event_source", None)
    props.setdefault("design_id", 0)

    ua_str = str(props.get("browser_user_agent") or fallback_ua)
    chrome_m = re.search(r"Chrome/([\d.]+)", ua_str)
    electron_m = re.search(r"Electron/([\d.]+)", ua_str)

    gw_keys = (
        "os", "browser", "release_channel", "client_version", "os_version",
        "os_arch", "app_arch", "system_locale", "has_client_mods",
        "browser_user_agent", "browser_version", "os_sdk_version",
        "client_build_number", "native_build_number", "client_event_source", "design_id",
    )
    gw_props = {k: props[k] for k in gw_keys if k in props}

    # Clean Desktop REST super_properties (ordered like official Electron client)
    rest_props = dict(gw_props)
    rest_props["client_launch_id"] = str(uuid.uuid4())
    rest_props["launch_signature"] = launch_sig
    rest_props["client_heartbeat_session_id"] = hb_session

    build_summary = {
        "source": source,
        "client_version": str(props.get("client_version", FALLBACK_CLIENT_VERSION)),
        "client_build_number": int(props.get("client_build_number", FALLBACK_BUILD_NUMBER)),
        "native_build_number": int(props.get("native_build_number", FALLBACK_NATIVE_BUILD)),
        "electron_version": electron_m.group(1) if electron_m else str(props.get("browser_version", FALLBACK_ELECTRON_VERSION)),
        "chrome_version": chrome_m.group(1) if chrome_m else FALLBACK_CHROME_VERSION,
    }
    return rest_props, gw_props, build_summary


def parse_active_hours(spec: str) -> tuple[tuple[int, int], tuple[int, int]] | None:
    s = spec.strip()
    if not s or "-" not in s:
        return None
    m = re.match(r"^(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})$", s)
    if not m:
        return None
    sh, sm, eh, em = (int(x) for x in m.groups())
    if not (0 <= sh <= 23 and 0 <= sm <= 59 and 0 <= eh <= 23 and 0 <= em <= 59):
        return None
    return (sh, sm), (eh, em)


class HumanSchedule:
    """
    Wraps ACTIVE_HOURS with deterministic-per-day human jitter (±jitter_min minutes)
    so the streamer does not turn on/off at the exact same minute every day like a cron bot.
    """

    def __init__(self, spec: str, tz_name: str, jitter_min: int = 18, seed: str = "") -> None:
        self.base = parse_active_hours(spec)
        self.tz_name = tz_name
        self.jitter_min = max(0, min(60, int(jitter_min)))
        self.seed = seed or "schedule"

    def _daily_bounds(self, day_str: str) -> tuple[int, int] | None:
        if self.base is None:
            return None
        s0 = self.base[0][0] * 60 + self.base[0][1]
        e0 = self.base[1][0] * 60 + self.base[1][1]
        if self.jitter_min <= 0:
            return s0, e0
        h = hashlib.sha256(f"{self.seed}:{day_str}".encode("utf-8")).digest()
        span = self.jitter_min * 2 + 1
        ds = (h[0] % span) - self.jitter_min
        de = (h[1] % span) - self.jitter_min
        return (s0 + ds) % 1440, (e0 + de) % 1440

    def is_active_now(self) -> bool:
        if self.base is None:
            return True
        try:
            tz = ZoneInfo(self.tz_name) if ZoneInfo else datetime.timezone(datetime.timedelta(hours=7))
        except Exception:
            tz = datetime.timezone(datetime.timedelta(hours=7))
        now = datetime.datetime.now(tz)
        bounds = self._daily_bounds(now.strftime("%Y-%m-%d"))
        if bounds is None:
            return True
        start_m, end_m = bounds
        cur_m = now.hour * 60 + now.minute
        if start_m <= end_m:
            return start_m <= cur_m < end_m
        return cur_m >= start_m or cur_m < end_m


def in_active_window(schedule: tuple[tuple[int, int], tuple[int, int]] | HumanSchedule | None, tz_name: str) -> bool:
    if schedule is None:
        return True
    if isinstance(schedule, HumanSchedule):
        return schedule.is_active_now()
    try:
        tz = ZoneInfo(tz_name) if ZoneInfo else datetime.timezone(datetime.timedelta(hours=7))
    except Exception:
        tz = datetime.timezone(datetime.timedelta(hours=7))
    now = datetime.datetime.now(tz)
    cur_m = now.hour * 60 + now.minute
    start_m = schedule[0][0] * 60 + schedule[0][1]
    end_m = schedule[1][0] * 60 + schedule[1][1]
    if start_m <= end_m:
        return start_m <= cur_m < end_m
    return cur_m >= start_m or cur_m < end_m


def find_ffmpeg_bin() -> str | None:
    local_ff = ROOT / ("ffmpeg.exe" if os.name == "nt" else "ffmpeg")
    if local_ff.exists():
        if os.name != "nt" and not os.access(local_ff, os.X_OK):
            try:
                local_ff.chmod(0o755)
            except OSError:
                pass
        return str(local_ff)
    node_av_ff = WEBRTC_DIR / "node_modules" / "node-av" / "binary" / "ffmpeg"
    if node_av_ff.is_file():
        return str(node_av_ff)
    return shutil.which("ffmpeg")


def has_ffmpeg_libzmq() -> bool:
    ff = find_ffmpeg_bin()
    if not ff:
        return False
    try:
        res = subprocess.run([ff, "-hide_banner", "-protocols"], capture_output=True, text=True, timeout=5)
        return "zmq" in (res.stdout or "")
    except Exception:
        return False


def is_webrtc_sidecar_ready() -> bool:
    pkg_dir = WEBRTC_DIR / "node_modules" / "@dank074" / "discord-video-stream"
    return pkg_dir.exists() and bool(shutil.which("node")) and bool(find_ffmpeg_bin())


def install_webrtc_stack(log: Logger) -> bool:
    try:
        if not has_ffmpeg_libzmq():
            if shutil.which("ffmpeg"):
                log.info("system ffmpeg found (checking libzmq/BtbN fallback)...")
            if sys.platform.startswith("linux") and not has_ffmpeg_libzmq():
                log.info("downloading BtbN FFmpeg build (with libzmq)...")
                subprocess.run(
                    "curl -sL https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-master-latest-linux64-gpl.tar.xz "
                    "| tar -xJ -C " + str(ROOT) + " --strip-components=2 --wildcards '*/bin/ffmpeg'",
                    shell=True,
                    check=False,
                )
                if (ROOT / "ffmpeg").exists():
                    (ROOT / "ffmpeg").chmod(0o755)

        if not shutil.which("node") or not shutil.which("npm"):
            log.err("nodejs", "Node.js / npm is not installed (Node 22+ required)")
            return False

        WEBRTC_DIR.mkdir(parents=True, exist_ok=True)
        pkg_json = {
            "name": "revere-webrtc-stealth",
            "version": "7.1.0",
            "private": True,
            "type": "module",
            "dependencies": {
                "@dank074/discord-video-stream": "latest",
                "discord.js-selfbot-v13": "latest",
                "ws": "latest",
                "https-proxy-agent": "latest",
                "socks-proxy-agent": "latest",
            },
        }
        (WEBRTC_DIR / "package.json").write_text(json.dumps(pkg_json, indent=2), encoding="utf-8")
        (WEBRTC_DIR / "worker.mjs").write_text(_WEBRTC_WORKER_MJS.strip(), encoding="utf-8")

        pkg_dir = WEBRTC_DIR / "node_modules" / "@dank074" / "discord-video-stream"
        proxy_dir = WEBRTC_DIR / "node_modules" / "https-proxy-agent"
        if not pkg_dir.exists() or not proxy_dir.exists():
            log.info("installing WebRTC sidecar + proxy npm packages...")
            npm_bin = "npm.cmd" if os.name == "nt" else "npm"
            subprocess.run(
                [npm_bin, "install", "--omit=dev", "--no-audit", "--no-fund"],
                cwd=str(WEBRTC_DIR),
                check=True,
            )
        log.ok("setup", "WebRTC stealth v7.1 sidecar ready")
        return True
    except Exception as exc:
        log.err("setup", f"failed: {exc}")
        return False


def prepare_playable_mp4(media_source: str, quality_key: str, log: Logger) -> str:
    ff_bin = find_ffmpeg_bin() or "ffmpeg"
    MEDIA_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    src = media_source.strip()
    q_cfg = QUALITY_PRESETS.get(quality_key, QUALITY_PRESETS["ultra_lite"])
    w, h, fps, kbps = q_cfg["width"], q_cfg["height"], q_cfg["fps"], q_cfg["kbps"]
    max_kbps = q_cfg["max_kbps"]
    video_mode = str(q_cfg.get("video_mode", "black"))

    src_tag = src or video_mode
    cache_hash = hashlib.sha256(f"stealth71:{quality_key}:{src_tag}:{w}x{h}@{fps}".encode("utf-8")).hexdigest()[:16]
    cached_mp4 = MEDIA_CACHE_DIR / f"media_{cache_hash}.mp4"
    if cached_mp4.exists() and cached_mp4.stat().st_size > 1_024:
        log.ok("media", f"loaded cached {h}p{fps} MP4 ({cached_mp4.name})")
        return str(cached_mp4)

    vf_filter = f"scale={w}:{h}:force_original_aspect_ratio=decrease,pad={w}:{h}:(ow-iw)/2:(oh-ih)/2,format=yuv420p"
    fallback_ua = build_desktop_user_agent(
        "windows", FALLBACK_CLIENT_VERSION, FALLBACK_CHROME_VERSION, FALLBACK_ELECTRON_VERSION
    )

    def _encode(in_path: str, is_img: bool = False, is_gif: bool = False) -> None:
        cmd = [ff_bin, "-y", "-hide_banner", "-loglevel", "error", "-threads", "1"]
        if is_img:
            cmd += ["-loop", "1", "-t", "6", "-i", in_path, "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo", "-shortest"]
        elif is_gif:
            cmd += ["-ignore_loop", "0", "-t", "8", "-i", in_path, "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo", "-shortest"]
        else:
            cmd += ["-i", in_path, "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo", "-map", "0:v:0", "-map", "0:a:0?", "-map", "1:a:0", "-shortest"]
        cmd += [
            "-vf", vf_filter,
            "-c:v", "libx264", "-preset", "ultrafast", "-profile:v", "baseline", "-level", "3.1",
            "-pix_fmt", "yuv420p", "-b:v", f"{kbps}k", "-maxrate", f"{max_kbps}k", "-bufsize", f"{max_kbps * 2}k",
            "-r", str(fps), "-g", str(fps * 2),
            "-c:a", "aac", "-ar", "48000", "-ac", "2", "-b:a", "64k",
            "-movflags", "+faststart",
            str(cached_mp4),
        ]
        subprocess.run(cmd, check=True, timeout=60)

    temp_in: Path | None = None
    try:
        if src:
            local_p = Path(src).expanduser()
            if local_p.exists():
                ext = local_p.suffix.lower()
                _encode(str(local_p.resolve()), is_img=ext in (".jpg", ".jpeg", ".png", ".webp", ".bmp"), is_gif=ext == ".gif")
                return str(cached_mp4)
            if src.startswith("http"):
                req = urllib.request.Request(src, headers={"User-Agent": fallback_ua})
                with urllib.request.urlopen(req, timeout=15) as r:
                    raw_b = r.read(15 * 1024 * 1024)
                    ctype = (r.headers.get("Content-Type") or "").lower()
                clean_u = src.split("?", 1)[0].lower()
                ext = next((c for c in (".png", ".jpg", ".jpeg", ".gif", ".webp", ".mp4", ".webm") if clean_u.endswith(c)), ".jpg")
                temp_in = Path(tempfile.gettempdir()) / f"rev_in_{uuid.uuid4().hex[:6]}{ext}"
                temp_in.write_bytes(raw_b)
                is_vid = ext in (".mp4", ".webm") or "video/" in ctype
                is_gif = ext == ".gif" or "gif" in ctype
                _encode(str(temp_in), is_img=(not is_vid and not is_gif), is_gif=is_gif)
                return str(cached_mp4)
    except Exception as exc:
        log.warn("media", f"fallback to synthetic {video_mode} {h}p{fps} ({exc})")
    finally:
        if temp_in and temp_in.exists():
            try:
                temp_in.unlink()
            except OSError:
                pass

    if video_mode == "black":
        lavfi_video = f"color=c=black:s={w}x{h}:r={fps}"
        extra_vf: list[str] = []
    elif video_mode == "ambient_hud":
        # Ultra-low bitrate dark HUD-like subtle dual motion so frame entropy is non-zero
        lavfi_video = f"color=c=0x0d1117:s={w}x{h}:r={fps}"
        extra_vf = [
            "-vf",
            f"drawgrid=w=40:h=40:t=1:c=0x161b22@0.4,"
            f"drawbox=x='mod(t*14,{w})':y={h - 8}:w=26:h=4:color=0x238636@0.65:t=fill,"
            f"drawbox=x='mod(t*7+60,{w})':y=10:w=14:h=3:color=0x30363d@0.8:t=fill,format=yuv420p",
        ]
    elif video_mode == "dark_motion":
        lavfi_video = f"color=c=0x0b0e14:s={w}x{h}:r={fps}"
        extra_vf = ["-vf", f"drawbox=x='mod(t*18,{w})':y={h - 6}:w=18:h=4:color=0x1f2937:t=fill,format=yuv420p"]
    else:
        lavfi_video = f"testsrc=size={w}x{h}:rate={fps}"
        extra_vf = []

    cmd = [
        ff_bin, "-y", "-hide_banner", "-loglevel", "error", "-threads", "1",
        "-f", "lavfi", "-i", lavfi_video,
        "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo",
        "-t", "10",
        *extra_vf,
        "-c:v", "libx264", "-preset", "ultrafast", "-profile:v", "baseline", "-level", "3.1",
        "-pix_fmt", "yuv420p", "-b:v", f"{kbps}k", "-maxrate", f"{max_kbps}k", "-bufsize", f"{max_kbps * 2}k",
        "-r", str(fps), "-g", str(fps * 2),
        "-c:a", "aac", "-ar", "48000", "-ac", "2", "-b:a", "64k",
        "-movflags", "+faststart",
        str(cached_mp4),
    ]
    subprocess.run(cmd, check=False, timeout=25)
    if not cached_mp4.exists() or cached_mp4.stat().st_size == 0:
        raise RuntimeError(f"FFmpeg could not generate MP4 at {cached_mp4}")
    log.ok("media", f"generated {h}p{fps} ({video_mode}) MP4 ({cached_mp4.stat().st_size // 1024} KB)")
    return str(cached_mp4)


def load_games() -> list[Game]:
    if GAMES_CACHE_FILE.exists():
        try:
            raw_list = json.loads(GAMES_CACHE_FILE.read_text(encoding="utf-8"))
            if raw_list and isinstance(raw_list[0], list):
                return [Game(gid, name, exe) for gid, name, exe in raw_list]
        except Exception:
            pass
    try:
        ua = build_desktop_user_agent(
            "windows", FALLBACK_CLIENT_VERSION, FALLBACK_CHROME_VERSION, FALLBACK_ELECTRON_VERSION
        )
        req = urllib.request.Request(GAMES_CDN_URL, headers={"User-Agent": ua, "Accept-Encoding": "gzip"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = resp.read()
            raw = gzip.decompress(data) if resp.headers.get("Content-Encoding") == "gzip" else data
        entries = json.loads(raw.decode("utf-8"))
        compact: list[list[str]] = []
        seen: set[str] = set()
        for entry in entries:
            gid = str(entry.get("id", ""))
            if not gid.isdigit() or gid in seen:
                continue
            for ex in entry.get("executables", ()):
                if ex.get("os") == "win32" and (exe_name := ex.get("name")):
                    seen.add(gid)
                    compact.append([gid, entry.get("name", "Unknown"), exe_name])
                    break
        if compact:
            try:
                GAMES_CACHE_FILE.write_text(json.dumps(compact, separators=(",", ":")), encoding="utf-8")
            except OSError:
                pass
            return [Game(gid, name, exe) for gid, name, exe in compact]
    except Exception:
        pass
    return []


def pick_game(games: list[Game], query: str) -> Game:
    q = query.strip().lower()
    if q:
        exact = next((g for g in games if g.id == q or g.name.lower() == q), None)
        if exact:
            return exact
        partial = next((g for g in games if q in g.name.lower()), None)
        if partial:
            return partial
    safe = [g for g in games if not g.is_nsfw_or_trash]
    aaa = [g for g in safe if g.is_aaa_mainstream]
    return aaa[0] if aaa else (safe[0] if safe else games[0])


class FingerprintEngine:
    __slots__ = ("_machine_user_hash", "_cert_hash", "_xor_mask_int", "_cache")

    def __init__(self, user_id: str, machine_guid: str) -> None:
        self._machine_user_hash = hashlib.sha256((machine_guid + user_id).encode()).digest()
        self._cert_hash = hashlib.sha256(b"Authenticode_Verified_Publisher_Cert").digest()
        key32 = hashlib.sha256((user_id + "https://discord.com/careers").encode()).digest()
        self._xor_mask_int = int.from_bytes((key32 * 5)[:145], "big")
        self._cache: dict[str, str] = {}

    def for_game(self, game_id: str, exe_name: str) -> str:
        if cached := self._cache.get(game_id):
            return cached
        seed = hashlib.sha256(f"{game_id}:{exe_name}".encode()).digest()
        file_size = 15_000_000 + (int.from_bytes(seed[0:4], "little") % 75_000_000)
        timestamp = 1650000000 + (int.from_bytes(seed[4:8], "little") % 70_000_000)
        size_of_image = ((file_size + 0xFFF) & ~0xFFF) + 0x10000
        header_hash = hashlib.sha256(b"PE32+_HDR:" + seed[:16]).digest()
        section_hash = hashlib.sha256(b"PE32+_SEC:" + seed[16:]).digest()
        payload = _PACK_FP_STRUCT.pack(
            3, file_size, timestamp, size_of_image,
            header_hash, self._machine_user_hash, section_hash, self._cert_hash,
        )
        encrypted = (int.from_bytes(payload, "big") ^ self._xor_mask_int).to_bytes(145, "big")
        fp = base64.b64encode(b"\x01" + encrypted).decode("ascii")
        self._cache[game_id] = fp
        return fp


class LowLatencyHTTPSConnection(http.client.HTTPSConnection):
    def connect(self) -> None:
        super().connect()
        if self.sock:
            try:
                self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                pass


class StreamTelemetryClient:
    """
    Unified Fake Device + Fake Discord Desktop App HTTP/2 Client.
    Drives both Python telemetry AND Node's LocalChromeTLSBridge with a single coherent identity.
    """

    def __init__(
        self, token: str, user_id: str, status: str = "online",
        locale: str = "vi", timezone: str = "Asia/Ho_Chi_Minh",
        accept_lang: str = "vi-VN,vi;q=0.9,en-US;q=0.8",
        spoof_os: str = "windows", device_seed: str = "", device_tier: str = "",
        proxy: str = "", guild_id: str = "", channel_id: str = "",
        device_cookies: str = "",
        upstream_base: str = f"https://{DISCORD_HOST}",
    ) -> None:
        self.token = token
        self.user_id = user_id
        self.status = status
        self.locale = locale
        self.timezone = timezone
        self.accept_lang = accept_lang
        self.spoof_os = "macos" if spoof_os == "macos" else "windows"
        self.proxy = proxy.strip()
        self.upstream_base = upstream_base.rstrip("/")

        self.referer = (
            f"https://discord.com/channels/{guild_id}/{channel_id}"
            if guild_id and channel_id else "https://discord.com/channels/@me"
        )
        self.cookie = ""
        self.cookies_warmed = False
        self.last_lock_reason: str | None = None
        self.egress_ip = ""
        self.egress_loc = ""
        self.egress_city = ""
        self.egress_region = ""
        self.egress_isp = ""
        self.egress_warp = ""
        self.analytics_token = ""
        self.fetched_at = 0.0

        # 1. Fake Device Profile
        self.device = build_device_profile(user_id, spoof_os=self.spoof_os, seed=device_seed, tier_hint=device_tier)
        self.fp_engine = FingerprintEngine(user_id, self.device.machine_guid)
        self.focus_sim = FocusSimulator()

        # 2. Fake Discord Desktop App Build & Properties
        self.heartbeat_session = str(uuid.uuid4())
        self.launch_signature = str(uuid.uuid4())
        self.super_props_dict, self.gateway_props_dict, self.build_summary = resolve_discord_app_props(
            self.device, self.launch_signature, self.heartbeat_session, locale=locale
        )
        self.user_agent = str(self.super_props_dict.get("browser_user_agent") or "")
        self.super_props_b64 = base64.b64encode(
            json.dumps(self.super_props_dict, separators=(",", ":")).encode("utf-8")
        ).decode("ascii")
        self.sec_ch_headers = build_sec_ch_headers(self.user_agent, self.device)

        self._seq_counter = itertools.count(1)
        self._ssl_ctx = ssl.create_default_context()
        self._tls_local = threading.local()
        self._rate_limit_until = 0.0
        self._rl_lock = threading.Lock()
        self._curl_lock = threading.Lock()
        self.last_ping_ms = 0.0

        self._curl_sess = None
        if HAS_CURL_CFFI and curl_requests is not None:
            proxies = {"http": self.proxy, "https": self.proxy} if self.proxy else None
            self._curl_sess = curl_requests.Session(impersonate="chrome", proxies=proxies)
        if device_cookies.strip():
            self._seed_device_cookies(device_cookies)
        self._rebuild_headers()

    @property
    def is_stale(self) -> bool:
        return not self.analytics_token or (time.time() - self.fetched_at) > AUTH_MAX_AGE

    def set_active_channel_referer(self, guild_id: str, channel_id: str) -> None:
        if guild_id and channel_id:
            self.referer = f"https://discord.com/channels/{guild_id}/{channel_id}"
        else:
            self.referer = "https://discord.com/channels/@me"
        self._rebuild_headers()

    def _rebuild_headers(self) -> None:
        self._headers = {
            "accept": "*/*",
            "accept-language": self.accept_lang,
            "authorization": self.token,
            "content-type": "application/json",
            "origin": "https://discord.com",
            "referer": self.referer,
            **self.sec_ch_headers,
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": self.user_agent,
            "x-debug-options": "bugReporterEnabled",
            "x-discord-locale": self.locale,
            "x-discord-timezone": self.timezone,
            "x-super-properties": self.super_props_b64,
        }
        if self.cookie.strip() and self._curl_sess is None:
            self._headers["cookie"] = self.cookie

    def _fetch_text_via_proxy(self, url: str, timeout: float = 12.0) -> str:
        if self._curl_sess is not None:
            with self._curl_lock:
                resp = self._curl_sess.get(url, timeout=timeout, headers={"user-agent": self.user_agent})
            return resp.text
        handlers = []
        if self.proxy:
            handlers.append(urllib.request.ProxyHandler({"http": self.proxy, "https": self.proxy}))
        opener = urllib.request.build_opener(*handlers)
        req = urllib.request.Request(url, headers={"User-Agent": self.user_agent})
        with opener.open(req, timeout=timeout) as r:
            return r.read().decode("utf-8", errors="replace")

    @staticmethod
    def _norm_geo(text: str) -> str:
        # Normalize accents/spacing so "Đà Nẵng", "Da Nang", "da nang" match consistently
        s = text.strip().lower()
        rep = (
            ("à", "a"), ("á", "a"), ("ả", "a"), ("ã", "a"), ("ạ", "a"),
            ("ă", "a"), ("ằ", "a"), ("ắ", "a"), ("ẳ", "a"), ("ẵ", "a"), ("ặ", "a"),
            ("â", "a"), ("ầ", "a"), ("ấ", "a"), ("ẩ", "a"), ("ẫ", "a"), ("ậ", "a"),
            ("đ", "d"), ("è", "e"), ("é", "e"), ("ẻ", "e"), ("ẽ", "e"), ("ẹ", "e"),
            ("ê", "e"), ("ề", "e"), ("ế", "e"), ("ể", "e"), ("ễ", "e"), ("ệ", "e"),
            ("ì", "i"), ("í", "i"), ("ỉ", "i"), ("ĩ", "i"), ("ị", "i"),
            ("ò", "o"), ("ó", "o"), ("ỏ", "o"), ("õ", "o"), ("ọ", "o"),
            ("ô", "o"), ("ồ", "o"), ("ố", "o"), ("ổ", "o"), ("ỗ", "o"), ("ộ", "o"),
            ("ơ", "o"), ("ờ", "o"), ("ớ", "o"), ("ở", "o"), ("ỡ", "o"), ("ợ", "o"),
            ("ù", "u"), ("ú", "u"), ("ủ", "u"), ("ũ", "u"), ("ụ", "u"),
            ("ư", "u"), ("ừ", "u"), ("ứ", "u"), ("ử", "u"), ("ữ", "u"), ("ự", "u"),
            ("ỳ", "y"), ("ý", "y"), ("ỷ", "y"), ("ỹ", "y"), ("ỵ", "y"),
        )
        for src_c, dst_c in rep:
            s = s.replace(src_c, dst_c)
        return re.sub(r"\s+", " ", s)

    def verify_egress_ip(
        self,
        require_proxy: bool = False,
        expected_ip: str = "",
        allowed_countries: Sequence[str] = (),
        expected_city: str = "",
        expected_region: str = "",
        expected_isp: str = "",
        trace_url: str = CF_TRACE_URL,
        geoip_urls: Sequence[str] = (GEOIP_PRIMARY_URL, GEOIP_FALLBACK_URL),
    ) -> dict[str, str]:
        if require_proxy and not self.proxy:
            raise EgressGuardError("REQUIRE_PROXY=1 is set, but DISCORD_PROXY is empty")

        try:
            text = self._fetch_text_via_proxy(trace_url, timeout=12.0)
        except Exception as exc:
            raise EgressGuardError(f"failed to reach egress check ({exc})") from exc

        kv: dict[str, str] = {}
        for line in text.splitlines():
            if "=" in line:
                k, v = line.split("=", 1)
                kv[k.strip()] = v.strip()

        ip = kv.get("ip", "")
        loc = kv.get("loc", "").upper()
        warp = kv.get("warp", "off")
        if not ip:
            raise EgressGuardError("egress trace returned no IP address")

        # Resolve city / region / ISP from GeoIP via the exact same proxy
        city, region, isp = "", "", ""
        for g_url in geoip_urls:
            try:
                raw_geo = self._fetch_text_via_proxy(g_url, timeout=8.0)
                g_data = json.loads(raw_geo)
                if isinstance(g_data, dict):
                    city = str(g_data.get("city") or "").strip()
                    region = str(g_data.get("region") or g_data.get("regionName") or "").strip()
                    conn_obj = g_data.get("connection")
                    if isinstance(conn_obj, dict):
                        isp = str(conn_obj.get("isp") or conn_obj.get("org") or "").strip()
                    else:
                        isp = str(g_data.get("org") or g_data.get("isp") or "").strip()
                    if not loc and g_data.get("country_code"):
                        loc = str(g_data.get("country_code")).upper()
                    elif not loc and g_data.get("country"):
                        loc = str(g_data.get("country")).upper()
                    if city or region or isp:
                        break
            except Exception:
                continue

        self.egress_ip = ip
        self.egress_loc = loc
        self.egress_city = city
        self.egress_region = region
        self.egress_isp = isp
        self.egress_warp = warp

        exp = expected_ip.strip()
        if exp:
            allowed_ips = [x.strip() for x in exp.split(",") if x.strip()]
            if not any(ip == cand or ip.startswith(cand) for cand in allowed_ips):
                raise EgressGuardError(f"egress IP {ip} does not match EXPECTED_IP={exp}")

        clean_countries = [c.strip().upper() for c in allowed_countries if c.strip()]
        if clean_countries and loc not in clean_countries:
            raise EgressGuardError(f"egress country '{loc}' not in ALLOWED_COUNTRIES={','.join(clean_countries)}")

        if expected_city.strip():
            want_cities = [self._norm_geo(c) for c in expected_city.split(",") if c.strip()]
            got_city = self._norm_geo(city)
            if not got_city or not any(wc in got_city or got_city in wc for wc in want_cities):
                raise EgressGuardError(
                    f"egress city '{city or 'unknown'}' does not match EXPECTED_CITY='{expected_city}' "
                    f"(Discord login location comes strictly from your proxy IP, not headers)"
                )

        if expected_region.strip():
            want_regions = [self._norm_geo(r) for r in expected_region.split(",") if r.strip()]
            got_region = self._norm_geo(region)
            if not got_region or not any(wr in got_region or got_region in wr for wr in want_regions):
                raise EgressGuardError(
                    f"egress region '{region or 'unknown'}' does not match EXPECTED_REGION='{expected_region}'"
                )

        if expected_isp.strip():
            want_isps = [self._norm_geo(x) for x in expected_isp.split(",") if x.strip()]
            got_isp = self._norm_geo(isp)
            if not got_isp or not any(wi in got_isp for wi in want_isps):
                raise EgressGuardError(f"egress ISP '{isp or 'unknown'}' does not match EXPECTED_ISP='{expected_isp}'")

        return {"ip": ip, "loc": loc, "city": city, "region": region, "isp": isp, "warp": warp}

    def _seed_device_cookies(self, raw_cookie_env: str) -> None:
        """
        Optionally seeds ONLY Discord's long-lived device cookies (__dcfduid, __sdcfduid).
        Intentionally strips IP/TLS-bound Cloudflare cookies (cf_clearance, __cfruid, _cfuvid)
        so users can never accidentally trigger session-hijack flags.
        """
        allowed = {}
        for part in raw_cookie_env.split(";"):
            if "=" in part:
                k, v = part.strip().split("=", 1)
                if k in ("__dcfduid", "__sdcfduid") and v.strip():
                    allowed[k] = v.strip()
        if not allowed:
            return
        self.cookie = "; ".join(f"{k}={v}" for k, v in allowed.items())
        if self._curl_sess is not None:
            for k, v in allowed.items():
                try:
                    self._curl_sess.cookies.set(k, v, domain=".discord.com", path="/")
                except Exception:
                    pass
        if hasattr(self, "_headers"):
            self._rebuild_headers()

    @staticmethod
    def _detect_account_lock(status_code: int, raw: bytes) -> str | None:
        if status_code in (401, 403):
            try:
                j = json.loads(raw.decode("utf-8")) if raw else {}
                code = j.get("code") if isinstance(j, dict) else None
                msg = j.get("message", "") if isinstance(j, dict) else ""
                if code in (40002, 40007, 20011) or "verify" in str(msg).lower() or "unauthorized" in str(msg).lower():
                    return f"HTTP {status_code} code={code} ({msg})"
            except Exception:
                pass
            return f"HTTP {status_code}"
        if status_code == 400 and raw:
            low = raw[:400].lower()
            if b"captcha_key" in low or b"captcha_service" in low:
                return "HTTP 400 captcha_required"
        return None

    def warm_up_cookies(self) -> int:
        nav_headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": self.accept_lang,
            **self.sec_ch_headers,
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": self.user_agent,
        }
        try:
            if self._curl_sess is not None:
                with self._curl_lock:
                    resp = self._curl_sess.get(f"{self.upstream_base}/app", headers=nav_headers, timeout=15)
                count = len(self._curl_sess.cookies.keys())
                self.cookies_warmed = count > 0 or resp.status_code in (200, 302)
                return count

            parsed_up = urllib.parse.urlparse(self.upstream_base)
            if parsed_up.scheme == "http":
                conn: http.client.HTTPConnection = http.client.HTTPConnection(
                    parsed_up.hostname or "127.0.0.1", parsed_up.port or 80, timeout=15
                )
                conn.request("GET", "/app", headers={**nav_headers, "connection": "close"})
                resp = conn.getresponse()
                resp.read()
                self._merge_cookies(resp)
                conn.close()
            else:
                conn = self._get_conn()
                conn.request("GET", "/app", headers={**nav_headers, "accept-encoding": "gzip", "connection": "keep-alive"})
                resp = conn.getresponse()
                resp.read()
                self._merge_cookies(resp)
            count = len([p for p in self.cookie.split(";") if "=" in p])
            self.cookies_warmed = count > 0 or resp.status in (200, 302)
            return count
        except Exception:
            self._reset_conn()
            return 0

    def _get_conn(self) -> LowLatencyHTTPSConnection:
        conn = getattr(self._tls_local, "conn", None)
        if conn is None:
            parsed_up = urllib.parse.urlparse(self.upstream_base)
            up_host = parsed_up.hostname or DISCORD_HOST
            up_port = parsed_up.port or (443 if parsed_up.scheme == "https" else 80)
            if self.proxy and self.proxy.startswith(("http://", "https://")):
                pu = urllib.parse.urlparse(self.proxy)
                conn = LowLatencyHTTPSConnection(pu.hostname, pu.port or 8080, timeout=15, context=self._ssl_ctx)
                tunnel_hdrs = {}
                if pu.username:
                    creds = f"{urllib.parse.unquote(pu.username)}:{urllib.parse.unquote(pu.password or '')}"
                    tunnel_hdrs["Proxy-Authorization"] = "Basic " + base64.b64encode(creds.encode()).decode("ascii")
                conn.set_tunnel(up_host, up_port, headers=tunnel_hdrs or None)
            else:
                conn = LowLatencyHTTPSConnection(up_host, up_port, timeout=15, context=self._ssl_ctx)
            self._tls_local.conn = conn
        return conn

    def _reset_conn(self) -> None:
        conn = getattr(self._tls_local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
            self._tls_local.conn = None

    def _merge_cookies(self, resp: http.client.HTTPResponse) -> None:
        set_cookies = resp.msg.get_all("Set-Cookie") or []
        if not set_cookies:
            return
        jar: dict[str, str] = {}
        for part in self.cookie.split(";"):
            if "=" in part:
                k, v = part.strip().split("=", 1)
                jar[k.strip()] = v.strip()
        changed = False
        for sc in set_cookies:
            pair = sc.split(";", 1)[0].strip()
            if "=" in pair:
                k, v = pair.split("=", 1)
                if jar.get(k) != v:
                    jar[k] = v
                    changed = True
        if changed:
            self.cookie = "; ".join(f"{k}={v}" for k, v in jar.items())
            self._rebuild_headers()

    def raw_rest_request(
        self,
        method: str,
        path: str,
        body_bytes: bytes | None = None,
        content_type: str | None = None,
        authorization: str | None = None,
        context_properties: str | None = None,
    ) -> tuple[int, dict[str, str], bytes]:
        for attempt in range(3):
            with self._rl_lock:
                wait_now = self._rate_limit_until - time.monotonic()
            if wait_now > 0:
                time.sleep(wait_now)

            headers = dict(self._headers)
            if authorization:
                headers["authorization"] = authorization
            if context_properties:
                headers["x-context-properties"] = context_properties
            if content_type:
                headers["content-type"] = content_type
            elif body_bytes is None and method.upper() in ("GET", "HEAD", "DELETE"):
                headers.pop("content-type", None)

            if self._curl_sess is not None:
                try:
                    t0 = time.perf_counter()
                    with self._curl_lock:
                        resp = self._curl_sess.request(
                            method,
                            f"{self.upstream_base}{path}",
                            data=body_bytes,
                            headers=headers,
                            timeout=15,
                        )
                    self.last_ping_ms = (time.perf_counter() - t0) * 1000
                    resp_headers = {str(k).lower(): str(v) for k, v in resp.headers.items()}
                    raw = bytes(resp.content or b"")
                    if resp.status_code == 429:
                        retry_after = float(resp_headers.get("retry-after", "5"))
                        try:
                            j = json.loads(raw.decode("utf-8"))
                            if isinstance(j, dict) and "retry_after" in j:
                                retry_after = float(j["retry_after"])
                        except Exception:
                            pass
                        with self._rl_lock:
                            self._rate_limit_until = max(self._rate_limit_until, time.monotonic() + retry_after + 0.5)
                        continue
                    self.last_lock_reason = self._detect_account_lock(resp.status_code, raw)
                    return resp.status_code, resp_headers, raw
                except Exception as exc:
                    if attempt == 2:
                        return 0, {}, json.dumps({"error": str(exc)}).encode("utf-8")
                    continue

            parsed_up = urllib.parse.urlparse(self.upstream_base)
            use_http = parsed_up.scheme == "http"
            headers["accept-encoding"] = "gzip"
            headers["connection"] = "keep-alive"
            try:
                t0 = time.perf_counter()
                if use_http:
                    conn: http.client.HTTPConnection = http.client.HTTPConnection(
                        parsed_up.hostname or "127.0.0.1", parsed_up.port or 80, timeout=15
                    )
                    conn.request(method, path, body=body_bytes, headers=headers)
                    resp = conn.getresponse()
                    raw = resp.read()
                    conn.close()
                else:
                    conn = self._get_conn()
                    conn.request(method, path, body=body_bytes, headers=headers)
                    resp = conn.getresponse()
                    raw = resp.read()
                self.last_ping_ms = (time.perf_counter() - t0) * 1000
                self._merge_cookies(resp)
                if resp.headers.get("Content-Encoding") == "gzip" and raw:
                    raw = gzip.decompress(raw)
                resp_headers = {k.lower(): v for k, v in resp.getheaders()}
                if resp.status == 429:
                    retry_after = float(resp_headers.get("retry-after", "5"))
                    try:
                        j = json.loads(raw.decode("utf-8"))
                        if isinstance(j, dict) and "retry_after" in j:
                            retry_after = float(j["retry_after"])
                    except Exception:
                        pass
                    with self._rl_lock:
                        self._rate_limit_until = max(self._rate_limit_until, time.monotonic() + retry_after + 0.5)
                    continue
                self.last_lock_reason = self._detect_account_lock(resp.status, raw)
                return resp.status, resp_headers, raw
            except Exception as exc:
                self._reset_conn()
                if attempt == 2:
                    return 0, {}, json.dumps({"error": str(exc)}).encode("utf-8")
        return 0, {}, b'{"error":"max retries"}'

    def rest_request(self, method: str, path: str, payload: dict | None = None) -> tuple[int, dict | list]:
        body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8") if payload is not None else None
        st, _, raw = self.raw_rest_request(method, path, body_bytes=body_bytes)
        if not raw:
            return st, {}
        try:
            return st, json.loads(raw.decode("utf-8"))
        except Exception:
            return st, {}

    def ensure_analytics_token(self) -> str:
        st, data = self.rest_request("GET", "/api/v9/users/@me?with_analytics_token=true")
        if self.last_lock_reason:
            raise AuthFatalError(f"Discord rejected/flagged account ({self.last_lock_reason})")
        if st != 200 or not isinstance(data, dict):
            raise RuntimeError(f"users/@me failed (HTTP {st})")
        tok = data.get("analytics_token")
        if not tok:
            raise RuntimeError("no analytics_token in response (enable Data & Privacy personalization in Discord)")
        self.analytics_token = tok
        self.fetched_at = time.time()
        return str(data.get("username") or "user")

    def rotate_launch_session(self) -> None:
        """Generate a fresh Desktop app launch identity (used when waking up from daily rest)."""
        self.heartbeat_session = str(uuid.uuid4())
        self.launch_signature = str(uuid.uuid4())
        self.super_props_dict, self.gateway_props_dict, self.build_summary = resolve_discord_app_props(
            self.device, self.launch_signature, self.heartbeat_session, locale=self.locale
        )
        self.super_props_b64 = base64.b64encode(
            json.dumps(self.super_props_dict, separators=(",", ":")).encode("utf-8")
        ).decode("ascii")
        self._rebuild_headers()

    def desktop_cold_boot_read(self) -> dict[str, int]:
        """Benign read-only Desktop startup endpoints spaced naturally like official Electron client."""
        results: dict[str, int] = {}
        for name, path in (
            ("settings_proto_1", "/api/v9/users/@me/settings-proto/1"),
            ("settings_proto_2", "/api/v9/users/@me/settings-proto/2"),
            ("country_code", "/api/v9/users/@me/billing/country-code"),
        ):
            st, _ = self.rest_request("GET", path)
            results[name] = st
            time.sleep(random.uniform(0.35, 0.95))
        return results

    def build_app_opened(self) -> list[dict]:
        """Simulates authentic Discord Desktop cold-boot app_opened telemetry event."""
        now_ms = int(time.time() * 1000)
        cpu_pct, mem_kb = self.device.sample_perf()
        return [
            {
                "type": "app_opened",
                "properties": {
                    "client_track_timestamp": now_ms,
                    "client_heartbeat_session_id": self.heartbeat_session,
                    "event_sequence_number": next(self._seq_counter),
                    "launch_signature": self.launch_signature,
                    "client_performance_cpu": cpu_pct,
                    "client_performance_memory": mem_kb,
                    "cpu_core_count": self.device.cpu_cores,
                    "system_ram_mb": self.device.ram_total_mb,
                    "hardware_display_count": self.device.display_count,
                    "screen_width": self.device.display_w,
                    "screen_height": self.device.display_h,
                    "rendered_locale": self.locale,
                    "system_uptime_seconds": self.device.simulated_uptime_sec, "accessibility_features": 0,
                    "client_app_state": "focused",
                    "client_send_timestamp": now_ms + random.randint(25, 85),
                },
            }
        ]

    def build_game_launch(
        self, game: Game, gsid: str, guild_id: str, channel_id: str,
        rtc_id: str = "", media_id: str = "",
    ) -> list[dict]:
        """Real Desktop order: game process is detected BEFORE user clicks Go Live."""
        now_ms = int(time.time() * 1000)
        fp = self.fp_engine.for_game(game.id, game.exe)
        cpu_pct, mem_kb = self.device.sample_perf()
        return [
            {
                "type": "launch_game",
                "properties": {
                    "client_track_timestamp": now_ms,
                    "client_heartbeat_session_id": self.heartbeat_session,
                    "event_sequence_number": next(self._seq_counter), "game": game.name, "game_id": game.id,
                    "verified": True, "elevated": False, "is_launcher": False, "game_platform": "desktop",
                    "detection_method": "verified_game", "is_overlay_enabled": True, "is_overlay_game_enabled": True,
                    "is_overlay_game_source": "OOP_DEFAULT_DATABASE", "fullscreen_type": "BORDERLESS_FULLSCREEN",
                    "hardware_display_count": self.device.display_count, "overlay_method": "OutOfProcess",
                    "activity_status_enabled": True, "activity_status_shared_guilds": [guild_id],
                    "current_user_status": self.status, "game_detection_enabled": True, "executable_path": game.exe,
                    "voice_channel_id": channel_id, "voice_channel_type": 2, "voice_channel_bitrate": 64000,
                    "voice_channel_guild_id": guild_id, "hidden_by_distributor": False, "game_metadata": None,
                    "executable_fingerprint": fp,
                    "client_performance_cpu": cpu_pct,
                    "client_performance_memory": mem_kb,
                    "cpu_core_count": self.device.cpu_cores,
                    "accessibility_features": 0, "rendered_locale": self.locale,
                    "launch_signature": self.launch_signature, "client_rtc_state": "CONNECTED",
                    "client_app_state": "focused", "client_send_timestamp": now_ms + random.randint(24, 65),
                },
            },
            {
                "type": "running_game_heartbeat",
                "properties": {
                    "client_track_timestamp": now_ms + random.randint(35, 90),
                    "client_heartbeat_session_id": self.heartbeat_session,
                    "event_sequence_number": next(self._seq_counter), "game_id": game.id, "game_name": game.name,
                    "game_metadata": None, "game_executable": game.exe, "game_detection_enabled": True,
                    "initial_heartbeat": True, "final_heartbeat": False, "game_session_id": gsid,
                    "duration_tracked_ms": 0, "rtc_connection_id": rtc_id or None, "media_session_id": media_id or None,
                    "launch_signature": self.launch_signature, "discord_window_state": "focused",
                    "client_app_state": "focused", "client_send_timestamp": now_ms + random.randint(95, 160),
                },
            },
        ]

    def build_stream_start(
        self, game: Game, gsid: str, guild_id: str, channel_id: str,
        rtc_id: str, media_id: str, region: str, host: str,
        viewer_count: int, fps: int, height: int, has_sound: bool = False,
    ) -> list[dict]:
        now_ms = int(time.time() * 1000)
        v_cnt = max(1, viewer_count)
        return [
            {
                "type": "media_session_joined",
                "properties": {
                    "client_track_timestamp": now_ms, "client_heartbeat_session_id": self.heartbeat_session,
                    "event_sequence_number": next(self._seq_counter), "channel_id": channel_id, "channel_type": 2,
                    "guild_id": guild_id, "media_session_id": media_id, "rtc_connection_id": rtc_id,
                    "context": "stream", "stream_region": region, "hostname": host or f"{region}.discord.media",
                    "game_id": game.id, "game_name": game.name, "launch_signature": self.launch_signature,
                    "audio_input_device": self.device.audio_in, "audio_output_device": self.device.audio_out,
                    "client_app_state": "focused", "client_send_timestamp": now_ms + random.randint(14, 48),
                },
            },
            {
                "type": "video_stream_started",
                "properties": {
                    "client_track_timestamp": now_ms + random.randint(25, 60),
                    "client_heartbeat_session_id": self.heartbeat_session,
                    "event_sequence_number": next(self._seq_counter), "rtc_connection_id": rtc_id,
                    "media_session_id": media_id, "stream_region": region, "guild_id": guild_id,
                    "channel_id": channel_id, "channel_type": 2, "stream_type": "guild", "share_type": "application",
                    "capture_method": "game_hook", "share_application_name": game.name, "share_application_id": game.id,
                    "share_application_executable": game.exe, "game_id": game.id, "game_name": game.name,
                    "target_fps": fps, "target_resolution_height": height, "has_sound": has_sound,
                    "gpu_brand": self.device.gpu_vendor, "hardware_display_count": self.device.display_count,
                    "num_viewers": v_cnt, "max_viewers": v_cnt, "launch_signature": self.launch_signature,
                    "client_app_state": "focused", "client_send_timestamp": now_ms + random.randint(65, 110),
                },
            },
        ]

    def build_stream_heartbeat(
        self, game: Game, gsid: str, delta_sec: float, cumulative_sec: float,
        guild_id: str, channel_id: str, rtc_id: str, media_id: str,
        region: str, viewer_count: int, fps: int, height: int, has_sound: bool = False,
        include_game_hb: bool = True,
    ) -> list[dict]:
        now_ms = int(time.time() * 1000)
        dur_ms = max(1000, int(round(delta_sec * 1000)))
        dur_sec = max(1, dur_ms // 1000)
        cum_ms = max(dur_ms, int(round(cumulative_sec * 1000)))
        v_cnt = max(1, viewer_count)
        app_state = self.focus_sim.current_state()
        cpu_pct, mem_kb = self.device.sample_perf()
        send_off = random.randint(12, 85)
        events: list[dict] = [
            {
                "type": "video_stream_heartbeat",
                "properties": {
                    "client_track_timestamp": now_ms, "client_heartbeat_session_id": self.heartbeat_session,
                    "event_sequence_number": next(self._seq_counter), "rtc_connection_id": rtc_id,
                    "media_session_id": media_id, "stream_region": region, "guild_id": guild_id,
                    "channel_id": channel_id, "channel_type": 2, "stream_type": "guild", "share_type": "application",
                    "capture_method": "game_hook", "share_application_name": game.name, "share_application_id": game.id,
                    "share_application_executable": game.exe, "game_id": game.id, "game_name": game.name,
                    "duration": dur_sec, "duration_ms": dur_ms, "duration_tracked_ms": dur_ms,
                    "duration_streaming_ms": dur_ms,
                    "frames_sent": max(1, dur_sec * fps - random.randint(0, max(1, fps // 2))),
                    "target_fps": fps, "target_resolution_height": height, "has_sound": has_sound,
                    "video_encoder": self.device.encoder_name,
                    "average_encode_time_ms": round(random.uniform(2.4, 7.8), 2),
                    "packet_loss_rate": round(random.uniform(0.0, 0.003), 4),
                    "rtt_ms": int(max(18.0, min(180.0, self.last_ping_ms or random.uniform(32.0, 68.0)))),
                    "client_performance_cpu": cpu_pct, "client_performance_memory": mem_kb,
                    "num_viewers": v_cnt, "max_viewers": v_cnt, "launch_signature": self.launch_signature,
                    "client_app_state": app_state, "client_send_timestamp": now_ms + send_off,
                },
            },
        ]
        if include_game_hb:
            events.append(
                {
                    "type": "running_game_heartbeat",
                    "properties": {
                        "client_track_timestamp": now_ms + random.randint(4, 15),
                        "client_heartbeat_session_id": self.heartbeat_session,
                        "event_sequence_number": next(self._seq_counter), "game_id": game.id, "game_name": game.name,
                        "game_metadata": None, "game_executable": game.exe, "game_detection_enabled": True,
                        "initial_heartbeat": False, "final_heartbeat": False, "game_session_id": gsid,
                        "duration_tracked_ms": min(2_000_000_000, cum_ms), "rtc_connection_id": rtc_id,
                        "media_session_id": media_id, "launch_signature": self.launch_signature,
                        "discord_window_state": app_state, "client_app_state": app_state,
                        "client_send_timestamp": now_ms + send_off + random.randint(5, 25),
                    },
                }
            )
        return events

    def build_stream_end(
        self, game: Game, gsid: str, total_sec: float, guild_id: str, channel_id: str,
        rtc_id: str, media_id: str, region: str, viewer_count: int, fps: int, height: int, has_sound: bool = False,
    ) -> list[dict]:
        now_ms = int(time.time() * 1000)
        dur_ms = max(1000, int(round(total_sec * 1000)))
        dur_sec = max(1, dur_ms // 1000)
        v_cnt = max(1, viewer_count)
        base = {
            "client_heartbeat_session_id": self.heartbeat_session, "rtc_connection_id": rtc_id,
            "media_session_id": media_id, "stream_region": region, "guild_id": guild_id,
            "channel_id": channel_id, "channel_type": 2, "stream_type": "guild", "share_type": "application",
            "capture_method": "game_hook", "share_application_name": game.name, "share_application_id": game.id,
            "share_application_executable": game.exe, "game_id": game.id, "game_name": game.name,
            "target_fps": fps, "target_resolution_height": height, "has_sound": has_sound,
            "launch_signature": self.launch_signature, "client_app_state": "focused",
        }
        return [
            {
                "type": "screenshare_finished",
                "properties": {
                    **base, "client_track_timestamp": now_ms, "event_sequence_number": next(self._seq_counter),
                    "duration": dur_sec, "duration_ms": dur_ms, "duration_tracked_ms": dur_ms,
                    "frames_sent": dur_sec * fps, "num_viewers": v_cnt, "max_viewers": v_cnt,
                    "client_send_timestamp": now_ms + random.randint(10, 30),
                },
            },
            {
                "type": "video_stream_ended",
                "properties": {
                    **base, "client_track_timestamp": now_ms + 15, "event_sequence_number": next(self._seq_counter),
                    "duration": dur_sec, "duration_ms": dur_ms, "duration_tracked_ms": dur_ms,
                    "duration_streaming_ms": dur_ms, "num_viewers": v_cnt, "max_viewers": v_cnt,
                    "reason": "user_requested", "client_send_timestamp": now_ms + random.randint(25, 55),
                },
            },
            {
                "type": "running_game_heartbeat",
                "properties": {
                    "client_track_timestamp": now_ms + 30, "client_heartbeat_session_id": self.heartbeat_session,
                    "event_sequence_number": next(self._seq_counter), "game_id": game.id, "game_name": game.name,
                    "game_metadata": None, "game_executable": game.exe, "game_detection_enabled": True,
                    "initial_heartbeat": False, "final_heartbeat": True, "game_session_id": gsid,
                    "duration_tracked_ms": dur_ms, "rtc_connection_id": rtc_id, "media_session_id": media_id,
                    "launch_signature": self.launch_signature, "discord_window_state": "focused",
                    "client_app_state": "focused", "client_send_timestamp": now_ms + random.randint(40, 75),
                },
            },
        ]

    def post_science(self, events: Sequence[dict]) -> int:
        st, _ = self.rest_request("POST", "/api/v9/science", {"token": self.analytics_token, "events": events})
        return st


class LocalChromeWSBridge:
    """
    Loopback-only RFC 6455 WebSocket bridge (127.0.0.1:<ephemeral>) that relays Node's Discord
    Gateway WebSocket traffic through Python's curl_cffi Chrome-TLS WebSocket client.
    Eliminates the Node OpenSSL JA3/JA4 mismatch on wss://gateway.discord.gg while preserving
    exact text & binary (zlib-stream) framing, cookies, headers, and pinned proxy.
    """

    _WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

    def __init__(self, client: StreamTelemetryClient, log: Logger, upstream_factory=None) -> None:
        self.client = client
        self.log = log
        self._upstream_factory = upstream_factory
        self.secret = secrets.token_urlsafe(16)
        self._prefix = f"/gw/{self.secret}"
        self.upstream_host = "wss://gateway.discord.gg"
        self.port = 0
        self._sock: socket.socket | None = None
        self._stop = threading.Event()
        self.active = False

    @staticmethod
    def is_supported(client: StreamTelemetryClient) -> bool:
        return bool(client._curl_sess is not None and hasattr(client._curl_sess, "ws_connect"))

    @property
    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.port}{self._prefix}"

    def start(self) -> bool:
        if self._upstream_factory is None and not self.is_supported(self.client):
            return False
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(8)
        srv.settimeout(1.0)
        self._sock = srv
        self.port = int(srv.getsockname()[1])
        self.active = True
        threading.Thread(target=self._accept_loop, daemon=True).start()
        return True

    def stop(self) -> None:
        self._stop.set()
        self.active = False
        if self._sock:
            try:
                self._sock.close()
            except Exception:
                pass
            self._sock = None

    @staticmethod
    def _recv_exact(conn: socket.socket, n: int) -> bytes | None:
        buf = bytearray()
        while len(buf) < n:
            try:
                chunk = conn.recv(n - len(buf))
            except socket.timeout:
                continue
            except OSError:
                return None
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf)

    @classmethod
    def read_ws_frame(cls, conn: socket.socket) -> tuple[int, bytes] | None:
        """Read one RFC 6455 frame (unmasking client payloads, reassembling fragmented frames)."""
        msg_opcode = -1
        fragments = bytearray()
        while True:
            hdr = cls._recv_exact(conn, 2)
            if not hdr:
                return None
            b0, b1 = hdr[0], hdr[1]
            fin = bool(b0 & 0x80)
            opcode = b0 & 0x0F
            masked = bool(b1 & 0x80)
            length = b1 & 0x7F
            if length == 126:
                ext = cls._recv_exact(conn, 2)
                if not ext:
                    return None
                length = struct.unpack("!H", ext)[0]
            elif length == 127:
                ext = cls._recv_exact(conn, 8)
                if not ext:
                    return None
                length = struct.unpack("!Q", ext)[0]
            mask_key = cls._recv_exact(conn, 4) if masked else b""
            if masked and not mask_key:
                return None
            payload = cls._recv_exact(conn, length) if length > 0 else b""
            if payload is None:
                return None
            if masked and length > 0:
                payload = bytes(b ^ mask_key[i & 3] for i, b in enumerate(payload))

            # Control frames (ping/pong/close) are never fragmented
            if opcode >= 0x8:
                return opcode, payload

            if opcode != 0:
                msg_opcode = opcode
            fragments.extend(payload)
            if fin:
                return (msg_opcode if msg_opcode != -1 else opcode), bytes(fragments)

    @staticmethod
    def build_ws_frame(opcode: int, payload: bytes) -> bytes:
        n = len(payload)
        b0 = 0x80 | (opcode & 0x0F)
        if n < 126:
            hdr = struct.pack("!BB", b0, n)
        elif n <= 0xFFFF:
            hdr = struct.pack("!BBH", b0, 126, n)
        else:
            hdr = struct.pack("!BBQ", b0, 127, n)
        return hdr + payload

    def _accept_loop(self) -> None:
        while not self._stop.is_set() and self._sock:
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._handle_conn, args=(conn,), daemon=True).start()

    def _open_upstream(self, target_url: str):
        if self._upstream_factory is not None:
            return self._upstream_factory(target_url)
        ws_headers = {
            "Origin": "https://discord.com",
            "User-Agent": self.client.user_agent,
            "Accept-Language": self.client.accept_lang,
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
        }
        with self.client._curl_lock:
            return self.client._curl_sess.ws_connect(target_url, headers=ws_headers, timeout=20)

    def _handle_conn(self, conn: socket.socket) -> None:
        upstream = None
        closed = threading.Event()
        write_lock = threading.Lock()
        try:
            conn.settimeout(10.0)
            raw_req = b""
            while b"\r\n\r\n" not in raw_req and len(raw_req) < 16384:
                chunk = conn.recv(4096)
                if not chunk:
                    return
                raw_req += chunk
            head_txt = raw_req.split(b"\r\n\r\n", 1)[0].decode("iso-8859-1", errors="replace")
            lines = head_txt.split("\r\n")
            if not lines:
                return
            parts = lines[0].split(" ")
            req_path = parts[1] if len(parts) >= 2 else ""
            if not req_path.startswith(self._prefix):
                conn.sendall(b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\n\r\n")
                return

            hdrs: dict[str, str] = {}
            for ln in lines[1:]:
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    hdrs[k.strip().lower()] = v.strip()
            ws_key = hdrs.get("sec-websocket-key", "")
            if not ws_key:
                conn.sendall(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                return

            suffix = req_path[len(self._prefix):]
            if not suffix:
                suffix = "/"
            elif suffix.startswith("?"):
                suffix = "/" + suffix
            target_url = f"{self.upstream_host.rstrip('/')}{suffix}"

            upstream = self._open_upstream(target_url)

            accept_val = base64.b64encode(
                hashlib.sha1((ws_key + self._WS_GUID).encode("ascii")).digest()
            ).decode("ascii")
            handshake_resp = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept_val}\r\n\r\n"
            ).encode("ascii")
            conn.sendall(handshake_resp)
            conn.settimeout(1.0)

            def _send_local(opcode: int, data: bytes) -> None:
                frame = self.build_ws_frame(opcode, data)
                with write_lock:
                    conn.sendall(frame)

            def _upstream_to_local() -> None:
                try:
                    while not self._stop.is_set() and not closed.is_set():
                        try:
                            res = upstream.recv()
                        except Exception as exc:
                            if "timed out" in str(exc).lower() or "timeout" in str(exc).lower():
                                continue
                            break
                        if res is None:
                            break
                        if isinstance(res, tuple):
                            data, flags = res[0], res[1]
                            if not data and flags == 0:
                                continue
                            # curl_cffi CURLWS flags: TEXT=1, BINARY=2, CLOSE=8, PING=16, PONG=64
                            if flags & 8:
                                _send_local(0x8, data if isinstance(data, bytes) else b"")
                                break
                            if flags & 16:
                                _send_local(0x9, data if isinstance(data, bytes) else b"")
                                continue
                            if flags & 64:
                                _send_local(0xA, data if isinstance(data, bytes) else b"")
                                continue
                            op = 0x1 if (flags & 1) else 0x2
                            payload = data.encode("utf-8") if isinstance(data, str) else bytes(data)
                            _send_local(op, payload)
                        elif isinstance(res, str):
                            _send_local(0x1, res.encode("utf-8"))
                        elif isinstance(res, (bytes, bytearray)):
                            _send_local(0x2, bytes(res))
                except Exception:
                    pass
                finally:
                    closed.set()
                    try:
                        conn.shutdown(socket.SHUT_RDWR)
                    except Exception:
                        pass

            t_up = threading.Thread(target=_upstream_to_local, daemon=True)
            t_up.start()

            while not self._stop.is_set() and not closed.is_set():
                frm = self.read_ws_frame(conn)
                if frm is None:
                    break
                op, payload = frm
                if op == 0x8:  # Close
                    try:
                        upstream.close()
                    except Exception:
                        pass
                    break
                if op == 0x9:  # Ping
                    _send_local(0xA, payload)
                    continue
                if op == 0xA:  # Pong
                    continue
                if op == 0x1:
                    if hasattr(upstream, "send_str"):
                        upstream.send_str(payload.decode("utf-8", errors="replace"))
                    else:
                        upstream.send(payload.decode("utf-8", errors="replace"))
                elif op == 0x2:
                    if hasattr(upstream, "send_bytes"):
                        upstream.send_bytes(payload)
                    else:
                        upstream.send(payload)
        except Exception as exc:
            self.log.warn("ws-bridge", f"relay closed ({exc})")
        finally:
            closed.set()
            if upstream is not None:
                try:
                    upstream.close()
                except Exception:
                    pass
            try:
                conn.close()
            except Exception:
                pass


class LocalChromeTLSBridge:
    """
    Loopback-only HTTP bridge (127.0.0.1:<ephemeral>) routing Node's Discord REST traffic
    through Python's Chrome-impersonating curl_cffi session. Scrubs all Node/selfbot headers
    and injects normalized Fake Discord Desktop headers + organic cookies.
    """

    _HOP_BY_HOP = frozenset({
        "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailers", "transfer-encoding", "upgrade", "content-encoding", "content-length",
    })

    def __init__(
        self,
        client: StreamTelemetryClient,
        role: str,
        ws_bridge: LocalChromeWSBridge | None = None,
        on_auth_fatal: Callable[[str], None] | None = None,
    ) -> None:
        self.client = client
        self.role = role
        self.ws_bridge = ws_bridge
        self.on_auth_fatal = on_auth_fatal
        self.secret = secrets.token_urlsafe(16)
        self._prefix = f"/b/{self.secret}"
        self._server: http.server.ThreadingHTTPServer | None = None
        self.port = 0

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}{self._prefix}"

    @property
    def api_url(self) -> str:
        return f"{self.base_url}/api"

    def start(self) -> None:
        bridge = self

        class BridgeHandler(http.server.BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def _forward(self) -> None:
                if not self.path.startswith(bridge._prefix + "/"):
                    self.send_response(403)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return

                upstream_path = self.path[len(bridge._prefix):]
                length = int(self.headers.get("Content-Length") or 0)
                body = self.rfile.read(length) if length > 0 else None
                ctype = self.headers.get("Content-Type")
                auth = self.headers.get("Authorization")
                ctx_props = self.headers.get("X-Context-Properties")

                st, resp_hdrs, raw = bridge.client.raw_rest_request(
                    self.command,
                    upstream_path,
                    body_bytes=body,
                    content_type=ctype,
                    authorization=auth,
                    context_properties=ctx_props,
                )

                if st == 0:
                    err_body = raw or b'{"message":"bridge upstream error"}'
                    self.send_response(502)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(err_body)))
                    self.end_headers()
                    self.wfile.write(err_body)
                    return

                lock_reason = StreamTelemetryClient._detect_account_lock(st, raw)
                if lock_reason and bridge.role == "streamer" and bridge.on_auth_fatal:
                    if "/users/@me" in upstream_path or "/gateway" in upstream_path or "captcha" in lock_reason:
                        bridge.on_auth_fatal(f"Node REST via bridge {lock_reason} on {upstream_path}")

                # Rewrite Gateway URL to local Chrome-TLS WS Bridge when active
                if (
                    st == 200
                    and bridge.ws_bridge is not None
                    and bridge.ws_bridge.active
                    and self.command == "GET"
                    and re.match(r"^/api/v\d+/gateway(?:\?|$)", upstream_path)
                    and raw
                ):
                    try:
                        gw_obj = json.loads(raw.decode("utf-8"))
                        if isinstance(gw_obj, dict) and isinstance(gw_obj.get("url"), str) and gw_obj["url"].startswith("wss://"):
                            bridge.ws_bridge.upstream_host = gw_obj["url"]
                            gw_obj["url"] = bridge.ws_bridge.ws_url
                            raw = json.dumps(gw_obj, separators=(",", ":")).encode("utf-8")
                    except Exception:
                        pass

                self.send_response(st)
                for k, v in resp_hdrs.items():
                    if k.lower() in bridge._HOP_BY_HOP:
                        continue
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(raw)))
                self.send_header("Connection", "close")
                self.end_headers()
                if self.command != "HEAD" and raw:
                    self.wfile.write(raw)

            def do_GET(self) -> None:
                self._forward()

            def do_POST(self) -> None:
                self._forward()

            def do_PUT(self) -> None:
                self._forward()

            def do_PATCH(self) -> None:
                self._forward()

            def do_DELETE(self) -> None:
                self._forward()

            def do_HEAD(self) -> None:
                self._forward()

            def log_message(self, format: str, *args) -> None:
                return

        self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), BridgeHandler)
        self.port = int(self._server.server_address[1])
        threading.Thread(target=self._server.serve_forever, daemon=True).start()

    def stop(self) -> None:
        if self._server:
            try:
                self._server.shutdown()
                self._server.server_close()
            except Exception:
                pass
            self._server = None


class WebRTCSidecar:
    def __init__(
        self, token: str, guild_id: str, channel_id: str, media_file: str,
        quality_key: str, status: str, activities: list[dict], log: Logger,
        telemetry_client: StreamTelemetryClient, role: str = "streamer",
        target_streamer_id: str = "", self_mute: bool = True, self_deaf: bool = False,
        capabilities: int = FALLBACK_DESKTOP_CAPABILITIES,
        on_auth_fatal: Callable[[str], None] | None = None,
    ) -> None:
        self.token = token
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.media_file = media_file
        self.quality_key = quality_key
        self.status = status
        self.activities = activities
        self.log = log
        self.telemetry_client = telemetry_client
        self.role = role
        self.target_streamer_id = target_streamer_id
        self.self_mute = self_mute
        self.self_deaf = self_deaf
        self.capabilities = capabilities
        self.on_auth_fatal = on_auth_fatal

        self.enable_ws_bridge = os.environ.get("ENABLE_WS_TLS_BRIDGE", "1").strip().lower() not in ("0", "false", "no", "off")
        self.ws_bridge = LocalChromeWSBridge(telemetry_client, log=log) if self.enable_ws_bridge else None
        self.bridge = LocalChromeTLSBridge(telemetry_client, role=role, ws_bridge=self.ws_bridge, on_auth_fatal=on_auth_fatal)
        self.joined_event = threading.Event()
        self.meta_event = threading.Event()
        self.sfu_meta: dict = {}
        self.viewer_ids: list[str] = []
        self.proc: subprocess.Popen | None = None
        self.stop_event = threading.Event()
        self.cfg_file = WEBRTC_DIR / f"cfg_{uuid.uuid4().hex[:8]}.json"

    def start(self) -> None:
        if self.ws_bridge is not None:
            if self.ws_bridge.start():
                self.log.ok("ws-bridge", f"[{self.role}] Chrome-TLS Gateway WS bridge active on 127.0.0.1:{self.ws_bridge.port}")
            else:
                self.ws_bridge = None
                self.bridge.ws_bridge = None
        self.bridge.start()
        WEBRTC_DIR.mkdir(parents=True, exist_ok=True)
        (WEBRTC_DIR / "worker.mjs").write_text(_WEBRTC_WORKER_MJS.strip(), encoding="utf-8")
        q_cfg = QUALITY_PRESETS.get(self.quality_key, QUALITY_PRESETS["ultra_lite"])
        include_audio = bool(q_cfg.get("include_audio", False)) and (sys.platform != "darwin")

        tc = self.telemetry_client
        payload = {
            "token": self.token,
            "role": self.role,
            "targetStreamerId": self.target_streamer_id,
            "enableSfuHook": True,
            "guildId": self.guild_id,
            "channelId": self.channel_id,
            "mediaFile": self.media_file,
            "streamType": "go-live",
            "height": q_cfg["height"],
            "fps": q_cfg["fps"],
            "kbps": q_cfg["kbps"],
            "maxKbps": q_cfg["max_kbps"],
            "includeAudio": include_audio,
            "status": self.status,
            "activities": self.activities,
            "selfMute": self.self_mute,
            "selfDeaf": self.self_deaf,
            "capabilities": self.capabilities,
            "superProps": tc.super_props_dict,
            "gatewayProps": tc.gateway_props_dict,
            "superPropsB64": tc.super_props_b64,
            "userAgent": tc.user_agent,
            "locale": tc.locale,
            "timezone": tc.timezone,
            "acceptLanguage": tc.accept_lang,
            "proxy": tc.proxy,
            "restBridgeBase": self.bridge.base_url,
            "restBridgeApiUrl": self.bridge.api_url,
            "wsBridgeActive": bool(self.ws_bridge and self.ws_bridge.active),
        }
        tmp = self.cfg_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, self.cfg_file)

        env = dict(os.environ)
        env["PATH"] = str(ROOT) + os.pathsep + env.get("PATH", "")
        env.setdefault("NODE_OPTIONS", "--max-old-space-size=160")
        self.proc = subprocess.Popen(
            ["node", str(WEBRTC_DIR / "worker.mjs"), str(self.cfg_file)],
            cwd=str(WEBRTC_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
        )
        threading.Thread(target=self._log_reader, daemon=True).start()

    @property
    def is_alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def _log_reader(self) -> None:
        if not self.proc or not self.proc.stdout:
            return
        for line in self.proc.stdout:
            if self.stop_event.is_set():
                break
            txt = line.strip()
            if not txt:
                continue
            if "[WEBRTC-AUTH-FATAL]" in txt:
                reason = txt.split("[WEBRTC-AUTH-FATAL]", 1)[-1].strip()
                self.log.err("auth-guard", f"[{self.role}] {reason}")
                if self.on_auth_fatal:
                    self.on_auth_fatal(f"{self.role}: {reason}")
            elif "[WEBRTC-META]" in txt:
                try:
                    meta = json.loads(txt.split("[WEBRTC-META]", 1)[-1].strip())
                    if isinstance(meta, dict):
                        self.sfu_meta = meta
                        if isinstance(meta.get("viewer_ids"), list):
                            self.viewer_ids = meta["viewer_ids"]
                        self.meta_event.set()
                        self.log.ok(
                            "sfu-bind",
                            f"media_id={meta.get('media_session_id', '')[:8]} · rtc_id={meta.get('rtc_connection_id', '')[:8]} · region={meta.get('stream_region', 'singapore')}",
                        )
                except Exception:
                    pass
            elif "[WEBRTC-VIEWERS]" in txt:
                try:
                    v_list = json.loads(txt.split("[WEBRTC-VIEWERS]", 1)[-1].strip())
                    if isinstance(v_list, list):
                        self.viewer_ids = v_list
                        self.log.ok("sfu-viewers", f"confirmed {len(v_list)} active viewer(s)")
                except Exception:
                    pass
            elif "[WEBRTC-VIEWER-OK]" in txt:
                self.log.ok("alt-webrtc", txt.split("]", 1)[-1].strip())
            elif "[WEBRTC-JOINED]" in txt:
                self.joined_event.set()
                self.log.ok("webrtc", txt.split("]", 1)[-1].strip())
            elif "[WEBRTC-READY]" in txt or "[WEBRTC-LIVE]" in txt or "[WEBRTC-VOICE]" in txt:
                self.log.ok("webrtc", txt.split("]", 1)[-1].strip())
            elif any(tag in txt for tag in ("[WEBRTC-ERROR]", "[WEBRTC-LOGIN-ERR]", "[WEBRTC-VIEWER-ERR]", "Error")):
                self.log.err("webrtc", txt)

    def stop(self) -> None:
        self.stop_event.set()
        if self.proc:
            try:
                self.proc.terminate()
                self.proc.wait(timeout=3)
            except Exception:
                try:
                    self.proc.kill()
                except Exception:
                    pass
            self.proc = None
        self.bridge.stop()
        if self.ws_bridge is not None:
            self.ws_bridge.stop()
        if self.cfg_file.exists():
            try:
                self.cfg_file.unlink()
            except OSError:
                pass


class StreamRunnerStealth:
    def __init__(
        self, token: str, alt_tokens: list[str], guild_id: str, channel_id: str,
        games_pool: list[Game], quality_key: str, heartbeat_sec: float, refresh_hours: float,
        media_url: str, status: str, custom_status: str, locale: str, timezone: str,
        accept_lang: str, spoof_os: str, alt_spoof_os: str,
        device_seed: str, alt_device_seed: str, device_tier: str,
        self_mute: bool, self_deaf: bool, capabilities: int,
        main_proxy: str, alt_proxy: str, enable_science: bool, active_hours_spec: str,
        require_proxy: bool, expected_ip: str, allowed_countries: list[str],
        expected_city: str, expected_region: str, expected_isp: str,
        lock_initial_ip: bool, ip_watchdog_min: float, session_break_min: tuple[float, float],
        log: Logger,
    ) -> None:
        self.token = token
        self.user_id = extract_user_id(token)
        self.alt_tokens = alt_tokens
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.games_pool = list(games_pool)
        self._game_idx = 0
        self.game = self.games_pool[0]
        self.quality_key = quality_key
        self.heartbeat_sec = heartbeat_sec
        self.refresh_base_sec = max(3600.0, refresh_hours * 3600.0)
        self.current_refresh_sec = self._pick_refresh_window()
        self.media_url = media_url
        self.status = status
        self.custom_status = custom_status
        self.locale = locale
        self.timezone = timezone
        self.accept_lang = accept_lang
        self.spoof_os = spoof_os
        self.alt_spoof_os = alt_spoof_os
        self.device_seed = device_seed
        self.alt_device_seed = alt_device_seed
        self.device_tier = device_tier
        self.self_mute = self_mute
        self.self_deaf = self_deaf
        self.capabilities = capabilities
        self.main_proxy = main_proxy
        self.alt_proxy = alt_proxy
        self.enable_science = enable_science
        jitter_m = int(os.environ.get("ACTIVE_HOURS_JITTER_MIN", "18") or "18")
        self.schedule = HumanSchedule(active_hours_spec, timezone, jitter_min=jitter_m, seed=device_seed)
        self.require_proxy = require_proxy
        self.expected_ip = expected_ip
        self.allowed_countries = allowed_countries
        self.expected_city = expected_city
        self.expected_region = expected_region
        self.expected_isp = expected_isp
        self.lock_initial_ip = lock_initial_ip
        self.ip_watchdog_min = ip_watchdog_min
        self.session_break_min = session_break_min
        self.pinned_ip = ""
        self.log = log

        self.telemetry = StreamTelemetryClient(
            token, self.user_id, status=status, locale=locale, timezone=timezone,
            accept_lang=accept_lang, spoof_os=spoof_os, device_seed=device_seed,
            device_tier=device_tier, proxy=main_proxy,
            guild_id=guild_id, channel_id=channel_id,
            device_cookies=os.environ.get("DISCORD_DEVICE_COOKIES", ""),
        )
        self.stop_event = threading.Event()
        self.halted = False
        self.halt_reason = ""
        self.resting = False

        self.sidecar: WebRTCSidecar | None = None
        self.viewer_sidecars: list[tuple[str, WebRTCSidecar]] = []
        self._mp4_file = ""

        self.started_mono = time.monotonic()
        self.real_hours_credited = 0.0
        self.total_heartbeats_ok = 0
        self.reconnect_count = 0

        self._active_gsid = str(uuid.uuid4())
        self._active_meta: dict = {}
        self._session_start_mono = 0.0
        self._session_start_epoch_ms = int(time.time() * 1000)
        self._session_credited_sec = 0.0
        self._last_game_hb_mono = 0.0
        self._next_game_hb_delay = random.uniform(180.0, 300.0)
        self._zero_viewer_since: float | None = None
        self._meta_lock = threading.Lock()

    def _pick_refresh_window(self) -> float:
        return self.refresh_base_sec * random.uniform(0.85, 1.20)

    def _pick_next_hb_delay(self) -> float:
        return max(30.0, self.heartbeat_sec * random.uniform(0.86, 1.16))

    def trigger_halt(self, reason: str) -> None:
        if self.halted:
            return
        self.halted = True
        self.halt_reason = reason
        self.log.err("circuit-breaker", f"HALTING ALL ACTIVITY TO PROTECT ACCOUNT: {reason}")
        self._teardown_sessions()

    def status_snapshot(self) -> dict:
        return {
            "ok": not self.stop_event.is_set() and not self.halted,
            "halted": self.halted,
            "halt_reason": self.halt_reason,
            "resting": self.resting,
            "tls_impersonation": "chrome_curl_cffi_rest_and_ws" if (self.sidecar and self.sidecar.ws_bridge and self.sidecar.ws_bridge.active) else ("chrome_curl_cffi_rest" if HAS_CURL_CFFI else "stdlib_bridge"),
            "ws_tls_bridge_active": bool(self.sidecar and self.sidecar.ws_bridge and self.sidecar.ws_bridge.active),
            "pinned_ip": self.pinned_ip,
            "games_pool_size": len(self.games_pool),
            "cookies_warmed": self.telemetry.cookies_warmed,
            "proxy_enabled": bool(self.main_proxy),
            "egress_ip": self.telemetry.egress_ip,
            "egress_loc": self.telemetry.egress_loc,
            "egress_city": self.telemetry.egress_city,
            "egress_region": self.telemetry.egress_region,
            "egress_isp": self.telemetry.egress_isp,
            "egress_warp": self.telemetry.egress_warp,
            "fake_device": self.telemetry.device.public_summary(),
            "fake_discord_app": self.telemetry.build_summary,
            "locale": self.locale,
            "timezone": self.timezone,
            "science_enabled": self.enable_science,
            "game": self.game.name,
            "quality": self.quality_key,
            "uptime": format_hms(max(0.0, time.monotonic() - self.started_mono)),
            "credited_hours": round(self.real_hours_credited, 4),
            "heartbeats_ok": self.total_heartbeats_ok,
            "active_viewers": len(self.sidecar.viewer_ids) if self.sidecar else 0,
            "reconnects": self.reconnect_count,
            "streamer_alive": bool(self.sidecar and self.sidecar.is_alive),
        }

    def _presence_activities(self) -> list[dict]:
        acts: list[dict] = [{
            "name": self.game.name,
            "type": 0,
            "application_id": self.game.id,
            "timestamps": {"start": self._session_start_epoch_ms},
            "flags": 1,
        }]
        if self.custom_status:
            acts.append({"name": "Custom Status", "type": 4, "state": self.custom_status})
        return acts

    def _spawn_viewer(self, alt_tok: str, idx: int) -> WebRTCSidecar:
        alt_uid = extract_user_id(alt_tok)
        alt_seed = f"{self.alt_device_seed}:{idx}"
        alt_client = StreamTelemetryClient(
            alt_tok, alt_uid, status="online", locale=self.locale, timezone=self.timezone,
            accept_lang=self.accept_lang, spoof_os=self.alt_spoof_os, device_seed=alt_seed,
            device_tier="__alt__", proxy=self.alt_proxy, guild_id=self.guild_id, channel_id=self.channel_id,
            device_cookies=os.environ.get("ALT_DEVICE_COOKIES", ""),
        )
        alt_client.warm_up_cookies()
        self.log.ok(
            "alt-device",
            f"alt #{idx} virtual PC={alt_client.device.tier} ({alt_client.device.gpu_model}) · app={alt_client.build_summary['client_version']}",
        )
        v = WebRTCSidecar(
            token=alt_tok, guild_id=self.guild_id, channel_id=self.channel_id,
            media_file=self._mp4_file, quality_key=self.quality_key, status="online",
            activities=[], log=self.log, telemetry_client=alt_client,
            role="viewer", target_streamer_id=self.user_id,
            self_mute=True, self_deaf=False, capabilities=self.capabilities,
            on_auth_fatal=lambda msg: self.log.err("alt-auth", f"Alt viewer auth error: {msg}"),
        )
        v.start()
        return v

    def _ensure_viewers_alive(self, force_restart_all: bool = False) -> None:
        if not self.alt_tokens or self.halted:
            return
        if force_restart_all:
            for _, old_v in self.viewer_sidecars:
                old_v.stop()
            self.viewer_sidecars.clear()
            if self.stop_event.is_set() or self.halted:
                return
            for idx, tok in enumerate(self.alt_tokens, 1):
                self.log.info(f"restarting alt viewer #{idx}...")
                self.viewer_sidecars.append((tok, self._spawn_viewer(tok, idx)))
            return

        existing = {tok: proc for tok, proc in self.viewer_sidecars}
        updated: list[tuple[str, WebRTCSidecar]] = []
        for idx, tok in enumerate(self.alt_tokens, 1):
            proc = existing.get(tok)
            if proc is None or not proc.is_alive:
                if proc is not None:
                    proc.stop()
                    self.log.warn("alt-watchdog", f"alt viewer #{idx} dropped · reconnecting...")
                updated.append((tok, self._spawn_viewer(tok, idx)))
            else:
                updated.append((tok, proc))
        self.viewer_sidecars = updated

    def _spawn_streamer(self) -> bool:
        if self.halted:
            return False
        if self.sidecar:
            self.sidecar.stop()
            self.sidecar = None
            if self.stop_event.wait(random.uniform(2.0, 4.0)):
                return False
        self._session_start_epoch_ms = int(time.time() * 1000)
        self.telemetry.set_active_channel_referer(self.guild_id, self.channel_id)
        self.sidecar = WebRTCSidecar(
            token=self.token, guild_id=self.guild_id, channel_id=self.channel_id,
            media_file=self._mp4_file, quality_key=self.quality_key, status=self.status,
            activities=self._presence_activities(), log=self.log,
            telemetry_client=self.telemetry, role="streamer",
            self_mute=self.self_mute, self_deaf=self.self_deaf,
            capabilities=self.capabilities, on_auth_fatal=self.trigger_halt,
        )
        self.sidecar.start()
        if not self.sidecar.joined_event.wait(timeout=50.0):
            if not self.halted:
                self.log.warn("stream", "streamer did not join Voice Channel within 50s")
            return False
        self.log.info("waiting for Discord Media SFU media_session_id & rtc_connection_id...")
        if not self.sidecar.meta_event.wait(timeout=40.0):
            if not self.halted:
                self.log.warn("stream", "timed out waiting for SFU Opcode 4 media_session_id")
            return False
        self._session_start_mono = time.monotonic()
        self.current_refresh_sec = self._pick_refresh_window()
        if self.alt_tokens and self.viewer_sidecars and not self.sidecar.viewer_ids:
            deadline = time.monotonic() + 12.0
            while not self.sidecar.viewer_ids and time.monotonic() < deadline and not self.stop_event.is_set() and not self.halted:
                time.sleep(0.4)
        return not self.halted

    def _send_session_start(self) -> None:
        if not self.enable_science or self.halted:
            return
        q_cfg = QUALITY_PRESETS[self.quality_key]
        if not self.sidecar or not self.sidecar.sfu_meta.get("media_session_id"):
            return
        with self._meta_lock:
            self._active_meta = dict(self.sidecar.sfu_meta)
            self._active_gsid = str(uuid.uuid4())
            self._session_start_mono = time.monotonic()
            self._session_credited_sec = 0.0
            self.current_refresh_sec = self._pick_refresh_window()
            meta, gsid = dict(self._active_meta), self._active_gsid
        has_sound = bool(q_cfg.get("include_audio", False)) and (sys.platform != "darwin")
        # Authentic Desktop order: game_launch telemetry first, brief pause, then video_stream_started
        launch_evs = self.telemetry.build_game_launch(
            self.game, gsid, self.guild_id, self.channel_id,
            rtc_id=meta["rtc_connection_id"], media_id=meta["media_session_id"],
        )
        st_launch = self.telemetry.post_science(launch_evs)
        if st_launch in (401, 403) or self.telemetry.last_lock_reason:
            self.trigger_halt(f"science game_launch {self.telemetry.last_lock_reason or f'HTTP {st_launch}'}")
            return
        self._last_game_hb_mono = time.monotonic()
        self._next_game_hb_delay = random.uniform(180.0, 300.0)
        if self.stop_event.wait(random.uniform(2.5, 5.5)) or self.halted:
            return
        evs = self.telemetry.build_stream_start(
            self.game, gsid, self.guild_id, self.channel_id,
            meta["rtc_connection_id"], meta["media_session_id"],
            meta.get("stream_region") or "singapore", meta.get("endpoint_host") or "",
            max(1, len(self.sidecar.viewer_ids)), q_cfg["fps"], q_cfg["height"],
            has_sound=has_sound,
        )
        st = self.telemetry.post_science(evs)
        if st in (401, 403) or self.telemetry.last_lock_reason:
            self.trigger_halt(f"science start {self.telemetry.last_lock_reason or f'HTTP {st}'}")
            return
        if st == 204:
            self.log.ok("stream", f"session started · game={self.game.name} · media={meta['media_session_id'][:8]} · viewers={len(self.sidecar.viewer_ids)}")
        else:
            self.log.warn("stream", f"start telemetry HTTP [{st}]")

    def _send_session_end(self) -> None:
        if not self.enable_science:
            return
        q_cfg = QUALITY_PRESETS[self.quality_key]
        with self._meta_lock:
            meta, gsid, sess_sec = dict(self._active_meta), self._active_gsid, self._session_credited_sec
            self._active_meta, self._session_credited_sec = {}, 0.0
        if meta.get("media_session_id") and sess_sec > 0 and not self.halted:
            try:
                v_cnt = max(1, len(self.sidecar.viewer_ids) if self.sidecar else 1)
                has_sound = bool(q_cfg.get("include_audio", False)) and (sys.platform != "darwin")
                evs = self.telemetry.build_stream_end(
                    self.game, gsid, sess_sec, self.guild_id, self.channel_id,
                    meta["rtc_connection_id"], meta["media_session_id"],
                    meta.get("stream_region") or "singapore", v_cnt, q_cfg["fps"], q_cfg["height"],
                    has_sound=has_sound,
                )
                self.telemetry.post_science(evs)
                self.log.ok("stream", f"closed SFU session cleanly ({sess_sec / 3600.0:.2f}h)")
            except Exception:
                pass

    def _teardown_sessions(self, staggered_alt_leave: bool = False) -> None:
        self._send_session_end()
        if self.sidecar:
            self.sidecar.stop()
            self.sidecar = None
        if staggered_alt_leave and self.viewer_sidecars and not self.halted and not self.stop_event.is_set():
            alt_delay = random.uniform(5.0, 14.0)
            self.log.info(f"streamer left · alt viewer leaving voice naturally in {alt_delay:.1f}s...")
            self.stop_event.wait(alt_delay)
        for _, v in self.viewer_sidecars:
            v.stop()
        self.viewer_sidecars.clear()

    def _loop(self) -> None:
        q_cfg = QUALITY_PRESETS[self.quality_key]
        fps, height = q_cfg["fps"], q_cfg["height"]
        has_sound = bool(q_cfg.get("include_audio", False)) and (sys.platform != "darwin")

        next_hb_target = self._pick_next_hb_delay()
        last_hb_mono = time.monotonic()
        hb_idx = 0

        while not self.stop_event.is_set() and not self.halted:
            if not in_active_window(self.schedule, self.timezone):
                if not self.resting:
                    self.resting = True
                    self.log.info("outside ACTIVE_HOURS window · pausing stream cleanly until next active period")
                    self._teardown_sessions(staggered_alt_leave=True)
                self.stop_event.wait(60.0)
                continue

            if self.resting:
                self.resting = False
                wake_jitter = random.uniform(30.0, 240.0)
                self.log.info(f"entering ACTIVE_HOURS · waking up in {wake_jitter:.0f}s...")
                if self.stop_event.wait(wake_jitter):
                    break
                # Fresh Desktop cold-boot identity & egress check after daily rest
                self.telemetry.rotate_launch_session()
                self._rotate_game_if_configured()
                if not self._check_egress_runtime(tag="wake-guard"):
                    break
                if self.enable_science:
                    try:
                        self.telemetry.desktop_cold_boot_read()
                        self.telemetry.post_science(self.telemetry.build_app_opened())
                        self.log.ok("app-boot", f"fresh Desktop launch after rest · game={self.game.name}")
                    except Exception:
                        pass

            if self.sidecar is None:
                if self._spawn_streamer():
                    if self.alt_tokens and not self.viewer_sidecars:
                        stagger = random.uniform(8.0, 18.0)
                        self.log.info(f"staggering alt viewer join by {stagger:.1f}s (humanized timing)...")
                        if self.stop_event.wait(stagger) or self.halted:
                            break
                        self._ensure_viewers_alive()
                        wait_end = time.monotonic() + 25.0
                        while self.sidecar and not self.sidecar.viewer_ids and time.monotonic() < wait_end and not self.stop_event.is_set() and not self.halted:
                            time.sleep(0.5)
                    self._send_session_start()
                    last_hb_mono = time.monotonic()
                    next_hb_target = self._pick_next_hb_delay()
                else:
                    if self.halted:
                        break
                    self.reconnect_count += 1
                    backoff = min(120.0, 15.0 * self.reconnect_count + random.uniform(3.0, 10.0))
                    self.log.warn("stream", f"connect retry #{self.reconnect_count} in {backoff:.0f}s...")
                    self.stop_event.wait(backoff)
                    continue

            if self.stop_event.wait(5.0) or self.halted:
                break

            self._ensure_viewers_alive()

            if not self.sidecar or not self.sidecar.is_alive:
                if self.halted:
                    break
                self.reconnect_count += 1
                backoff = min(90.0, 12.0 + random.uniform(5.0, 15.0))
                self.log.warn("stream", f"streamer dropped (#{self.reconnect_count}) · healing in {backoff:.0f}s...")
                self._send_session_end()
                self.sidecar = None
                self.stop_event.wait(backoff)
                continue

            sfu_viewers = len(self.sidecar.viewer_ids)
            if sfu_viewers == 0:
                if self._zero_viewer_since is None:
                    self._zero_viewer_since = time.monotonic()
                    self.log.warn("sfu-viewer", "v=0 on SFU · waiting for alt viewer...")
                elif self.alt_tokens and (time.monotonic() - self._zero_viewer_since) >= 60.0:
                    self.log.warn("alt-watchdog", "v=0 for 60s · restarting alt viewer(s)...")
                    self._zero_viewer_since = time.monotonic()
                    self._ensure_viewers_alive(force_restart_all=True)
            else:
                if self._zero_viewer_since is not None:
                    self.log.ok("sfu-viewer", f"{sfu_viewers} active viewer(s) confirmed · resuming heartbeats")
                    self._zero_viewer_since = None
                    last_hb_mono = time.monotonic()

            if self._session_start_mono > 0 and (time.monotonic() - self._session_start_mono) >= self.current_refresh_sec:
                self.log.info(f"refreshing SFU session after {self.current_refresh_sec / 3600.0:.2f}h...")
                self._teardown_sessions(staggered_alt_leave=True)
                self._rotate_game_if_configured()
                brk_min, brk_max = self.session_break_min
                if brk_max > 0:
                    pause_s = random.uniform(brk_min * 60.0, brk_max * 60.0)
                    self.log.info(f"taking human inter-session break ({pause_s / 60.0:.1f} min) · next game={self.game.name}")
                    if self.stop_event.wait(pause_s):
                        break
                else:
                    if self.stop_event.wait(random.uniform(8.0, 18.0)):
                        break
                continue

            now_mono = time.monotonic()
            delta_sec = now_mono - last_hb_mono
            if delta_sec < next_hb_target:
                continue
            last_hb_mono = now_mono
            next_hb_target = self._pick_next_hb_delay()

            if sfu_viewers == 0:
                continue

            if not self.enable_science:
                self.real_hours_credited += delta_sec / 3600.0
                self.total_heartbeats_ok += 1
                hb_idx += 1
                self.log.ok(f"tick#{hb_idx:04d}", f"{self.game.name} +{delta_sec:.0f}s (passive mode) | total={format_hms(self.real_hours_credited * 3600)} | v={sfu_viewers}")
                continue

            if self.telemetry.is_stale:
                try:
                    self.telemetry.ensure_analytics_token()
                except AuthFatalError as exc:
                    self.trigger_halt(str(exc))
                    break
                except Exception:
                    pass

            with self._meta_lock:
                meta, gsid = dict(self._active_meta), self._active_gsid
                cum_sec = self._session_credited_sec + delta_sec
            if not meta.get("media_session_id"):
                continue

            hb_idx += 1
            include_game_hb = (now_mono - self._last_game_hb_mono) >= self._next_game_hb_delay
            evs = self.telemetry.build_stream_heartbeat(
                self.game, gsid, delta_sec, cum_sec,
                self.guild_id, self.channel_id,
                meta["rtc_connection_id"], meta["media_session_id"],
                meta.get("stream_region") or "singapore",
                viewer_count=sfu_viewers, fps=fps, height=height,
                has_sound=has_sound, include_game_hb=include_game_hb,
            )
            st = self.telemetry.post_science(evs)
            if st in (401, 403) or self.telemetry.last_lock_reason:
                try:
                    self.telemetry.ensure_analytics_token()
                    st = self.telemetry.post_science(evs)
                except AuthFatalError as exc:
                    self.trigger_halt(str(exc))
                    break
                except Exception:
                    pass
                if st in (401, 403) or self.telemetry.last_lock_reason:
                    self.trigger_halt(f"science heartbeat {self.telemetry.last_lock_reason or f'HTTP {st}'}")
                    break
            if st == 204 and include_game_hb:
                self._last_game_hb_mono = now_mono
                self._next_game_hb_delay = random.uniform(180.0, 300.0)

            if st == 204:
                with self._meta_lock:
                    self._session_credited_sec += delta_sec
                self.real_hours_credited += delta_sec / 3600.0
                self.total_heartbeats_ok += 1
                self.log.ok(
                    f"hb#{hb_idx:04d}",
                    f"{self.game.name} +{delta_sec:.0f}s | state={self.telemetry.focus_sim.current_state()} | total={format_hms(self.real_hours_credited * 3600)} | v={sfu_viewers} | ping={self.telemetry.last_ping_ms:.0f}ms",
                )
            else:
                self.log.warn(f"hb#{hb_idx:04d}", f"HTTP [{st}] · retrying next tick")

    def _rotate_game_if_configured(self) -> None:
        if len(self.games_pool) <= 1:
            return
        self._game_idx = (self._game_idx + 1) % len(self.games_pool)
        self.game = self.games_pool[self._game_idx]
        self.log.ok("game-rotate", f"switched active game to {self.game.name} ({self.game.exe})")

    def _check_egress_runtime(self, tag: str = "ip-watchdog") -> bool:
        try:
            exp_ip = self.expected_ip or (self.pinned_ip if self.lock_initial_ip else "")
            eg = self.telemetry.verify_egress_ip(
                require_proxy=self.require_proxy,
                expected_ip=exp_ip,
                allowed_countries=self.allowed_countries,
                expected_city=self.expected_city,
                expected_region=self.expected_region,
                expected_isp=self.expected_isp,
            )
            self.log.ok(tag, f"egress ok · ip={eg['ip']} · city={eg.get('city') or 'n/a'} · loc={eg['loc']}")
            return True
        except EgressGuardError as exc:
            self.trigger_halt(f"Runtime IP Drift detected ({tag}): {exc}")
            return False

    def _ip_watchdog_loop(self) -> None:
        if self.ip_watchdog_min <= 0:
            return
        while not self.stop_event.is_set() and not self.halted:
            wait_s = max(120.0, self.ip_watchdog_min * 60.0 * random.uniform(0.82, 1.18))
            if self.stop_event.wait(wait_s) or self.halted:
                break
            if self.resting:
                continue
            self._check_egress_runtime(tag="ip-watchdog")

    def start(self) -> bool:
        dev = self.telemetry.device
        bld = self.telemetry.build_summary
        self.log.ok("fake-device", f"tier={dev.tier} · {dev.os_name} {dev.os_version} · {dev.cpu_brand} ({dev.cpu_cores}c) · {dev.ram_total_mb}MB · {dev.gpu_model}")
        self.log.ok("fake-app", f"Discord Desktop {bld['client_version']} (build={bld['client_build_number']}, native={bld['native_build_number']}, Electron/{bld['electron_version']}, Chrome/{bld['chrome_version']}, src={bld['source']})")

        if HAS_CURL_CFFI:
            self.log.ok("tls", "curl_cffi Chrome TLS/HTTP2 + Node REST Bridge active")
        else:
            self.log.warn("tls", "curl_cffi not installed · using stdlib HTTPS fallback")

        # 1. Fail-Closed Egress IP, Country, City & ISP Guard
        try:
            eg = self.telemetry.verify_egress_ip(
                require_proxy=self.require_proxy,
                expected_ip=self.expected_ip,
                allowed_countries=self.allowed_countries,
                expected_city=self.expected_city,
                expected_region=self.expected_region,
                expected_isp=self.expected_isp,
            )
            self.pinned_ip = eg["ip"]
            geo_label = ", ".join(x for x in (eg.get("city"), eg.get("region"), eg.get("loc")) if x) or eg.get("loc", "")
            self.log.ok("ip-guard", f"egress verified & pinned · ip={eg['ip']} · location={geo_label} · isp={eg.get('isp') or 'n/a'} · warp={eg['warp']}")
        except EgressGuardError as exc:
            self.trigger_halt(f"Egress IP Guard blocked startup: {exc}")
            return True

        # 2. Organic Cookie Warm-up (/app) bound to active IP + Chrome TLS
        ck_count = self.telemetry.warm_up_cookies()
        self.log.ok("cookies", f"warmed {ck_count} organic Discord/Cloudflare cookie(s) via Chrome TLS")

        # 3. Authentic Desktop Cold-Boot sequence (/users/@me -> settings-proto -> app_opened)
        if self.enable_science:
            try:
                username = self.telemetry.ensure_analytics_token()
                self.log.ok("auth", f"authenticated as {username} ({self.user_id}) · locale={self.locale} · tz={self.timezone} · ping {self.telemetry.last_ping_ms:.0f}ms")
                time.sleep(random.uniform(0.9, 2.1))
                boot_map = self.telemetry.desktop_cold_boot_read()
                self.log.ok("app-boot", f"Desktop cold-boot reads complete ({boot_map})")
                time.sleep(random.uniform(0.8, 1.8))
                boot_st = self.telemetry.post_science(self.telemetry.build_app_opened())
                if boot_st == 204:
                    self.log.ok("app-boot", "sent Desktop app_opened cold-boot telemetry")
            except AuthFatalError as exc:
                self.trigger_halt(str(exc))
                return True
        else:
            self.log.ok("auth", f"passive WebRTC mode (no Python REST /science calls) · uid={self.user_id}")

        if not is_webrtc_sidecar_ready():
            if not install_webrtc_stack(self.log):
                return False
        self._mp4_file = prepare_playable_mp4(self.media_url, self.quality_key, self.log)
        self.started_mono = time.monotonic()
        if self.ip_watchdog_min > 0:
            threading.Thread(target=self._ip_watchdog_loop, daemon=True).start()
        threading.Thread(target=self._loop, daemon=True).start()
        return True

    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        self._teardown_sessions()


def start_health_server(port: int, runner: StreamRunnerStealth, log: Logger) -> http.server.ThreadingHTTPServer:
    class HealthHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            body = json.dumps(runner.status_snapshot()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_HEAD(self) -> None:
            self.send_response(200)
            self.end_headers()

        def log_message(self, format: str, *args) -> None:
            return

    server = http.server.ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.ok("http", f"health endpoint listening on 0.0.0.0:{port}")
    return server


def main() -> int:
    log = Logger()
    if "--setup-only" in sys.argv:
        ok = install_webrtc_stack(log)
        if ok:
            try:
                q = os.environ.get("STREAM_QUALITY", "ultra_lite").strip().lower()
                if q not in QUALITY_PRESETS:
                    q = "ultra_lite"
                prepare_playable_mp4(os.environ.get("STREAM_MEDIA_URL", ""), q, log)
            except Exception as exc:
                log.warn("setup", f"pre-cache mp4 skipped ({exc})")
        return 0 if ok else 1

    token = os.environ.get("DISCORD_TOKEN", "").strip()
    alt_tokens = [t.strip() for t in os.environ.get("ALT_TOKEN", "").split(",") if t.strip()]
    guild_id = os.environ.get("GUILD_ID", "").strip()
    channel_id = os.environ.get("CHANNEL_ID", "").strip()
    game_query = os.environ.get("STREAM_GAME", "Grand Theft Auto: Vice City").strip()
    quality_key = os.environ.get("STREAM_QUALITY", "ultra_lite").strip().lower()
    if quality_key not in QUALITY_PRESETS:
        quality_key = "ultra_lite"
    media_url = os.environ.get("STREAM_MEDIA_URL", "").strip()
    status = os.environ.get("STREAM_STATUS", "online").strip().lower()
    if status not in ("online", "idle", "dnd", "invisible"):
        status = "online"
    custom_status = os.environ.get("CUSTOM_STATUS", "").strip()

    # Fake Device & Fake Discord App Configuration
    locale = os.environ.get("DISCORD_LOCALE", "vi").strip() or "vi"
    timezone = os.environ.get("DISCORD_TIMEZONE", "Asia/Ho_Chi_Minh").strip() or "Asia/Ho_Chi_Minh"
    accept_lang = os.environ.get("ACCEPT_LANGUAGE", "vi-VN,vi;q=0.9,en-US;q=0.8").strip()
    spoof_os = os.environ.get("SPOOF_OS", "windows").strip().lower()
    if spoof_os not in ("windows", "macos"):
        spoof_os = "windows"
    alt_spoof_os = os.environ.get("ALT_SPOOF_OS", spoof_os).strip().lower()
    if alt_spoof_os not in ("windows", "macos"):
        alt_spoof_os = spoof_os
    device_seed = os.environ.get("DEVICE_SEED", "vn-home-pc-01").strip()
    alt_device_seed = os.environ.get("ALT_DEVICE_SEED", "vn-alt-laptop-02").strip()
    device_tier = os.environ.get("DEVICE_TIER", "").strip().lower()
    self_mute = os.environ.get("SELF_MUTE", "1").strip().lower() not in ("0", "false", "no", "off")
    self_deaf = os.environ.get("SELF_DEAF", "0").strip().lower() in ("1", "true", "yes", "on")
    try:
        capabilities = int(os.environ.get("DISCORD_CAPABILITIES", str(FALLBACK_DESKTOP_CAPABILITIES)))
    except ValueError:
        capabilities = FALLBACK_DESKTOP_CAPABILITIES

    # Egress IP Pinning & Guard
    main_proxy = os.environ.get("DISCORD_PROXY", "").strip()
    alt_proxy = os.environ.get("ALT_PROXY", "").strip() or main_proxy
    require_proxy = os.environ.get("REQUIRE_PROXY", "0").strip().lower() in ("1", "true", "yes", "on")
    expected_ip = os.environ.get("EXPECTED_IP", "").strip()
    allowed_countries = [c.strip().upper() for c in os.environ.get("ALLOWED_COUNTRIES", "").split(",") if c.strip()]
    expected_city = os.environ.get("EXPECTED_CITY", "").strip()
    expected_region = os.environ.get("EXPECTED_REGION", "").strip()
    expected_isp = os.environ.get("EXPECTED_ISP", "").strip()
    lock_initial_ip = os.environ.get("LOCK_INITIAL_IP", "1" if require_proxy else "0").strip().lower() in ("1", "true", "yes", "on")
    try:
        ip_watchdog_min = max(0.0, float(os.environ.get("IP_WATCHDOG_MIN", "25")))
    except ValueError:
        ip_watchdog_min = 25.0

    brk_spec = os.environ.get("SESSION_BREAK_MIN", "3-9").strip()
    session_break_min = (0.0, 0.0)
    if brk_spec and brk_spec != "0":
        m_brk = re.match(r"^(\d+(?:\.\d+)?)\s*-\s*(\d+(?:\.\d+)?)$", brk_spec)
        if m_brk:
            a, b = float(m_brk.group(1)), float(m_brk.group(2))
            session_break_min = (min(a, b), max(a, b))
        else:
            try:
                v_b = max(0.0, float(brk_spec))
                session_break_min = (v_b, v_b)
            except ValueError:
                session_break_min = (0.0, 0.0)

    enable_science = os.environ.get("ENABLE_SCIENCE_TELEMETRY", "1").strip().lower() not in ("0", "false", "no", "off")
    active_hours_spec = os.environ.get("ACTIVE_HOURS", "").strip()

    try:
        hb_sec = max(45.0, min(300.0, float(os.environ.get("STREAM_HB_SEC", "60"))))
    except ValueError:
        hb_sec = 60.0
    try:
        refresh_hours = max(2.0, float(os.environ.get("STREAM_REFRESH_HOURS", "8")))
    except ValueError:
        refresh_hours = 8.0

    if not token or not guild_id or not channel_id:
        log.err("config", "missing required env vars: DISCORD_TOKEN, GUILD_ID, CHANNEL_ID")
        return 1

    games = load_games()
    if not games:
        log.err("games", "could not load game catalog from Discord CDN")
        return 1

    rot_spec = os.environ.get("GAME_ROTATION", "").strip()
    rot_queries = [q.strip() for q in rot_spec.split(",") if q.strip()] if rot_spec else [game_query]
    games_pool: list[Game] = []
    for rq in rot_queries:
        g = pick_game(games, rq)
        if g not in games_pool:
            games_pool.append(g)
    if not games_pool:
        games_pool = [pick_game(games, game_query)]
    log.ok("game", f"loaded {len(games_pool)} game(s): " + ", ".join(f"{g.name} ({g.exe})" for g in games_pool))

    runner = StreamRunnerStealth(
        token=token, alt_tokens=alt_tokens, guild_id=guild_id, channel_id=channel_id,
        games_pool=games_pool, quality_key=quality_key, heartbeat_sec=hb_sec,
        refresh_hours=refresh_hours, media_url=media_url, status=status,
        custom_status=custom_status, locale=locale, timezone=timezone,
        accept_lang=accept_lang, spoof_os=spoof_os, alt_spoof_os=alt_spoof_os,
        device_seed=device_seed, alt_device_seed=alt_device_seed, device_tier=device_tier,
        self_mute=self_mute, self_deaf=self_deaf, capabilities=capabilities,
        main_proxy=main_proxy, alt_proxy=alt_proxy, enable_science=enable_science,
        active_hours_spec=active_hours_spec,
        require_proxy=require_proxy, expected_ip=expected_ip,
        allowed_countries=allowed_countries,
        expected_city=expected_city, expected_region=expected_region,
        expected_isp=expected_isp, lock_initial_ip=lock_initial_ip,
        ip_watchdog_min=ip_watchdog_min, session_break_min=session_break_min,
        log=log,
    )

    health_srv = None
    port_env = os.environ.get("PORT", "").strip()
    if port_env.isdigit():
        health_srv = start_health_server(int(port_env), runner, log)

    stop_sig = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, lambda *_: stop_sig.set())
        except (ValueError, OSError):
            pass

    def _cleanup() -> None:
        runner.stop()
        if health_srv:
            try:
                health_srv.shutdown()
            except Exception:
                pass

    atexit.register(_cleanup)

    if not runner.start():
        return 1

    log.ok(
        "ready",
        f"stealth v7.1 live · Guild {guild_id} › Channel {channel_id} · quality={quality_key} · science={enable_science} · proxy={bool(main_proxy)} · schedule={active_hours_spec or '24/7'}",
    )
    stop_sig.wait()
    log.info("shutting down cleanly...")
    _cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
