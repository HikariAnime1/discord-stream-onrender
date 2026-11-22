#!/usr/bin/env python3
"""
Standalone Lightweight 24/7 Discord Go-Live Streamer + Auto Alt Viewer
Optimized for Render (Web Service health endpoint + low-RAM defaults) & GitHub Actions.

Usage:
  Setup dependencies (recommended during build):
    python -u stream_render.py --setup-only
  Run 24/7 stream:
    python -u stream_render.py
"""
from __future__ import annotations

import atexit
import base64
import gzip
import hashlib
import http.client
import http.server
import itertools
import json
import os
import random
import re
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
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace", line_buffering=True)
    except (AttributeError, OSError):
        pass

ROOT = Path(__file__).resolve().parent
WEBRTC_DIR = ROOT / ".webrtc_engine"
MEDIA_CACHE_DIR = ROOT / ".media_cache"
GAMES_CACHE_FILE = ROOT / "games_compact.json"

os.environ["PATH"] = str(ROOT) + os.pathsep + os.environ.get("PATH", "")

DISCORD_HOST = "discord.com"
PROPERTIES_URL = "https://cordapi.dolfi.es/api/v2/properties/windows"
GAMES_CDN_URL = "https://cdn.discordapp.com/detectables/games.json"

AUTH_MAX_AGE = 12 * 3600
CLIENT_VERSION = "1.0.9253"
CLIENT_BUILD_NUMBER = 594031
NATIVE_BUILD_NUMBER = 88414
OS_VERSION = "10.0.26200"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) discord/1.0.9253 Chrome/148.0.7778.280 "
    "Electron/42.7.1 Safari/537.36"
)

_PACK_FP_STRUCT = struct.Struct("<BQII32s32s32s32s")

QUALITY_PRESETS = {
    "ultra_lite": {
        "width": 256,
        "height": 144,
        "fps": 5,
        "kbps": 50,
        "max_kbps": 100,
        "include_audio": False,
        "static_black": True,
    },
    "render_lite": {
        "width": 480,
        "height": 270,
        "fps": 8,
        "kbps": 150,
        "max_kbps": 250,
        "include_audio": True,
        "static_black": False,
    },
    "farm10fps": {
        "width": 640,
        "height": 360,
        "fps": 10,
        "kbps": 250,
        "max_kbps": 400,
        "include_audio": True,
        "static_black": False,
    },
    "720p30": {
        "width": 1280,
        "height": 720,
        "fps": 30,
        "kbps": 2200,
        "max_kbps": 3500,
        "include_audio": True,
        "static_black": False,
    },
    "720p60": {
        "width": 1280,
        "height": 720,
        "fps": 60,
        "kbps": 3200,
        "max_kbps": 4800,
        "include_audio": True,
        "static_black": False,
    },
    "1080p30": {
        "width": 1920,
        "height": 1080,
        "fps": 30,
        "kbps": 4000,
        "max_kbps": 6000,
        "include_audio": True,
        "static_black": False,
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
    "black myth", "resident evil", "final fantasy", "god of war", "spider-man",
    "forza horizon", "the witcher 3", "terraria", "stardew valley", "hollow knight",
    "dead by daylight", "destiny 2", "warframe", "path of exile", "diablo",
    "rust", "ark:", "palworld", "honkai", "wuthering waves", "ea sports fc",
)

# Native C++ WebRTC Sidecar Worker (Streamer + Alt Viewer)
_WEBRTC_WORKER_MJS = r"""
import { Client } from "discord.js-selfbot-v13";
import { Streamer, prepareStream, playStream, Utils, Encoders } from "@dank074/discord-video-stream";
import crypto from "crypto";
import fs from "fs";
import path from "path";
import { createRequire } from "module";

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

    if (u.includes("gateway.discord.gg") && pkt.op === 2 && pkt.d && cfg.superProps) {
      pkt.d.properties = { ...(pkt.d.properties || {}), ...cfg.superProps };
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

const client = new Client({ checkUpdate: false });
let voiceSessionId = "";
let viewerStreamRtcServerId = "";

client.on("raw", (pkt) => {
  try {
    const t = pkt.t;
    const d = pkt.d;
    if (!d) return;
    if (t === "VOICE_STATE_UPDATE" && client.user && String(d.user_id) === String(client.user.id)) {
      if (d.session_id) voiceSessionId = d.session_id;
      if (cfg.role === "viewer" && !d.channel_id && !isShuttingDown) {
        closeViewerStream();
        console.log(`[WEBRTC-VIEWER-ERR] Alt viewer left voice channel, scheduling auto-rejoin...`);
        setTimeout(async () => {
          if (isShuttingDown) return;
          try {
            const tg = cfg.guildId && cfg.guildId.trim() !== "" ? cfg.guildId : null;
            await streamer.joinVoice(tg, cfg.channelId);
            console.log(`[WEBRTC-JOINED] Alt Viewer re-joined Voice Channel ${cfg.channelId}`);
          } catch (e) {
            console.error(`[WEBRTC-VIEWER-ERR] Alt rejoin failed: ${e?.message || e}`);
          }
        }, 3500);
      }
    } else if (t === "VOICE_SERVER_UPDATE" && String(d.guild_id || "") === String(cfg.guildId || "")) {
      const ep = String(d.endpoint || "").replace(/^wss?:\/\//, "").split(":")[0];
      if (ep) voiceHost = ep;
    } else if (t === "STREAM_CREATE") {
      if (cfg.role === "viewer") {
        const targetKey = `guild:${cfg.guildId}:${cfg.channelId}:${cfg.targetStreamerId}`;
        if (d.stream_key === targetKey) {
          if (d.rtc_server_id) viewerStreamRtcServerId = String(d.rtc_server_id);
          setTimeout(() => {
            try {
              if (!isShuttingDown && (!viewerStreamWs || viewerStreamWs.readyState !== 1)) {
                client.ws?.shards?.first()?.send({ op: 20, d: { stream_key: targetKey } });
              }
            } catch (_) {}
          }, 300);
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

function applyRichPresence() {
  try {
    if (isShuttingDown) return;
    const shard = client.ws?.shards?.first();
    if (cfg.activities && cfg.activities.length > 0 && shard) {
      shard.send({
        op: 3,
        d: { since: Date.now(), activities: cfg.activities, status: cfg.status || "online", afk: false },
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

function gracefulShutdown() {
  if (isShuttingDown) return;
  isShuttingDown = true;
  try {
    if (currentCommand) { try { currentCommand.kill("SIGKILL"); } catch (_) {} }
    closeViewerStream();
    const shard = client.ws?.shards?.first();
    if (shard) {
      if (currentStreamKey) shard.send({ op: 19, d: { stream_key: currentStreamKey } });
      shard.send({ op: 4, d: { guild_id: null, channel_id: null, self_mute: true, self_deaf: true, self_video: false } });
    }
  } catch (_) {}
  setTimeout(() => process.exit(0), 250);
}

process.on("SIGTERM", gracefulShutdown);
process.on("SIGINT", gracefulShutdown);

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
    const ws = new WebSocket(`wss://${host}/?v=8`);
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
  applyRichPresence();
  setInterval(applyRichPresence, 20000);

  const targetGuild = cfg.guildId && cfg.guildId.trim() !== "" ? cfg.guildId : null;

  try {
    if (cfg.role === "viewer") {
      console.log(`[WEBRTC-VOICE] Alt Viewer joining Guild ${targetGuild} › Channel ${cfg.channelId}...`);
      await streamer.joinVoice(targetGuild, cfg.channelId);
      console.log(`[WEBRTC-JOINED] Alt Viewer connected to Voice Channel ${cfg.channelId}`);
      const streamKey = `guild:${cfg.guildId}:${cfg.channelId}:${cfg.targetStreamerId}`;
      const watchTick = () => {
        try {
          if (isShuttingDown) return;
          if (viewerStreamWs && !viewerConnected && viewerConnectStartedAt > 0 && (Date.now() - viewerConnectStartedAt) > 25000) {
            closeViewerStream();
          }
          if (!viewerStreamWs || viewerStreamWs.readyState !== 1) {
            client.ws?.shards?.first()?.send({ op: 20, d: { stream_key: streamKey } });
          }
        } catch (_) {}
      };
      watchTick();
      setInterval(watchTick, 3000);
      return;
    }

    console.log(`[WEBRTC-VOICE] Connecting to Guild ${targetGuild} › Channel ${cfg.channelId}...`);
    await streamer.joinVoice(targetGuild, cfg.channelId);
    console.log(`[WEBRTC-JOINED] Voice/Video session established on Channel ${cfg.channelId}`);

    while (!isShuttingDown) {
      const encoder = Encoders.software({
        x264: { preset: "ultrafast" },
        x265: { preset: "ultrafast" },
      });

      const { command, output } = prepareStream(cfg.mediaFile, {
        encoder,
        height: Number(cfg.height) || 360,
        frameRate: Number(cfg.fps) || 10,
        bitrateVideo: Number(cfg.kbps) || 250,
        bitrateVideoMax: Number(cfg.maxKbps) || 400,
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

      console.log(`[WEBRTC-LIVE] Streaming ${cfg.height || 360}p${cfg.fps || 10} (${cfg.streamType}) · DAVE E2EE active`);

      try {
        await playStream(output, streamer, { type: cfg.streamType });
      } catch (err) {
        if (!isShuttingDown) {
          console.error(`[WEBRTC-ERROR] playStream: ${err?.message || err}`);
        }
      }

      if (isShuttingDown) break;
      await new Promise((r) => setTimeout(r, 500));
    }
  } catch (err) {
    console.error(`[WEBRTC-ERROR] ${err?.message || err}`);
    process.exit(1);
  }
});

client.login(cfg.token).catch((err) => {
  console.error(`[WEBRTC-LOGIN-ERR] ${err?.message || err}`);
  process.exit(1);
});
"""


class Logger:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    def _emit(self, level: str, tag: str, msg: str) -> None:
        ts = time.strftime("%H:%M:%S")
        with self._lock:
            print(f"[{ts}] [{level}] [{tag}] {msg}", flush=True)

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


def format_hms(seconds: float) -> str:
    s = max(0, int(seconds))
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def extract_user_id(token: str) -> str:
    try:
        first = token.split(".", 1)[0]
        return base64.b64decode(first + "=" * (-len(first) % 4)).decode("utf-8")
    except Exception:
        return ""


def get_machine_guid() -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(uuid.getnode())))


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
                log.info("downloading lightweight BtbN FFmpeg build (with libzmq)...")
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
            "name": "revere-webrtc-lite",
            "version": "1.0.0",
            "private": True,
            "type": "module",
            "dependencies": {
                "@dank074/discord-video-stream": "latest",
                "discord.js-selfbot-v13": "latest",
                "ws": "latest",
            },
        }
        (WEBRTC_DIR / "package.json").write_text(json.dumps(pkg_json, indent=2), encoding="utf-8")
        (WEBRTC_DIR / "worker.mjs").write_text(_WEBRTC_WORKER_MJS.strip(), encoding="utf-8")

        pkg_dir = WEBRTC_DIR / "node_modules" / "@dank074" / "discord-video-stream"
        if not pkg_dir.exists():
            log.info("installing WebRTC sidecar npm packages...")
            npm_bin = "npm.cmd" if os.name == "nt" else "npm"
            subprocess.run(
                [npm_bin, "install", "--omit=dev", "--no-audit", "--no-fund"],
                cwd=str(WEBRTC_DIR),
                check=True,
            )
        log.ok("setup", "WebRTC sidecar ready")
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
    use_black = bool(q_cfg.get("static_black", False))

    src_tag = src or ("black" if use_black else "testsrc")
    cache_hash = hashlib.sha256(f"lite2:{quality_key}:{src_tag}:{w}x{h}@{fps}".encode("utf-8")).hexdigest()[:16]
    cached_mp4 = MEDIA_CACHE_DIR / f"media_{cache_hash}.mp4"
    if cached_mp4.exists() and cached_mp4.stat().st_size > 1_024:
        log.ok("media", f"loaded cached {h}p{fps} MP4 ({cached_mp4.name})")
        return str(cached_mp4)

    vf_filter = f"scale={w}:{h}:force_original_aspect_ratio=decrease,pad={w}:{h}:(ow-iw)/2:(oh-ih)/2,format=yuv420p"

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
            "-pix_fmt", "yuv420p", "-b:v", f"{kbps}k", "-r", str(fps), "-g", str(fps * 2),
            "-c:a", "aac", "-ar", "48000", "-ac", "2", "-b:a", "96k",
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
                req = urllib.request.Request(src, headers={"User-Agent": USER_AGENT})
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
        log.warn("media", f"fallback to SMPTE {h}p{fps} test pattern ({exc})")
    finally:
        if temp_in and temp_in.exists():
            try:
                temp_in.unlink()
            except OSError:
                pass

    lavfi_video = f"color=c=black:s={w}x{h}:r={fps}" if use_black else f"testsrc=size={w}x{h}:rate={fps}"
    subprocess.run(
        [
            ff_bin, "-y", "-hide_banner", "-loglevel", "error", "-threads", "1",
            "-f", "lavfi", "-i", lavfi_video,
            "-f", "lavfi", "-i", "anullsrc=r=48000:cl=stereo",
            "-t", "10",
            "-c:v", "libx264", "-preset", "ultrafast", "-profile:v", "baseline", "-level", "3.1",
            "-pix_fmt", "yuv420p", "-b:v", f"{kbps}k", "-maxrate", f"{max_kbps}k", "-bufsize", f"{max_kbps * 2}k",
            "-r", str(fps), "-g", str(fps * 2),
            "-c:a", "aac", "-ar", "48000", "-ac", "2", "-b:a", "64k",
            "-movflags", "+faststart",
            str(cached_mp4),
        ],
        check=False,
        timeout=25,
    )
    if not cached_mp4.exists() or cached_mp4.stat().st_size == 0:
        raise RuntimeError(f"FFmpeg could not generate MP4 at {cached_mp4}")
    log.ok("media", f"generated {h}p{fps} MP4 ({cached_mp4.stat().st_size // 1024} KB)")
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
        req = urllib.request.Request(GAMES_CDN_URL, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"})
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


def fetch_super_props_dict(launch_sig: str, hb_session: str) -> dict:
    try:
        req = urllib.request.Request(
            PROPERTIES_URL,
            method="POST",
            data=b"{}",
            headers={"content-type": "application/json", "user-agent": USER_AGENT, "accept-encoding": "gzip"},
        )
        with urllib.request.urlopen(req, timeout=6) as resp:
            raw = resp.read()
            if resp.headers.get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
            props = json.loads(raw.decode("utf-8"))["properties"]
    except Exception:
        props = {
            "os": "Windows", "browser": "Discord Client", "release_channel": "stable",
            "client_version": CLIENT_VERSION, "os_version": OS_VERSION, "os_arch": "x64",
            "app_arch": "x64", "system_locale": "en-GB", "has_client_mods": False,
            "browser_user_agent": USER_AGENT, "browser_version": "42.7.1", "os_sdk_version": "26200",
            "client_build_number": CLIENT_BUILD_NUMBER, "native_build_number": NATIVE_BUILD_NUMBER,
            "client_event_source": None, "client_app_state": "focused",
        }
    props["client_launch_id"] = str(uuid.uuid4())
    props["launch_signature"] = launch_sig
    props["client_heartbeat_session_id"] = hb_session
    return props


class FingerprintEngine:
    __slots__ = ("_machine_user_hash", "_cert_hash", "_xor_mask_int", "_cache")

    def __init__(self, user_id: str) -> None:
        m_guid = get_machine_guid()
        self._machine_user_hash = hashlib.sha256((m_guid + user_id).encode()).digest()
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
    def __init__(self, token: str, user_id: str, status: str = "online") -> None:
        self.token = token
        self.user_id = user_id
        self.status = status
        self.cookie = ""
        self.analytics_token = ""
        self.fetched_at = 0.0
        self.fp_engine = FingerprintEngine(user_id)
        self.heartbeat_session = str(uuid.uuid4())
        self.launch_signature = str(uuid.uuid4())
        self.super_props_dict = fetch_super_props_dict(self.launch_signature, self.heartbeat_session)
        self.super_props_b64 = base64.b64encode(
            json.dumps(self.super_props_dict, separators=(",", ":")).encode("utf-8")
        ).decode("ascii")
        self._seq_counter = itertools.count(1)
        self._ssl_ctx = ssl.create_default_context()
        self._tls_local = threading.local()
        self._rate_limit_until = 0.0
        self._rl_lock = threading.Lock()
        self.last_ping_ms = 0.0
        self._rebuild_headers()

    @property
    def is_stale(self) -> bool:
        return not self.analytics_token or (time.time() - self.fetched_at) > AUTH_MAX_AGE

    def _rebuild_headers(self) -> None:
        self._headers = {
            "accept": "*/*", "accept-language": "en-GB", "authorization": self.token,
            "content-type": "application/json", "origin": "https://discord.com",
            "referer": "https://discord.com/channels/@me", "user-agent": USER_AGENT,
            "x-debug-options": "bugReporterEnabled", "x-discord-locale": "en-GB",
            "x-discord-timezone": "Europe/Oslo", "x-super-properties": self.super_props_b64,
            "connection": "keep-alive",
        }
        if self.cookie.strip():
            self._headers["cookie"] = self.cookie

    def _get_conn(self) -> LowLatencyHTTPSConnection:
        conn = getattr(self._tls_local, "conn", None)
        if conn is None:
            conn = LowLatencyHTTPSConnection(DISCORD_HOST, timeout=15, context=self._ssl_ctx)
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

    def rest_request(self, method: str, path: str, payload: dict | None = None) -> tuple[int, dict | list]:
        for attempt in range(3):
            with self._rl_lock:
                wait_now = self._rate_limit_until - time.monotonic()
            if wait_now > 0:
                time.sleep(wait_now)
            conn = self._get_conn()
            headers = dict(self._headers)
            headers["accept-encoding"] = "gzip"
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8") if payload is not None else None
            try:
                t0 = time.perf_counter()
                conn.request(method, path, body=body, headers=headers)
                resp = conn.getresponse()
                raw = resp.read()
                self.last_ping_ms = (time.perf_counter() - t0) * 1000
                self._merge_cookies(resp)
                if resp.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                data = json.loads(raw.decode("utf-8")) if raw else {}
                if resp.status == 429:
                    retry_after = float(resp.getheader("Retry-After", "5"))
                    if isinstance(data, dict):
                        retry_after = float(data.get("retry_after", retry_after))
                    with self._rl_lock:
                        self._rate_limit_until = max(self._rate_limit_until, time.monotonic() + retry_after + 0.5)
                    continue
                return resp.status, data
            except Exception as exc:
                self._reset_conn()
                if attempt == 2:
                    return 0, {"error": str(exc)}
        return 0, {"error": "max retries"}

    def bootstrap_auth(self) -> str:
        self.rest_request("GET", "/api/v9/experiments")
        username = self.ensure_analytics_token()
        self.rest_request("POST", "/api/v9/users/@me/consent", {"grant": ["personalization", "usage_statistics"], "revoke": []})
        return username

    def ensure_analytics_token(self) -> str:
        st, data = self.rest_request("GET", "/api/v9/users/@me?with_analytics_token=true")
        if st != 200 or not isinstance(data, dict):
            raise RuntimeError(f"token rejected (HTTP {st})")
        tok = data.get("analytics_token")
        if not tok:
            raise RuntimeError("no analytics_token in response")
        self.analytics_token = tok
        self.fetched_at = time.time()
        return str(data.get("username") or "user")

    def _ui_camo_event(self, now_ms: int) -> dict:
        return {
            "type": random.choice(("app_ui_viewed", "guild_viewed", "channel_opened")),
            "properties": {
                "client_track_timestamp": now_ms, "client_heartbeat_session_id": self.heartbeat_session,
                "event_sequence_number": next(self._seq_counter), "launch_signature": self.launch_signature,
                "client_app_state": "focused", "client_send_timestamp": now_ms + random.randint(5, 65),
            },
        }

    def build_stream_start(
        self, game: Game, gsid: str, guild_id: str, channel_id: str,
        rtc_id: str, media_id: str, region: str, host: str,
        viewer_count: int, fps: int, height: int, has_sound: bool = True,
    ) -> list[dict]:
        now_ms = int(time.time() * 1000)
        fp = self.fp_engine.for_game(game.id, game.exe)
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
                    "client_app_state": "focused", "client_send_timestamp": now_ms + 12,
                },
            },
            {
                "type": "video_stream_started",
                "properties": {
                    "client_track_timestamp": now_ms + 30, "client_heartbeat_session_id": self.heartbeat_session,
                    "event_sequence_number": next(self._seq_counter), "rtc_connection_id": rtc_id,
                    "media_session_id": media_id, "stream_region": region, "guild_id": guild_id,
                    "channel_id": channel_id, "channel_type": 2, "stream_type": "guild", "share_type": "application",
                    "capture_method": "game_hook", "share_application_name": game.name, "share_application_id": game.id,
                    "share_application_executable": game.exe, "game_id": game.id, "game_name": game.name,
                    "target_fps": fps, "target_resolution_height": height, "has_sound": has_sound,
                    "num_viewers": v_cnt, "max_viewers": v_cnt, "launch_signature": self.launch_signature,
                    "client_app_state": "focused", "client_send_timestamp": now_ms + 45,
                },
            },
            {
                "type": "running_game_heartbeat",
                "properties": {
                    "client_track_timestamp": now_ms + 50, "client_heartbeat_session_id": self.heartbeat_session,
                    "event_sequence_number": next(self._seq_counter), "game_id": game.id, "game_name": game.name,
                    "game_metadata": None, "game_executable": game.exe, "game_detection_enabled": True,
                    "initial_heartbeat": True, "final_heartbeat": False, "game_session_id": gsid,
                    "duration_tracked_ms": 0, "rtc_connection_id": rtc_id, "media_session_id": media_id,
                    "launch_signature": self.launch_signature, "discord_window_state": "focused",
                    "client_app_state": "focused", "client_send_timestamp": now_ms + 60,
                },
            },
            {
                "type": "launch_game",
                "properties": {
                    "client_track_timestamp": now_ms + 70, "client_heartbeat_session_id": self.heartbeat_session,
                    "event_sequence_number": next(self._seq_counter), "game": game.name, "game_id": game.id,
                    "verified": True, "elevated": False, "is_launcher": False, "game_platform": "desktop",
                    "detection_method": "verified_game", "is_overlay_enabled": True, "is_overlay_game_enabled": True,
                    "is_overlay_game_source": "OOP_DEFAULT_DATABASE", "fullscreen_type": "BORDERLESS_FULLSCREEN",
                    "hardware_display_count": 1, "overlay_method": "OutOfProcess", "activity_status_enabled": True,
                    "activity_status_shared_guilds": [guild_id], "current_user_status": self.status,
                    "game_detection_enabled": True, "executable_path": game.exe, "voice_channel_id": channel_id,
                    "voice_channel_type": 2, "voice_channel_bitrate": 64000, "voice_channel_guild_id": guild_id,
                    "hidden_by_distributor": False, "game_metadata": None, "executable_fingerprint": fp,
                    "client_performance_cpu": round(random.uniform(2.5, 9.4), 2),
                    "client_performance_memory": random.randint(480_000, 860_000),
                    "cpu_core_count": 16, "accessibility_features": 0, "rendered_locale": "en-GB",
                    "launch_signature": self.launch_signature, "client_rtc_state": "CONNECTED",
                    "client_app_state": "focused", "client_send_timestamp": now_ms + 85,
                },
            },
        ]

    def build_stream_heartbeat(
        self, game: Game, gsid: str, delta_sec: float, cumulative_sec: float,
        guild_id: str, channel_id: str, rtc_id: str, media_id: str,
        region: str, viewer_count: int, fps: int, height: int, include_ui_camo: bool = False, has_sound: bool = True,
    ) -> list[dict]:
        now_ms = int(time.time() * 1000)
        dur_ms = max(1000, int(round(delta_sec * 1000)))
        dur_sec = max(1, dur_ms // 1000)
        cum_ms = max(dur_ms, int(round(cumulative_sec * 1000)))
        v_cnt = max(1, viewer_count)
        evs: list[dict] = []
        if include_ui_camo:
            evs.append(self._ui_camo_event(now_ms))
        evs.append({
            "type": "video_stream_heartbeat",
            "properties": {
                "client_track_timestamp": now_ms, "client_heartbeat_session_id": self.heartbeat_session,
                "event_sequence_number": next(self._seq_counter), "rtc_connection_id": rtc_id,
                "media_session_id": media_id, "stream_region": region, "guild_id": guild_id,
                "channel_id": channel_id, "channel_type": 2, "stream_type": "guild", "share_type": "application",
                "capture_method": "game_hook", "share_application_name": game.name, "share_application_id": game.id,
                "share_application_executable": game.exe, "game_id": game.id, "game_name": game.name,
                "duration": dur_sec, "duration_ms": dur_ms, "duration_tracked_ms": dur_ms,
                "duration_streaming_ms": dur_ms, "frames_sent": dur_sec * fps,
                "target_fps": fps, "target_resolution_height": height, "has_sound": has_sound,
                "num_viewers": v_cnt, "max_viewers": v_cnt, "launch_signature": self.launch_signature,
                "client_app_state": "focused", "client_send_timestamp": now_ms + 10,
            },
        })
        evs.append({
            "type": "running_game_heartbeat",
            "properties": {
                "client_track_timestamp": now_ms + 5, "client_heartbeat_session_id": self.heartbeat_session,
                "event_sequence_number": next(self._seq_counter), "game_id": game.id, "game_name": game.name,
                "game_metadata": None, "game_executable": game.exe, "game_detection_enabled": True,
                "initial_heartbeat": False, "final_heartbeat": False, "game_session_id": gsid,
                "duration_tracked_ms": min(2_000_000_000, cum_ms), "rtc_connection_id": rtc_id,
                "media_session_id": media_id, "launch_signature": self.launch_signature,
                "discord_window_state": "focused", "client_app_state": "focused", "client_send_timestamp": now_ms + 14,
            },
        })
        return evs

    def build_stream_end(
        self, game: Game, gsid: str, total_sec: float, guild_id: str, channel_id: str,
        rtc_id: str, media_id: str, region: str, viewer_count: int, fps: int, height: int, has_sound: bool = True,
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
                    "client_send_timestamp": now_ms + 10,
                },
            },
            {
                "type": "video_stream_ended",
                "properties": {
                    **base, "client_track_timestamp": now_ms + 15, "event_sequence_number": next(self._seq_counter),
                    "duration": dur_sec, "duration_ms": dur_ms, "duration_tracked_ms": dur_ms,
                    "duration_streaming_ms": dur_ms, "num_viewers": v_cnt, "max_viewers": v_cnt,
                    "reason": "user_requested", "client_send_timestamp": now_ms + 25,
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
                    "client_app_state": "focused", "client_send_timestamp": now_ms + 40,
                },
            },
        ]

    def post_science(self, events: Sequence[dict]) -> int:
        with self._rl_lock:
            wait_now = self._rate_limit_until - time.monotonic()
        if wait_now > 0:
            time.sleep(wait_now)
        body = json.dumps({"token": self.analytics_token, "events": events}, separators=(",", ":")).encode("utf-8")
        for attempt in range(2):
            try:
                t0 = time.perf_counter()
                conn = self._get_conn()
                conn.request("POST", "/api/v9/science", body=body, headers=self._headers)
                resp = conn.getresponse()
                raw = resp.read()
                self.last_ping_ms = (time.perf_counter() - t0) * 1000
                self._merge_cookies(resp)
                if resp.status == 429:
                    retry_after = float(resp.getheader("Retry-After", "5"))
                    try:
                        retry_after = float(json.loads(raw).get("retry_after", retry_after))
                    except Exception:
                        pass
                    with self._rl_lock:
                        self._rate_limit_until = max(self._rate_limit_until, time.monotonic() + retry_after + 0.5)
                    return 429
                return resp.status
            except Exception:
                self._reset_conn()
                if attempt == 1:
                    return 0
        return 0


class WebRTCSidecar:
    def __init__(
        self, token: str, guild_id: str, channel_id: str, media_file: str,
        quality_key: str, status: str, activities: list[dict], log: Logger,
        super_props: dict | None = None, role: str = "streamer", target_streamer_id: str = "",
    ) -> None:
        self.token = token
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.media_file = media_file
        self.quality_key = quality_key
        self.status = status
        self.activities = activities
        self.log = log
        self.super_props = super_props or {}
        self.role = role
        self.target_streamer_id = target_streamer_id

        self.joined_event = threading.Event()
        self.meta_event = threading.Event()
        self.sfu_meta: dict = {}
        self.viewer_ids: list[str] = []
        self.proc: subprocess.Popen | None = None
        self.stop_event = threading.Event()
        self.cfg_file = WEBRTC_DIR / f"cfg_{uuid.uuid4().hex[:8]}.json"

    def start(self) -> None:
        WEBRTC_DIR.mkdir(parents=True, exist_ok=True)
        (WEBRTC_DIR / "worker.mjs").write_text(_WEBRTC_WORKER_MJS.strip(), encoding="utf-8")
        q_cfg = QUALITY_PRESETS.get(self.quality_key, QUALITY_PRESETS["ultra_lite"])
        include_audio = bool(q_cfg.get("include_audio", True)) and (sys.platform != "darwin")
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
            "superProps": self.super_props,
        }
        tmp = self.cfg_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, self.cfg_file)

        env = dict(os.environ)
        env["PATH"] = str(ROOT) + os.pathsep + env.get("PATH", "")
        # Cap Node V8 heap to stay comfortably inside Render Free 512MB RAM
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
            if "[WEBRTC-META]" in txt:
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
        if self.cfg_file.exists():
            try:
                self.cfg_file.unlink()
            except OSError:
                pass


class StreamRunner247:
    def __init__(
        self, token: str, alt_tokens: list[str], guild_id: str, channel_id: str,
        game: Game, quality_key: str, heartbeat_sec: float, refresh_hours: float,
        media_url: str, status: str, custom_status: str, log: Logger,
    ) -> None:
        self.token = token
        self.user_id = extract_user_id(token)
        self.alt_tokens = alt_tokens
        self.guild_id = guild_id
        self.channel_id = channel_id
        self.game = game
        self.quality_key = quality_key
        self.heartbeat_sec = heartbeat_sec
        self.refresh_sec = max(1800.0, refresh_hours * 3600.0)
        self.media_url = media_url
        self.status = status
        self.custom_status = custom_status
        self.log = log

        self.telemetry = StreamTelemetryClient(token, self.user_id, status=status)
        self.stop_event = threading.Event()
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
        self._zero_viewer_since: float | None = None
        self._meta_lock = threading.Lock()

    def status_snapshot(self) -> dict:
        return {
            "ok": not self.stop_event.is_set(),
            "game": self.game.name,
            "game_id": self.game.id,
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

    def _spawn_viewer(self, alt_tok: str) -> WebRTCSidecar:
        v = WebRTCSidecar(
            token=alt_tok, guild_id=self.guild_id, channel_id=self.channel_id,
            media_file=self._mp4_file, quality_key=self.quality_key, status="online",
            activities=[], log=self.log, role="viewer", target_streamer_id=self.user_id,
        )
        v.start()
        return v

    def _ensure_viewers_alive(self, force_restart_all: bool = False) -> None:
        if not self.alt_tokens:
            return
        if force_restart_all:
            for _, old_v in self.viewer_sidecars:
                old_v.stop()
            self.viewer_sidecars.clear()
            if self.stop_event.is_set():
                return
            for idx, tok in enumerate(self.alt_tokens, 1):
                self.log.info(f"restarting alt viewer #{idx}...")
                self.viewer_sidecars.append((tok, self._spawn_viewer(tok)))
            return

        existing = {tok: proc for tok, proc in self.viewer_sidecars}
        updated: list[tuple[str, WebRTCSidecar]] = []
        for idx, tok in enumerate(self.alt_tokens, 1):
            proc = existing.get(tok)
            if proc is None or not proc.is_alive:
                if proc is not None:
                    proc.stop()
                    self.log.warn("alt-watchdog", f"alt viewer #{idx} dropped · reconnecting...")
                updated.append((tok, self._spawn_viewer(tok)))
            else:
                updated.append((tok, proc))
        self.viewer_sidecars = updated

    def _spawn_streamer(self) -> bool:
        if self.sidecar:
            self.sidecar.stop()
            self.sidecar = None
            if self.stop_event.wait(1.5):
                return False
        self._session_start_epoch_ms = int(time.time() * 1000)
        self.sidecar = WebRTCSidecar(
            token=self.token, guild_id=self.guild_id, channel_id=self.channel_id,
            media_file=self._mp4_file, quality_key=self.quality_key, status=self.status,
            activities=self._presence_activities(), log=self.log,
            super_props=self.telemetry.super_props_dict, role="streamer",
        )
        self.sidecar.start()
        if not self.sidecar.joined_event.wait(timeout=45.0):
            self.log.warn("stream-247", "streamer did not join Voice Channel within 45s")
            return False
        self.log.info("waiting for Discord Media SFU media_session_id & rtc_connection_id...")
        if not self.sidecar.meta_event.wait(timeout=35.0):
            self.log.warn("stream-247", "timed out waiting for SFU Opcode 4 media_session_id")
            return False
        if self.alt_tokens and not self.sidecar.viewer_ids:
            deadline = time.monotonic() + 8.0
            while not self.sidecar.viewer_ids and time.monotonic() < deadline and not self.stop_event.is_set():
                time.sleep(0.3)
        return True

    def _send_session_start(self) -> None:
        q_cfg = QUALITY_PRESETS[self.quality_key]
        if not self.sidecar or not self.sidecar.sfu_meta.get("media_session_id"):
            return
        with self._meta_lock:
            self._active_meta = dict(self.sidecar.sfu_meta)
            self._active_gsid = str(uuid.uuid4())
            self._session_start_mono = time.monotonic()
            self._session_credited_sec = 0.0
            meta, gsid = dict(self._active_meta), self._active_gsid
        evs = self.telemetry.build_stream_start(
            self.game, gsid, self.guild_id, self.channel_id,
            meta["rtc_connection_id"], meta["media_session_id"],
            meta.get("stream_region") or "singapore", meta.get("endpoint_host") or "",
            max(1, len(self.sidecar.viewer_ids)), q_cfg["fps"], q_cfg["height"],
            has_sound=bool(q_cfg.get("include_audio", True)) and (sys.platform != "darwin"),
        )
        st = self.telemetry.post_science(evs)
        if st in (401, 403):
            try:
                self.telemetry.ensure_analytics_token()
                st = self.telemetry.post_science(evs)
            except Exception:
                pass
        if st == 204:
            self.log.ok("stream-247", f"session started · game={self.game.name} · media={meta['media_session_id'][:8]} · viewers={len(self.sidecar.viewer_ids)}")
        else:
            self.log.warn("stream-247", f"start telemetry HTTP [{st}]")

    def _send_session_end(self) -> None:
        q_cfg = QUALITY_PRESETS[self.quality_key]
        with self._meta_lock:
            meta, gsid, sess_sec = dict(self._active_meta), self._active_gsid, self._session_credited_sec
            self._active_meta, self._session_credited_sec = {}, 0.0
        if meta.get("media_session_id") and sess_sec > 0:
            try:
                v_cnt = max(1, len(self.sidecar.viewer_ids) if self.sidecar else 1)
                evs = self.telemetry.build_stream_end(
                    self.game, gsid, sess_sec, self.guild_id, self.channel_id,
                    meta["rtc_connection_id"], meta["media_session_id"],
                    meta.get("stream_region") or "singapore", v_cnt, q_cfg["fps"], q_cfg["height"],
                    has_sound=bool(q_cfg.get("include_audio", True)) and (sys.platform != "darwin"),
                )
                self.telemetry.post_science(evs)
                self.log.ok("stream-247", f"closed SFU session cleanly ({sess_sec / 3600.0:.2f}h)")
            except Exception:
                pass

    def _loop(self) -> None:
        q_cfg = QUALITY_PRESETS[self.quality_key]
        fps, height = q_cfg["fps"], q_cfg["height"]

        while not self.stop_event.is_set():
            self._ensure_viewers_alive()
            if self._spawn_streamer():
                self._send_session_start()
                break
            self.reconnect_count += 1
            self.log.warn("stream-247", f"connect retry #{self.reconnect_count} in 10s...")
            if self.stop_event.wait(10.0):
                return

        last_hb_mono = time.monotonic()
        hb_idx = 0

        while not self.stop_event.is_set():
            if self.stop_event.wait(5.0):
                break

            self._ensure_viewers_alive()

            if not self.sidecar or not self.sidecar.is_alive:
                self.reconnect_count += 1
                self.log.warn("stream-247", f"streamer dropped (#{self.reconnect_count}) · healing...")
                self._send_session_end()
                if self._spawn_streamer():
                    self._send_session_start()
                    last_hb_mono = time.monotonic()
                else:
                    self.stop_event.wait(10.0)
                continue

            sfu_viewers = len(self.sidecar.viewer_ids)
            if sfu_viewers == 0:
                if self._zero_viewer_since is None:
                    self._zero_viewer_since = time.monotonic()
                    self.log.warn("sfu-viewer", "v=0 on SFU · waiting for alt viewer...")
                elif self.alt_tokens and (time.monotonic() - self._zero_viewer_since) >= 45.0:
                    self.log.warn("alt-watchdog", "v=0 for 45s · restarting alt viewer(s)...")
                    self._zero_viewer_since = time.monotonic()
                    self._ensure_viewers_alive(force_restart_all=True)
            else:
                if self._zero_viewer_since is not None:
                    self.log.ok("sfu-viewer", f"{sfu_viewers} active viewer(s) confirmed · resuming 1:1 heartbeats")
                    self._zero_viewer_since = None
                    last_hb_mono = time.monotonic()

            if self._session_start_mono > 0 and (time.monotonic() - self._session_start_mono) >= self.refresh_sec:
                self.log.info(f"refreshing SFU session after {self.refresh_sec / 3600.0:.1f}h...")
                self._send_session_end()
                if self._spawn_streamer():
                    self._send_session_start()
                    last_hb_mono = time.monotonic()
                continue

            now_mono = time.monotonic()
            delta_sec = now_mono - last_hb_mono
            if delta_sec < self.heartbeat_sec:
                continue
            last_hb_mono = now_mono

            if self.telemetry.is_stale:
                try:
                    self.telemetry.ensure_analytics_token()
                except Exception:
                    pass

            if sfu_viewers == 0:
                continue

            with self._meta_lock:
                meta, gsid = dict(self._active_meta), self._active_gsid
                cum_sec = self._session_credited_sec + delta_sec
            if not meta.get("media_session_id"):
                continue

            hb_idx += 1
            evs = self.telemetry.build_stream_heartbeat(
                self.game, gsid, delta_sec, cum_sec,
                self.guild_id, self.channel_id,
                meta["rtc_connection_id"], meta["media_session_id"],
                meta.get("stream_region") or "singapore",
                viewer_count=sfu_viewers, fps=fps, height=height,
                include_ui_camo=(hb_idx % 5 == 1),
                has_sound=bool(q_cfg.get("include_audio", True)) and (sys.platform != "darwin"),
            )
            st = self.telemetry.post_science(evs)
            if st in (401, 403):
                try:
                    self.telemetry.ensure_analytics_token()
                    st = self.telemetry.post_science(evs)
                except Exception:
                    pass

            if st == 204:
                with self._meta_lock:
                    self._session_credited_sec += delta_sec
                self.real_hours_credited += delta_sec / 3600.0
                self.total_heartbeats_ok += 1
                self.log.ok(
                    f"hb#{hb_idx:04d}",
                    f"{self.game.name} +{delta_sec:.0f}s | total={format_hms(self.real_hours_credited * 3600)} | v={sfu_viewers} | ping={self.telemetry.last_ping_ms:.0f}ms",
                )
            else:
                self.log.warn(f"hb#{hb_idx:04d}", f"HTTP [{st}] · retrying next tick")

    def start(self) -> bool:
        username = self.telemetry.bootstrap_auth()
        self.log.ok("auth", f"authenticated as {username} ({self.user_id}) · ping {self.telemetry.last_ping_ms:.0f}ms")
        if not is_webrtc_sidecar_ready():
            if not install_webrtc_stack(self.log):
                return False
        self._mp4_file = prepare_playable_mp4(self.media_url, self.quality_key, self.log)
        self.started_mono = time.monotonic()
        threading.Thread(target=self._loop, daemon=True).start()
        return True

    def stop(self) -> None:
        if self.stop_event.is_set():
            return
        self.stop_event.set()
        self._send_session_end()
        if self.sidecar:
            self.sidecar.stop()
            self.sidecar = None
        for _, v in self.viewer_sidecars:
            v.stop()
        self.viewer_sidecars.clear()


def start_health_server(port: int, runner: StreamRunner247, log: Logger) -> http.server.ThreadingHTTPServer:
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
    log.ok("http", f"Render health endpoint listening on 0.0.0.0:{port}")
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

    try:
        hb_sec = max(20.0, min(300.0, float(os.environ.get("STREAM_HB_SEC", "60"))))
    except ValueError:
        hb_sec = 60.0
    try:
        refresh_hours = max(0.5, float(os.environ.get("STREAM_REFRESH_HOURS", "6")))
    except ValueError:
        refresh_hours = 6.0

    if not token or not guild_id or not channel_id:
        log.err("config", "missing required env vars: DISCORD_TOKEN, GUILD_ID, CHANNEL_ID")
        return 1

    games = load_games()
    if not games:
        log.err("games", "could not load game catalog from Discord CDN")
        return 1
    chosen_game = pick_game(games, game_query)
    log.ok("game", f"selected {chosen_game.name} (id={chosen_game.id}, exe={chosen_game.exe})")

    runner = StreamRunner247(
        token=token, alt_tokens=alt_tokens, guild_id=guild_id, channel_id=channel_id,
        game=chosen_game, quality_key=quality_key, heartbeat_sec=hb_sec,
        refresh_hours=refresh_hours, media_url=media_url, status=status,
        custom_status=custom_status, log=log,
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
        f"streaming 24/7 on Guild {guild_id} › Channel {channel_id} · quality={quality_key} · alts={len(alt_tokens)}",
    )
    stop_sig.wait()
    log.info("shutting down cleanly...")
    _cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
