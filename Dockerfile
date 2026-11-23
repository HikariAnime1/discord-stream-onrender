FROM node:22-trixie-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip ffmpeg curl ca-certificates xz-utils \
    && pip3 install --no-cache-dir --break-system-packages "curl_cffi>=0.7.4" \
    && curl -fsSL https://github.com/ViRb3/wgcf/releases/download/v2.2.22/wgcf_2.2.22_linux_amd64 \
       -o /usr/local/bin/wgcf \
    && chmod +x /usr/local/bin/wgcf \
    && mkdir -p /tmp/wp \
    && curl -fsSL https://github.com/pufferffish/wireproxy/releases/download/v1.0.9/wireproxy_linux_amd64.tar.gz \
       | tar -xz -C /tmp/wp \
    && mv /tmp/wp/wireproxy /usr/local/bin/wireproxy \
    && chmod +x /usr/local/bin/wireproxy \
    && rm -rf /tmp/wp /var/lib/apt/lists/*

WORKDIR /app
COPY stream.py start.sh ./
RUN sed -i 's/\r$//' start.sh && chmod +x start.sh && python3 -u stream.py --setup-only

CMD ["./start.sh"]
