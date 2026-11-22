FROM node:22-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 ffmpeg curl ca-certificates xz-utils \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY stream.py .

# Cài sẵn gói WebRTC & tạo sẵn MP4 ngay lúc Build
RUN python3 -u stream.py --setup-only

CMD ["python3", "-u", "stream.py"]
