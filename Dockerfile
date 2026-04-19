# syntax=docker/dockerfile:1.6
#
# tier — Unraid media tiering script
#
# Built by GitHub Actions and published to Docker Hub as a multi-arch image
# (amd64 primary — Unraid host is x86_64; arm64 included so it also runs on
# ARM workstations if we ever move this off Unraid).
#
# Runtime expects:
#   /config           bind-mounted — holds tiering.yaml + tier.log
#   /mnt/user/...     bind-mounted read/write — Plex-visible union share
#   /mnt/diskN/...    bind-mounted read/write — direct array disk access
#                     (needed in P2 for co-located warm-tier writes)
#   /mnt/<zfs_pool>/  bind-mounted read/write — hot tier destination
#
# For notifications.unraid.enabled=true, also bind-mount:
#   /usr/local/emhttp/webGui/scripts/notify  (read-only)

FROM python:3.12-slim AS base

# rsync is the move engine for P2+; ca-certificates keeps HTTPS to Plex clean.
# tzdata so timestamps in the log match the host's clock.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        rsync \
        ca-certificates \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    TZ=UTC

WORKDIR /app

# Install Python deps first so layer cache survives script edits.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Then the script itself, plus the config template.
# example.tiering.yaml is baked in (NOT tiering.yaml — that stays on the
# /config volume and never goes into the image) so the container can
# auto-seed the config volume on first run.
COPY tier.py /app/tier.py
COPY example.tiering.yaml /app/example.tiering.yaml
RUN chmod +x /app/tier.py

# /config is the one persistent volume — tiering.yaml lives here, tier.log
# rotates here. Declaring it as VOLUME makes the intent explicit.
VOLUME ["/config"]

# Default to a dry read-only run; CA template / cron override this.
ENTRYPOINT ["python3", "/app/tier.py"]
CMD ["--config", "/config/tiering.yaml"]
