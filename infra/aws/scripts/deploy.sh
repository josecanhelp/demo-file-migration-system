#!/bin/bash
# Brings the instance from a bare Amazon Linux 2023 boot (or an already
# running one) to the full docker compose stack, pulled from ghcr.io and
# fronted by Caddy. Safe to run more than once: every step checks whether
# its work is already done before doing it again. EC2 user data runs this
# once on first boot; every later deploy runs the same script again, now
# fetched fresh from the repo so a change to the script itself ships with
# the next deploy.
set -euo pipefail

PARAM_PREFIX="/file-migration-system"
APP_DIR="/opt/app/repo"

log() {
  echo "[deploy] $*"
}

imds_region() {
  local token
  token=$(curl -sX PUT "http://169.254.169.254/latest/api/token" \
    -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
  curl -s -H "X-aws-ec2-metadata-token: $token" \
    "http://169.254.169.254/latest/meta-data/placement/region"
}

REGION=$(imds_region)

ssm_param() {
  aws ssm get-parameter --name "${PARAM_PREFIX}/$1" --with-decryption \
    --query "Parameter.Value" --output text --region "$REGION"
}

ssm_param_default() {
  aws ssm get-parameter --name "${PARAM_PREFIX}/$1" --with-decryption \
    --query "Parameter.Value" --output text --region "$REGION" 2>/dev/null || echo "$2"
}

log "reading configuration from SSM Parameter Store under ${PARAM_PREFIX}"
DOMAIN=$(ssm_param "domain")
GITHUB_REPO=$(ssm_param "github-repo")
IMAGE_TAG=$(ssm_param_default "image-tag" "latest")

if ! command -v docker >/dev/null 2>&1; then
  log "installing docker"
  dnf install -y docker git
  systemctl enable --now docker
fi

if ! docker compose version >/dev/null 2>&1; then
  log "installing the docker compose plugin"
  mkdir -p /usr/libexec/docker/cli-plugins
  curl -fsSL \
    "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-aarch64" \
    -o /usr/libexec/docker/cli-plugins/docker-compose
  chmod +x /usr/libexec/docker/cli-plugins/docker-compose
fi

mkdir -p "$(dirname "$APP_DIR")"
if [ -d "$APP_DIR/.git" ]; then
  log "updating existing checkout at $APP_DIR"
  git -C "$APP_DIR" fetch origin main
  git -C "$APP_DIR" reset --hard origin/main
else
  log "cloning ${GITHUB_REPO} into $APP_DIR"
  git clone --branch main "https://github.com/${GITHUB_REPO}.git" "$APP_DIR"
fi

cd "$APP_DIR"

GHCR_NAMESPACE="ghcr.io/$(echo "$GITHUB_REPO" | tr '[:upper:]' '[:lower:]')"

cat > "$APP_DIR/.env.prod" <<EOF
DOMAIN=${DOMAIN}
GHCR_NAMESPACE=${GHCR_NAMESPACE}
IMAGE_TAG=${IMAGE_TAG}
EOF
chmod 600 "$APP_DIR/.env.prod"

log "pulling images tagged ${IMAGE_TAG} from ${GHCR_NAMESPACE}"
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml pull

log "starting the stack"
docker compose --env-file .env.prod -f docker-compose.yml -f docker-compose.prod.yml up -d --remove-orphans

log "done"
