# Runs once, on first boot only. Kept short on purpose: it does just
# enough to find and run deploy.sh from the repo, so every later deploy
# (including a change to the deploy logic itself) uses the same script
# instead of a copy frozen into this file.
TOKEN=$(curl -sX PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
REGION=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" "http://169.254.169.254/latest/meta-data/placement/region")
REPO=$(aws ssm get-parameter --name /file-migration-system/github-repo --query Parameter.Value --output text --region "$REGION")
mkdir -p /opt/app
curl -fsSL "https://raw.githubusercontent.com/${REPO}/main/infra/aws/scripts/deploy.sh" -o /opt/app/deploy.sh
chmod +x /opt/app/deploy.sh
/opt/app/deploy.sh
