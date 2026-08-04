# Deployment

The whole stack runs on one ARM EC2 instance. Caddy is the only thing reachable from the
internet: it terminates real HTTPS on your domain, sits in front of the dashboard behind basic
auth, and everything else (MySQL, Postgres, Kafka, Connect, MinIO, the migrator services) stays
on the instance's internal docker network. There is no load balancer and no NAT gateway; neither
is needed for a single instance in a public subnet.

## Prerequisites

- An AWS account, with the CLI configured to it locally for the one-time `cdk bootstrap` and
  `cdk deploy`.
- A domain you control, able to add a DNS A record.
- This repository, pushed to a public GitHub repo (container images go to ghcr.io, which is free
  for public repos).
- Node.js 20 and Yarn, for running the CDK app in `infra/aws`.

## SSM parameters

The instance reads its configuration from AWS Systems Manager Parameter Store at boot and on
every deploy, not from anything baked into the CDK stack or the AMI. Set these before the first
deploy, using the standard (free) parameter tier:

| Parameter | Example value | Notes |
| --- | --- | --- |
| `/file-migration-system/domain` | `files.example.com` | The domain Caddy requests a certificate for. |
| `/file-migration-system/basic-auth-user` | `admin` | Dashboard username. |
| `/file-migration-system/basic-auth-hash` | `$2a$14$...` | Bcrypt hash of the dashboard password. Store as `SecureString`. See below for how to generate it. |
| `/file-migration-system/github-repo` | `youruser/file-migration-system` | Used to find deploy.sh on first boot and to derive the ghcr.io image path. |
| `/file-migration-system/image-tag` | `latest` | Which image tag to run. The deploy workflow overwrites this with the commit sha on every push to main; it only needs a manual value the first time. |

Generate the bcrypt hash with the caddy image itself, so the hashing algorithm always matches
what the running Caddyfile expects:

```bash
docker run --rm caddy:2.8-alpine caddy hash-password --plaintext 'your-password-here'
```

Set the parameters:

```bash
aws ssm put-parameter --name /file-migration-system/domain --type String --value "files.example.com"
aws ssm put-parameter --name /file-migration-system/basic-auth-user --type String --value "admin"
aws ssm put-parameter --name /file-migration-system/basic-auth-hash --type SecureString --value '$2a$14$...'
aws ssm put-parameter --name /file-migration-system/github-repo --type String --value "youruser/file-migration-system"
aws ssm put-parameter --name /file-migration-system/image-tag --type String --value "latest"
```

## First deploy

```bash
cd infra/aws
yarn install
yarn cdk bootstrap   # once per account and region
yarn cdk deploy
```

`cdk deploy` prints the Elastic IP, the instance id, and a reminder to create the DNS record.
Point an A record for the domain you stored above at that Elastic IP now, before the instance's
user data runs deploy.sh; Caddy needs the DNS to already resolve to request its certificate. A
stopped-and-restarted instance keeps the same Elastic IP, so this record does not need to change
again.

The instance installs docker, clones the repo, reads the SSM parameters above, and starts the
full stack on first boot; that takes a few minutes. Compare `docker compose ps` over SSM (below)
against a fresh local `docker compose up` if anything looks slow to settle.

## Deploying from CI

`.github/workflows/deploy.yml` builds arm64 images on push to main, pushes them to ghcr.io tagged
with the commit sha, and tells the instance to pull and restart. It authenticates to AWS with
OIDC role assumption rather than long-lived access keys.

Create the OIDC identity provider once per account, if it does not already exist from another
project:

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

Create a role trusted only by this repository's workflows on the `main` branch:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::<ACCOUNT_ID>:oidc-provider/token.actions.githubusercontent.com"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
        },
        "StringLike": {
          "token.actions.githubusercontent.com:sub": "repo:<OWNER>/<REPO>:ref:refs/heads/main"
        }
      }
    }
  ]
}
```

Attach a permissions policy scoped to exactly what the deploy job does: write the `image-tag`
parameter and send one SSM command to one instance.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "ssm:PutParameter",
      "Resource": "arn:aws:ssm:<REGION>:<ACCOUNT_ID>:parameter/file-migration-system/image-tag"
    },
    {
      "Effect": "Allow",
      "Action": "ssm:SendCommand",
      "Resource": [
        "arn:aws:ssm:<REGION>:<ACCOUNT_ID>:document/AWS-RunShellScript",
        "arn:aws:ec2:<REGION>:<ACCOUNT_ID>:instance/<INSTANCE_ID>"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["ssm:GetCommandInvocation", "ssm:ListCommandInvocations"],
      "Resource": "*"
    }
  ]
}
```

Then set, in the repository's GitHub Actions settings:

- Secret `AWS_DEPLOY_ROLE_ARN`: the role's ARN.
- Variable `AWS_REGION`: the region the stack was deployed to.
- Variable `AWS_INSTANCE_ID`: the instance id from the `cdk deploy` output.

No secret needs to hold ghcr credentials; the workflow pushes images using the automatically
issued `GITHUB_TOKEN`, scoped by the `packages: write` permission already set in the workflow
file.

## Getting a shell

No SSH port is open; the security group has no rule for it. Use Session Manager instead, which
needs only the IAM role already attached to the instance:

```bash
aws ssm start-session --target <INSTANCE_ID>
```

## Reading logs

From inside a Session Manager shell:

```bash
cd /opt/app/repo
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod logs -f
docker compose -f docker-compose.yml -f docker-compose.prod.yml --env-file .env.prod logs -f control-plane
```

`deploy.sh` itself logs to stdout of whichever mechanism ran it; for the first boot, that is in
`/var/log/cloud-init-output.log`.

## Cost

Estimates for `us-east-1`, on-demand pricing; a different region will vary somewhat. Check the
AWS Pricing Calculator for current numbers before relying on these for budgeting.

| Item | Monthly cost |
| --- | --- |
| t4g.medium, always on (730 hours at about $0.0336/hour) | about $24.50 |
| EBS gp3, 30 GiB | about $2.40 |
| Elastic IP, attached to a running instance | $0 |
| Data transfer out, first 100 GB | $0 |
| SSM Parameter Store, standard parameters | $0 |
| ghcr.io, public repo | $0 |
| NAT gateway | $0 (none provisioned) |
| Load balancer | $0 (none provisioned) |
| **Total** | **about $27/month** |

Data transfer beyond the free monthly allowance bills per GB; a low-traffic demo behind basic
auth is unlikely to reach it. Switching the instance type to t4g.large roughly doubles the
compute line and removes the need for the memory tuning in `docker-compose.prod.yml`.

## Tearing down

```bash
cd infra/aws
yarn cdk destroy
```

This removes the instance, its security group, the VPC, and the Elastic IP. It does not remove
the SSM parameters (they cost nothing to leave behind) or the images already pushed to ghcr.io.
Delete either by hand if you want them gone too.

## Security posture, honestly

This deploys a demo system with no application-level authentication of its own. Basic auth in
front of the dashboard, backed by a single shared username and password, is the only thing
standing between the internet and the control plane's API. There is no per-user access, no
audit log of who did what, and no rate limiting beyond what Caddy does by default. The security
group, which allows only 80 and 443 in and nothing else, is the primary control; treat the
basic auth password as sensitive but not as the main line of defense, and do not point this setup
at real user data.
