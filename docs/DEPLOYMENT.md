# Deployment

The whole stack runs on one ARM EC2 instance. Caddy is the only thing reachable from the
internet: it terminates real HTTPS on your domain and reverse proxies to the dashboard, and
everything else (MySQL, Postgres, Kafka, Connect, MinIO, the migrator services) stays on the
instance's internal docker network. There is no load balancer and no NAT gateway; neither is
needed for a single instance in a public subnet. See "Security posture, honestly" below before
pointing this at anything you care about keeping private.

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
| `/file-migration-system/github-repo` | `josecanhelp/demo-file-migration-system` | Used to find deploy.sh on first boot and to derive the ghcr.io image path. |
| `/file-migration-system/image-tag` | `latest` | Which image tag to run. The deploy workflow overwrites this with the commit sha on every push to main; it only needs a manual value the first time. |

Set the parameters:

```bash
aws ssm put-parameter --name /file-migration-system/domain --type String --value "files.example.com"
aws ssm put-parameter --name /file-migration-system/github-repo --type String --value "josecanhelp/demo-file-migration-system"
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

Setting up the role this authenticates as, and diagnosing it when it fails, are both covered
below: first how to create it, then the sequence of errors that show up if a piece of that setup
is wrong, in the order they tend to surface.

Then set, in the repository's GitHub Actions settings:

- Secret `AWS_DEPLOY_ROLE_ARN`: the deploy role's ARN, created below.
- Variable `AWS_REGION`: the region the stack was deployed to.
- Variable `AWS_INSTANCE_ID`: the instance id from the `cdk deploy` output.

No secret needs to hold ghcr credentials; the workflow pushes images using the automatically
issued `GITHUB_TOKEN`, scoped by the `packages: write` permission already set in the workflow
file.

### Setting up the deploy role

Create the OIDC identity provider once per account, if it does not already exist from another
project:

```bash
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1
```

Create the deploy role, trusted only by this repository, with the trust policy below saved as
`trust-policy.json`:

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
          "token.actions.githubusercontent.com:sub": "repo:josecanhelp/demo-file-migration-system:ref:refs/heads/main"
        }
      }
    }
  ]
}
```

```bash
aws iam create-role \
  --role-name file-migration-system-deploy \
  --assume-role-policy-document file://trust-policy.json
```

Attach a permissions policy scoped to exactly what the `deploy` job does: write the one SSM
parameter under `/file-migration-system`, send the `AWS-RunShellScript` document to the one
instance, and read back what it did. This grants none of `ssm:*` or `ec2:*`, only the specific
actions and resources below, saved as `permissions-policy.json`:

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

```bash
aws iam put-role-policy \
  --role-name file-migration-system-deploy \
  --policy-name file-migration-system-deploy \
  --policy-document file://permissions-policy.json
```

Then set `AWS_DEPLOY_ROLE_ARN` on the repository to that role's ARN, exactly as named: this is the
one secret the `deploy` job reads for `role-to-assume`.

### Troubleshooting OIDC role assumption

The `configure-aws-credentials` step fails in a specific order as each piece of the setup above
gets fixed, one error uncovering the next. That ordering is itself useful: which message you are
looking at tells you roughly how much of the setup is already correct.

**1. Empty or misnamed secret.**

```
Credentials could not be loaded, please check your action inputs: Could not load credentials
from any providers
```

This is what the action reports whenever `role-to-assume` resolves to an empty string, before it
ever talks to AWS. The workflow reads that value from the `AWS_DEPLOY_ROLE_ARN` secret; check
that name specifically, since a mismatch there is the single most common cause: the secret was
never set on this repository, was set on an environment the `deploy` job does not use, or was
created under a slightly different name (`AWS_ROLE_ARN`, `DEPLOY_ROLE_ARN`, a typo) than the one
the workflow actually reads. Also confirm the `deploy` job still has `permissions: id-token:
write`; without it, GitHub never mints an OIDC token for the job to present, and
`configure-aws-credentials` has nothing to exchange regardless of how correct the secret is.

**2. Missing OIDC provider.**

```
Error: Could not assume role with OIDC: The web identity token provided could not be validated
```

Once the secret resolves to something, this is what AWS returns when the
`token.actions.githubusercontent.com` OIDC identity provider does not exist yet in the target
account. Create it with the `create-open-id-connect-provider` command above.

**3. Invalid ARN.**

```
Error: Could not assume role with OIDC: Request ARN is invalid
```

This shows up when the secret holds an ARN that is not a role `configure-aws-credentials` can
assume, most often the OIDC provider's own ARN pasted in by mistake instead of the role's, or a
role ARN with a typo in the account id or region. Confirm the secret is the role ARN from
`create-role`'s output, not the provider ARN from the step before it.

**4. Immutable subject claims.**

```
Error: Could not assume role with OIDC: Not authorized to perform sts:AssumeRoleWithWebIdentity
```

With the provider in place, the role existing, and the audience correct, this is the case that
took four rounds of debugging to run down, because no public example warns about it: some
repositories, this one included, have GitHub's immutable subject claims setting turned on, which
appends the numeric owner and repository ids to the `sub` claim with an `@`:

```
repo:OWNER@OWNER_ID/REPO@REPO_ID:ref:refs/heads/main
```

rather than the form every standard example uses:

```
repo:OWNER/REPO:ref:refs/heads/main
```

A trust policy copied from any standard example, including the one earlier in this section,
matches the second form and will never match the first, and the resulting error gives no hint
that the subject is the problem; it looks identical to a garden-variety trust policy typo.

To confirm this is what is happening, add a step before `configure-aws-credentials` that requests
the job's own OIDC token and prints only its decoded claims, never the token itself, the token is
a live credential and must not be echoed:

```yaml
- name: Print the OIDC subject this job presents
  run: |
    token=$(curl -sS \
      -H "Authorization: bearer $ACTIONS_ID_TOKEN_REQUEST_TOKEN" \
      "$ACTIONS_ID_TOKEN_REQUEST_URL&audience=sts.amazonaws.com" | jq -r .value)
    echo "$token" | cut -d. -f2 | base64 -d 2>/dev/null \
      | jq '{sub, aud, repository, ref, event_name, workflow}'
```

Compare the printed `sub` against the trust policy's `StringLike` condition. If it carries the
`OWNER@OWNER_ID/REPO@REPO_ID` form, that is the mismatch. Fix it by setting the trust policy's
`sub` condition to the exact string the token presents, ids and all. Matching on the numeric ids
rather than the name form is also the more robust choice going forward, since the ids survive an
owner or repository rename and the names do not. Remove the debug step again once the trust
policy matches; it has no reason to run on every deploy.

**On `sub` matching the ref that actually ran.** The trust policy in this section matches only a
push to `main`, whatever form the `sub` takes. A pull request run carries a different `sub`
(`repo:<owner>/<repo>:pull_request`), and a tag build carries yet another one
(`repo:<owner>/<repo>:ref:refs/tags/<tag>`); neither matches this trust policy, and
`AssumeRoleWithWebIdentity` will simply be denied rather than fall back to anything else. This
surfaces as the immutable-subject-claims error above even when the real cause is a workflow
running under a different ref than the trust policy expects, so rule out the ref before assuming
the ids are wrong.

This matters for the split between `deploy.yml` and `integration.yml`: the integration suite now
also runs on pull requests, but it never assumes this role or touches AWS at all, so nothing about
it needs to match this trust policy. If AWS credentials are ever added to a pull-request-triggered
job, its `sub` will not match `ref:refs/heads/main`, and the trust policy (or the workflow's
`permissions: id-token: write`) will need to account for that deliberately rather than by
assuming whatever works on `main` also works on a pull request.

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

Data transfer beyond the free monthly allowance bills per GB; a low-traffic demo is unlikely to
reach it. Switching the instance type to t4g.large roughly doubles the
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

There is no authentication. Every endpoint, the dashboard and the control plane's API behind it,
is open to anyone who has the URL. Nothing in front of Caddy asks for a credential, and nothing
in the application layer checks for one either.

The URL itself is not a secret even if you never share it. Certificate Transparency logs publish
every certificate Let's Encrypt issues, including the domain name on it, so the hostname is
publicly discoverable the moment Caddy requests its first certificate, independent of who you
send the link to.

The consequence is specific: anyone who finds or is given the URL can add files, change the
vendor's failure mode, and trigger the restart endpoint, which wipes the databases and object
store and reseeds them from scratch.

What actually limits the blast radius: the demo resets to a known state on restart, it holds no
real data, MySQL, Postgres, and MinIO all use their documented default credentials, and the
security group admits only ports 80 and 443, nothing else.

This is a temporary, disposable demo meant to be clicked by whoever receives the link. The
honest recommendation is to stop the instance when it is not being shown, and to leave the old
`/file-migration-system/basic-auth-user` and `/file-migration-system/basic-auth-hash` SSM
parameters out of any future setup; deploy.sh no longer reads them, and they can be deleted from
the account if nothing else uses them.
