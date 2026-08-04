import { Stack, StackProps, CfnOutput, Fn } from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import * as fs from 'fs';
import * as path from 'path';

// The one thing an owner is likely to change after launch: the instance
// size. t4g.medium (4 GiB) is the default and needs the memory tuning in
// docker-compose.prod.yml to fit the whole stack. t4g.large (8 GiB) fits
// everything with room to spare and removes the need for that tuning.
// Override at synth/deploy time with `-c instanceType=t4g.large` instead
// of editing this file, if that is preferred.
const DEFAULT_INSTANCE_TYPE = 't4g.medium';

// Standard parameter names this stack expects to already exist in SSM
// Parameter Store before the instance boots. See docs/DEPLOYMENT.md for
// how to set each one. None of them are read by CDK itself: the instance
// reads them at boot and on every deploy, so rotating a value never
// requires a stack update.
const PARAMETER_PREFIX = '/file-migration-system';
export const SSM_PARAMETER_NAMES = {
  domain: `${PARAMETER_PREFIX}/domain`,
  basicAuthUser: `${PARAMETER_PREFIX}/basic-auth-user`,
  basicAuthHash: `${PARAMETER_PREFIX}/basic-auth-hash`,
  githubRepo: `${PARAMETER_PREFIX}/github-repo`,
  imageTag: `${PARAMETER_PREFIX}/image-tag`,
};

export class FileMigrationInfraStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const instanceTypeId =
      (this.node.tryGetContext('instanceType') as string | undefined) ?? DEFAULT_INSTANCE_TYPE;

    // One availability zone, public subnets only. No NAT gateway: nothing
    // in this stack runs in a private subnet, so there is nothing a NAT
    // gateway would be for, and it would cost about $33/month doing
    // nothing.
    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 1,
      natGateways: 0,
      subnetConfiguration: [
        {
          name: 'public',
          subnetType: ec2.SubnetType.PUBLIC,
          cidrMask: 24,
        },
      ],
    });

    // Only Caddy is reachable from the internet. Every other service
    // (mysql, postgres, kafka, connect, minio, the migrator services, the
    // control plane) stays on the instance's internal docker network and
    // is never given a security group rule of its own. Admin access is
    // SSM Session Manager, which needs no inbound rule at all.
    const securityGroup = new ec2.SecurityGroup(this, 'InstanceSecurityGroup', {
      vpc,
      description: 'file-migration-system: Caddy only, everything else stays internal',
      allowAllOutbound: true,
    });
    securityGroup.addIngressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(80),
      'browser to Caddy, HTTP, redirected to HTTPS'
    );
    securityGroup.addIngressRule(
      ec2.Peer.anyIpv4(),
      ec2.Port.tcp(443),
      'browser to Caddy, HTTPS'
    );
    securityGroup.addIngressRule(
      ec2.Peer.anyIpv6(),
      ec2.Port.tcp(80),
      'browser to Caddy, HTTP, redirected to HTTPS, IPv6'
    );
    securityGroup.addIngressRule(
      ec2.Peer.anyIpv6(),
      ec2.Port.tcp(443),
      'browser to Caddy, HTTPS, IPv6'
    );

    // SSM Session Manager for a shell, plus read access to this app's own
    // parameters. Nothing broader: no S3, no other services' parameters,
    // no ability to change anything in the account.
    const role = new iam.Role(this, 'InstanceRole', {
      assumedBy: new iam.ServicePrincipal('ec2.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSSMManagedInstanceCore'),
      ],
    });
    role.addToPolicy(
      new iam.PolicyStatement({
        actions: ['ssm:GetParameter', 'ssm:GetParameters'],
        resources: [
          `arn:${this.partition}:ssm:${this.region}:${this.account}:parameter${PARAMETER_PREFIX}/*`,
        ],
      })
    );

    const userDataScript = fs.readFileSync(
      path.join(__dirname, '..', 'scripts', 'user-data.sh'),
      'utf8'
    );
    const userData = ec2.UserData.forLinux();
    userData.addCommands(userDataScript);

    const instance = new ec2.Instance(this, 'Instance', {
      vpc,
      vpcSubnets: { subnetType: ec2.SubnetType.PUBLIC },
      instanceType: new ec2.InstanceType(instanceTypeId),
      machineImage: ec2.MachineImage.latestAmazonLinux2023({
        cpuType: ec2.AmazonLinuxCpuType.ARM_64,
      }),
      securityGroup,
      role,
      userData,
      blockDevices: [
        {
          deviceName: '/dev/xvda',
          volume: ec2.BlockDeviceVolume.ebs(30, {
            volumeType: ec2.EbsDeviceVolumeType.GP3,
            encrypted: true,
            deleteOnTermination: true,
          }),
        },
      ],
      // First-boot user data already covers the deploy; nothing else in
      // this stack needs the instance to signal readiness back to
      // CloudFormation.
      requireImdsv2: true,
    });

    // A stopped-and-restarted instance keeps this address, so the DNS A
    // record the owner points at it never needs to change. An EIP is free
    // while it is attached to a running instance and billed hourly only
    // while it sits unattached, which never happens here since it is
    // associated for the life of the stack.
    const eip = new ec2.CfnEIP(this, 'Eip', {
      domain: 'vpc',
    });
    new ec2.CfnEIPAssociation(this, 'EipAssociation', {
      allocationId: eip.attrAllocationId,
      instanceId: instance.instanceId,
    });

    new CfnOutput(this, 'ElasticIp', {
      value: eip.ref,
      description: 'Point the domain A record at this address',
    });
    new CfnOutput(this, 'InstanceId', {
      value: instance.instanceId,
    });
    new CfnOutput(this, 'DnsReminder', {
      value: Fn.sub(
        'Create an A record for the domain stored in ${paramName} pointing at the Elastic IP above, before the first deploy runs, so Caddy can obtain its certificate',
        { paramName: SSM_PARAMETER_NAMES.domain }
      ),
    });
  }
}
