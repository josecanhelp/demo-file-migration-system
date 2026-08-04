#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { FileMigrationInfraStack } from '../lib/stack';

const app = new cdk.App();

new FileMigrationInfraStack(app, 'FileMigrationInfraStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
});
