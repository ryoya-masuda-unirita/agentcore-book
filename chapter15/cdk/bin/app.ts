#!/usr/bin/env node
// CDKアプリのエントリーポイント
import * as cdk from 'aws-cdk-lib';
import { ExpenseAgentStack } from '../lib/expense-agent-stack';

// CDKアプリのインスタンスを作成
const app = new cdk.App();

// 経費精算エージェントのスタックを作成
new ExpenseAgentStack(app, 'ExpenseAgentStack', {
  env: {
    // CDK CLIが自動的に設定するAWSアカウントID
    account: process.env.CDK_DEFAULT_ACCOUNT,
    // Bedrockが利用可能なバージニア北部をデフォルトに設定
    region: process.env.CDK_DEFAULT_REGION ?? 'us-east-1',
  },
  description: 'Expense Agent Infrastructure',
});
