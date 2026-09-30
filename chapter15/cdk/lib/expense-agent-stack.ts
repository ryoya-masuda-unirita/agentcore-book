// CDKコアライブラリ（スタック・Duration・RemovalPolicyなど）
import * as cdk from 'aws-cdk-lib';
// S3バケットの定義
import * as s3 from 'aws-cdk-lib/aws-s3';
// S3イベント通知（Lambda連携）
import * as s3n from 'aws-cdk-lib/aws-s3-notifications';
// Lambda関数の定義
import * as lambda from 'aws-cdk-lib/aws-lambda';
// IAMロール・ポリシーの定義
import * as iam from 'aws-cdk-lib/aws-iam';
// DynamoDBテーブルの定義
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
// SNSトピックの定義
import * as sns from 'aws-cdk-lib/aws-sns';
// SNSへのメールサブスクリプション
import * as subscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
// ECRへのDockerイメージのプラットフォーム設定
import * as ecrAssets from 'aws-cdk-lib/aws-ecr-assets';
// CDKデプロイ時にS3へファイルをアップロード
import * as s3deploy from 'aws-cdk-lib/aws-s3-deployment';
// AgentCoreランタイムのCDK Alpha構成
import * as agentcore from '@aws-cdk/aws-bedrock-agentcore-alpha';
// CDKデプロイ時にDockerイメージをビルドしてECRにプッシュ
import { ContainerImageBuild } from '@cdklabs/deploy-time-build';
// CDKコンストラクトの基底クラス
import { Construct } from 'constructs';
// .envファイルを環境変数として読み込む
import * as dotenv from 'dotenv';
// ファイルパスの組み立てに使用
import * as path from 'path';
// ファイルの読み込みに使用（users.json）
import * as fs from 'fs';

// .envファイルを読み込み（chapter15/.env）
dotenv.config({
  path: path.join(__dirname, '../../.env'),
  override: true,
});

export class ExpenseAgentStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // users.jsonのパスを組み立て
    const usersJsonPath = path.join(
      __dirname, '../../data/users.json'
    );
    // ユーザーマスタを読み込む
    const usersData = this.loadUsersJson(usersJsonPath);
    // ユーザーのメールアドレス一覧を抽出
    const emailAddresses = usersData.map(u => u.email);

    // 領収書画像・マスタデータを格納するS3バケット
    const bucket = new s3.Bucket(this, 'ExpenseAgentBucket', {
      // アカウントIDをバケット名に含めてグローバル一意にする
      bucketName: `expense-agent-${this.account}`,
      // スタック削除時にバケットも一緒に削除
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      // 削除時にオブジェクトも自動削除（DESTROY に必要な設定）
      autoDeleteObjects: true,
    });

    // CDKデプロイ時にマスタデータをS3にアップロード
    new s3deploy.BucketDeployment(this, 'DataDeployment', {
      sources: [
        s3deploy.Source.asset(
          path.join(__dirname, '../../data')
        ),
      ],
      destinationBucket: bucket,
      // "data/" プレフィックスを付けて配置
      destinationKeyPrefix: 'data',
    });

    // 承認の状態（pending/approved/rejected）を管理するテーブル
    const approvalTable = new dynamodb.Table(
      this, 'ApprovalTable', {
        tableName: 'expense-agent-approvals',
        // パーティションキー: SHA256ハッシュから生成した承認ID
        partitionKey: {
          name: 'approval_id',
          type: dynamodb.AttributeType.STRING,
        },
        // ソートキー: 作成日時（UTC ISO 8601形式）
        sortKey: {
          name: 'created_at',
          type: dynamodb.AttributeType.STRING,
        },
        // 従量課金（リクエスト数に応じた課金）
        billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      },
    );

    // ユーザーごとのSNSトピックと対応マップを初期化
    const snsTopics: Record<string, sns.Topic> = {};
    // Lambda環境変数に渡す email→topicArn のマッピング
    const snsTopicMap: Record<string, string> = {};

    emailAddresses.forEach((email, i) => {
      // SNSトピック名に使えない文字を安全な文字に置換
      const safeEmail = email
        .replace('@', '-at-')
        .replace(/\./g, '-')
        .replace(/\+/g, '-plus-');
      // ユーザーごとにSNSトピックを作成
      const topic = new sns.Topic(
        this, `NotificationTopic${i}`, {
          topicName: `expense-notification-${safeEmail}`,
          displayName: `経費精算通知: ${email}`,
        },
      );
      // そのユーザーのメールアドレスをサブスクライブ
      topic.addSubscription(
        new subscriptions.EmailSubscription(email)
      );
      // email → topicオブジェクト のマップに登録
      snsTopics[email] = topic;
      // email → topicArn のマップに登録（環境変数として渡す）
      snsTopicMap[email] = topic.topicArn;
    });

    // 承認コールバックLambda用の実行ロール
    const lambdaRole = new iam.Role(
      this, 'LambdaExecutionRole', {
        // Lambdaサービスがこのロールを引き受けられるように設定
        assumedBy: new iam.ServicePrincipal(
          'lambda.amazonaws.com'
        ),
        managedPolicies: [
          // CloudWatch Logsへのログ書き込み権限
          iam.ManagedPolicy.fromAwsManagedPolicyName(
            'service-role/AWSLambdaBasicExecutionRole'
          ),
        ],
      },
    );
    // DynamoDB承認テーブルへの読み書き権限を付与
    approvalTable.grantReadWriteData(lambdaRole);
    // SNS経由の通知メール送信権限を付与
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['sns:Publish'],
      resources: [
        `arn:aws:sns:${this.region}:${this.account}:expense-notification-*`,
      ],
    }));

    // AgentInvoker Lambda用の実行ロール
    const agentInvokerRole = new iam.Role(
      this, 'AgentInvokerRole', {
        assumedBy: new iam.ServicePrincipal(
          'lambda.amazonaws.com'
        ),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName(
            'service-role/AWSLambdaBasicExecutionRole'
          ),
        ],
      },
    );
    // S3からユーザーマスタを読む権限を付与
    bucket.grantRead(agentInvokerRole);

    // AgentCoreランタイムコンテナが引き受ける実行ロール
    const agentExecutionRole = new iam.Role(
      this, 'AgentExecutionRole', {
        roleName: 'expense-agent-execution-role',
        // 複数サービスから引き受けられるよう複合プリンシパルを設定
        assumedBy: new iam.CompositePrincipal(
          new iam.ServicePrincipal(
            'bedrock-agentcore.amazonaws.com'
          ),
          new iam.ServicePrincipal(
            'bedrock.amazonaws.com'
          ),
          new iam.ServicePrincipal(
            'lambda.amazonaws.com'
          ),
        ),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName(
            'service-role/AWSLambdaBasicExecutionRole'
          ),
        ],
      },
    );
    // 領収書画像・マスタデータの読み書き権限を付与
    bucket.grantReadWrite(agentExecutionRole);
    // 承認管理テーブルへの読み書き権限を付与
    approvalTable.grantReadWriteData(agentExecutionRole);
    // Bedrockモデル呼び出しとECR認証トークン取得権限を付与
    agentExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'bedrock:InvokeModel',
          'bedrock:InvokeModelWithResponseStream',
          'ecr:GetAuthorizationToken',
        ],
        resources: ['*'],
      }),
    );
    // ECR公開イメージ利用に必要なMarketplace権限を付与
    agentExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'aws-marketplace:Subscribe',
          'aws-marketplace:Unsubscribe',
          'aws-marketplace:ViewSubscriptions',
        ],
        resources: ['*'],
      }),
    );
    // AgentCoreランタイムのCloudWatch Logsへの書き込み権限
    agentExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: [
          'logs:DescribeLogStreams',
          'logs:CreateLogGroup',
          'logs:DescribeLogGroups',
          'logs:CreateLogStream',
          'logs:PutLogEvents',
        ],
        resources: [
          `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/bedrock-agentcore/runtimes/*`,
          `arn:aws:logs:${this.region}:${this.account}:log-group:/aws/bedrock-agentcore/runtimes/*:log-stream:*`,
          `arn:aws:logs:${this.region}:${this.account}:log-group:*`,
        ],
      }),
    );
    // SNS経由で承認メールを送る権限を付与
    agentExecutionRole.addToPolicy(
      new iam.PolicyStatement({
        actions: ['sns:Publish'],
        resources: [
          `arn:aws:sns:${this.region}:${this.account}:expense-notification-*`,
        ],
      }),
    );

    // 承認・却下ボタンのクリックを受け付けるLambda
    const approvalLambda = new lambda.Function(
      this, 'ApprovalCallbackFunction', {
        functionName: 'expense-agent-approval-callback',
        runtime: lambda.Runtime.PYTHON_3_14,
        handler: 'approval_callback.handler',
        code: lambda.Code.fromAsset(
          path.join(
            __dirname, '../../src/lambda/approval_callback'
          )
        ),
        role: lambdaRole,
        // AgentCore起動を待つため60秒に設定
        timeout: cdk.Duration.seconds(60),
        environment: {
          APPROVAL_TABLE: approvalTable.tableName,
          AGENT_RUNTIME_NAME: 'expense_agent',
          // JSON文字列に変換して環境変数として渡す
          SNS_TOPIC_MAP: cdk.Fn.toJsonString(snsTopicMap),
        },
      },
    );

    // 承認メールのリンクから直接アクセスできるパブリックHTTPエンドポイント
    const approvalFunctionUrl = approvalLambda.addFunctionUrl({
      // 認証なし（メール内のリンクはトークンで保護）
      authType: lambda.FunctionUrlAuthType.NONE,
      cors: {
        allowedOrigins: ['*'],
        allowedMethods: [
          lambda.HttpMethod.GET,
          lambda.HttpMethod.POST,
        ],
        allowedHeaders: ['Content-Type', 'Authorization'],
      },
    });

    // CDKデプロイ時にDockerfileをビルドしてECRにプッシュ
    const agentImage = new ContainerImageBuild(
      this, 'AgentImage', {
        directory: '..',
        file: 'docker/Dockerfile',
        // AgentCoreが動くARM64アーキテクチャ用にビルド
        platform: ecrAssets.Platform.LINUX_ARM64,
        // ビルドに不要なディレクトリを除外してビルド時間を短縮
        exclude: ['cdk', '.git', '.venv', 'node_modules'],
      },
    );
    // エージェント実行ロールにECRイメージのプル権限を付与
    agentImage.repository.grantPull(agentExecutionRole);

    // AgentCoreランタイムのアーティファクト（ECRイメージ）を指定
    const mainAgentArtifact =
      agentcore.AgentRuntimeArtifact.fromEcrRepository(
        agentImage.repository,
        agentImage.imageTag,
      );

    // AgentCoreランタイムを作成
    const agentRuntime = new agentcore.Runtime(
      this, 'ExpenseAgentRuntime', {
        runtimeName: 'expense_agent',
        agentRuntimeArtifact: mainAgentArtifact,
        description: '経費精算エージェント（マルチモーダル解析・分類・承認）',
        executionRole: agentExecutionRole,
        lifecycleConfiguration: {
          // 30分操作がなければセッションを終了してコスト削減
          idleRuntimeSessionTimeout:
            cdk.Duration.minutes(30),
        },
        // エージェントコンテナに渡す環境変数
        environmentVariables: {
          AWS_REGION: 'us-east-1',
          BEDROCK_MODEL_ID: process.env.BEDROCK_MODEL_ID!,
          BUCKET_NAME: bucket.bucketName,
          APPROVAL_TABLE: approvalTable.tableName,
          // 承認コールバックLambdaのFunction URL
          APPROVAL_API_URL: approvalFunctionUrl.url,
          // JSON文字列に変換して環境変数として渡す
          SNS_TOPIC_MAP: cdk.Fn.toJsonString(snsTopicMap),
          CONFLUENCE_URL: process.env.CONFLUENCE_URL ?? '',
          CONFLUENCE_SPACE_KEY: process.env.CONFLUENCE_SPACE_KEY ?? '',
          CONFLUENCE_USERNAME: process.env.CONFLUENCE_EMAIL ?? '',
          CONFLUENCE_API_TOKEN: process.env.CONFLUENCE_API_TOKEN ?? '',
        },
      },
    );

    // AgentInvokerLambdaにRuntimeの呼び出し権限を付与
    agentRuntime.grantInvokeRuntime(agentInvokerRole);

    // 承認コールバックLambdaにRuntime呼び出し権限を付与
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['bedrock-agentcore:InvokeAgentRuntime'],
      resources: [
        `arn:aws:bedrock-agentcore:${this.region}:${this.account}:runtime/*`,
      ],
    }));
    // Runtime一覧取得権限（ARNを動的に取得するため）
    lambdaRole.addToPolicy(new iam.PolicyStatement({
      actions: ['bedrock-agentcore:ListAgentRuntimes'],
      resources: ['*'],
    }));

    // S3に領収書がアップロードされたらエージェントを起動するLambda
    const agentInvokerLambda = new lambda.Function(
      this, 'AgentInvokerFunction', {
        functionName: 'expense-agent-invoker',
        runtime: lambda.Runtime.PYTHON_3_14,
        handler: 'agent_invoker.handler',
        code: lambda.Code.fromAsset(
          path.join(
            __dirname, '../../src/lambda/agent_invoker'
          )
        ),
        role: agentInvokerRole,
        // AgentCore起動を待つため15分に設定
        timeout: cdk.Duration.minutes(15),
        environment: {
          AGENT_RUNTIME_ARN:
            agentRuntime.agentRuntimeArn,
          BUCKET_NAME: bucket.bucketName,
        },
      },
    );

    // "receipts/" プレフィックスのオブジェクト作成でLambdaを起動
    bucket.addEventNotification(
      s3.EventType.OBJECT_CREATED,
      new s3n.LambdaDestination(agentInvokerLambda),
      // "receipts/" のみ対象（data/ などのアップロードは除外）
      { prefix: 'receipts/' },
    );
  }

  // users.jsonを読み込んでユーザー一覧を返す
  private loadUsersJson(
    filePath: string
  ): Array<{ email: string }> {
    // ファイルをUTF-8で読み込んでJSON解析
    const data = JSON.parse(
      fs.readFileSync(filePath, 'utf-8')
    );
    return data.users;
  }
}
