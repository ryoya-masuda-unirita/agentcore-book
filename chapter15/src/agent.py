# Confluence Basic認証ヘッダーのBase64エンコードに使用
import base64
# 経費ID・承認IDの一意なハッシュ生成に使用
import hashlib
# JSON形式の読み書き全般に使用
import json
# 環境変数の読み込みに使用
import os
# 重い処理をバックグラウンドで非同期実行するために使用
import threading
# Confluence REST APIへのHTTPリクエストに使用
import urllib.request
# UTC現在時刻のタイムスタンプ生成に使用
from datetime import datetime, timezone

import boto3
# DynamoDBのクエリ条件式を組み立てるビルダー
from boto3.dynamodb.conditions import Key
# AgentCoreランタイムへのデプロイ基盤
from bedrock_agentcore import BedrockAgentCoreApp
# 構造化出力のスキーマ（型チェック）定義に使用
from pydantic import BaseModel
# エージェント本体とツールデコレータ
from strands import Agent, tool
# Strands AgentsからBedrockモデルを使うラッパー
from strands.models import BedrockModel


# 領収書の明細1行分を表すデータクラス
class ReceiptItem(BaseModel):
    name: str    # 商品名
    amount: int  # 金額（円）

# 領収書1枚分の情報を表すデータクラス
class ReceiptInfo(BaseModel):
    vendor_name: str       # 支払先名
    transaction_date: str  # 取引日
    total: int             # 合計金額（円）
    items: list[ReceiptItem]  # 明細一覧

# 経費分類の結果を表すデータクラス
class ClassificationInfo(BaseModel):
    category: str  # 経費カテゴリ名


# 環境変数
# 使用するBedrockモデルのID
BEDROCK_MODEL_ID = os.environ["BEDROCK_MODEL_ID"]
APPROVAL_AMOUNT_THRESHOLD = 100000  # 承認閾値（10万円）
# デプロイ先のAWSリージョン
AWS_REGION = os.environ["AWS_REGION"]
# 領収書画像・マスタデータを格納するS3バケット名
BUCKET_NAME = os.environ["BUCKET_NAME"]
# 承認コールバックLambdaのFunction URL
APPROVAL_API_URL = os.environ["APPROVAL_API_URL"]
# DynamoDB承認管理テーブル名
APPROVAL_TABLE = os.environ["APPROVAL_TABLE"]
# {"email": "topicArn"} 形式のJSON文字列を辞書に変換
SNS_TOPIC_MAP = json.loads(os.environ["SNS_TOPIC_MAP"])
# Confluenceの基底URL（末尾スラッシュを除去して統一）
CONFLUENCE_URL = os.environ["CONFLUENCE_URL"].rstrip("/")
# Confluenceログイン用メールアドレス
CONFLUENCE_USERNAME = os.environ["CONFLUENCE_USERNAME"]
# Confluence APIアクセストークン
CONFLUENCE_API_TOKEN = os.environ["CONFLUENCE_API_TOKEN"]
# 経費記録を書き込むConfluenceスペースキー
CONFLUENCE_SPACE_KEY = os.environ["CONFLUENCE_SPACE_KEY"]

# AWSクライアント
# 領収書画像・マスタデータの読み書き用
s3 = boto3.client("s3", region_name=AWS_REGION)
# 承認依頼・結果通知メール送信用
sns = boto3.client("sns", region_name=AWS_REGION)
# テーブル操作に便利な高レベルDynamoDB APIを使用
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
# 承認管理テーブルオブジェクトを取得
table = dynamodb.Table(APPROVAL_TABLE)


# S3からユーザーマスタを読み込む
def load_users_from_s3() -> list[dict]:
    # S3からusers.jsonを取得
    response = s3.get_object(Bucket=BUCKET_NAME, Key="data/users.json")
    # JSONを解析してusersリストを返す
    return json.loads(response["Body"].read().decode("utf-8"))["users"]


SYSTEM_PROMPT = """
あなたは経費精算を支援するAIエージェントです。
領収書の解析、経費分類、承認プロセス、承認後の記録を担当します。

## 領収書処理フロー（S3アップロード時）
1. `process_receipt_image`でマルチモーダル解析
2. `search_classification_info`で経費分類
3. `get_approver_by_amount`で承認者候補を取得し、金額に基づいて1人選定
   - 金額 <= 10万: 課長 / 金額 > 10万: 部長
4. `send_approval_request`で承認依頼メールを送信
   - 必要な全情報（経費ID、金額、カテゴリー、内容、ベンダー名、取引日、申請者、承認者）を渡す

## 承認後処理フロー（承認コールバック時）
- 承認（approved）の場合: `write_to_confluence`でConfluenceに経費記録を書き込む
- 却下（rejected）の場合: 何もしない
"""


# S3から領収書画像を取得してマルチモーダル解析
@tool
def process_receipt_image(receipt_key: str) -> dict:
    # S3から領収書画像バイナリを取得
    response = s3.get_object(Bucket=BUCKET_NAME, Key=receipt_key)
    # レスポンスヘッダーからMIMEタイプを取得（デフォルトはJPEG）
    content_type = response.get("ContentType", "image/jpeg")
    # "image/jpeg" のようなMIMEタイプから拡張子部分を取り出す
    media_type = content_type.split("/")[-1]
    # Bedrock APIが受け付ける"png"または"jpeg"に正規化
    image_format = "png" if media_type == "png" else "jpeg"
    # 画像バイナリを読み込む
    image_data = response["Body"].read()

    # 画像解析専用エージェントを作成（ツールは不要なので空リスト）
    model = BedrockModel(model_id=BEDROCK_MODEL_ID)
    extraction_agent = Agent(model=model, tools=[])

    # 画像とテキスト指示をまとめてマルチモーダル解析を実行
    result = extraction_agent(
        [
            {"image": {
                "format": image_format,
                "source": {"bytes": image_data},
            }},
            {"text": "この領収書を解析してください"}
        ],
        # 出力をReceiptInfo形式の構造化データとして取得
        structured_output_model=ReceiptInfo,
    )
    # Pydanticモデルをエージェントが返せる辞書に変換
    receipt = result.structured_output.model_dump()
    return {"success": True, "receipt": receipt}


# 経費分類のための情報を検索・推論
@tool
def search_classification_info(
    vendor_name: str, description: str, amount: int
) -> dict:
    # デバッグ用ログ出力
    print(f"[分類] ベンダー={vendor_name},"
          f" 内容={description}, 金額={amount}")

    # S3から経費分類ルールJSONを取得
    rules = json.loads(
        s3.get_object(
            Bucket=BUCKET_NAME,
            Key="data/classification_rules.json",
        )["Body"].read().decode("utf-8")
    )["rules"]

    # vendor_keywordsにマッチするカテゴリを最初の1件取得
    matched = next(
        # ルール一覧からカテゴリーを探す
        (r["category"] for r in rules
         # vendor_nameにキーワードが含まれるか確認
         if any(kw in vendor_name for kw in r.get("vendor_keywords", []))),
        # マッチしない場合のデフォルト値
        None
    )

    # 社内ルールにマッチした場合はそのまま返す
    if matched:
        print(f"[分類] 社内ルールにマッチ: {matched}")
        return {
            "success": True,
            "category": matched,
            # マッチ元を記録（社内ルール適用であることを示す）
            "source": "internal_rule",
        }

    # ルールにマッチしない場合はLLMで推論
    print("[分類] 社内ルールにマッチなし、LLM推論を実行")
    # 経費情報を含んだ推論プロンプトを構築
    query = f"""
    以下の経費情報から適切な経費カテゴリーを判断してください。
    支払先: {vendor_name}
    内容: {description}
    金額: {amount:,}円
    カテゴリー: 交通費、宿泊費、交際費、消耗品費、通信費、備品費、研修費、その他"""

    # LLMで経費カテゴリーを推論
    agent = Agent(
        model=BedrockModel(model_id=BEDROCK_MODEL_ID),
        tools=[]
    )
    result = agent(query, structured_output_model=ClassificationInfo)
    print(f"[分類] LLM推論結果: {result.structured_output.category}")
    # Pydanticモデルを辞書に変換して返す
    output = result.structured_output.model_dump()
    return {"success": True, **output, "source": "llm_inference"}


# 金額に基づいて承認者を決定するための情報を取得
@tool
def get_approver_by_amount(amount: int) -> dict:
    # S3からユーザーマスタを読み込む
    users = load_users_from_s3()
    # 課長・部長のみを承認者候補として抽出
    approvers = [
        {"name": u["name"], "email": u["email"], "role": u["role"]}
        for u in users if u.get("role") in ["課長", "部長"]
    ]
    # 閾値も返してエージェントが承認者を選べるようにする
    return {
        "success": True,
        "amount": amount,
        "threshold": APPROVAL_AMOUNT_THRESHOLD,
        "approvers": approvers
    }


# 承認依頼メールを送信
@tool
def send_approval_request(
    expense_id: str, amount: int,
    category: str, description: str,
    vendor_name: str,
    submitter_name: str, submitter_email: str,
    approver_name: str, approver_email: str,
    transaction_date: str = "", items: list = None,
) -> dict:
    # 経費IDのハッシュから16文字の承認IDを生成（重複送信防止）
    approval_id = hashlib.sha256(expense_id.encode()).hexdigest()[:16]
    # 同じ承認IDが既に存在するか確認
    existing = table.query(
        KeyConditionExpression=Key("approval_id").eq(approval_id),
        Limit=1,
    )
    # 既に送信済みの場合はスキップして既存IDを返す
    if existing.get("Items"):
        return {
            "success": True, "already_exists": True,
            "approval_id": approval_id,
        }

    # 承認・却下リンクのベースURLを構築
    approval_url = f"{APPROVAL_API_URL}?token={approval_id}"
    # DynamoDBに承認レコードを保存
    table.put_item(Item={
        "approval_id": approval_id,
        # ソートキー（UTC時刻でレコードを一意に識別）
        "created_at": datetime.now(timezone.utc).isoformat(),
        "expense_id": expense_id,
        "amount": amount,
        "category": category,
        "description": description,
        "vendor_name": vendor_name,
        "submitter_name": submitter_name,
        "submitter_email": submitter_email,
        "approver_name": approver_name,
        "approver_email": approver_email,
        "transaction_date": transaction_date,
        "items": items or [],
        # 初期ステータスは保留中
        "status": "pending",
    })

    # メール件名と本文を構築
    subject = f"【承認依頼】経費精算: {expense_id} - {vendor_name}"
    body = f"""
    経費精算の承認依頼です。
    経費ID: {expense_id}
    金額: {amount:,}円
    カテゴリ: {category}
    支払先: {vendor_name}
    内容: {description}
    申請者: {submitter_name} ({submitter_email})

    ▼ 承認: {approval_url}&action=approve
    ▼ 却下: {approval_url}&action=reject
    """
    # 承認者のSNSトピックにメールを送信
    sns.publish(
        TopicArn=SNS_TOPIC_MAP[approver_email],
        Subject=subject,
        Message=body
    )
    return {"success": True, "approval_id": approval_id}


# 承認済み経費をConfluenceに書き込む
@tool
def write_to_confluence(
    expense_id: str, amount: int,
    category: str, vendor_name: str,
    description: str, transaction_date: str,
    submitter_name: str, approver_name: str,
) -> dict:
    # Basic認証用の認証情報を組み立て
    credentials = f"{CONFLUENCE_USERNAME}:{CONFLUENCE_API_TOKEN}"
    # バイト列→Base64→文字列 の順にエンコード
    encoded = base64.b64encode(credentials.encode()).decode()
    # HTTPヘッダーに付与するBasic認証文字列
    auth_header = f"Basic {encoded}"

    # 経費情報をHTMLテーブルとして整形
    content = f"""
    <h1>経費精算記録: {expense_id}</h1>
    <table><tbody>
    <tr><th>金額</th><td>{amount:,}円</td></tr>
    <tr><th>カテゴリ</th><td>{category}</td></tr>
    <tr><th>支払先</th><td>{vendor_name}</td></tr>
    <tr><th>内容</th><td>{description}</td></tr>
    <tr><th>取引日</th><td>{transaction_date}</td></tr>
    <tr><th>申請者</th><td>{submitter_name}</td></tr>
    <tr><th>承認者</th><td>{approver_name}</td></tr>
    </tbody></table>"""

    # Confluence APIのリクエストボディを構築
    page_data = {
        "type": "page",  # ページとして作成
        "title": f"経費精算記録: {expense_id}",
        # 書き込み先スペースを指定
        "space": {"key": CONFLUENCE_SPACE_KEY},
        # ストレージ形式でHTML本文を指定
        "body": {"storage": {
            "value": content,
            "representation": "storage",
        }},
    }
    # ページ作成APIのエンドポイント
    url = f"{CONFLUENCE_URL}/wiki/rest/api/content"
    headers = {
        "Authorization": auth_header,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    # HTTPリクエストオブジェクトを構築してPOST送信
    req = urllib.request.Request(
        url,
        data=json.dumps(page_data).encode("utf-8"),
        headers=headers, method="POST",
    )
    # タイムアウト30秒でConfluence APIを呼び出し
    with urllib.request.urlopen(req, timeout=30) as response:
        result = json.loads(response.read().decode("utf-8"))

    # 作成ページのリンク情報を取得してフルURLに組み立て
    links = result.get("_links", {})
    page_url = f"{CONFLUENCE_URL}/wiki{links.get('webui', '')}"
    return {"success": True, "page_id": result.get("id"), "url": page_url}


# エージェントを生成して返す
def create_agent() -> Agent:
    model = BedrockModel(model_id=BEDROCK_MODEL_ID)
    # エージェントが呼び出せるツールを一覧で指定
    tools = [
        process_receipt_image,
        search_classification_info,
        get_approver_by_amount,
        send_approval_request,
        write_to_confluence,
    ]
    return Agent(model=model, system_prompt=SYSTEM_PROMPT, tools=tools)


# AgentCoreランタイムのアプリインスタンスを作成
app = BedrockAgentCoreApp()


# ペイロードにactionキーがあるかでルーティング
@app.entrypoint
def handle_request(request: dict) -> dict:
    # actionキーがあれば承認コールバック処理
    if request.get("action"):
        return process_approval(request)
    # なければS3アップロード起点の経費処理
    return process_expense(request)


# S3アップロードイベントを受けて経費処理を開始
def process_expense(request: dict) -> dict:
    # リクエストから直接S3キーを取得
    key = request.get("receipt_key")
    # なければEventBridgeイベント形式から取得
    if not key:
        detail = request.get("detail", {})
        key = detail.get("object", {}).get("key")
    submitter_name = request.get("submitter_name")
    submitter_email = request.get("submitter_email")
    # S3キーのハッシュから冪等な経費IDを生成
    expense_id = f"EXP-{hashlib.sha256(key.encode()).hexdigest()[:12]}"

    # エージェントへの指示プロンプトを構築
    prompt = f"""
以下の領収書を処理してください。

- 経費ID: {expense_id}
- S3キー: {key}
- 申請者名: {submitter_name}
- 申請者メール: {submitter_email}
"""

    # 非同期タスクを登録してIDを取得
    task_id = app.add_async_task(
        "expense_processing", {"expense_id": expense_id})

    # バックグラウンドスレッドで実行する内部関数
    def worker():
        try:
            # エージェントを生成してプロンプトを実行
            create_agent()(prompt)
        finally:
            # エラー時も確実に完了を通知
            app.complete_async_task(task_id)

    # daemon=True でメイン終了時にスレッドも自動停止
    threading.Thread(target=worker, daemon=True).start()

    # 即時レスポンス（処理はバックグラウンドで継続）
    return {"accepted": True, "expense_id": expense_id}


# 承認コールバックからの処理
def process_approval(request: dict) -> dict:
    # "approve" または "reject"
    action = request.get("action")
    # 承認レコード一式（金額・カテゴリ等）を取得
    record = request.get("approval_record", {})
    expense_id = record.get("expense_id")

    # 承認結果をエージェントに伝えるプロンプトを構築
    prompt = f"""
以下の承認結果を処理してください。

- アクション: {action}
- 経費ID: {expense_id}
- 金額: {record.get('amount')}
- カテゴリー: {record.get('category')}
- 支払先: {record.get('vendor_name')}
- 内容: {record.get('description')}
- 取引日: {record.get('transaction_date')}
- 申請者: {record.get('submitter_name')}
- 承認者: {record.get('approver_name')}
"""

    # エージェントを生成して承認後処理を実行
    result = create_agent()(prompt)
    # 結果を文字列に変換して返す
    output = str(result)
    return {"success": True, "expense_id": expense_id, "result": output}


# メインエントリーポイント
def main():
    # AgentCoreランタイムのHTTPサーバーを起動
    app.run()


if __name__ == "__main__":
    main()
