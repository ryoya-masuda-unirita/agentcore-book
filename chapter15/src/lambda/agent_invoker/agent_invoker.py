# 一意なセッションIDのハッシュ生成に使用
import hashlib
# JSON形式の読み書きに使用
import json
# 環境変数の読み込みに使用
import os
# S3キーのURLデコードに使用
import urllib.parse
import boto3

# 環境変数
# 呼び出すAgentCoreランタイムのARN
AGENT_RUNTIME_ARN = os.environ["AGENT_RUNTIME_ARN"]
# 領収書・ユーザー情報が格納されているS3バケット名
BUCKET_NAME = os.environ["BUCKET_NAME"]

# AWSクライアント
# S3からユーザーマスタを読み込むために使用
s3 = boto3.client("s3")
# AgentCoreランタイムを呼び出すAPI用クライアント
agentcore = boto3.client("bedrock-agentcore")


# S3からユーザーIDに対応するユーザー情報を取得
def get_user_info(user_id: str) -> dict:
    # S3からユーザーマスタJSONを取得
    response = s3.get_object(Bucket=BUCKET_NAME, Key="data/users.json")
    # JSONを解析してusersリストを取得
    users = json.loads(response["Body"].read().decode("utf-8"))["users"]
    # user_idが一致するユーザーを検索して返す
    for user in users:
        if user["user_id"] == user_id:
            return user


# AgentCoreランタイムを呼び出して経費処理を開始
def invoke_agent_runtime(
    receipt_key: str, bucket_name: str, user_id: str
) -> dict:
    # receipt_keyのハッシュからセッションIDの素材を生成
    session_hash = hashlib.sha256(receipt_key.encode()).hexdigest()[:16]

    # 申請者情報をS3から取得
    user_info = get_user_info(user_id)

    # エージェントに渡すペイロードを構築
    payload = {
        "receipt_key": receipt_key,
        "submitter_email": user_info["email"],
        "submitter_name": user_info["name"],
    }

    # receipt_keyのハッシュからセッションIDを生成
    # （リトライ時に同じセッションを再利用できる）
    session_id = f"expense-agent-session-{session_hash}"
    # AgentCoreランタイムにペイロードを送信して処理を開始
    response = agentcore.invoke_agent_runtime(
        agentRuntimeArn=AGENT_RUNTIME_ARN,
        runtimeSessionId=session_id,
        # ペイロードはJSON文字列をUTF-8バイト列にエンコード
        payload=json.dumps(payload).encode("utf-8"),
    )
    # ストリーミングレスポンスのチャンクを結合して文字列化
    result = b"".join(response.get("response", [])).decode("utf-8")
    return {"success": True, "session_id": session_id, "result": result}


# S3アップロードイベントを処理してAgentCoreランタイムを起動
def handler(event: dict, context) -> dict:
    # S3イベントの最初のレコードを取得
    record = event["Records"][0]
    bucket_name = record["s3"]["bucket"]["name"]
    # S3キーに含まれるURLエンコードを元の文字列に戻す
    object_key = urllib.parse.unquote_plus(
        record["s3"]["object"]["key"]
    )
    # パス構造 "receipts/USER_ID/filename" からユーザーIDを取得
    user_id = object_key.split("/")[1]

    # AgentCoreランタイムを呼び出して処理を開始
    result = invoke_agent_runtime(object_key, bucket_name, user_id)
    # 日本語をエスケープしない
    body = json.dumps(result, ensure_ascii=False)
    return {"statusCode": 200, "body": body}
