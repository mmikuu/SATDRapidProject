import mysql.connector
from mysql.connector import Error
import os
import requests
import sys

# --- 1. 設定 (ここを環境に合わせて確認) ---

# ▼▼▼ MySQLデータベース接続設定 (ベース) ▼▼▼
# 'database' (スキーマ) は下のPROJECTSリストから自動で設定されます
DB_BASE_CONFIG = {
    'host': 'mussel.naist.jp',  # MySQLサーバーのホスト名
    'user': 'root',  # MySQLのユーザー名
    'password': 'hoge',  # MySQLのパスワード
    'autocommit': False  # 最後に明示的にcommit()するため
}
# ▲▲▲ ここまで ▲▲▲

# ▼▼▼ 処理対象プロジェクトリスト ▼▼▼
PROJECTS = [
    {
        'project_name': 'Electron',
        'schema': 'electron_check_release',  # MySQLのデータベース名
        'owner': 'electron',
        'repo': 'electron'
    },
    {
        'project_name': 'JDT.CORE',
        'schema': 'eclipse_check',
        'owner': 'eclipse-jdt',
        'repo': 'eclipse.jdt.core'
    },
    {
        'project_name': 'pde',
        'schema': 'eclipse_check_pde',
        'owner': 'eclipse-pde',
        'repo': 'eclipse.pde'
    },
    {
        'project_name': 'PLATFORM.SWT',
        'schema': 'eclipse_check_swt',
        'owner': 'eclipse-platform',
        'repo': 'eclipse.platform.swt'
    },
    {
        'project_name': 'PLATFORM.UI',
        'schema': 'eclipse_check_ui',
        'owner': 'eclipse-platform',
        'repo': 'eclipse.platform.ui'
    },
    {
        'project_name': 'PLATFORM.EQUINOX',
        'schema': 'eclipse_check_equinox',
        'owner': 'eclipse-equinox',
        'repo': 'equinox'
    },
]
# ▲▲▲ ここまで ▲▲▲

# GitHub GraphQL API エンドポイント
GITHUB_API_URL = 'https://api.github.com/graphql'

# 環境変数からGitHubトークンを取得
GITHUB_TOKEN = 'xxxx'

# --- 2. GitHub API 関連 (変更なし) ---

GRAPHQL_QUERY = """
query GetCommitStats($owner: String!, $repo: String!, $oid: GitObjectID!) {
  repository(owner: $owner, name: $repo) {
    object(oid: $oid) {
      ... on Commit {
        additions
        deletions
      }
    }
  }
}
"""


def get_github_commit_stats(owner, repo, commit_hash):
    """
    GitHub GraphQL API を呼び出して、コミットの追加・削除行数を取得する
    """
    headers = {
        'Authorization': f'bearer {GITHUB_TOKEN}',
        'Content-Type': 'application/json',
    }

    variables = {
        'owner': owner,
        'repo': repo,
        'oid': commit_hash,
    }

    try:
        response = requests.post(
            GITHUB_API_URL,
            headers=headers,
            json={'query': GRAPHQL_QUERY, 'variables': variables}
        )
        response.raise_for_status()

        data = response.json()

        if 'errors' in data:
            print(f"    [APIエラー] {commit_hash[:10]}: {data['errors']}", file=sys.stderr)
            return None

        commit_data = data.get('data', {}).get('repository', {}).get('object')

        if commit_data and 'additions' in commit_data:
            return {
                'additions': commit_data['additions'],
                'deletions': commit_data['deletions'],
            }
        else:
            print(f"    [データ取得失敗] {commit_hash[:10]}: コミットが見つからないか、データ形式が不正です。",
                  file=sys.stderr)
            return None

    except requests.exceptions.RequestException as e:
        print(f"    [HTTPエラー] {commit_hash[:10]}: {e}", file=sys.stderr)
        return None


# --- 3. データベース関連 (MySQL版) ---

def connect_to_db(config):
    """
    指定された設定でMySQLデータベースに接続し、
    コネクションとカーソルを返す
    """
    try:
        conn = mysql.connector.connect(**config)
        if conn.is_connected():
            return conn, conn.cursor(dictionary=True)
    except Error as e:
        # 接続自体に失敗した場合 (例: スキーマが存在しない、認証失敗)
        print(f"データベース接続エラー ({config.get('database')}): {e}", file=sys.stderr)
        return None, None


# --- 4. メイン処理 (プロジェクト・イテレーション版) ---

def main():
    if not GITHUB_TOKEN:
        print("致命的エラー: 環境変数 GITHUB_TOKEN が設定されていません。", file=sys.stderr)
        print("スクリプトを終了します。")
        return

    print("--- バッチ処理開始 ---")

    for project in PROJECTS:
        print(f"\n==============================================")
        print(f"プロジェクト '{project['project_name']}' (スキーマ: '{project['schema']}') の処理を開始します。")

        # このプロジェクト専用のDB設定を作成 (ベース設定 + スキーマ)
        current_db_config = DB_BASE_CONFIG.copy()
        current_db_config['database'] = project['schema']

        # プロジェクト固有のオーナーとリポジトリ
        project_owner = project['owner']
        project_repo = project['repo']

        conn, cursor = connect_to_db(current_db_config)

        # 接続に失敗したら (例: スキーマが存在しない)、次のプロジェクトへ
        if not conn:
            print(f"'{project['project_name']}' の処理をスキップします。")
            continue

        try:
            # 取得するカラムは commit_hash のみ
            # owner, repo はDBから取得しない
            query_select = """
            SELECT commit_hash 
            FROM Commits 
            WHERE `addition` IS NULL
            """
            cursor.execute(query_select)
            commits_to_process = cursor.fetchall()

            if not commits_to_process:
                print("処理対象のコミット（行数がNULL）が見つかりませんでした。")
                # tryブロックを抜けてfinallyで接続を閉じる
                continue

            print(f"{len(commits_to_process)} 件のコミット情報を取得します...")

            query_update = """
            UPDATE Commits 
            SET `addition` = %s, `delition` = %s 
            WHERE commit_hash = %s
            """

            for row in commits_to_process:
                commit_hash = row['commit_hash']

                print(f"  処理中: {project_repo} @ {commit_hash[:10]}...")

                # API呼び出しにはプロジェクトの owner/repo を使用
                stats = get_github_commit_stats(project_owner, project_repo, commit_hash)

                if stats:
                    additions = stats['additions']
                    deletions = stats['deletions']
                    print(f"    -> 結果: +{additions} / -{deletions}")
                    # データベースを更新
                    cursor.execute(query_update, (additions, deletions, commit_hash))
                else:
                    print(f"    -> 失敗: {commit_hash} の情報取得に失敗しました。")

            # このプロジェクトの変更をデータベースにコミット
            conn.commit()
            print(f"プロジェクト '{project['project_name']}' のデータベース更新が完了しました。")

        except Error as e:
            print(f"データベース処理エラー: {e}", file=sys.stderr)
            if conn:
                conn.rollback()  # エラーが発生した場合は変更をロールバック
        finally:
            # 1つのプロジェクトが完了するごとに接続を閉じる
            if conn and conn.is_connected():
                cursor.close()
                conn.close()
                print(f"'{project['project_name']}' の接続を閉じました。")

    print("\n==============================================")
    print("--- 全てのプロジェクト処理が完了しました ---")


if __name__ == '__main__':
    main()