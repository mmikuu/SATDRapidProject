import requests
import json
import mysql.connector
import time


# --- 設定ファイルを読み込む関数 ---
def load_config():
    """settings.jsonを読み込み、設定を返す"""
    try:
        with open('settings.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("エラー: 'settings.json' が見つかりません。スクリプトを終了します。")
        exit()
    except json.JSONDecodeError:
        print("エラー: 'settings.json' の形式が正しくありません。")
        exit()


# --- データベース接続関数 ---
def connect_to_database():
    """設定ファイルの情報を使ってMySQLデータベースに接続する"""
    try:
        conn = mysql.connector.connect(
            host=config.get('db_host'),
            port=config.get('db_port'),
            user=config.get('db_user'),
            password=config.get('db_password'),
            database=config.get('db_name')
        )
        return conn
    except mysql.connector.Error as err:
        print(f"データベース接続エラー: {err}")
        return None


# --- タスクキューを準備・登録するメイン関数 ---
def populate_task_queue():
    """GitHubからリポジトリリストを取得し、タスクキューに登録する"""
    print("--- GitHubから人気リポジトリのリストを検索中 ---")

    conn = connect_to_database()
    if not conn: return

    cursor = conn.cursor()

    # task_queueテーブルを作成（存在しない場合のみ）
    try:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS task_queue (
                task_id INT AUTO_INCREMENT PRIMARY KEY,
                repo_name VARCHAR(255) UNIQUE NOT NULL,
                status ENUM('queued', 'processing', 'completed', 'failed') DEFAULT 'queued',
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at DATETIME NULL,
                completed_at DATETIME NULL
            );
        ''')
        conn.commit()
    except mysql.connector.Error as err:
        print(f"task_queueテーブルの作成に失敗: {err}")
        cursor.close()
        conn.close()
        return

    star_ranges = []

    # 1000 から 1499 までは 10 刻み
    for i in range(1000, 1500, 10):
        star_ranges.append(f"{i}..{i + 9}")

    # 1500 から 4999 までは 50 刻み
    for i in range(1500, 5000, 50):
        star_ranges.append(f"{i}..{i + 49}")

    # 5000以上は大きな括り
    star_ranges.extend([
        "5000..10000",
        "10001..50000",
        ">50000"
    ])

    total_added_count = 0

    # 各範囲に対してループ処理を実行
    for s_range in star_ranges:
        print(f"\n--- スター数範囲: {s_range} のリポジトリを検索 ---")
        all_repo_names_in_range = []

        # 各範囲の中で、ページネーションを使って最大1000件取得
        for page in range(1, 11):
            print(f"{page}ページ目のリポジトリを検索中...")

            query = f"stars:{s_range} forks:>50"
            search_url = f"https://api.github.com/search/repositories"
            params = {'q': query, 'sort': 'stars', 'order': 'desc', 'per_page': 100, 'page': page}

            try:
                response = requests.get(search_url, headers=headers, params=params)
                if response.status_code != 200:
                    print(f"APIリクエストに失敗: {response.status_code}, {response.text}")
                    break

                items = response.json().get('items', [])
                if not items:
                    print("この範囲ではこれ以上リポジトリが見つかりませんでした。")
                    break

                all_repo_names_in_range.extend([item['full_name'] for item in items])
                time.sleep(2)

            except requests.exceptions.RequestException as e:
                print(f"ネットワークエラー: {e}")
                break

        print(f"範囲 {s_range} で {len(all_repo_names_in_range)} 件のリポジトリを発見。タスクキューに登録します。")

        # DBにタスクを登録 (既にあれば無視)
        insert_count_in_range = 0
        for repo_name in all_repo_names_in_range:
            try:
                cursor.execute(
                    "INSERT IGNORE INTO task_queue (repo_name, status) VALUES (%s, 'queued')",
                    (repo_name,)
                )
                if cursor.rowcount == 1:
                    insert_count_in_range += 1
            except mysql.connector.Error as err:
                print(f"タスク登録エラー ({repo_name}): {err}")

        conn.commit()
        print(f"新たに {insert_count_in_range} 件のタスクを登録しました。")
        total_added_count += insert_count_in_range

    cursor.close()
    conn.close()

    print(f"\n合計で新たに {total_added_count} 件のタスクを登録しました。")
    print("タスクの登録が完了しました。")


# --- メインの処理 ---
if __name__ == "__main__":
    config = load_config()
    github_token = config.get('github_token')

    headers = {}
    if github_token:
        headers['Authorization'] = f'token {github_token}'
        print("GitHubトークンを読み込みました。")
    else:
        print("警告: GitHubトークンが見つかりません。")

    populate_task_queue()