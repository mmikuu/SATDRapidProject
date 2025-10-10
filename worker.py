import requests
import json
from datetime import datetime
from packaging.version import Version, InvalidVersion
import time
import os
import mysql.connector

# --- グローバル設定 ---
STATE_FILE = 'batch_state.json'  # このスクリプトでは直接使いませんが、構成要素として残します
RAPID_THRESHOLD_DAYS = 180  # この日数未満を「Rapid Release」と定義
headers = {}


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


# --- テーブル作成関数 ---
def setup_database(conn):
    """データベースとテーブルを初期化する（存在しない場合のみ作成）"""
    try:
        cursor = conn.cursor()
        # projectテーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS project (
                p_id INT AUTO_INCREMENT PRIMARY KEY, repo_name VARCHAR(255) UNIQUE NOT NULL,
                total_commits INT, stars INT, forks INT, published_date DATETIME
            )
        ''')
        # rapid-projectテーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS rapid_project (
                rp_id INT AUTO_INCREMENT PRIMARY KEY, repo_name VARCHAR(255) UNIQUE NOT NULL,
                total_commits INT, stars INT, forks INT, published_date DATETIME
            )
        ''')
        # major-release-cycleテーブル
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS major_release_cycle (
                cycle_id INT AUTO_INCREMENT PRIMARY KEY, rp_id INT NOT NULL,
                total_major_release INT, major_release_cycle JSON, is_rapid BOOLEAN,
                FOREIGN KEY (rp_id) REFERENCES rapid_project (rp_id)
            )
        ''')
        conn.commit()
        cursor.close()
    except mysql.connector.Error as err:
        print(f"テーブル作成エラー: {err}")


# --- メインのワーカー処理 ---
def main_worker_loop():
    """タスクキューからタスクを取得し、処理を続けるメインループ"""
    print(f"ワーカーを開始しました。タスクキューを監視中...")

    while True:
        conn = connect_to_database()
        if not conn:
            print("DB接続に失敗。10秒後に再試行します。")
            time.sleep(10)
            continue

        cursor = conn.cursor(dictionary=True)
        task = None

        try:
            # --- トランザクションを開始し、タスクを安全に取得 ---
            conn.start_transaction()
            # まだ処理されていないタスクを1件取得し、他のワーカーが取得できないようにロック
            cursor.execute(
                "SELECT task_id, repo_name FROM task_queue WHERE status = 'queued' ORDER BY task_id LIMIT 1 FOR UPDATE")
            task = cursor.fetchone()

            if task:
                # タスクの状態を「処理中」に更新
                cursor.execute("UPDATE task_queue SET status = 'processing', started_at = NOW() WHERE task_id = %s",
                               (task['task_id'],))
                conn.commit()

                # --- ここからが単一リポジトリの分析処理 ---
                repo_name = task['repo_name']
                print(f"\n--- {repo_name}の分析を開始 ---")

                # プロジェクトテーブルに既に存在するか最終確認
                cursor.execute("SELECT p_id FROM project WHERE repo_name = %s", (repo_name,))
                if cursor.fetchone():
                    print(f"{repo_name}は既に処理済みのためスキップします。")
                    cursor.execute(
                        "UPDATE task_queue SET status = 'completed', completed_at = NOW() WHERE task_id = %s",
                        (task['task_id'],))
                    conn.commit()
                    continue

                # リポジトリのメタデータを取得
                repo_info_url = f"https://api.github.com/repos/{repo_name}"
                repo_response = requests.get(repo_info_url, headers=headers)
                if repo_response.status_code != 200:
                    raise Exception(f"リポジトリメタデータの取得に失敗: {repo_response.status_code}")

                repo_item = repo_response.json()

                # 全リリースを取得
                releases_url = repo_item['releases_url'].replace('{/id}', '')
                all_releases, next_url = [], releases_url
                while next_url:
                    time.sleep(1)
                    releases_response = requests.get(next_url, headers=headers, params={'per_page': 100})
                    if releases_response.status_code != 200: break
                    page_data = releases_response.json()
                    if not page_data: break
                    all_releases.extend(page_data)
                    next_url = releases_response.links.get('next', {}).get('url')

                # メジャーリリースを特定
                all_versions = []
                for release in all_releases:
                    if not release.get('draft', False) and not release.get('prerelease', False) and release.get(
                            'published_at'):
                        try:
                            all_versions.append({
                                'version': Version(release['tag_name']),
                                'date': datetime.strptime(release['published_at'], "%Y-%m-%dT%H:%M:%SZ")
                            })
                        except (InvalidVersion, TypeError):
                            continue

                all_versions.sort(key=lambda x: x['version'])

                major_releases = []
                last_major_version = -1
                for v_item in all_versions:
                    if v_item['version'].major > last_major_version:
                        major_releases.append(v_item)
                        last_major_version = v_item['version'].major

                # データベースへの登録処理
                if len(major_releases) > 0:
                    published_date_str = repo_item.get('created_at')
                    mysql_compatible_date = None
                    if published_date_str:
                        dt_object = datetime.strptime(published_date_str, "%Y-%m-%dT%H:%M:%SZ")
                        mysql_compatible_date = dt_object.strftime("%Y-%m-%d %H:%M:%S")

                    repo_info = {
                        'repo_name': repo_name, 'total_commits': repo_item.get('size'),
                        'stars': repo_item.get('stargazers_count'), 'forks': repo_item.get('forks_count'),
                        'published_date': mysql_compatible_date
                    }
                    cursor.execute(
                        "INSERT INTO project (repo_name, total_commits, stars, forks, published_date) VALUES (%(repo_name)s, %(total_commits)s, %(stars)s, %(forks)s, %(published_date)s)",
                        repo_info)
                    print(f"-> `project`テーブルに追加しました。")

                    is_rapid, average_cycle = False, 0
                    major_release_intervals = []
                    if len(major_releases) > 1:
                        for i in range(1, len(major_releases)):
                            major_release_intervals.append(
                                (major_releases[i]['date'] - major_releases[i - 1]['date']).days)
                        average_cycle = sum(major_release_intervals) / len(major_release_intervals)
                        if average_cycle < RAPID_THRESHOLD_DAYS:
                            is_rapid = True

                    if is_rapid:
                        print(f"-> Rapid Releaseと判定 (平均: {average_cycle:.1f}日)。")
                        cursor.execute(
                            "INSERT INTO rapid_project (repo_name, total_commits, stars, forks, published_date) VALUES (%(repo_name)s, %(total_commits)s, %(stars)s, %(forks)s, %(published_date)s)",
                            repo_info)
                        rp_id = cursor.lastrowid

                        cycle_info = {'rp_id': rp_id, 'total_major_release': len(major_releases),
                                      'major_release_cycle': json.dumps(major_release_intervals), 'is_rapid': is_rapid}
                        cursor.execute(
                            "INSERT INTO major_release_cycle (rp_id, total_major_release, major_release_cycle, is_rapid) VALUES (%(rp_id)s, %(total_major_release)s, %(major_release_cycle)s, %(is_rapid)s)",
                            cycle_info)
                        print(f"-> `rapid-project`と`major-release-cycle`に追加しました。")

                # タスク完了
                cursor.execute("UPDATE task_queue SET status = 'completed', completed_at = NOW() WHERE task_id = %s",
                               (task['task_id'],))
                conn.commit()

        except Exception as err:
            print(f"処理中に予期せぬエラーが発生: {err}")
            if conn.is_connected():
                conn.rollback()
                if task:
                    cursor.execute("UPDATE task_queue SET status = 'failed', completed_at = NOW() WHERE task_id = %s",
                                   (task['task_id'],))
                    conn.commit()
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()

        if not task:
            print("現在処理すべきタスクはありません。60秒待機します...")
            time.sleep(60)


if __name__ == "__main__":
    config = load_config()
    github_token = config.get('github_token')
    if github_token:
        headers['Authorization'] = f'token {github_token}'

    # 起動時に一度だけテーブル構造を確認・作成
    conn = connect_to_database()
    if conn:
        setup_database(conn)
        conn.close()
        # メインループを開始
        main_worker_loop()