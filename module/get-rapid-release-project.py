import requests
import json
from datetime import datetime, timezone
from packaging.version import Version, InvalidVersion
import time
import os
import mysql.connector

# --- グローバル設定 ---
STATE_FILE = '../batch_state.json'
RAPID_THRESHOLD_DAYS = 180
headers = {}


# --- 設定ファイルを読み込む ---
def load_config():
    """settings.jsonを読み込み、設定を返す"""
    try:
        with open('../settings.json', 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print("エラー: 'settings.json' が見つかりません。スクリプトを終了します。")
        exit()  # 設定ファイルがないとDBに接続できないため終了
    except json.JSONDecodeError:
        print("エラー: 'settings.json' の形式が正しくありません。")
        exit()


config = load_config()
github_token = config.get('github_token')
if github_token:
    headers['Authorization'] = f'token {github_token}'
    print("GitHubトークンを読み込みました。")
else:
    print("警告: GitHubトークンが見つかりません。APIレートリミットが厳しくなります。")


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


def setup_database(conn):
    """データベースとテーブルを初期化する関数"""
    try:
        cursor = conn.cursor()
        # projectテーブル (MySQL用に AUTO_INCREMENT を使用)
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS project (
                    p_id INT AUTO_INCREMENT PRIMARY KEY,
                    repo_name VARCHAR(255) UNIQUE NOT NULL,
                    total_commits INT,
                    stars INT,
                    forks INT,
                    published_date DATETIME
                )
            ''')
        # rapid-projectテーブル
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS rapid_project (
                    rp_id INT AUTO_INCREMENT PRIMARY KEY,
                    repo_name VARCHAR(255) UNIQUE NOT NULL,
                    total_commits INT,
                    stars INT,
                    forks INT,
                    published_date DATETIME
                )
            ''')
        # major-release-cycleテーブル
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS major_release_cycle (
                    cycle_id INT AUTO_INCREMENT PRIMARY KEY,
                    rp_id INT NOT NULL,
                    total_major_release INT,
                    major_release_cycle JSON, -- MySQL 5.7+ で利用可能なJSON型
                    is_rapid BOOLEAN,
                    FOREIGN KEY (rp_id) REFERENCES rapid_project (rp_id)
                )
            ''')
        conn.commit()
        cursor.close()
    except mysql.connector.Error as err:
        print(f"テーブル作成エラー: {err}")


# --- 状態管理の関数 ---
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            try:
                return json.load(f)
            except json.JSONDecodeError:
                return {'last_processed_page': 0}
    return {'last_processed_page': 0}


def save_state(state):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)


# --- リリース分析関数 ---
def analyze_repo_releases(repo_item, cursor, conn):
    repo_name = repo_item['full_name']
    print(f"\n--- {repo_name}を分析 ---")

    cursor.execute("SELECT p_id FROM project WHERE repo_name = %s", (repo_name,))
    if cursor.fetchone():
        print(f"{repo_name}は既に処理済みのためスキップします。")
        return

    # ...(リリース取得のロジックは変更なし)...
    releases_url = repo_item['releases_url'].replace('{/id}', '')
    all_releases, next_url = [], releases_url
    while next_url:
        time.sleep(1)
        try:
            releases_response = requests.get(next_url, headers=headers, params={'per_page': 100})
            if releases_response.status_code != 200:
                print(f"リリース情報の取得に失敗: {releases_response.status_code} - {repo_name}")
                break
            page_data = releases_response.json()
            if not page_data: break
            all_releases.extend(page_data)
            next_url = releases_response.links.get('next', {}).get('url')
        except requests.exceptions.RequestException as e:
            print(f"ネットワークエラー: {e}")
            break

    all_versions = []
    for release in all_releases:
        if not release.get('draft', False) and not release.get('prerelease', False) and release.get('published_at'):
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

    if len(major_releases) > 0:
        # ★★★ ここが修正部分です ★★★
        published_date_str = repo_item.get('created_at')
        mysql_compatible_date = None
        if published_date_str:
            # APIからのISO 8601形式の文字列をパース
            dt_object = datetime.strptime(published_date_str, "%Y-%m-%dT%H:%M:%SZ")
            # MySQLが理解できる形式の文字列に変換
            mysql_compatible_date = dt_object.strftime("%Y-%m-%d %H:%M:%S")

        repo_info = {
            'repo_name': repo_name,
            'total_commits': repo_item.get('size'),
            'stars': repo_item.get('stargazers_count'),
            'forks': repo_item.get('forks_count'),
            'published_date': mysql_compatible_date  # 新しくフォーマットした日付を使用
        }
        # ★★★ 修正はここまで ★★★

        cursor.execute(
            "INSERT INTO project (repo_name, total_commits, stars, forks, published_date) "
            "VALUES (%(repo_name)s, %(total_commits)s, %(stars)s, %(forks)s, %(published_date)s)", repo_info
        )
        conn.commit()
        print(f"-> `project`テーブルに追加しました。")

        is_rapid, average_cycle = False, 0
        major_release_intervals = []
        if len(major_releases) > 1:
            for i in range(1, len(major_releases)):
                major_release_intervals.append((major_releases[i]['date'] - major_releases[i - 1]['date']).days)
            average_cycle = sum(major_release_intervals) / len(major_release_intervals)
            if average_cycle < RAPID_THRESHOLD_DAYS:
                is_rapid = True

        if is_rapid:
            print(f"-> Rapid Releaseと判定しました (平均間隔: {average_cycle:.1f}日)。")
            # rapid_projectテーブルにもフォーマット済みの日付を使用
            cursor.execute(
                "INSERT INTO rapid_project (repo_name, total_commits, stars, forks, published_date) "
                "VALUES (%(repo_name)s, %(total_commits)s, %(stars)s, %(forks)s, %(published_date)s)", repo_info
            )
            rp_id = cursor.lastrowid

            cycle_info = {
                'rp_id': rp_id, 'total_major_release': len(major_releases),
                'major_release_cycle': json.dumps(major_release_intervals), 'is_rapid': is_rapid
            }
            cursor.execute(
                "INSERT INTO major_release_cycle (rp_id, total_major_release, major_release_cycle, is_rapid) "
                "VALUES (%(rp_id)s, %(total_major_release)s, %(major_release_cycle)s, %(is_rapid)s)", cycle_info
            )
            conn.commit()
            print(f"-> `rapid-project`と`major-release-cycle`テーブルに追加しました。")


# --- メインのバッチ処理関数 ---
def process_batch():
    state = load_state()
    current_page = state['last_processed_page'] + 1

    print(f"--- GitHubリポジトリ検索: {current_page}ページ目を処理中 ---")
    search_url = "https://api.github.com/search/repositories?q=stars:>1000+forks:>50&sort=stars&order=desc"

    items = []
    try:
        response = requests.get(search_url, headers=headers, params={'per_page': 100, 'page': current_page})
        if response.status_code != 200:
            print(f"リポジトリ検索に失敗: HTTPステータスコード {response.status_code}")
            try:
                error_details = response.json()
                print(f"APIからのエラーメッセージ: {error_details.get('message', 'N/A')}")
            except ValueError:
                print(f"APIからのレスポンス(非JSON): {response.text}")
            return

        search_results = response.json()
        items = search_results.get('items')

        if items is None:
            print("エラー: APIレスポンスに 'items' キーが含まれていません。")
            print(f"受け取ったデータ: {search_results}")
            return

    except requests.exceptions.RequestException as e:
        print(f"ネットワークリクエスト中にエラーが発生しました: {e}")
        return

    if not items:
        print("処理すべき新しいリポジトリが見つかりませんでした。全件処理完了。")
        return

    conn = connect_to_database()
    if not conn: return

    setup_database(conn)
    cursor = conn.cursor()

    for item in items:
        analyze_repo_releases(item, cursor, conn)

    cursor.close()
    conn.close()

    state['last_processed_page'] = current_page
    save_state(state)
    print(f"\nバッチ処理完了。最後に処理したページ: {current_page}")


# --- メインの処理 ---
if __name__ == "__main__":
    process_batch()