import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import mysql.connector
from datetime import datetime

# --- DB設定 ---
DB_CONFIG = {
    'host': 'mussel.naist.jp',
    'port': 3306,
    'user': 'root',
    'password': 'hoge',
}

# --- 対象プロジェクト ---
STUDY_PROJECTS = [
    {
        'project_name': 'Electron',
        'schema': 'electron_check_release',
    }
]

# --- commit_date を取得 ---
def resolve_commit_dates(db_conn, schema, commit_ids, version):
    dates = []
    cursor = db_conn.cursor()
    for cid in commit_ids:
        query = f"""
            SELECT DATE(commit_date)
            FROM {schema}.Commits
            WHERE commit_hash = %s AND commit_date IS NOT NULL;
        """
        cursor.execute(query, (cid,))
        rows = cursor.fetchall()
        if len(rows) == 1:
            dates.append(rows[0][0])
        elif len(rows) == 0:
            print(f"  ❌ v{version} No match for {cid}")
        else:
            print(f"  ⚠️  v{version} Multiple matches for {cid}")
    cursor.close()
    return pd.to_datetime(dates)

# --- ヒートマップ描画 ---
def plot_commit_heatmap(db_conn, project_info):
    print(f"\n=== 🔥 Commit Heatmap: {project_info['project_name']} ===")
    all_data = []

    for vnum in range(6, 28):
        version = f"v{vnum}"
        csv_path = f"../commit-his/{vnum}-dev.csv"

        if not os.path.exists(csv_path):
            print(f"⚠️  {version}: CSV not found")
            continue

        try:
            df_csv = pd.read_csv(csv_path)
            if 'commit_id' not in df_csv.columns:
                print(f"❌ {version}: Missing 'commit_id'")
                continue

            commit_ids = df_csv['commit_id'].dropna().astype(str).unique()
            commit_dates = resolve_commit_dates(db_conn, project_info['schema'], commit_ids, version)

            if commit_dates.empty:
                print(f"⚠️  {version}: No valid commits")
                continue

            df = pd.DataFrame({'commit_date': commit_dates})
            df['week'] = df['commit_date'].dt.to_period('W').dt.start_time
            weekly_counts = df.groupby('week').size().rename(version)
            all_data.append(weekly_counts)

        except Exception as e:
            print(f"❌ {version}: Error: {e}")
            continue

    if not all_data:
        print("❌ No data collected. Aborting.")
        return

    heatmap_df = pd.concat(all_data, axis=1).fillna(0).astype(int).T.sort_index()

    # --- プロット ---
    fig, ax = plt.subplots(figsize=(20, 10))
    sns.heatmap(heatmap_df, cmap="YlGnBu", linewidths=0.1, linecolor='gray',
                ax=ax, cbar_kws={'label': 'Commits per Week'})

    ax.set_title(f"Commit Activity Heatmap per Version - {project_info['project_name']}", fontsize=20)
    ax.set_xlabel("Week")
    ax.set_ylabel("Version")
    ax.set_xticks([i for i in range(0, len(heatmap_df.columns), 8)])
    ax.set_xticklabels([str(p.date()) for i, p in enumerate(heatmap_df.columns) if i % 8 == 0], rotation=45)

    plt.tight_layout()
    outdir = "../plots-commits(release-version)"
    os.makedirs(outdir, exist_ok=True)
    path = os.path.join(outdir, f"{project_info['project_name'].replace('.', '_')}_CommitHeatmap.pdf")
    plt.savefig(path)
    plt.close()
    print(f"✅ Heatmap saved: {path}")

# --- メイン ---
def main():
    try:
        db_conn = mysql.connector.connect(**DB_CONFIG)
        if db_conn.is_connected():
            print("✅ DB接続成功")
            for project in STUDY_PROJECTS:
                plot_commit_heatmap(db_conn, project)
    except mysql.connector.Error as e:
        print(f"❌ DB接続エラー: {e}")
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("✅ DB切断完了")

if __name__ == '__main__':
    main()
