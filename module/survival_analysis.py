# ファイル名: 3_survival_analysis_completed_only.py

import mysql.connector
import pandas as pd
from mysql.connector import Error
import matplotlib.pyplot as plt
from lifelines import KaplanMeierFitter
from lifelines.statistics import logrank_test
import os

# --- データベース接続情報 (XXXの部分を自身の情報に書き換えてください) ---
DB_CONFIG = {
    'host': 'mussel.naist.jp',
    'port': 3306,
    'user': 'root',
    'password': 'hoge',
}

# --- 分析対象のプロジェクト ---
STUDY_PROJECTS = [
    {
        'project_name': 'JDT.CORE',
        'schema': 'eclipse_check',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        }
    },
    {
        'project_name': 'pde',
        'schema': 'eclipse_check_pde',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        }
    },
    {
        'project_name': 'PLATFORM.SWT',
        'schema': 'eclipse_check_swt',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        }
    },
    {
        'project_name': 'PLATFORM.UI',
        'schema': 'eclipse_check_ui',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        }
    },
    {
        'project_name': 'PLATFORM.EQUINOX',
        'schema': 'eclipse_check_equinox',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        }
    },
    {
        'project_name': 'Electron-1',
        'schema': 'electron_check',
        'releases': {
            'LR': {'start_date': '2017-04-24', 'end_date': '2019-04-23'},
            'SR': {'start_date': '2019-04-24', 'end_date': '2021-04-24'}
        }
    },
    {
        'project_name': 'Electron-2',
        'schema': 'electron_check',
        'releases': {
            'LR': {'start_date': '2019-09-20', 'end_date': '2021-09-20'},
            'SR': {'start_date': '2021-09-21', 'end_date': '2023-09-21'}
        }
    },
    {
        'project_name': 'Firefox',
        'schema': 'firefox_check',
        'releases': {
            'LR': {'start_date': '2009-03-21', 'end_date': '2011-03-21'},
            'SR': {'start_date': '2011-03-22', 'end_date': '2013-03-21'}
        }
    },
]


def fetch_satd_lifetimes(db_conn, schema):
    """
    データベースから各SATDインスタンスの追加日と除去日を取得する。
    INNER JOINを使い、追加と除去の両方が存在するSATDのみを取得する。
    """
    print(f"Fetching completed SATD lifetime data from schema: {schema}...")
    query = f"""
        SELECT
            added.satd_instance_id,
            added_commit.commit_date AS added_date,
            removed_commit.commit_date AS removed_date
        FROM
            (SELECT * FROM {schema}.SATD WHERE resolution = 'SATD_ADDED') AS added
        INNER JOIN
            (SELECT * FROM {schema}.SATD WHERE resolution = 'SATD_REMOVED') AS removed
            ON added.satd_instance_id = removed.satd_instance_id
        INNER JOIN
            {schema}.Commits AS added_commit ON added.second_commit = added_commit.commit_hash
        INNER JOIN
            {schema}.Commits AS removed_commit ON removed.second_commit = removed_commit.commit_hash
        INNER JOIN
            {schema}.SATDInFile AS added_file ON added.first_file = added_file.f_id
        WHERE
            added_file.f_comment NOT LIKE '%Copyright@%';
    """
    try:
        df = pd.read_sql_query(query, db_conn)
        df['added_date'] = pd.to_datetime(df['added_date'])
        df['removed_date'] = pd.to_datetime(df['removed_date'])
        print(f"Successfully fetched {len(df)} completed SATD instances.")
        return df
    except Exception as e:
        print(f"Error fetching data for schema {schema}: {e}")
        return pd.DataFrame()


def perform_survival_analysis(project_info, all_satd_df):
    """
    指定されたプロジェクトのデータで生存分析を行い、結果をプロットする。
    期間内に完了したSATDのみを対象とする。
    """
    project_name = project_info['project_name']
    print(f"\n--- Performing Survival Analysis for {project_name} (Completed SATDs only) ---")

    lr_start = pd.to_datetime(project_info['releases']['LR']['start_date'])
    lr_end = pd.to_datetime(project_info['releases']['LR']['end_date'])
    sr_start = pd.to_datetime(project_info['releases']['SR']['start_date'])
    sr_end = pd.to_datetime(project_info['releases']['SR']['end_date'])

    survival_data = []

    for _, row in all_satd_df.iterrows():
        added_date = row['added_date']
        removed_date = row['removed_date']

        if (lr_start <= added_date) and (removed_date <= lr_end):
            duration = (removed_date - added_date).days
            survival_data.append({'duration': duration, 'event': 1, 'group': 'LR'})

        elif (sr_start <= added_date) and (removed_date <= sr_end):
            duration = (removed_date - added_date).days
            survival_data.append({'duration': duration, 'event': 1, 'group': 'SR'})

    if not survival_data:
        print("No SATD instances completed within the specified periods.")
        return

    df = pd.DataFrame(survival_data)
    df['duration'] = df['duration'].clip(lower=1)

    df_lr = df[df['group'] == 'LR']
    df_sr = df[df['group'] == 'SR']

    if df_lr.empty or df_sr.empty:
        print("Not enough data in one or both groups to perform log-rank test.")
        return

    # Log-rank検定の実行
    results = logrank_test(
        durations_A=df_lr['duration'],
        durations_B=df_sr['duration'],
        event_observed_A=df_lr['event'],
        event_observed_B=df_sr['event']
    )
    print("\n--- Log-Rank Test Results ---")
    results.print_summary()

    # Kaplan-Meierモデルの準備
    kmf_lr = KaplanMeierFitter()
    kmf_sr = KaplanMeierFitter()

    kmf_lr.fit(df_lr['duration'], event_observed=df_lr['event'], label=f"LR Period (n={len(df_lr)})")
    kmf_sr.fit(df_sr['duration'], event_observed=df_sr['event'], label=f"SR Period (n={len(df_sr)})")

    # ===== ✨ ここから変更 ✨ =====
    # 各グループの中央生存期間を取得して表示
    median_lr = kmf_lr.median_survival_time_
    median_sr = kmf_sr.median_survival_time_

    print("\n--- Median Survival Times ---")
    print(f"LR Period Median Lifetime: {median_lr} days")
    print(f"SR Period Median Lifetime: {median_sr} days")
    # ===== ✨ ここまで変更 ✨ =====

    # Kaplan-Meier曲線のプロット
    fig, ax = plt.subplots(figsize=(12, 8))
    kmf_lr.plot_survival_function(ax=ax)
    kmf_sr.plot_survival_function(ax=ax)

    plt.title(f'SATD Survival Analysis (Completed Only): {project_name}\nLog-Rank Test p-value: {results.p_value:.4f}')
    plt.xlabel("Lifetime (days)")
    plt.ylabel("Survival Probability")
    plt.grid(True)
    plt.legend()

    output_dir = '../plots-survival-completed'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    sanitized_name = project_name.replace(' ', '_').replace('.', '')
    filename = os.path.join(output_dir, f"{sanitized_name}_survival_curve_completed.png")
    plt.savefig(filename)
    plt.close(fig)
    print(f"Survival curve plot saved to {filename}")


def main():
    db_conn = None
    try:
        db_conn = mysql.connector.connect(**DB_CONFIG)
        if db_conn.is_connected():
            print("✅ Successfully connected to the database.")

            for project in STUDY_PROJECTS:
                all_satd_df = fetch_satd_lifetimes(db_conn, project['schema'])

                if not all_satd_df.empty:
                    perform_survival_analysis(project, all_satd_df)

    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("\nDatabase connection closed.")


if __name__ == '__main__':
    main()
