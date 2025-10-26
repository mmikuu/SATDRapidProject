# -*- coding: utf-8 -*-
"""
RDD-Switch-Time.py

Per-project Regression Discontinuity Design (RDD) analysis focused on
the LR→SR release cadence switch. Reuses the SATD aggregation logic from
count_satd_added.py to build weekly metrics, then fits a local RDD model
centered at the SR start date for each project.
"""

import os
from datetime import datetime

import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import statsmodels.formula.api as smf
from mysql.connector import Error

# --- データベース接続情報 ---
DB_CONFIG = {
    'host': 'mussel.naist.jp',
    'port': 3306,
    'user': 'root',
    'password': 'hoge',
}

# --- 分析対象プロジェクト ---
STUDY_PROJECTS = [
    {
        'project_name': 'JDT.CORE',
        'schema': 'eclipse_check',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        },
    },
    {
        'project_name': 'pde',
        'schema': 'eclipse_check_pde',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        },
    },
    {
        'project_name': 'PLATFORM.SWT',
        'schema': 'eclipse_check_swt',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        },
    },
    {
        'project_name': 'PLATFORM.UI',
        'schema': 'eclipse_check_ui',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        },
    },
    {
        'project_name': 'PLATFORM.EQUINOX',
        'schema': 'eclipse_check_equinox',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        },
    },
    {
        'project_name': 'Electron-1',
        'schema': 'electron_check',
        'releases': {
            'LR': {'start_date': '2016-07-25', 'end_date': '2018-09-17'},
            'SR': {'start_date': '2018-09-18', 'end_date': '2020-11-17'}
        },
    },
    {
        'project_name': 'Electron-2',
        'schema': 'electron_check',
        'releases': {
            'LR': {'start_date': '2019-07-30', 'end_date': '2021-09-20'},
            'SR': {'start_date': '2021-09-21', 'end_date': '2023-10-10'}
        },
    },
    {
        'project_name': 'Firefox3.0-26',
        'schema': 'firefox_check',
        'releases': {
            'LR': {'start_date': '2008-06-17', 'end_date': '2011-03-21'},
            'SR': {'start_date': '2011-03-22', 'end_date': '2013-12-10'}
        },
    },
]


def _clean_sql(sql: str) -> str:
    return sql.strip().rstrip(';')


def fetch_weekly_satd_metrics(db_conn, project_info):
    """
    count_satd_added.py と同じロジックで SATD ADDED/REMOVED + コミット数を週次集計する。
    """
    schema = project_info['schema']
    releases = project_info['releases']

    lr_start = datetime.strptime(releases['LR']['start_date'], '%Y-%m-%d')
    sr_end = datetime.strptime(releases['SR']['end_date'], '%Y-%m-%d')
    start_date_str = lr_start.strftime('%Y-%m-%d')
    end_date_str = sr_end.strftime('%Y-%m-%d')

    cursor = db_conn.cursor()
    cursor.execute(f"SHOW TABLES FROM {schema} LIKE 'WaitRemove';")
    wait_remove_exists = cursor.fetchone() is not None
    cursor.execute(f"SHOW TABLES FROM {schema} LIKE 'WaitRemoveSATDInFile';")
    wait_remove_infile_exists = cursor.fetchone() is not None
    cursor.close()

    query_satd_added = f"""
        SELECT DATE(C.commit_date) AS commit_date, 'SATD_ADDED' AS resolution
        FROM {schema}.SATD AS S
        INNER JOIN {schema}.Commits AS C
            ON C.commit_hash = S.second_commit
        LEFT JOIN {schema}.SATDInFile AS F
            ON S.second_file = F.f_id
        WHERE S.resolution = 'SATD_ADDED'
          AND C.commit_date BETWEEN '{start_date_str}' AND '{end_date_str}'
          AND (F.f_comment IS NULL OR LOWER(F.f_comment) NOT LIKE '%copyright%')
    """

    query_satd_removed = f"""
        SELECT DATE(C.commit_date) AS commit_date, 'SATD_REMOVED' AS resolution
        FROM {schema}.SATD AS S
        INNER JOIN {schema}.Commits AS C
            ON C.commit_hash = S.second_commit
        LEFT JOIN {schema}.SATDInFile AS F
            ON S.first_file = F.f_id
        WHERE S.resolution = 'SATD_REMOVED'
          AND C.commit_date BETWEEN '{start_date_str}' AND '{end_date_str}'
          AND (F.f_comment IS NULL OR LOWER(F.f_comment) NOT LIKE '%copyright%')
    """

    query_waitremove_removed = f"""
        SELECT DATE(C.commit_date) AS commit_date, 'SATD_REMOVED' AS resolution
        FROM {schema}.WaitRemove AS WR
        INNER JOIN {schema}.Commits AS C
            ON C.commit_hash = WR.newCommitId
        LEFT JOIN {schema}.WaitRemoveSATDInFile AS WF
            ON WR.oldFileId = WF.f_id
        WHERE C.commit_date BETWEEN '{start_date_str}' AND '{end_date_str}'
          AND (WF.f_comment IS NULL OR LOWER(WF.f_comment) NOT LIKE '%copyright%')
          AND (WR.sync_status IS NULL OR WR.sync_status = 0)
    """

    query_removed_union = "\n        UNION ALL\n        ".join([
        _clean_sql(query_satd_removed),
        _clean_sql(query_waitremove_removed)
    ])
    query_commits = f"""
        SELECT DATE(commit_date) AS commit_date, COUNT(*) as commit_count
        FROM {schema}.Commits
        WHERE commit_date BETWEEN '{start_date_str}' AND '{end_date_str}'
        GROUP BY DATE(commit_date)
    """

    satd_added_df = pd.read_sql_query(_clean_sql(query_satd_added), db_conn)

    if wait_remove_exists and wait_remove_infile_exists:
        print("  ↳ WaitRemove テーブルを検出。SATD_REMOVED に WaitRemove 分を合算します。")
        satd_removed_df = pd.read_sql_query(query_removed_union, db_conn)
    else:
        print("  ↳ WaitRemove テーブル無し。SATD テーブルの REMOVED のみを使用します。")
        satd_removed_df = pd.read_sql_query(_clean_sql(query_satd_removed), db_conn)

    if satd_added_df.empty and satd_removed_df.empty:
        satd_df = pd.DataFrame(columns=['commit_date', 'resolution'])
    elif satd_added_df.empty:
        satd_df = satd_removed_df
    elif satd_removed_df.empty:
        satd_df = satd_added_df
    else:
        satd_df = pd.concat([satd_added_df, satd_removed_df], ignore_index=True)

    commits_df = pd.read_sql_query(_clean_sql(query_commits), db_conn)
    if commits_df.empty:
        print("  ↳ コミットデータが存在しません。")
        return None

    commits_df['commit_date'] = pd.to_datetime(commits_df['commit_date'])
    commits_df.set_index('commit_date', inplace=True)
    weekly_commits = commits_df.resample('W-MON').sum().rename(columns={'commit_count': 'Commits'})

    if satd_df.empty:
        weekly_satd = pd.DataFrame(index=weekly_commits.index,
                                   columns=['SATD_ADDED', 'SATD_REMOVED']).fillna(0)
    else:
        satd_df['commit_date'] = pd.to_datetime(satd_df['commit_date'])
        satd_df.set_index('commit_date', inplace=True)
        weekly_satd = pd.concat([
            satd_df[satd_df['resolution'] == 'SATD_ADDED'].resample('W-MON').size().rename('SATD_ADDED'),
            satd_df[satd_df['resolution'] == 'SATD_REMOVED'].resample('W-MON').size().rename('SATD_REMOVED')
        ], axis=1).fillna(0)

    merged_df = pd.concat([weekly_satd, weekly_commits], axis=1, join='outer').fillna(0)
    merged_df['Normalized_ADDED'] = merged_df.apply(
        lambda row: (row['SATD_ADDED'] / row['Commits']) * 1000 if row['Commits'] > 0 else 0, axis=1)
    merged_df['Normalized_REMOVED'] = merged_df.apply(
        lambda row: (row['SATD_REMOVED'] / row['Commits']) * 1000 if row['Commits'] > 0 else 0, axis=1)

    merged_df.rename(columns={
        'SATD_ADDED': 'ADDED',
        'SATD_REMOVED': 'REMOVED',
        'Commits': 'total_commits'
    }, inplace=True)
    merged_df.index.name = 'date'

    return merged_df.reset_index()


def run_switch_rdd(project_info, weekly_df, outcome_column, window_weeks=52):
    """
    LR → SR 切り替え境界を中心とした RDD を実行し、結果を表示・保存する。
    """
    project_name = project_info['project_name']
    sr_start = pd.to_datetime(project_info['releases']['SR']['start_date'])

    outcome_label = outcome_column.replace('_', ' ')
    print(f"\n--- RDD Switch Analysis for {project_name} | Outcome: {outcome_label} ---")

    df = weekly_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df[outcome_column] = pd.to_numeric(df[outcome_column], errors='coerce')
    df = df.dropna(subset=[outcome_column])

    window_start = sr_start - pd.Timedelta(weeks=window_weeks)
    window_end = sr_start + pd.Timedelta(weeks=window_weeks - 1)
    window_df = df[(df['date'] >= window_start) & (df['date'] <= window_end)].copy()

    if window_df.empty or window_df[outcome_column].nunique() <= 1:
        print("  ↳ 不足データのため解析をスキップします。")
        return

    window_df['time_from_switch'] = (window_df['date'] - sr_start).dt.days / 7.0
    window_df['treatment'] = (window_df['time_from_switch'] >= 0).astype(int)

    formula = 'outcome ~ time_from_switch + treatment + time_from_switch:treatment'
    window_df.rename(columns={outcome_column: 'outcome'}, inplace=True)

    try:
        model = smf.ols(formula=formula, data=window_df).fit()
        print(model.summary())
        print("  ↳ treatment 項は切替後の水準変化、time_from_switch:treatment は傾きの変化を示します。")
    except Exception as err:
        print(f"  ↳ モデル推定中にエラー: {err}")
        return

    # 可視化
    plt.figure(figsize=(12, 8))
    plt.scatter(window_df[window_df['treatment'] == 0]['time_from_switch'],
                window_df[window_df['treatment'] == 0]['outcome'],
                color='royalblue', alpha=0.4, label='LR Weeks')
    plt.scatter(window_df[window_df['treatment'] == 1]['time_from_switch'],
                window_df[window_df['treatment'] == 1]['outcome'],
                color='orangered', alpha=0.4, label='SR Weeks')

    grid = np.linspace(window_df['time_from_switch'].min(),
                       window_df['time_from_switch'].max(), 200)
    pre_df = pd.DataFrame({
        'time_from_switch': grid[grid < 0],
        'treatment': 0
    })
    post_df = pd.DataFrame({
        'time_from_switch': grid[grid >= 0],
        'treatment': 1
    })
    preds_pre = model.predict(pre_df)
    preds_post = model.predict(post_df)

    plt.plot(grid[grid < 0], preds_pre, color='navy', linewidth=3, label='Fit (LR)')
    plt.plot(grid[grid >= 0], preds_post, color='darkred', linewidth=3, label='Fit (SR)')
    plt.axvline(x=0, color='gray', linestyle='--', linewidth=2, label='Switch (SR start)')

    plt.title(f"RDD at LR→SR Switch: {project_name}\nOutcome: {outcome_label}", fontsize=18)
    plt.xlabel('Weeks from Switch', fontsize=16)
    plt.ylabel(outcome_label, fontsize=16)
    plt.legend(fontsize=14)
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()

    out_dir = '../rdd_plots_switch'
    os.makedirs(out_dir, exist_ok=True)
    safe_name = project_name.replace(' ', '_').replace('.', '')
    fig_path = os.path.join(out_dir, f"{safe_name}_{outcome_column}_switch.pdf")
    plt.savefig(fig_path, format='pdf')
    plt.close()
    print(f"  ↳ プロットを保存しました: {fig_path}")


def main():
    db_conn = None
    try:
        db_conn = mysql.connector.connect(**DB_CONFIG)
        if not db_conn.is_connected():
            print("❌ データベース接続に失敗しました。")
            return
        print("✅ データベースに接続しました。")

        for project in STUDY_PROJECTS:
            print(f"\n{'=' * 30}\nProcessing: {project['project_name']}\n{'=' * 30}")
            weekly_df = fetch_weekly_satd_metrics(db_conn, project)
            if weekly_df is None or weekly_df.empty:
                print("  ↳ 週次データが取得できなかったためスキップします。")
                continue

            for outcome in ['Normalized_ADDED', 'Normalized_REMOVED']:
                if outcome in weekly_df.columns:
                    run_switch_rdd(project, weekly_df, outcome)
                else:
                    print(f"  ↳ Outcome 列が見つかりません: {outcome}")

    except Error as db_err:
        print(f"❌ MySQL エラー: {db_err}")
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("\n✅ データベース接続をクローズしました。")


if __name__ == '__main__':
    main()
