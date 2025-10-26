# 必要なライブラリをインストールしてください:
# pip install pandas mysql-connector-python matplotlib python-dateutil numpy scipy

import mysql.connector
import pandas as pd
from mysql.connector import Error
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import numpy as np
from scipy.stats import linregress

# --- データベース接続情報 ---
DB_CONFIG = {
    'host': 'mussel.naist.jp',
    'port': 3306,
    'user': 'root',
    'password': 'hoge',
}

# --- 分析対象プロジェクト (historical_releases を含む) ---
STUDY_PROJECTS = [
    {
        'project_name': 'JDT.CORE',
        'schema': 'eclipse_check',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        },
        'historical_releases': [
            {'date': '2016-06-22', 'type': 'LR'}, {'date': '2017-06-28', 'type': 'LR'},
            {'date': '2018-06-27', 'type': 'SR'}, {'date': '2018-09-19', 'type': 'SR'},
            {'date': '2018-12-19', 'type': 'SR'}, {'date': '2019-03-20', 'type': 'SR'},
            {'date': '2019-06-19', 'type': 'SR'}, {'date': '2019-09-18', 'type': 'SR'},
            {'date': '2019-12-18', 'type': 'SR'}, {'date': '2020-03-18', 'type': 'SR'},
        ]
    },
    {
        'project_name': 'pde',
        'schema': 'eclipse_check_pde',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        },
        'historical_releases': [
            {'date': '2016-06-22', 'type': 'LR'}, {'date': '2017-06-28', 'type': 'LR'},
            {'date': '2018-06-27', 'type': 'SR'}, {'date': '2018-09-19', 'type': 'SR'},
            {'date': '2018-12-19', 'type': 'SR'}, {'date': '2019-03-20', 'type': 'SR'},
            {'date': '2019-06-19', 'type': 'SR'}, {'date': '2019-09-18', 'type': 'SR'},
            {'date': '2019-12-18', 'type': 'SR'}, {'date': '2020-03-18', 'type': 'SR'},
        ]
    },
    {
        'project_name': 'PLATFORM.SWT',
        'schema': 'eclipse_check_swt',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        },
        'historical_releases': [
            {'date': '2016-06-22', 'type': 'LR'}, {'date': '2017-06-28', 'type': 'LR'},
            {'date': '2018-06-27', 'type': 'SR'}, {'date': '2018-09-19', 'type': 'SR'},
            {'date': '2018-12-19', 'type': 'SR'}, {'date': '2019-03-20', 'type': 'SR'},
            {'date': '2019-06-19', 'type': 'SR'}, {'date': '2019-09-18', 'type': 'SR'},
            {'date': '2019-12-18', 'type': 'SR'}, {'date': '2020-03-18', 'type': 'SR'},
        ]
    },
    {
        'project_name': 'PLATFORM.UI',
        'schema': 'eclipse_check_ui',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        },
        'historical_releases': [
            {'date': '2016-06-22', 'type': 'LR'}, {'date': '2017-06-28', 'type': 'LR'},
            {'date': '2018-06-27', 'type': 'SR'}, {'date': '2018-09-19', 'type': 'SR'},
            {'date': '2018-12-19', 'type': 'SR'}, {'date': '2019-03-20', 'type': 'SR'},
            {'date': '2019-06-19', 'type': 'SR'}, {'date': '2019-09-18', 'type': 'SR'},
            {'date': '2019-12-18', 'type': 'SR'}, {'date': '2020-03-18', 'type': 'SR'},
        ]
    },
    {
        'project_name': 'PLATFORM.EQUINOX',
        'schema': 'eclipse_check_equinox',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        },
        'historical_releases': [
            {'date': '2016-06-22', 'type': 'LR'}, {'date': '2017-06-28', 'type': 'LR'},
            {'date': '2018-06-27', 'type': 'SR'}, {'date': '2018-09-19', 'type': 'SR'},
            {'date': '2018-12-19', 'type': 'SR'}, {'date': '2019-03-20', 'type': 'SR'},
            {'date': '2019-06-19', 'type': 'SR'}, {'date': '2019-09-18', 'type': 'SR'},
            {'date': '2019-12-18', 'type': 'SR'}, {'date': '2020-03-18', 'type': 'SR'},
        ]
    },
    {
        'project_name': 'Electron-1',
        'schema': 'electron_check',
        'releases': {
            'LR': {'start_date': '2016-07-25', 'end_date': '2018-09-17'},
            'SR': {'start_date': '2018-09-18', 'end_date': '2020-11-17'}
        },
        'historical_releases': [
            {'date': '2018-05-01', 'type': 'LR'}, {'date': '2018-09-18', 'type': 'SR'},
            {'date': '2018-12-20', 'type': 'SR'}, {'date': '2019-04-24', 'type': 'SR'},
            {'date': '2019-07-30', 'type': 'SR'}, {'date': '2019-10-22', 'type': 'SR'},
            {'date': '2020-02-04', 'type': 'SR'}, {'date': '2020-05-19', 'type': 'SR'},
            {'date': '2020-08-25', 'type': 'SR'}, {'date': '2020-11-17', 'type': 'SR'},
            {'date': '2021-03-02', 'type': 'SR'},
        ]
    },
    {
        'project_name': 'Electron-2',
        'schema': 'electron_check',
        'releases': {
            'LR': {'start_date': '2019-07-30', 'end_date': '2021-09-20'},
            'SR': {'start_date': '2021-09-21', 'end_date': '2023-10-10'}
        },
        'historical_releases': [
            {'date': '2019-07-30', 'type': 'LR'},
            {'date': '2019-10-22', 'type': 'LR'}, {'date': '2020-02-04', 'type': 'LR'},
            {'date': '2020-05-19', 'type': 'LR'}, {'date': '2020-08-25', 'type': 'LR'},
            {'date': '2020-11-17', 'type': 'LR'}, {'date': '2021-03-02', 'type': 'LR'},
            {'date': '2021-05-25', 'type': 'LR'}, {'date': '2021-09-21', 'type': 'LR'},
            {'date': '2021-11-16', 'type': 'SR'}, {'date': '2022-02-01', 'type': 'SR'},
            {'date': '2022-04-26', 'type': 'SR'}, {'date': '2022-06-02', 'type': 'SR'},
            {'date': '2022-08-02', 'type': 'SR'}, {'date': '2022-09-27', 'type': 'SR'},
            {'date': '2022-11-29', 'type': 'SR'}, {'date': '2023-02-07', 'type': 'SR'},
            {'date': '2023-04-04', 'type': 'SR'}, {'date': '2023-05-30', 'type': 'SR'},
            {'date': '2023-08-15', 'type': 'SR'}, {'date': '2023-10-10', 'type': 'SR'},
        ]
    },
    {
            'project_name': 'Firefox3.0-26',
            'schema': 'firefox_check',  # スキーマ名はご自身の環境に合わせてください
            'releases': {
                'LR': {'start_date': '2008-06-17', 'end_date': '2011-03-21'},  # 12-week cycle period
                'SR': {'start_date': '2011-03-22', 'end_date': '2013-12-10'}  # 8-week cycle period
            },
            'historical_releases': [
                {'date': '2006-10-24', 'type': 'LR'}, {'date': '2008-06-17', 'type': 'LR'},
                {'date': '2009-06-30', 'type': 'LR'}, {'date': '2010-01-21', 'type': 'LR'},
                {'date': '2011-03-22', 'type': 'SR'}, {'date': '2011-06-21', 'type': 'SR'},
                {'date': '2011-08-16', 'type': 'SR'}, {'date': '2011-09-27', 'type': 'SR'},
                {'date': '2011-11-08', 'type': 'SR'}, {'date': '2011-12-20', 'type': 'SR'},
                {'date': '2012-01-31', 'type': 'SR'}, {'date': '2012-03-13', 'type': 'SR'},
                {'date': '2012-04-24', 'type': 'SR'}, {'date': '2012-06-05', 'type': 'SR'},
                {'date': '2012-07-17', 'type': 'SR'}, {'date': '2012-08-28', 'type': 'SR'},
                {'date': '2012-10-09', 'type': 'SR'}, {'date': '2012-11-20', 'type': 'SR'},
                {'date': '2013-01-08', 'type': 'SR'}, {'date': '2013-02-19', 'type': 'SR'},
                {'date': '2013-04-02', 'type': 'SR'}, {'date': '2013-05-14', 'type': 'SR'},
                {'date': '2013-06-25', 'type': 'SR'}, {'date': '2013-08-06', 'type': 'SR'},
                {'date': '2013-09-17', 'type': 'SR'}, {'date': '2013-10-29', 'type': 'SR'},
                {'date': '2013-12-10', 'type': 'SR'},
            ]
        },
]

# --- 回帰トレンドライン描画用ヘルパー (元のコードから。現在は未使用) ---
def add_regression_trend(ax, df_monthly, period_start, period_end, color, label_suffix):
    df_period = df_monthly[(df_monthly.index >= period_start) & (df_monthly.index <= period_end)].copy()
    if len(df_period) < 2:
        return
    x_dates = mdates.date2num(df_period.index)
    y_commits = df_period['Commits'].values
    _ = linregress(x_dates, y_commits)
    # 可視化は省略

# --- データ取得＆集計 (ADDED と REMOVED の両方を取得) ---
def get_satd_and_commit_data(db_conn, project_info, release_type, dates):
    """
    SATD (ADDED/REMOVED) とコミットデータを取得し、週次で集計する
    """
    print(f"\n--- Fetching Data for {project_info['project_name']} ({release_type}) ---")

    # ADDED（SATD側・second_file）
    def _clean_sql(sql: str) -> str:
        # この関数がセミコロンを除去します
        return sql.strip().rstrip(';')

    query_satd_added = f"""
        SELECT DATE(C.commit_date) AS commit_date, 'SATD_ADDED' AS resolution
        FROM {project_info['schema']}.SATD AS S
        INNER JOIN {project_info['schema']}.Commits AS C
            ON C.commit_hash = S.second_commit
        LEFT JOIN {project_info['schema']}.SATDInFile AS F
            ON S.second_file = F.f_id
        WHERE S.resolution = 'SATD_ADDED'
          AND C.commit_date BETWEEN '{dates['start_date']}' AND '{dates['end_date']}'
          AND (F.f_comment IS NULL OR LOWER(F.f_comment) NOT LIKE '%copyright%')
    """ # 末尾に ; がないことを確認

    # REMOVED（SATD側・first_file）
    query_satd_removed = f"""
        SELECT DATE(C.commit_date) AS commit_date, 'SATD_REMOVED' AS resolution
        FROM {project_info['schema']}.SATD AS S
        INNER JOIN {project_info['schema']}.Commits AS C
            ON C.commit_hash = S.second_commit
        LEFT JOIN {project_info['schema']}.SATDInFile AS F
            ON S.first_file = F.f_id
        WHERE S.resolution = 'SATD_REMOVED'
          AND C.commit_date BETWEEN '{dates['start_date']}' AND '{dates['end_date']}'
          AND (F.f_comment IS NULL OR LOWER(F.f_comment) NOT LIKE '%copyright%')
    """ # 末尾に ; がないことを確認

    # REMOVED（WaitRemove側・first_file）
    query_waitremove_removed = f"""
        SELECT DATE(C.commit_date) AS commit_date, 'SATD_REMOVED' AS resolution
        FROM {project_info['schema']}.WaitRemove AS WR
        INNER JOIN {project_info['schema']}.Commits AS C
            ON C.commit_hash = WR.newCommitId
        LEFT JOIN {project_info['schema']}.WaitRemoveSATDInFile AS WF
            ON WR.oldFileId = WF.f_id
        WHERE C.commit_date BETWEEN '{dates['start_date']}' AND '{dates['end_date']}'
          AND (WF.f_comment IS NULL OR LOWER(WF.f_comment) NOT LIKE '%copyright%')
          AND (WR.sync_status IS NULL OR WR.sync_status = 0)
    """ # 末尾に ; がないことを確認

    # _clean_sql を使って結合するため、セミコロンがあってもここで除去される
    query_removed_union_with_waitremove = "\n        UNION ALL\n        ".join([
        _clean_sql(query_satd_removed),
        _clean_sql(query_waitremove_removed)
    ])
    query_removed_satd_only = _clean_sql(query_satd_removed)

    # 分母（コミット数）
    query_commits = f"""
        SELECT DATE(commit_date) AS commit_date, COUNT(*) as commit_count
        FROM {project_info['schema']}.Commits
        WHERE commit_date BETWEEN '{dates['start_date']}' AND '{dates['end_date']}'
        GROUP BY DATE(commit_date)
    """ # 末尾に ; がないことを確認

    try:
        # WaitRemoveテーブル存在チェック
        cursor = db_conn.cursor()
        cursor.execute(f"SHOW TABLES FROM {project_info['schema']} LIKE 'WaitRemove';")
        wait_remove_exists = cursor.fetchone() is not None
        cursor.execute(f"SHOW TABLES FROM {project_info['schema']} LIKE 'WaitRemoveSATDInFile';")
        wait_remove_infile_exists = cursor.fetchone() is not None
        cursor.close()

        # ADDED取得
        print("\nExecuting query_satd_added:")
        # _clean_sql を通して実行
        satd_added_df = pd.read_sql_query(_clean_sql(query_satd_added), db_conn)

        # REMOVED取得
        if wait_remove_exists and wait_remove_infile_exists:
            print("WaitRemove と WaitRemoveSATDInFile を検出。SATD_REMOVED を SATD + WaitRemove(非同期) で合算します。")
            # query_removed_union_with_waitremove は _clean_sql 適用済み
            satd_removed_df = pd.read_sql_query(query_removed_union_with_waitremove, db_conn)
        else:
            print("WaitRemove / WaitRemoveSATDInFile が見つからないため、SATDテーブルのREMOVEDのみを使用します。")
            # query_removed_satd_only は _clean_sql 適用済み
            satd_removed_df = pd.read_sql_query(query_removed_satd_only, db_conn)

        # SATD(ADDED+REMOVED)統合
        if (satd_added_df is None or satd_added_df.empty) and (satd_removed_df is None or satd_removed_df.empty):
            satd_df = pd.DataFrame(columns=['commit_date', 'resolution'])
        elif satd_added_df is None or satd_added_df.empty:
            satd_df = satd_removed_df
        elif satd_removed_df is None or satd_removed_df.empty:
            satd_df = satd_added_df
        else:
            satd_df = pd.concat([satd_added_df, satd_removed_df], ignore_index=True)

        # Commits取得
        # _clean_sql を通して実行
        commits_df = pd.read_sql_query(_clean_sql(query_commits), db_conn)
        if commits_df.empty:
            print(f"Warning: No commits found for {project_info['project_name']} in period {release_type}. Returning None.")
            return None

        total_commits = commits_df['commit_count'].sum()
        commits_df['commit_date'] = pd.to_datetime(commits_df['commit_date'])
        commits_df.set_index('commit_date', inplace=True)
        weekly_commits = commits_df.resample('W-MON').sum().rename(columns={'commit_count': 'Commits'})

        if satd_df is None or satd_df.empty:
            print(f"No SATD data found for {project_info['project_name']} in period {release_type}.")
            total_added = 0
            total_removed = 0
            weekly_satd = pd.DataFrame(index=weekly_commits.index, columns=['SATD_ADDED', 'SATD_REMOVED']).fillna(0)
        else:
            total_added  = (satd_df['resolution'] == 'SATD_ADDED').sum()
            total_removed = (satd_df['resolution'] == 'SATD_REMOVED').sum()
            satd_df['commit_date'] = pd.to_datetime(satd_df['commit_date'])
            satd_df.set_index('commit_date', inplace=True)
            weekly_satd = pd.concat([
                satd_df[satd_df['resolution'] == 'SATD_ADDED' ].resample('W-MON').size().rename('SATD_ADDED'),
                satd_df[satd_df['resolution'] == 'SATD_REMOVED'].resample('W-MON').size().rename('SATD_REMOVED')
            ], axis=1).fillna(0)

        print(f"Fetched {total_added + total_removed} SATD records and {total_commits} commits.")

        merged_df = pd.concat([weekly_satd, weekly_commits], axis=1, join='outer').fillna(0)
        for col in ['SATD_ADDED', 'SATD_REMOVED', 'Commits']:
            if col not in merged_df:
                merged_df[col] = 0
        merged_df.fillna(0, inplace=True)

        # 正規化（1000コミットあたり）
        merged_df['Normalized_ADDED'] = merged_df.apply(
            lambda row: (row['SATD_ADDED'] / row['Commits']) * 1000 if row['Commits'] > 0 else 0, axis=1)
        merged_df['Normalized_REMOVED'] = merged_df.apply(
            lambda row: (row['SATD_REMOVED'] / row['Commits']) * 1000 if row['Commits'] > 0 else 0, axis=1)

        merged_df['SATD_ADDED'] = merged_df['SATD_ADDED'].astype(int)
        merged_df['SATD_REMOVED'] = merged_df['SATD_REMOVED'].astype(int)
        merged_df['Commits'] = merged_df['Commits'].astype(int)

        return {
            "weekly_df": merged_df,
            "total_added": total_added,
            "total_removed": total_removed,
            "total_commits": total_commits if total_commits > 0 else 1
        }

    except Exception as e:
        print(f"An error occurred in get_satd_and_commit_data for {project_info['project_name']} ({release_type}): {e}")
        return None

# --- 週次トレンドの描画 (REMOVED 専用) ---
def plot_weekly_removed_trends(project_info, lr_data, sr_data, major_releases):
    print(f"\n--- Plotting Combined Weekly SATD REMOVED Trends for {project_info['project_name']} ---")

    if not lr_data or not sr_data:
        print(f"Skipping plot for {project_info['project_name']} due to missing LR or SR data.")
        return

    lr_df = lr_data['weekly_df']
    sr_df = sr_data['weekly_df']

    if lr_df.empty or sr_df.empty:
        print(f"Skipping plot for {project_info['project_name']} due to empty LR or SR dataframe after processing.")
        return

    prefix_map = {
        'JDT.CORE': 'Jdt', 'pde': 'Pde', 'PLATFORM.SWT': 'Swt',
        'PLATFORM.UI': 'Ui', 'PLATFORM.EQUINOX': 'Equinox',
        'Electron-1': 'Electron1', 'Electron-2': 'Electron2', 'Firefox3.0-26': 'Firefox'
    }
    prefix = prefix_map.get(project_info['project_name'],
                            project_info['project_name'].replace(' ', '_').replace('.', ''))

    # REMOVEDのみのプロット設定
    config = {'metric': 'SATD_REMOVED', 'norm_metric': 'Normalized_REMOVED', 'title': 'REMOVED',
              'ylabel_raw': 'Weekly Raw Count', 'ylabel_norm': 'Count per 1000 Commits',
              'label_raw': 'Weekly REMOVED', 'label_norm': 'Normalized REMOVED'}

    min_date = lr_df.index.min()
    max_date = sr_df.index.max()

    try:
        lr_start = pd.to_datetime(project_info['releases']['LR']['start_date'])
        lr_end   = pd.to_datetime(project_info['releases']['LR']['end_date'])
        sr_start = pd.to_datetime(project_info['releases']['SR']['start_date'])
        sr_end   = pd.to_datetime(project_info['releases']['SR']['end_date'])
    except Exception:
        lr_start, lr_end, sr_start, sr_end = None, None, None, None

    # --- 1. RAW Count Plot (REMOVED) ---
    fig1, ax1 = plt.subplots(figsize=(15, 7))
    if lr_start and sr_start:
        ax1.axvspan(lr_start, lr_end, color='lightgray', alpha=0.4, label='LR Period', zorder=0)
        ax1.axvspan(sr_start, sr_end, color='lightgreen', alpha=0.4, label='SR Period', zorder=0)
    ax1.plot(lr_df.index, lr_df[config['metric']], label=f"LR {config['metric']}", color='darkcyan', marker='s', linestyle='-', zorder=2)
    ax1.plot(sr_df.index, sr_df[config['metric']], label=f"SR {config['metric']}", color='orange', marker='^', linestyle='--', zorder=2)

    # --- Major Releaseの点線 (RAW) ---
    current_max_y_raw = max(lr_df[config['metric']].max(), sr_df[config['metric']].max()) * 1.2
    ax1.set_ylim(top=current_max_y_raw) # y軸の最大値を設定
    text_y_pos_raw = current_max_y_raw * 0.9 # 上部90%の位置
    release_counter_lr = 1
    release_counter_sr = 1
    ax1.axvline(x=sr_start, color='red', linestyle='--', linewidth=2, zorder=4, label='Transition to SR')
    for release in major_releases:
        release_date = release['date']
        release_type = release['type']
        if min_date <= release_date <= max_date:
            color = 'blue' if release_type == 'LR' else 'green'
            linestyle = ':'
            if release_type == 'LR':
                if release_date == lr_start: continue
                label = f"R{release_counter_lr}(LR)"
                release_counter_lr += 1
            else:
                if release_date == sr_start: continue
                label = f"R{release_counter_sr}(SR)"
                release_counter_sr += 1
            ax1.axvline(x=release_date, color=color, linestyle=linestyle, linewidth=1.5, zorder=1)
            ax1.text(release_date, text_y_pos_raw, label, rotation=90, verticalalignment='top', horizontalalignment='center', color=color, fontsize=20)

    ax1.legend(loc='upper right')
    ax1.set_title(f"Weekly {config['title']} SATD (Raw): {project_info['project_name']}", fontsize=22)
    ax1.set_xlabel("Date", fontsize=20)
    ax1.set_ylabel("Weekly Raw Count", fontsize=20)
    ax1.tick_params(axis='both', which='major', labelsize=16)
    ax1.grid(True, which='both', linestyle='--', linewidth=0.5, zorder=-1)
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax1.get_xticklabels(), rotation=45)
    plt.tight_layout()
    output_dir_raw = '../plots-raw(trend)'
    os.makedirs(output_dir_raw, exist_ok=True)
    filename_raw = os.path.join(output_dir_raw, f"{prefix}{config['title']}RawCountGraph.pdf")
    plt.savefig(filename_raw, format='pdf')
    plt.close(fig1)

    # --- 2. NORMALIZED Count Plot (REMOVED) ---
    fig2, ax2 = plt.subplots(figsize=(15, 7))
    if lr_start and sr_start:
        ax2.axvspan(lr_start, lr_end, color='lightgray', alpha=0.4, label='LR Period', zorder=0)
        ax2.axvspan(sr_start, sr_end, color='lightgreen', alpha=0.4, label='SR Period', zorder=0)
    ax2.plot(lr_df.index, lr_df[config['norm_metric']], label="LR", color='blue', marker='o', linestyle='-', zorder=2)
    ax2.plot(sr_df.index, sr_df[config['norm_metric']], label="SR", color='red', marker='x', linestyle='--', zorder=2)

    # --- Major Releaseの点線 (NORMALIZED) ---
    current_max_y_norm = max(lr_df[config['norm_metric']].max(), sr_df[config['norm_metric']].max()) * 1.2
    ax2.set_ylim(top=current_max_y_norm, bottom=-10) # y軸の最大・最小値を設定
    text_y_pos_norm = current_max_y_norm * 0.9 # 上部90%の位置
    release_counter_lr = 1
    release_counter_sr = 1
    ax2.axvline(x=sr_start, color='red', linestyle='--', linewidth=2, zorder=4, label='Transition to SR')
    for release in major_releases:
        release_date = release['date']
        release_type = release['type']
        if min_date <= release_date <= max_date:
            color = 'blue' if release_type == 'LR' else 'green'
            linestyle = ':'
            if release_type == 'LR':
                if release_date == lr_start: continue
                label = f"R{release_counter_lr}(LR)"
                release_counter_lr += 1
            else:
                if release_date == sr_start: continue
                label = f"R{release_counter_sr}(SR)"
                release_counter_sr += 1
            ax2.axvline(x=release_date, color=color, linestyle=linestyle, linewidth=1.5, zorder=1)
            ax2.text(release_date, text_y_pos_norm, label, rotation=90, verticalalignment='top', horizontalalignment='center', color=color, fontsize=20)

    ax2.legend(loc='upper right')
    ax2.set_title(f"Weekly {config['title']} SATD (Normalized): {project_info['project_name']}", fontsize=22)
    ax2.set_xlabel("Date", fontsize=20)
    ax2.set_ylabel("Count per 1000 Commits", fontsize=20)
    ax2.tick_params(axis='both', which='major', labelsize=16)
    ax2.grid(True, which='both', linestyle='--', linewidth=0.5, zorder=-1)
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    plt.setp(ax2.get_xticklabels(), rotation=45)
    plt.tight_layout()
    output_dir_norm = '../plots-percent(trend)'
    os.makedirs(output_dir_norm, exist_ok=True)
    filename_norm = os.path.join(output_dir_norm, f"{prefix}{config['title']}CommitPerCountGraph.pdf")
    plt.savefig(filename_norm, format='pdf')
    plt.close(fig2)

# --- リリース日ハンドリング ---
def generate_release_dates(project_info):
    return [{'date': pd.to_datetime(d['date']), 'type': d['type']}
            for d in project_info.get('historical_releases', [])]

# --- エントリーポイント ---
def main():
    db_conn = None
    removed_summary, removed_norm_summary = {}, {}
    commit_summary = {}

    try:
        db_conn = mysql.connector.connect(**DB_CONFIG)
        if db_conn.is_connected():
            print("✅ Successfully connected to the database.")

            for project in STUDY_PROJECTS:
                print("\n" + "#" * 25 + f" Processing Project: {project['project_name']} " + "#" * 25)
                summary_name = project['project_name']

                removed_summary[summary_name]     = {'LR': 0, 'SR': 0}
                removed_norm_summary[summary_name]= {'LR': 0.0, 'SR': 0.0}
                commit_summary[summary_name]      = {'LR': 0, 'SR': 0}

                major_releases_for_project = generate_release_dates(project)

                lr_data = get_satd_and_commit_data(db_conn, project, 'LR', project['releases']['LR'])
                sr_data = get_satd_and_commit_data(db_conn, project, 'SR', project['releases']['SR'])

                if lr_data:
                    commit_summary[summary_name]['LR'] = lr_data['total_commits'] if lr_data['total_commits'] > 1 else 0
                    removed_summary[summary_name]['LR']= lr_data['total_removed']
                    total_commits_lr = lr_data['total_commits']
                    if total_commits_lr > 1:
                        removed_norm_summary[summary_name]['LR']= (lr_data['total_removed'] / total_commits_lr) * 1000

                if sr_data:
                    commit_summary[summary_name]['SR'] = sr_data['total_commits'] if sr_data['total_commits'] > 1 else 0
                    removed_summary[summary_name]['SR']= sr_data['total_removed']
                    total_commits_sr = sr_data['total_commits']
                    if total_commits_sr > 1:
                        removed_norm_summary[summary_name]['SR']= (sr_data['total_removed'] / total_commits_sr) * 1000

                if lr_data and sr_data:
                    try:
                        plot_weekly_removed_trends(project, lr_data, sr_data,
                                                   major_releases_for_project if major_releases_for_project else [])
                    except Exception as plot_weekly_err:
                        print(f"Error plotting weekly REMOVED trends for {project['project_name']}: {plot_weekly_err}")
                else:
                    print(f"Skipping combined trend plot for {project['project_name']} due to missing data.")

    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
    except Exception as general_e:
        print(f"❌ An unexpected error occurred in main loop: {general_e}")

    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("\n✅ Database connection closed.")

        if commit_summary:
            print("\n\n" + "#" * 30 + "\n### Commit Summary (RAW Count) ###\n" + "#" * 30)
            df_commits = pd.DataFrame(commit_summary).T
            print(df_commits.fillna(0).astype(int))
        if removed_summary:
            print("\n\n" + "#" * 37 + "\n### SATD REMOVED Summary (RAW Count) ###\n" + "#" * 37)
            print(pd.DataFrame(removed_summary).T.fillna(0).astype(int))
        if removed_norm_summary:
            print("\n\n" + "#" * 52 + "\n### SATD REMOVED Summary (Count per 1000 Commits) ###\n" + "#" * 52)
            print(pd.DataFrame(removed_norm_summary).T.fillna(0).round(2))

if __name__ == '__main__':
    main()
