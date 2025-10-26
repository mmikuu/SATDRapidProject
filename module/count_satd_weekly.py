# ファイル名: 1_monthly_trend_analysis_combined_with_commits_modified_v4.py

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

# --- データベース接続情報 (XXXの部分を自身の情報に書き換えてください) ---
DB_CONFIG = {
    'host': 'mussel.naist.jp',  # 例: 'localhost'
    'port': 3306,
    'user': 'root',  # 例: 'root'
    'password': 'hoge',
}

# --- 分析対象のデータ (プロジェクトごとにLR/SRの期間をまとめて管理) ---
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
            {'date': '2018-12-20', 'type': 'SR'},
            {'date': '2019-04-24', 'type': 'SR'},
            {'date': '2019-07-30', 'type': 'SR'},
            {'date': '2019-10-22', 'type': 'SR'},
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
    # (Firefoxプロジェクトはコメントアウトのまま)
]


# --- 回帰トレンドライン描画用ヘルパー関数 ---
def add_regression_trend(ax, df_monthly, period_start, period_end, color, label_suffix):
    df_period = df_monthly[(df_monthly.index >= period_start) & (df_monthly.index <= period_end)].copy()
    if len(df_period) < 2:
        return
    x_dates = mdates.date2num(df_period.index)
    y_commits = df_period['Commits'].values
    slope, intercept, r_value, p_value, std_err = linregress(x_dates, y_commits)
    # line = slope * x_dates + intercept
    # ax.plot(df_period.index, line, color=color, linestyle='--', linewidth=2.5, zorder=4,
    #         label=f'{label_suffix} Trend (p={p_value:.3f})') # 凡例から削除


def plot_monthly_commits_combined(db_conn, project_info, all_dates, major_releases):
    print(f"\n--- Plotting Monthly Commit Trends for {project_info['project_name']} (Combined LR/SR) ---")
    lr_start = pd.to_datetime(project_info['releases']['LR']['start_date'])
    lr_end = pd.to_datetime(project_info['releases']['LR']['end_date'])
    sr_start = pd.to_datetime(project_info['releases']['SR']['start_date'])
    sr_end = pd.to_datetime(project_info['releases']['SR']['end_date'])
    min_date, max_date = lr_start, sr_end

    query_commits = f"""
        SELECT DATE(commit_date) AS commit_date, COUNT(*) as commit_count
        FROM {project_info['schema']}.Commits
        WHERE commit_date BETWEEN '{min_date.strftime('%Y-%m-%d')}' AND '{max_date.strftime('%Y-%m-%d')}'
        GROUP BY DATE(commit_date);
    """
    try:
        commits_df = pd.read_sql_query(query_commits, db_conn)
        if commits_df.empty:
            print("No commit data found for this period. Skipping plot.")
            return

        print(f"Fetched {commits_df['commit_count'].sum()} total commit records for plotting.")
        commits_df['commit_date'] = pd.to_datetime(commits_df['commit_date'])
        commits_df.set_index('commit_date', inplace=True)
        monthly_commits = commits_df.resample('MS').sum().rename(columns={'commit_count': 'Commits'})
        monthly_commits['Rolling_Avg_3M'] = monthly_commits['Commits'].rolling(window=3, center=True).mean()

        fig, ax = plt.subplots(figsize=(18, 8))

        ax.axvspan(lr_start, lr_end, color='lightgray', alpha=0.4, label='LR Period', zorder=0)
        ax.axvspan(sr_start, sr_end, color='lightgreen', alpha=0.4, label='SR Period', zorder=0)

        ax.bar(monthly_commits.index, monthly_commits['Commits'], color='skyblue', alpha=0.7, width=20,
               label='Monthly Commits')
        ax.plot(monthly_commits.index, monthly_commits['Rolling_Avg_3M'], label='3-Month Rolling Average',
                color='darkviolet', linestyle='-', linewidth=2, zorder=3)

        df_full = monthly_commits[(monthly_commits.index >= min_date) & (monthly_commits.index <= max_date)].copy()
        if len(df_full) >= 2:
            x_dates_full = mdates.date2num(df_full.index)
            y_commits_full = df_full['Commits'].values
            slope_full, intercept_full, _, _, _ = linregress(x_dates_full, y_commits_full)
            line_full = slope_full * x_dates_full + intercept_full
            ax.plot(df_full.index, line_full, color='black', linestyle='-', linewidth=4, zorder=5,
                    label='Overall Linear Trend')

        add_regression_trend(ax, monthly_commits, lr_start, lr_end, 'darkblue', 'LR')
        add_regression_trend(ax, monthly_commits, sr_start, sr_end, 'darkred', 'SR')

        ax.set_title(f"Monthly Commit Counts: {project_info['project_name']} (LR & SR Periods)", fontsize=22)
        ax.set_xlabel("Date", fontsize=24)
        ax.set_ylabel("Monthly Commit Count", fontsize=24)

        handles, labels = ax.get_legend_handles_labels()
        label_handle_map = dict(zip(labels, handles))
        desired_order = [
            'LR Period', 'SR Period', 'Monthly Commits', '3-Month Rolling Average',
            'Overall Linear Trend', 'LR Trend', 'SR Trend'
        ]
        new_handles = [label_handle_map[lbl] for lbl in desired_order if lbl in label_handle_map]
        new_labels = [lbl for lbl in desired_order if lbl in label_handle_map]
        ax.legend(new_handles, new_labels, loc='upper right', bbox_to_anchor=(1, 1), bbox_transform=ax.transAxes,
                  fontsize=18)

        ax.tick_params(axis='both', which='major', labelsize=16)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5, zorder=-1)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')

        current_max_y = monthly_commits['Commits'].max() if not monthly_commits['Commits'].empty else 10 # Handle empty
        ax.set_ylim(bottom=-0.05 * current_max_y, top=current_max_y * 1.5)  # Avoid cutting off bottom
        max_y = ax.get_ylim()[1]
        y_levels = [0.98, 0.94, 0.90, 0.86, 0.82, 0.78, 0.74, 0.70, 0.66, 0.62]

        for idx, release in enumerate(major_releases):
            release_date_dt = pd.to_datetime(release['date'])
            if min_date <= release_date_dt <= max_date:
                color = 'blue' if release['type'] == 'LR' else 'red'
                ax.axvline(x=release_date_dt, color=color, linestyle=':', linewidth=1.5, zorder=1)
                level_index = idx % len(y_levels)
                y_pos_factor = y_levels[level_index]
                if max_y > 0:  # Avoid text placement error if max_y is 0
                    ax.text(release_date_dt + relativedelta(days=5), max_y * y_pos_factor,
                            f' R{idx + 1}({release["type"]})', rotation=90, va='top', color=color, fontsize=16)

        plt.tight_layout()
        output_dir = '../plots-commits(monthly-combined)'
        os.makedirs(output_dir, exist_ok=True)  # Use exist_ok=True

        filename_map = {
            'JDT.CORE': 'JdtCommitGraph.pdf', 'pde': 'PdeCommitGraph.pdf', 'PLATFORM.SWT': 'SwtCommitGraph.pdf',
            'PLATFORM.UI': 'UiCommitGraph.pdf', 'PLATFORM.EQUINOX': 'EquinoxCommitGraph.pdf',
            'Electron-1': 'Electron1CommitGraph.pdf', 'Electron-2': 'Electron2CommitGraph.pdf',
            'Firefox3.0-26': 'FirefoxCommitGraph.pdf'
        }
        final_filename = filename_map.get(project_info['project_name'],
                                          f"{project_info['project_name'].replace(' ', '_').replace('.', '')}_combined_monthly_commit_trends.pdf")

        filename = os.path.join(output_dir, final_filename)
        plt.savefig(filename, format='pdf')
        plt.close(fig)
        print(f"Combined monthly commit trend chart saved to {filename}")

    except Exception as e:
        print(f"An error occurred in plot_monthly_commits_combined: {e}")


# --- データ取得と処理を行う関数 ---
def get_satd_and_commit_data(db_conn, project_info, release_type, dates):
    print(f"\n--- Fetching Data for {project_info['project_name']} ({release_type}) ---")

    # ★★★ ここからが修正されたクエリ定義です ★★★
    # クエリ1: WaitRemove* テーブルが存在しない場合のフォールバック (S.first_file / S.second_file で分岐)
    query_satd_simple = f"""
        SELECT DATE(C.commit_date) AS commit_date, S.resolution
        FROM {project_info['schema']}.SATD AS S
        INNER JOIN {project_info['schema']}.Commits AS C ON S.second_commit = C.commit_hash
        -- Corrected Join: Check content based on resolution type
        INNER JOIN {project_info['schema']}.SATDInFile AS F
            ON (S.resolution = 'SATD_ADDED' AND S.second_file = F.f_id)
            OR (S.resolution = 'SATD_REMOVED' AND S.first_file = F.f_id)
        WHERE S.resolution IN ('SATD_ADDED', 'SATD_REMOVED')
          AND C.commit_date BETWEEN '{dates['start_date']}' AND '{dates['end_date']}'
          AND F.f_comment NOT LIKE '%Copyright@%';
        """

    # クエリ2: WaitRemove* テーブルが存在する場合の、重複排除クエリ (S.first_file / S.second_file で分岐)
    query_satd_with_waitremove = f"""
        (
            -- Part 1: Standard SATD table (MODIFIED - CORRECTED JOIN)
            -- SATDテーブルから 'SATD_ADDED' と 'SATD_REMOVED' を取得
            -- Copyrightチェックを適切なファイル (ADDED->second, REMOVED->first) で行う
            SELECT DATE(C.commit_date) AS commit_date, S.resolution
            FROM {project_info['schema']}.SATD AS S
            INNER JOIN {project_info['schema']}.Commits AS C ON S.second_commit = C.commit_hash
            -- Corrected Join: Check content based on resolution type
            INNER JOIN {project_info['schema']}.SATDInFile AS F
                ON (S.resolution = 'SATD_ADDED' AND S.second_file = F.f_id)
                OR (S.resolution = 'SATD_REMOVED' AND S.first_file = F.f_id)
            WHERE S.resolution IN ('SATD_ADDED', 'SATD_REMOVED')
              AND C.commit_date BETWEEN '{dates['start_date']}' AND '{dates['end_date']}'
              AND F.f_comment NOT LIKE '%Copyright@%'
        ) UNION ALL (
            -- Part 2: WaitRemoveSATD table (UNCHANGED - already correct logic)
            -- WaitRemoveSATD から、まだSATDテーブルに同期されていない (sync_status = 0)
            -- 'SATD_REMOVED' のみを取得し、二重カウントを防止
            -- Copyrightチェックは WF.f_id (first_file の内容) で行う
            SELECT DATE(C.commit_date) AS commit_date, 'SATD_REMOVED' AS resolution
            FROM {project_info['schema']}.WaitRemoveSATD AS W
            INNER JOIN {project_info['schema']}.WaitRemove AS WR
                ON W.hash_code = WR.new_hash_code
            INNER JOIN {project_info['schema']}.Commits AS C
                ON W.second_commit = C.commit_hash
            INNER JOIN {project_info['schema']}.WaitRemoveSATDInFile AS WF
                ON W.first_file = WF.f_id
            WHERE C.commit_date BETWEEN '{dates['start_date']}' AND '{dates['end_date']}'
              AND WF.f_comment NOT LIKE '%Copyright@%'
              AND WR.sync_status = 0
        );"""
    # ★★★ 修正されたクエリ定義はここまで ★★★

    query_commits = f"SELECT DATE(commit_date) AS commit_date, COUNT(*) as commit_count FROM {project_info['schema']}.Commits WHERE commit_date BETWEEN '{dates['start_date']}' AND '{dates['end_date']}' GROUP BY DATE(commit_date);"

    # デバッグ用にSQLを出力 (必要に応じてコメントアウトしてください)
    print("\n--- SQL Query for SATD ---")

    try:
        # テーブル存在チェック (変更なし)
        cursor = db_conn.cursor()
        cursor.execute(f"SHOW TABLES FROM {project_info['schema']} LIKE 'WaitRemoveSATD';")
        wait_remove_satd_exists = cursor.fetchone() is not None

        cursor.execute(f"SHOW TABLES FROM {project_info['schema']} LIKE 'WaitRemove';")
        wait_remove_exists = cursor.fetchone() is not None
        cursor.close()

        # クエリ実行 (SQL出力追加)
        if wait_remove_satd_exists and wait_remove_exists:
            print("WaitRemoveSATD and WaitRemove tables found. Using combined query with sync_status check.")
            print(query_satd_with_waitremove) # SQLを出力
            satd_df = pd.read_sql_query(query_satd_with_waitremove, db_conn)
        else:
            print("WaitRemoveSATD or WaitRemove table not found. Using standard query (SATD table only).")
            print(query_satd_simple) # SQLを出力
            satd_df = pd.read_sql_query(query_satd_simple, db_conn)
        print("--- End SQL Query ---")


        commits_df = pd.read_sql_query(query_commits, db_conn)

        if commits_df.empty:
            print(
                f"Warning: No commits found for {project_info['project_name']} in period {release_type}. Returning None.")
            return None

        total_commits = commits_df['commit_count'].sum()
        commits_df['commit_date'] = pd.to_datetime(commits_df['commit_date'])
        commits_df.set_index('commit_date', inplace=True)
        weekly_commits = commits_df.resample('W-MON').sum().rename(columns={'commit_count': 'Commits'})

        if satd_df.empty:
            print(f"No SATD data found for {project_info['project_name']} in period {release_type}.")
            total_added = 0
            total_removed = 0
            weekly_satd = pd.DataFrame(index=weekly_commits.index, columns=['SATD_ADDED', 'SATD_REMOVED']).fillna(0)
        else:
            total_added = (satd_df['resolution'] == 'SATD_ADDED').sum()
            total_removed = (satd_df['resolution'] == 'SATD_REMOVED').sum()
            satd_df['commit_date'] = pd.to_datetime(satd_df['commit_date'])
            satd_df.set_index('commit_date', inplace=True)
            weekly_satd = pd.concat([
                satd_df[satd_df['resolution'] == 'SATD_ADDED'].resample('W-MON').size().rename('SATD_ADDED'),
                satd_df[satd_df['resolution'] == 'SATD_REMOVED'].resample('W-MON').size().rename('SATD_REMOVED')
            ], axis=1).fillna(0)

        print(f"Fetched {total_added + total_removed} SATD records and {total_commits} commits.")

        merged_df = pd.concat([weekly_satd, weekly_commits], axis=1, join='outer').fillna(0)

        if 'SATD_ADDED' not in merged_df: merged_df['SATD_ADDED'] = 0
        if 'SATD_REMOVED' not in merged_df: merged_df['SATD_REMOVED'] = 0
        if 'Commits' not in merged_df: merged_df['Commits'] = 0
        merged_df.fillna(0, inplace=True)

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


# --- 統合された週次トレンドをプロットする関数 ---
def plot_combined_weekly_trends(project_info, lr_data, sr_data, major_releases):
    print(f"\n--- Plotting Combined Weekly SATD Trends for {project_info['project_name']} ---")

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

    plot_configs = [
        {'metric': 'SATD_ADDED', 'norm_metric': 'Normalized_ADDED', 'title': 'ADDED', 'ylabel_raw': 'Weekly Raw Count',
         'ylabel_norm': 'Count per 1000 Commits', 'label_raw': 'Weekly ADDED', 'label_norm': 'Normalized ADDED'},
        {'metric': 'SATD_REMOVED', 'norm_metric': 'Normalized_REMOVED', 'title': 'REMOVED',
         'ylabel_raw': 'Weekly Raw Count', 'ylabel_norm': 'Count per 1000 Commits', 'label_raw': 'Weekly REMOVED',
         'label_norm': 'Normalized REMOVED'}
    ]

    y_levels = [0.98, 0.94, 0.90, 0.86, 0.82, 0.78, 0.74, 0.70, 0.66, 0.62]

    min_date = lr_df.index.min() if not lr_df.empty else pd.Timestamp.max
    max_date = sr_df.index.max() if not sr_df.empty else pd.Timestamp.min
    if not lr_df.empty and not sr_df.empty:
        min_date = min(lr_df.index.min(), sr_df.index.min())
        max_date = max(lr_df.index.max(), sr_df.index.max())
    elif not lr_df.empty:
        min_date = lr_df.index.min()
        max_date = lr_df.index.max()
    elif not sr_df.empty:
        min_date = sr_df.index.min()
        max_date = sr_df.index.max()
    else: # Should not happen based on earlier checks, but good to handle
        print(f"Both LR and SR dataframes seem empty for {project_info['project_name']} during plot. Skipping.")
        return


    try:
        lr_start = pd.to_datetime(project_info['releases']['LR']['start_date'])
        lr_end = pd.to_datetime(project_info['releases']['LR']['end_date'])
        sr_start = pd.to_datetime(project_info['releases']['SR']['start_date'])
        sr_end = pd.to_datetime(project_info['releases']['SR']['end_date'])
    except Exception as e:
        print(f"Could not get release dates for background color: {e}")
        lr_start, lr_end, sr_start, sr_end = None, None, None, None

    for config in plot_configs:
        # --- RAW COUNT PLOT ---
        fig1, ax1 = plt.subplots(figsize=(15, 7))

        if lr_start and sr_start:
            ax1.axvspan(lr_start, lr_end, color='lightgray', alpha=0.4, label='LR Period', zorder=0)
            ax1.axvspan(sr_start, sr_end, color='lightgreen', alpha=0.4, label='SR Period', zorder=0)

        if not lr_df.empty:
            ax1.plot(lr_df.index, lr_df[config['metric']], label=f"LR {config['label_raw']}", color='darkcyan',
                     marker='s', linestyle='-', zorder=2)
        if not sr_df.empty:
            ax1.plot(sr_df.index, sr_df[config['metric']], label=f"SR {config['label_raw']}", color='orange', marker='^',
                     linestyle='--', zorder=2)

        ax1.set_title(f"Weekly {config['title']} SATD (Raw Count): {project_info['project_name']}", fontsize=22)
        ax1.set_xlabel("Date", fontsize=20)
        ax1.set_ylabel(config['ylabel_raw'], fontsize=20)

        ax1.tick_params(axis='both', which='major', labelsize=16)
        ax1.grid(True, which='both', linestyle='--', linewidth=0.5, zorder=-1)

        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.setp(ax1.get_xticklabels(), rotation=45)

        current_max_y_raw = 0
        if not lr_df.empty and config['metric'] in lr_df and not lr_df[config['metric']].empty:
             current_max_y_raw = max(current_max_y_raw, lr_df[config['metric']].max())
        if not sr_df.empty and config['metric'] in sr_df and not sr_df[config['metric']].empty:
             current_max_y_raw = max(current_max_y_raw, sr_df[config['metric']].max())
        top_limit_raw = max(current_max_y_raw * 1.3, 10) # Ensure at least 10 for visibility
        ax1.set_ylim(bottom=- top_limit_raw * 0.05 , top=top_limit_raw)
        max_y_raw = ax1.get_ylim()[1]

        for idx, release in enumerate(major_releases):
            release_date_dt = pd.to_datetime(release['date'])
            if min_date <= release_date_dt <= max_date:
                color = 'blue' if release['type'] == 'LR' else 'green'
                ax1.axvline(x=release_date_dt, color=color, linestyle=':', linewidth=1.5, zorder=1)
                level_index = idx % len(y_levels)
                y_pos_factor = y_levels[level_index]
                if max_y_raw > 0:
                     ax1.text(release_date_dt + relativedelta(days=5), max_y_raw * y_pos_factor,
                              f' R{idx + 1}({release["type"]})', rotation=90, va='top', color=color, fontsize=12)

        try:
            if sr_start: # Check if sr_start is defined
                transition_date = sr_start
                ymin, ymax = ax1.get_ylim()
                ax1.vlines(x=transition_date, ymin=ymin, ymax=ymax,
                           color='red', linestyle='--', linewidth=2,
                           label='Transition to SR', zorder=3)
                ax1.set_ylim(bottom=- top_limit_raw * 0.05, top=top_limit_raw) # Reapply ylim after vlines might change it
        except Exception as e:
            print(f"Could not draw transition line: {e}")

        ax1.legend(loc='upper right', bbox_to_anchor=(1, 1), bbox_transform=ax1.transAxes, fontsize=18)

        plt.tight_layout()
        output_dir_raw = '../plots-raw(trend)'
        os.makedirs(output_dir_raw, exist_ok=True)
        filename_raw = os.path.join(output_dir_raw, f"{prefix}{config['title']}RawCountGraph.pdf")
        plt.savefig(filename_raw, format='pdf')
        plt.close(fig1)
        print(f"Raw count chart saved to {filename_raw}")

        # --- NORMALIZED COUNT PLOT ---
        fig2, ax2 = plt.subplots(figsize=(15, 7))

        if lr_start and sr_start:
            ax2.axvspan(lr_start, lr_end, color='lightgray', alpha=0.4, label='LR Period', zorder=0)
            ax2.axvspan(sr_start, sr_end, color='lightgreen', alpha=0.4, label='SR Period', zorder=0)

        if not lr_df.empty:
            ax2.plot(lr_df.index, lr_df[config['norm_metric']], label="LR", color='blue',
                     marker='o', linestyle='-', zorder=2)
        if not sr_df.empty:
            ax2.plot(sr_df.index, sr_df[config['norm_metric']], label="SR", color='red',
                     marker='x', linestyle='--', zorder=2)

        ax2.set_title(f"Weekly {config['title']} SATD (Normalized): {project_info['project_name']}", fontsize=22)
        ax2.set_xlabel("Date", fontsize=20)
        ax2.set_ylabel(config['ylabel_norm'], fontsize=20)

        ax2.tick_params(axis='both', which='major', labelsize=16)
        ax2.grid(True, which='both', linestyle='--', linewidth=0.5, zorder=-1)

        ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
        ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.setp(ax2.get_xticklabels(), rotation=45)

        current_max_y_norm = 0
        if not lr_df.empty and config['norm_metric'] in lr_df and not lr_df[config['norm_metric']].empty:
            current_max_y_norm = max(current_max_y_norm, lr_df[config['norm_metric']].max())
        if not sr_df.empty and config['norm_metric'] in sr_df and not sr_df[config['norm_metric']].empty:
            current_max_y_norm = max(current_max_y_norm, sr_df[config['norm_metric']].max())
        top_limit_norm = max(current_max_y_norm * 1.3, 10) # Ensure at least 10
        ax2.set_ylim(bottom=- top_limit_norm * 0.05, top=top_limit_norm)
        max_y_norm = ax2.get_ylim()[1]

        for idx, release in enumerate(major_releases):
            release_date_dt = pd.to_datetime(release['date'])
            if min_date <= release_date_dt <= max_date:
                color = 'blue' if release['type'] == 'LR' else 'green'
                ax2.axvline(x=release_date_dt, color=color, linestyle=':', linewidth=1.5, zorder=1)
                level_index = idx % len(y_levels)
                y_pos_factor = y_levels[level_index]
                if max_y_norm > 0:
                    ax2.text(release_date_dt + relativedelta(days=5), max_y_norm * y_pos_factor,
                             f' R{idx + 1}({release["type"]})', rotation=90, va='top', color=color, fontsize=12)

        try:
            if sr_start: # Check if sr_start is defined
                transition_date_norm = sr_start
                ymin_norm, ymax_norm = ax2.get_ylim()
                ax2.vlines(x=transition_date_norm, ymin=ymin_norm, ymax=ymax_norm,
                           color='red', linestyle='--', linewidth=2,
                           label='Transition to SR', zorder=3)
                ax2.set_ylim(bottom=- top_limit_norm * 0.05, top=top_limit_norm) # Reapply ylim
        except Exception as e:
            print(f"Could not draw transition line for normalized plot: {e}")

        ax2.legend(loc='upper right', bbox_to_anchor=(1, 1), bbox_transform=ax2.transAxes, fontsize=18)

        plt.tight_layout()
        output_dir_norm = '../plots-percent(trend)'
        os.makedirs(output_dir_norm, exist_ok=True)
        filename_norm = os.path.join(output_dir_norm, f"{prefix}{config['title']}CommitPerCountGraph.pdf")
        plt.savefig(filename_norm, format='pdf')
        plt.close(fig2)
        print(f"Normalized chart saved to {filename_norm}")


def generate_release_dates(project_info):
    return [{'date': pd.to_datetime(d['date']), 'type': d['type']}
            for d in project_info.get('historical_releases', [])]


def main():
    db_conn = None
    added_summary, removed_summary = {}, {}
    added_norm_summary, removed_norm_summary = {}, {}
    commit_summary = {}

    try:
        db_conn = mysql.connector.connect(**DB_CONFIG)
        if db_conn.is_connected():
            print("✅ Successfully connected to the database.")

            for project in STUDY_PROJECTS:
                print("\n" + "#" * 25 + f" Processing Project: {project['project_name']} " + "#" * 25)
                summary_name = project['project_name']

                # Initialize summary dicts for the project
                added_summary[summary_name] = {'LR': 0, 'SR': 0}
                removed_summary[summary_name] = {'LR': 0, 'SR': 0}
                added_norm_summary[summary_name] = {'LR': 0.0, 'SR': 0.0}
                removed_norm_summary[summary_name] = {'LR': 0.0, 'SR': 0.0}
                commit_summary[summary_name] = {'LR': 0, 'SR': 0}

                major_releases_for_project = generate_release_dates(project)

                # Ensure plot_monthly_commits_combined handles potential errors gracefully
                try:
                    plot_monthly_commits_combined(db_conn, project, project['releases'],
                                                  major_releases_for_project if major_releases_for_project else [])
                except Exception as plot_err:
                    print(f"Error plotting monthly commits for {project['project_name']}: {plot_err}")

                lr_data = get_satd_and_commit_data(db_conn, project, 'LR', project['releases']['LR'])
                sr_data = get_satd_and_commit_data(db_conn, project, 'SR', project['releases']['SR'])

                if lr_data:
                    commit_summary[summary_name]['LR'] = lr_data['total_commits'] if lr_data[
                                                                                         'total_commits'] > 1 else 0  # Display 0 if only placeholder 1
                    added_summary[summary_name]['LR'] = lr_data['total_added']
                    removed_summary[summary_name]['LR'] = lr_data['total_removed']
                    total_commits_lr = lr_data['total_commits']
                    if total_commits_lr > 1:
                        added_norm_summary[summary_name]['LR'] = (lr_data['total_added'] / total_commits_lr) * 1000
                        removed_norm_summary[summary_name]['LR'] = (lr_data['total_removed'] / total_commits_lr) * 1000
                    # else: Keep initialized 0.0

                if sr_data:
                    commit_summary[summary_name]['SR'] = sr_data['total_commits'] if sr_data[
                                                                                         'total_commits'] > 1 else 0  # Display 0 if only placeholder 1
                    added_summary[summary_name]['SR'] = sr_data['total_added']
                    removed_summary[summary_name]['SR'] = sr_data['total_removed']
                    total_commits_sr = sr_data['total_commits']
                    if total_commits_sr > 1:
                        added_norm_summary[summary_name]['SR'] = (sr_data['total_added'] / total_commits_sr) * 1000
                        removed_norm_summary[summary_name]['SR'] = (sr_data['total_removed'] / total_commits_sr) * 1000
                    # else: Keep initialized 0.0

                # Ensure plot_combined_weekly_trends is called only if both data exist
                if lr_data and sr_data:
                    try:
                        plot_combined_weekly_trends(project, lr_data, sr_data,
                                                    major_releases_for_project if major_releases_for_project else [])
                    except Exception as plot_weekly_err:
                        print(f"Error plotting weekly trends for {project['project_name']}: {plot_weekly_err}")

                else:
                    print(f"Skipping combined trend plot for {project['project_name']} due to missing data.")


    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
    except Exception as general_e:  # Catch other potential errors during loop
        print(f"❌ An unexpected error occurred in main loop: {general_e}")
        # import traceback
        # print(traceback.format_exc()) # Uncomment for detailed traceback

    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("\n✅ Database connection closed.")

        # Print summaries only if they contain data
        if commit_summary:
            print("\n\n" + "#" * 30 + "\n### Commit Summary (RAW Count) ###\n" + "#" * 30)
            df_commits = pd.DataFrame(commit_summary).T
            # df_commits[df_commits == 1] = 0 # Already handled during population
            print(df_commits.fillna(0).astype(int))
        if added_summary:
            print("\n\n" + "#" * 35 + "\n### SATD ADDED Summary (RAW Count) ###\n" + "#" * 35)
            print(pd.DataFrame(added_summary).T.fillna(0).astype(int))
        if removed_summary:
            print("\n\n" + "#" * 37 + "\n### SATD REMOVED Summary (RAW Count) ###\n" + "#" * 37)
            print(pd.DataFrame(removed_summary).T.fillna(0).astype(int))
        if added_norm_summary:
            print("\n\n" + "#" * 50 + "\n### SATD ADDED Summary (Count per 1000 Commits) ###\n" + "#" * 50)
            print(pd.DataFrame(added_norm_summary).T.fillna(0).round(2))
        if removed_norm_summary:
            print("\n\n" + "#" * 52 + "\n### SATD REMOVED Summary (Count per 1000 Commits) ###\n" + "#" * 52)
            print(pd.DataFrame(removed_norm_summary).T.fillna(0).round(2))


if __name__ == '__main__':
    main()