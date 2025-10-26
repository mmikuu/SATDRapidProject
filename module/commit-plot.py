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


def plot_monthly_commits_combined(db_conn, project_info, major_releases):
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

        # y軸の最大値を取得 (点線のテキスト配置用)
        current_max_y = monthly_commits['Commits'].max()
        text_y_pos = current_max_y * 1.3  # 上部 130% の位置 (ylim 1.5倍設定のため)

        release_counter_lr = 1
        release_counter_sr = 1

        # SR開始の遷移線 (major_releases に SR開始日がない場合も考慮)
        ax.axvline(x=sr_start, color='red', linestyle='--', linewidth=2, zorder=4, label='Transition to SR')

        for release in major_releases:
            release_date = release['date']
            release_type = release['type']

            # プロット範囲内のリリースのみ描画
            if min_date <= release_date <= max_date:
                color = 'blue' if release_type == 'LR' else 'green'
                linestyle = ':'  # 点線

                if release_type == 'LR':
                    # LR期間の開始日は除外 (LR Periodのラベルと被るため)
                    if release_date == lr_start: continue
                    label = f"R{release_counter_lr}(LR)"
                    release_counter_lr += 1
                else:
                    # SR期間の開始日は除外 (Transitionと被るため)
                    if release_date == sr_start: continue
                    label = f"R{release_counter_sr}(SR)"
                    release_counter_sr += 1

                ax.axvline(x=release_date, color=color, linestyle=linestyle, linewidth=1.5, zorder=1)
                ax.text(release_date, text_y_pos, label,
                        rotation=90, verticalalignment='top', horizontalalignment='center',
                        color=color, fontsize=10)

        ax.set_title(f"Monthly Commit Counts: {project_info['project_name']} (LR & SR Periods)", fontsize=22)
        ax.set_xlabel("Date", fontsize=24)
        ax.set_ylabel("Monthly Commit Count", fontsize=24)
        ax.legend(loc='upper right', fontsize=18)
        ax.tick_params(axis='both', which='major', labelsize=16)
        ax.grid(True, which='both', linestyle='--', linewidth=0.5, zorder=-1)
        ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right')

        ax.set_ylim(bottom=-0.05 * current_max_y, top=current_max_y * 1.5)

        plt.tight_layout()
        output_dir = '../plots-commits(monthly-combined)'
        os.makedirs(output_dir, exist_ok=True)
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


# --- リリース日ハンドリング ---
def generate_release_dates(project_info):
    return [{'date': pd.to_datetime(d['date']), 'type': d['type']}
            for d in project_info.get('historical_releases', [])]


# --- エントリーポイント ---
def main():
    db_conn = None
    try:
        db_conn = mysql.connector.connect(**DB_CONFIG)
        if db_conn.is_connected():
            print("✅ Successfully connected to the database.")

            for project in STUDY_PROJECTS:
                print("\n" + "#" * 25 + f" Processing Project: {project['project_name']} " + "#" * 25)

                major_releases_for_project = generate_release_dates(project)

                try:
                    plot_monthly_commits_combined(db_conn, project,
                                                  major_releases_for_project if major_releases_for_project else [])
                except Exception as plot_err:
                    print(f"Error plotting monthly commits for {project['project_name']}: {plot_err}")

    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
    except Exception as general_e:
        print(f"❌ An unexpected error occurred in main loop: {general_e}")

    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("\n✅ Database connection closed.")


if __name__ == '__main__':
    main()