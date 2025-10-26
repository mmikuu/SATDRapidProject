# ファイル名: rdd_unified_model.py

import mysql.connector
import pandas as pd
from mysql.connector import Error
import matplotlib.pyplot as plt
import statsmodels.formula.api as smf
import numpy as np
import os
import matplotlib.dates as mdates  # dateutil.relativedelta の代わりに使用する可能性がある
from dateutil.relativedelta import relativedelta  # 念のためインポートは残す
from scipy.stats import linregress  # add_regression_trend で使われているが、このファイルでは不要な可能性

# --- データベース接続情報 (★ご自身の環境に合わせて修正してください) ---
DB_CONFIG = {
    'host': 'mussel.naist.jp',
    'port': 3306,
    'user': 'root',
    'password': 'hoge',
}

# --- 分析対象の全プロジェクトを定義 ---
STUDY_PROJECTS = [
    {
        'project_name': 'JDT.CORE',
        'schema': 'eclipse_check',
        'releases': {
            'LR': {'start_date': '2016-06-22', 'end_date': '2018-06-26'},
            'SR': {'start_date': '2018-06-27', 'end_date': '2020-06-16'}
        },
        'historical_releases': [
            # --- LR (Annual Releases) ---
            {'date': '2016-06-22', 'type': 'LR'},  # Neon
            {'date': '2017-06-28', 'type': 'LR'},  # Oxygen
            # --- SR (Quarterly Releases) ---
            {'date': '2018-06-27', 'type': 'SR'},  # Photon (Transition point)
            {'date': '2018-09-19', 'type': 'SR'},  # 2018-09
            {'date': '2018-12-19', 'type': 'SR'},  # 2018-12
            {'date': '2019-03-20', 'type': 'SR'},  # 2019-03
            {'date': '2019-06-19', 'type': 'SR'},  # 2019-06
            {'date': '2019-09-18', 'type': 'SR'},  # 2019-09
            {'date': '2019-12-18', 'type': 'SR'},  # 2019-12
            {'date': '2020-03-18', 'type': 'SR'},  # 2020-03
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
        'project_name': 'PDE',
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


def fetch_and_prepare_data(db_conn, project_info):
    """
    データベースから SATD (ADDED/REMOVED) とコミット数を取得し、週次の時系列データに整形する。
    count_satd_added.py と同じ取得ロジックを使用する。
    """
    schema = project_info['schema']
    project_name = project_info['project_name']

    print(f"Fetching data for {project_name} using count_satd_added-style aggregation...")

    def _clean_sql(sql: str) -> str:
        return sql.strip().rstrip(';')

    try:
        # 集計対象期間をコミットテーブルから決定
        cursor = db_conn.cursor()
        cursor.execute(
            f"SELECT DATE(MIN(commit_date)) AS start_date, DATE(MAX(commit_date)) AS end_date FROM {schema}.Commits"
        )
        date_range = cursor.fetchone()
        if not date_range or not date_range[0] or not date_range[1]:
            print(f"Error: Could not determine commit date range for {project_name}.")
            cursor.close()
            return None
        start_date, end_date = date_range
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')

        # WaitRemove テーブル類の存在確認
        cursor.execute(f"SHOW TABLES FROM {schema} LIKE 'WaitRemove';")
        wait_remove_exists = cursor.fetchone() is not None
        cursor.execute(f"SHOW TABLES FROM {schema} LIKE 'WaitRemoveSATDInFile';")
        wait_remove_infile_exists = cursor.fetchone() is not None
        cursor.close()

        # SATD_ADDED 取得クエリ
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

        # SATD_REMOVED 取得クエリ
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

        # WaitRemove 由来の SATD_REMOVED
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

        query_removed_union_with_waitremove = "\n            UNION ALL\n            ".join([
            _clean_sql(query_satd_removed),
            _clean_sql(query_waitremove_removed)
        ])
        query_removed_satd_only = _clean_sql(query_satd_removed)

        query_commits = f"""
            SELECT DATE(commit_date) AS commit_date, COUNT(*) as commit_count
            FROM {schema}.Commits
            WHERE commit_date BETWEEN '{start_date_str}' AND '{end_date_str}'
            GROUP BY DATE(commit_date)
        """

        # SATD_ADDED 取得
        satd_added_df = pd.read_sql_query(_clean_sql(query_satd_added), db_conn)

        # SATD_REMOVED 取得 (WaitRemove の有無で分岐)
        if wait_remove_exists and wait_remove_infile_exists:
            print("WaitRemove と WaitRemoveSATDInFile を検出。SATD_REMOVED を SATD + WaitRemove(非同期) で合算します。")
            satd_removed_df = pd.read_sql_query(query_removed_union_with_waitremove, db_conn)
        else:
            print("WaitRemove / WaitRemoveSATDInFile が見つからないため、SATDテーブルのREMOVEDのみを使用します。")
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

        commits_df = pd.read_sql_query(_clean_sql(query_commits), db_conn)
        if commits_df.empty:
            print(f"Error: No commits found for {project_name}. Cannot calculate normalized values.")
            return None

        total_commits = commits_df['commit_count'].sum()
        if total_commits == 0:
            print(f"Warning: Total commits zero for {project_name}.")

        commits_df['commit_date'] = pd.to_datetime(commits_df['commit_date'])
        commits_df.set_index('commit_date', inplace=True)
        weekly_commits = commits_df.resample('W-MON').sum().rename(columns={'commit_count': 'Commits'})

        if satd_df is None or satd_df.empty:
            satd_df = pd.DataFrame(columns=['commit_date', 'resolution'])
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
        for col in ['SATD_ADDED', 'SATD_REMOVED', 'Commits']:
            if col not in merged_df:
                merged_df[col] = 0
        merged_df.fillna(0, inplace=True)

        merged_df['Normalized_ADDED'] = merged_df.apply(
            lambda row: (row['SATD_ADDED'] / row['Commits']) * 1000 if row['Commits'] > 0 else 0, axis=1)
        merged_df['Normalized_REMOVED'] = merged_df.apply(
            lambda row: (row['SATD_REMOVED'] / row['Commits']) * 1000 if row['Commits'] > 0 else 0, axis=1)

        merged_df['SATD_ADDED'] = merged_df['SATD_ADDED'].astype(int)
        merged_df['SATD_REMOVED'] = merged_df['SATD_REMOVED'].astype(int)
        merged_df['Commits'] = merged_df['Commits'].astype(int)

        merged_df.rename(columns={
            'SATD_ADDED': 'ADDED',
            'SATD_REMOVED': 'REMOVED',
            'Commits': 'total_commits'
        }, inplace=True)
        merged_df.index.name = 'date'

        print("Data preparation complete.")
        return merged_df.reset_index()

    except Error as e:
        print(f"❌ SQL Error during data fetching for {project_name}: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during data preparation for {project_name}: {e}")
        return None


def perform_unified_rdd_analysis(df, project_name, historical_releases, outcome_variable, window_weeks_half=52):
    """
    LRとSRの影響を直接比較する統合RDDモデルを実行する。
    """
    print(f"\n--- Performing Unified RDD Analysis for {project_name} ({outcome_variable}) ---")

    # 1. 各リリースの前後N週間のデータを集めて、分析用のデータフレームを構築
    all_rdd_data = []
    for release_info in historical_releases:
        release_date = pd.to_datetime(release_info['date'])
        release_type = release_info['type']

        start_date = release_date - pd.Timedelta(weeks=window_weeks_half)
        end_date = release_date + pd.Timedelta(weeks=window_weeks_half - 1)  # 重複を避ける

        window_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()

        # RDDモデル用の変数を計算
        window_df['time_from_release'] = (window_df['date'] - release_date).dt.days / 7.0
        window_df['release_type'] = release_type

        all_rdd_data.append(window_df)

    if not all_rdd_data:
        print("No data found around any release dates.")
        return

    rdd_df = pd.concat(all_rdd_data).reset_index(drop=True)

    # 欠損値を含む行を削除 (特にoutcome_variable)
    rdd_df = rdd_df.dropna(subset=[outcome_variable, 'time_from_release'])

    # outcome_variableが数値型であることを確認
    rdd_df[outcome_variable] = pd.to_numeric(rdd_df[outcome_variable], errors='coerce')
    rdd_df = rdd_df.dropna(subset=[outcome_variable])  # 数値変換でNaNになったものも削除

    if rdd_df.empty:
        print(f"DataFrame is empty after processing NaNs for {outcome_variable}. Skipping model.")
        return

    rdd_df.rename(columns={outcome_variable: 'outcome'}, inplace=True)

    # 2. モデルの変数を定義
    rdd_df['treatment'] = (rdd_df['time_from_release'] >= 0).astype(int)
    rdd_df['is_SR'] = (rdd_df['release_type'] == 'SR').astype(int)

    # 3. 相互作用項を含む回帰モデルを定義して実行
    model_formula = 'outcome ~ time_from_release * treatment * is_SR'
    try:
        model = smf.ols(formula=model_formula, data=rdd_df).fit()
        print("\n[Unified RDD Model Summary]")
        print(model.summary())

        # 結果の解釈のポイント
        print("\n" + "=" * 20 + " Interpretation Hint " + "=" * 20)
        print("To compare LR and SR, look at the 'treatment:is_SR' coefficient:")
        print("  - Its 'coef' shows the *additional* jump in SATD for SR releases compared to LR releases.")
        print(
            "  - If its 'P>|t|' (p-value) is small (e.g., < 0.05), the difference between LR and SR is statistically significant.")
        print("=" * 63 + "\n")

    except Exception as e:
        print(f"An error occurred during model fitting: {e}")
        return

    # 4. 結果をプロット
    plt.figure(figsize=(14, 9))  # figsizeを維持

    # Scatter plot for LR and SR data
    plt.scatter(rdd_df[rdd_df['is_SR'] == 0]['time_from_release'], rdd_df[rdd_df['is_SR'] == 0]['outcome'],
                color='blue', alpha=0.3, label='Weekly Data (LR)')
    plt.scatter(rdd_df[rdd_df['is_SR'] == 1]['time_from_release'], rdd_df[rdd_df['is_SR'] == 1]['outcome'],
                color='red', alpha=0.3, label='Weekly Data (SR)')

    # 予測用のデータを作成
    plot_x = np.linspace(-window_weeks_half, window_weeks_half, 200)

    # LR (is_SR=0) の予測
    pred_lr_pre = model.predict(pd.DataFrame({'time_from_release': plot_x[plot_x < 0], 'treatment': 0, 'is_SR': 0}))
    pred_lr_post = model.predict(pd.DataFrame({'time_from_release': plot_x[plot_x >= 0], 'treatment': 1, 'is_SR': 0}))
    # ★ 修正 ★: linewidth を増加 (例: 3 -> 4 or 5)
    plt.plot(plot_x[plot_x < 0], pred_lr_pre, color='blue', linewidth=4, label='Fit (LR)')
    plt.plot(plot_x[plot_x >= 0], pred_lr_post, color='blue', linewidth=4)

    # SR (is_SR=1) の予測
    pred_sr_pre = model.predict(pd.DataFrame({'time_from_release': plot_x[plot_x < 0], 'treatment': 0, 'is_SR': 1}))
    pred_sr_post = model.predict(pd.DataFrame({'time_from_release': plot_x[plot_x >= 0], 'treatment': 1, 'is_SR': 1}))
    # ★ 修正 ★: linewidth を増加
    plt.plot(plot_x[plot_x < 0], pred_sr_pre, color='red', linewidth=4, label='Fit (SR)')
    plt.plot(plot_x[plot_x >= 0], pred_sr_post, color='red', linewidth=4)

    plt.axvline(x=0, color='gray', linestyle='--', label='Release Cutoff')

    # ★ 修正 ★: 各種フォントサイズを指定
    plt.title(f'Unified RDD Analysis for {project_name}\n{outcome_variable} (LR vs SR)', fontsize=18)
    plt.xlabel('Weeks from Release Cutoff', fontsize=25)
    plt.ylabel(outcome_variable.replace('_', ' '), fontsize=25)  # アンダースコアをスペースに置換
    plt.legend(fontsize=20)
    plt.tick_params(axis='both', which='major', labelsize=20)  # 目盛りラベルのサイズ

    plt.grid(True)
    plt.tight_layout()  # ★ 修正 ★: レイアウト調整を追加

    # グラフをファイルに保存
    plot_dir = '../rdd_plots_unified'
    os.makedirs(plot_dir, exist_ok=True)
    safe_project_name = project_name.replace(' ', '_').replace('.', '')
    # ★ 修正 ★: ファイル拡張子を .pdf に変更
    plot_filename = f"{safe_project_name}_{outcome_variable}_unified.pdf"
    plt.savefig(os.path.join(plot_dir, plot_filename), format='pdf')  # format='pdf' を明示
    plt.close()
    print(f"Unified RDD plot saved to {os.path.join(plot_dir, plot_filename)}")


def main():
    """
    メインの実行関数。全プロジェクトに対して統合分析を実行する。
    """
    db_conn = None
    try:
        db_conn = mysql.connector.connect(**DB_CONFIG)
        if db_conn.is_connected():
            print("✅ Successfully connected to the database.")

            for project in STUDY_PROJECTS:
                if 'historical_releases' not in project or not project['historical_releases']:
                    print(f"\nSkipping {project['project_name']} as it has no historical releases defined.")
                    continue

                print(f"\n\n{'=' * 25} Starting Analysis for: {project['project_name']} {'=' * 25}")
                df = fetch_and_prepare_data(db_conn, project)

                if df is not None and not df.empty:
                    # ADDEDとREMOVEDの両方で分析
                    for outcome in ['Normalized_ADDED', 'Normalized_REMOVED']:
                        # Check if outcome column exists and has non-zero variance
                        if outcome in df and df[outcome].nunique() > 1:
                            perform_unified_rdd_analysis(
                                df, project['project_name'], project['historical_releases'], outcome,
                                window_weeks_half=52
                            )
                        elif outcome not in df:
                            print(f"Skipping RDD for {outcome} as column not found.")
                        else:
                            print(f"Skipping RDD for {outcome} due to zero variance (constant value).")
                else:
                    print(f"Skipping RDD analysis for {project['project_name']} due to missing or empty data.")


    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
    finally:
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("\n✅ Database connection closed.")


if __name__ == '__main__':
    main()
