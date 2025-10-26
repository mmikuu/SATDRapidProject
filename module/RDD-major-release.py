# ファイル名: rdd_per_release_final.py

import mysql.connector
import pandas as pd
from mysql.connector import Error
import matplotlib.pyplot as plt
import statsmodels.api as sm
from dateutil.relativedelta import relativedelta
import os

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
    # --- ここに新しいプロジェクトを追加しました ---
    {
        'project_name': 'PDE',
        'schema': 'eclipse_check_pde',
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
    # ------------------------------------
    {
        'project_name': 'Firefox Transition (LR vs SR)',
        'schema': 'firefox_check',
        'releases': {
            'LR': {'start_date': '2009-10-24', 'end_date': '2011-03-21'},
            'SR': {'start_date': '2011-03-22', 'end_date': '2013-03-21'}
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
        ]
    },
    {
        'project_name': 'Firefox 57-96',
        'schema': 'firefox_check',
        'releases': {
            'LR': {'start_date': '2017-11-14', 'end_date': '2019-12-02'},
            'SR': {'start_date': '2019-12-03', 'end_date': '2022-01-11'}
        }
    }
]


def fetch_and_prepare_data(db_conn, project_info):
    """
    データベースからデータを取得し、週次の時系列データに整形する。
    """
    schema = project_info['schema']
    project_name = project_info['project_name']

    print(f"Fetching data for {project_name} using the specified schema logic...")

    # SQLクエリ: SATD, Commits, SATDInFileをJOINしてSATDイベントの日時を取得
    query_satd_events = f"""
    SELECT
        C.commit_date,
        S.resolution AS change_type
    FROM {schema}.SATD AS S
    INNER JOIN {schema}.Commits AS C ON S.second_commit = C.commit_hash
    INNER JOIN {schema}.SATDInFile AS F ON S.second_file = F.f_id
    WHERE S.resolution IN ('SATD_ADDED', 'SATD_REMOVED')
      AND F.f_comment NOT LIKE '%Copyright@%';
    """

    # SQLクエリ: 正規化のために全てのコミット日時を取得
    query_all_commits = f"""
    SELECT commit_hash, commit_date FROM {schema}.Commits;
    """

    try:
        # 1. SATDイベント（追加と除去）のリストを日付付きで取得
        satd_events_df = pd.read_sql_query(query_satd_events, db_conn)
        satd_events_df['date'] = pd.to_datetime(satd_events_df['commit_date'])

        # 2. 日付と変更タイプを元にクロス集計し、週ごとのイベント数を計算
        satd_counts_df = pd.crosstab(index=satd_events_df['date'], columns=satd_events_df['change_type'])
        weekly_satd_df = satd_counts_df.resample('W-MON').sum()

        # イベントが発生しなかった場合に備え、カラムを確保
        if 'SATD_ADDED' not in weekly_satd_df: weekly_satd_df['SATD_ADDED'] = 0
        if 'SATD_REMOVED' not in weekly_satd_df: weekly_satd_df['SATD_REMOVED'] = 0

        # RDD分析関数が期待するカラム名に変更
        weekly_satd_df.rename(columns={'SATD_ADDED': 'ADDED', 'SATD_REMOVED': 'REMOVED'}, inplace=True)

        # 3. 正規化のため、週ごとの総コミット数を計算
        all_commits_df = pd.read_sql_query(query_all_commits, db_conn)
        all_commits_df['date'] = pd.to_datetime(all_commits_df['commit_date'])
        weekly_commits_count = all_commits_df.set_index('date')['commit_hash'].resample('W-MON').count()
        weekly_commits_count.name = "total_commits"

        # 4. SATD数と総コミット数を結合
        final_df = weekly_satd_df.join(weekly_commits_count, how='outer').fillna(0)

        # 5. 正規化（100コミットあたりのSATD数）
        final_df['Normalized_ADDED'] = (final_df['ADDED'] / final_df['total_commits']).where(
            final_df['total_commits'] > 0, 0) * 100
        final_df['Normalized_REMOVED'] = (final_df['REMOVED'] / final_df['total_commits']).where(
            final_df['total_commits'] > 0, 0) * 100

        print("Data preparation complete.")
        return final_df.reset_index()

    except Error as e:
        print(f"❌ SQL Error during data fetching for {project_name}: {e}")
        print("👉 クエリ内のテーブル名やカラム名が、実際のスキーマと一致しているか確認してください。")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during data preparation for {project_name}: {e}")
        return None


def generate_release_dates(project_info):
    """
    プロジェクト情報から分析対象となるメジャーリリース日のリストを生成する。
    """
    release_dates = []
    project_name = project_info['project_name']

    # historical_releasesが定義されていれば、それを優先的に使用
    if 'historical_releases' in project_info:
        print(f"Using historical release dates for {project_name}...")
        for d in project_info['historical_releases']:
            release_dates.append({'date': pd.to_datetime(d['date']), 'type': d['type']})
        return release_dates

    # (historical_releasesがない場合のフォールバック処理)
    releases_info = project_info['releases']
    lr_delta, sr_delta = None, None
    if 'Firefox 57-96' in project_name:
        print("Applying special release cycle for Firefox 57-96 (LR: 6 weeks, SR: 4 weeks)...")
        lr_delta, sr_delta = relativedelta(weeks=6), relativedelta(weeks=4)
    else:
        print("Applying standard Eclipse release cycle...")
        lr_delta, sr_delta = relativedelta(years=1), relativedelta(months=4)

    if lr_delta:
        lr_info = releases_info['LR']
        current_release = pd.to_datetime(lr_info['start_date']) + lr_delta
        while current_release <= pd.to_datetime(lr_info['end_date']):
            release_dates.append({'date': current_release, 'type': 'LR'})
            current_release += lr_delta

    if sr_delta:
        sr_info = releases_info['SR']
        current_release = pd.to_datetime(sr_info['start_date']) + sr_delta
        while current_release <= pd.to_datetime(sr_info['end_date']):
            release_dates.append({'date': current_release, 'type': 'SR'})
            current_release += sr_delta

    return release_dates


def perform_rdd_analysis_for_release(df, project_name, release_info, outcome_variable, window_weeks=104):
    """
    指定されたリリース日を境界としてRDD分析を実行し、結果とプロットを保存する。
    """
    release_date = release_info['date']
    release_type = release_info['type']
    release_date_str = release_date.strftime('%Y-%m-%d')
    print(
        f"\n--- Performing RDD Analysis for {project_name} ({outcome_variable}) around {release_date_str} ({release_type}) ---")

    # 分析ウィンドウ（リリース前後2年）のデータを抽出
    start_date = release_date - pd.Timedelta(weeks=window_weeks)
    end_date = release_date + pd.Timedelta(weeks=window_weeks)
    rdd_df = df[(df['date'] >= start_date) & (df['date'] <= end_date)].copy()

    if len(rdd_df) < 20:  # 分析に十分なデータがあるか確認
        print("Not enough data points within the window to perform analysis.")
        return None

    # RDDモデルの変数を準備
    rdd_df['time_from_cutoff'] = (rdd_df['date'] - release_date).dt.days / 7
    rdd_df['treatment'] = (rdd_df['date'] >= release_date).astype(int)
    rdd_df['interaction'] = rdd_df['time_from_cutoff'] * rdd_df['treatment']

    # OLS（最小二乗法）による回帰モデルの定義と実行
    Y = rdd_df[outcome_variable]
    X = rdd_df[['treatment', 'time_from_cutoff', 'interaction']]
    X = sm.add_constant(X)

    try:
        model = sm.OLS(Y, X).fit()
        print("\n[RDD Model Summary]")
        print(model.summary())

        # 分析結果のグラフを作成
        plt.figure(figsize=(12, 8))
        plt.scatter(rdd_df['time_from_cutoff'], Y, alpha=0.5, label='Weekly SATD Rate')

        pred_df = rdd_df.copy().sort_values('time_from_cutoff')
        pred_vals = model.predict(sm.add_constant(pred_df[['treatment', 'time_from_cutoff', 'interaction']]))

        pre_cutoff = pred_df[pred_df['treatment'] == 0]
        post_cutoff = pred_df[pred_df['treatment'] == 1]

        plt.plot(pre_cutoff['time_from_cutoff'], pred_vals[pre_cutoff.index], color='blue', linewidth=3,
                 label='Fit (Before)')
        plt.plot(post_cutoff['time_from_cutoff'], pred_vals[post_cutoff.index], color='red', linewidth=3,
                 label='Fit (After)')

        plt.axvline(x=0, color='gray', linestyle='--', label=f'Cutoff: {release_date_str}')
        plt.title(f'RDD Analysis for {project_name}\n{outcome_variable} around {release_date_str} ({release_type})')
        plt.xlabel('Weeks from Release Cutoff')
        plt.ylabel(outcome_variable)
        plt.legend()
        plt.grid(True)

        # グラフをファイルに保存
        plot_dir = 'rdd_plots_per_release'
        os.makedirs(plot_dir, exist_ok=True)
        safe_project_name = project_name.replace(' ', '_').replace('.', '')
        plot_filename = f"{safe_project_name}_{release_date_str}_{outcome_variable}.png"
        plt.savefig(os.path.join(plot_dir, plot_filename))
        plt.close()
        print(f"RDD plot saved to {os.path.join(plot_dir, plot_filename)}")

        # 結果を辞書形式で返す
        return {
            'project': project_name,
            'release_date': release_date_str,
            'release_type': release_type,
            'outcome': outcome_variable,
            'treatment_coef': model.params['treatment'],
            'p_value': model.pvalues['treatment'],
            'r_squared': model.rsquared
        }

    except Exception as e:
        print(f"An error occurred during RDD analysis for {release_date_str}: {e}")
        return None


def main():
    """
    メインの実行関数。全プロジェクト、全リリースに対して分析を実行する。
    """
    db_conn = None
    all_rdd_results = []

    try:
        # データベース接続
        db_conn = mysql.connector.connect(**DB_CONFIG)
        if db_conn.is_connected():
            print("✅ Successfully connected to the database.")

            # 設定された全プロジェクトをループ
            for project in STUDY_PROJECTS:
                print(f"\n\n{'=' * 25} Starting Analysis for: {project['project_name']} {'=' * 25}")

                # データを取得・準備
                df = fetch_and_prepare_data(db_conn, project)

                if df is not None and not df.empty:
                    # 分析対象のリリース日リストを取得
                    release_dates = generate_release_dates(project)
                    print(f"Found {len(release_dates)} major releases to analyze.")

                    # 各リリース日をループ
                    for release_info in release_dates:
                        # ADDEDとREMOVEDの両方で分析
                        for outcome in ['Normalized_ADDED', 'Normalized_REMOVED']:
                            result = perform_rdd_analysis_for_release(
                                df, project['project_name'], release_info, outcome, window_weeks=104
                            )
                            if result:
                                all_rdd_results.append(result)

    except Error as e:
        print(f"❌ Error connecting to MySQL: {e}")
    finally:
        # データベース切断と結果の保存
        if db_conn and db_conn.is_connected():
            db_conn.close()
            print("\n✅ Database connection closed.")

        if all_rdd_results:
            summary_df = pd.DataFrame(all_rdd_results)
            print("\n\n" + "=" * 30 + " Final RDD Analysis Summary " + "=" * 30)
            print(summary_df)

            # 最終サマリーをCSVファイルに保存
            summary_df.to_csv("final_rdd_summary_per_release.csv", index=False)
            print("\n✅ Summary saved to final_rdd_summary_per_release.csv")
        else:
            print("\nNo RDD analysis was successfully completed.")


if __name__ == '__main__':
    main()