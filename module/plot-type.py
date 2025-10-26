import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os


def create_subplot_charts(csv_file='classification_summary_detailed.csv'):
    """
    詳細な集計CSVを読み込み、プロジェクトごとに1つの画像内に
    「Added」と「Removed」の比較グラフを上下に並べて生成する。
    """
    # 1. 保存先ディレクトリを指定
    output_dir = 'plot-type'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"ディレクトリを作成しました: '{output_dir}'")

    try:
        # 2. CSVファイルからデータを読み込む
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print(f"エラー: '{csv_file}' が見つかりません。先にJavaの処理を実行してCSVを生成してください。")
        return

    # プロジェクト名の一覧を取得
    projects = df['Project'].unique()

    # 3. 各プロジェクトのグラフをループで作成
    for project in projects:
        print(f"'{project}' のサブプロットグラフを作成中...")

        # プロジェクトのデータを抽出
        project_data = df[df['Project'] == project]

        # グラフ描画のためのカテゴリを定義
        categories = ['Design', 'Implementation', 'Defect', 'Test']

        # 4. グラフのキャンバスを準備 (2行1列のサブプロット)
        # figsizeの縦を長くして、2つのグラフが余裕をもって表示されるようにする
        fig, axes = plt.subplots(2, 1, figsize=(12, 14))

        # 画像全体のメインタイトルを設定
        fig.suptitle(f'{project}: SATD Comparison (LR vs SR)', fontsize=18, y=0.98)

        # 5. 上段のグラフ (Added) を描画
        ax1 = axes[0]
        added_data = project_data[project_data['SatdType'] == 'Added'].set_index('ReleaseType')

        # Addedデータが存在する場合のみプロット
        if not added_data.empty:
            lr_added = added_data.loc['LR', categories].values if 'LR' in added_data.index else np.zeros(
                len(categories))
            sr_added = added_data.loc['SR', categories].values if 'SR' in added_data.index else np.zeros(
                len(categories))

            x = np.arange(len(categories))
            width = 0.35

            rects1 = ax1.bar(x - width / 2, lr_added, width, label='LR', color='#377eb8')
            rects2 = ax1.bar(x + width / 2, sr_added, width, label='SR', color='#ff7f00')

            ax1.set_title('Added SATD', fontsize=14)
            ax1.set_ylabel('Count', fontsize=12)
            ax1.set_xticks(x)
            ax1.set_xticklabels(categories)
            ax1.legend()
            ax1.grid(axis='y', linestyle='--', alpha=0.7)
            ax1.bar_label(rects1, padding=3)
            ax1.bar_label(rects2, padding=3)

        # 6. 下段のグラフ (Removed) を描画
        ax2 = axes[1]
        removed_data = project_data[project_data['SatdType'] == 'Removed'].set_index('ReleaseType')

        # Removedデータが存在する場合のみプロット
        if not removed_data.empty:
            lr_removed = removed_data.loc['LR', categories].values if 'LR' in removed_data.index else np.zeros(
                len(categories))
            sr_removed = removed_data.loc['SR', categories].values if 'SR' in removed_data.index else np.zeros(
                len(categories))

            x = np.arange(len(categories))
            width = 0.35

            rects3 = ax2.bar(x - width / 2, lr_removed, width, label='LR', color='#377eb8')
            rects4 = ax2.bar(x + width / 2, sr_removed, width, label='SR', color='#ff7f00')

            ax2.set_title('Removed SATD', fontsize=14)
            ax2.set_ylabel('Count', fontsize=12)
            ax2.set_xticks(x)
            ax2.set_xticklabels(categories)
            ax2.legend()
            ax2.grid(axis='y', linestyle='--', alpha=0.7)
            ax2.bar_label(rects3, padding=3)
            ax2.bar_label(rects4, padding=3)

        # 7. レイアウトを調整してファイルに保存
        fig.tight_layout(rect=[0, 0.03, 1, 0.95])  # メインタイトルとの重なりを避ける

        sanitized_project_name = project.replace(" ", "_").replace(".", "_")
        # 新しいファイル名で保存
        output_path = os.path.join(output_dir, f"{sanitized_project_name}_subplots_comparison.png")
        plt.savefig(output_path)
        plt.close(fig)

        print(f"  -> グラフを '{output_path}' に保存しました。")


if __name__ == '__main__':
    create_subplot_charts()
    print("\nすべてのサブプロットグラフの生成が完了しました！ ✨")