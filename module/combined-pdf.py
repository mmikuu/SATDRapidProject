# --- Configuration ---
# List the paths to the eight input PDF or PNG files
input_files = [
    # 8つのファイルパスを 4行x2列 の順序で指定します
    # (上から下へ、左から右へ)

    # # --- Row 1 ---
    # "plots-percent(trend)/JdtADDEDCommitPerCountGraph.pdf",  # (a)
    # "plots-percent(trend)/SwtADDEDCommitPerCountGraph.pdf",  # (b)
    # # --- Row 2 ---
    # "plots-percent(trend)/UiADDEDCommitPerCountGraph.pdf",  # (c)
    # "plots-percent(trend)/PdeADDEDCommitPerCountGraph.pdf",  # (d)
    # # --- Row 3 ---
    # "plots-percent(trend)/EquinoxADDEDCommitPerCountGraph.pdf",  # (e)
    # "plots-percent(trend)/Electron1ADDEDCommitPerCountGraph.pdf",  # (f)
    # # --- Row 4 ---
    # "plots-percent(trend)/Electron2ADDEDCommitPerCountGraph.pdf",  # (g)
    # "plots-percent(trend)/FirefoxADDEDCommitPerCountGraph.pdf"  # (h)
    # --- Row 1 ---
    "plots-percent(trend)/JdtREMOVEDCommitPerCountGraph.pdf",  # (a)
    "plots-percent(trend)/SwtREMOVEDCommitPerCountGraph.pdf",  # (b)
    # --- Row 2 ---
    "plots-percent(trend)/UiREMOVEDCommitPerCountGraph.pdf",  # (c)
    "plots-percent(trend)/PdeREMOVEDCommitPerCountGraph.pdf",  # (d)
    # --- Row 3 ---
    "plots-percent(trend)/EquinoxREMOVEDCommitPerCountGraph.pdf",  # (e)
    "plots-percent(trend)/Electron1REMOVEDCommitPerCountGraph.pdf",  # (f)
    # --- Row 4 ---
    "plots-percent(trend)/Electron2REMOVEDCommitPerCountGraph.pdf",  # (g)
    "plots-percent(trend)/FirefoxREMOVEDCommitPerCountGraph.pdf"  # (h)

    # "rdd_plots_unified/JDTCORE_Normalized_ADDED_unified.pdf",  # (a)
    # "rdd_plots_unified/PLATFORMSWT_Normalized_ADDED_unified.pdf",  # (b)
    # # --- Row 2 ---
    # "rdd_plots_unified/PLATFORMUI_Normalized_ADDED_unified.pdf",  # (c)
    # "rdd_plots_unified/PDE_Normalized_ADDED_unified.pdf",  # (d)
    # # --- Row 3 ---
    # "rdd_plots_unified/Electron-1_Normalized_ADDED_unified.pdf",  # (e)
    # "rdd_plots_unified/Electron-2_Normalized_ADDED_unified.pdf",  # (f)
    # # --- Row 4 ---
    # "rdd_plots_unified/PLATFORMEQUINOX_Normalized_ADDED_unified.pdf",  # (g)
    # "rdd_plots_unified/Firefox30-26_Normalized_ADDED_unified.pdf"  # (h)
    #
    # "rdd_plots_unified/JDTCORE_Normalized_REMOVED_unified.pdf",  # (a)
    # "rdd_plots_unified/PLATFORMSWT_Normalized_REMOVED_unified.pdf",  # (b)
    # # --- Row 2 ---
    # "rdd_plots_unified/PLATFORMUI_Normalized_REMOVED_unified.pdf",  # (c)
    # "rdd_plots_unified/PDE_Normalized_REMOVED_unified.pdf",  # (d)
    # # --- Row 3 ---
    # "rdd_plots_unified/Electron-1_Normalized_REMOVED_unified.pdf",  # (e)
    # "rdd_plots_unified/Electron-2_Normalized_REMOVED_unified.pdf",  # (f)
    # # --- Row 4 ---
    # "rdd_plots_unified/PLATFORMEQUINOX_Normalized_REMOVED_unified.pdf",  # (g)
    # "rdd_plots_unified/Firefox30-26_Normalized_REMOVED_unified.pdf"  # (h)

    # "plots-commits(monthly-combined)/Electron1CommitGraph.pdf",  # (a)
    # "plots-commits(monthly-combined)/Electron2CommitGraph.pdf",  # (b)
    # # --- Row 2 ---
    # "plots-commits(monthly-combined)/EquinoxCommitGraph.pdf",  # (b)
    # "plots-commits(monthly-combined)/FirefoxCommitGraph.pdf",  # (b)
    # # --- Row 3 ---
    # "plots-commits(monthly-combined)/JdtCommitGraph.pdf",  # (b)
    # "plots-commits(monthly-combined)/PdeCommitGraph.pdf",  # (b)
    # # --- Row 4 ---
    # "plots-commits(monthly-combined)/SwtCommitGraph.pdf",  # (b)
    # "plots-commits(monthly-combined)/UiCommitGraph.pdf",  # (b)

]
output_pdf = "REMOVEDCountGraph.pdf"  # Name of the merged output file

# --- Label Configuration ---
label_font = "Helvetica-Bold"  # フォント
label_font_size = 18  # フォントサイズ
label_x_offset = 15  # 各グラフの左端からのX座標オフセット
label_y_offset = -25  # 各グラフの上端からのY座標オフセット (マイナス値)
# --- End Configuration ---

import os
import io  # ★★★ インメモリバッファのために追加 ★★★
from pypdf import PdfReader, PdfWriter, Transformation
from PIL import Image
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

print(f"Starting PDF/PNG merge process for 4x2 grid...")
print(f"Input files: {input_files}")
print(f"Output file: {output_pdf}")

if len(input_files) != 8:
    print("Error: Please provide exactly eight file paths in the 'input_files' list.")
    exit()

temp_pdfs = []
readers = []
pages = []
page_dims = []

try:
    # (中略：ファイル処理のロジックは変更なし)
    for i, file_path in enumerate(input_files):
        pdf_to_read = None
        if not os.path.exists(file_path):
            print(f"Error: Input file not found: {file_path}")
            exit()

        if file_path.lower().endswith('.png'):
            print(f"Detected PNG: {file_path}. Converting to temporary PDF...")
            try:
                img = Image.open(file_path)
                img_width_px, img_height_px = img.size
                temp_pdf_path = f"__temp_image_{i}.pdf"
                temp_pdfs.append(temp_pdf_path)
                page_width_pt = img_width_px
                page_height_pt = img_height_px
                c = canvas.Canvas(temp_pdf_path, pagesize=(page_width_pt, page_height_pt))
                c.drawImage(file_path, 0, 0, width=page_width_pt, height=page_height_pt)
                c.save()
                print(f"Saved temporary PDF: {temp_pdf_path}")
                pdf_to_read = temp_pdf_path
            except Exception as img_err:
                print(f"Error processing image {file_path}: {img_err}")
                exit()
        elif file_path.lower().endswith('.pdf'):
            print(f"Detected PDF: {file_path}")
            pdf_to_read = file_path
        else:
            print(f"Error: Unsupported file type: {file_path}. Only PDF and PNG are supported.")
            exit()

        if pdf_to_read:
            reader = PdfReader(pdf_to_read)
            readers.append(reader)
            if not reader.pages:
                print(f"Error: No pages found in PDF: {pdf_to_read}")
                for temp_file in temp_pdfs:
                    if os.path.exists(temp_file): os.remove(temp_file)
                exit()
            page = reader.pages[0]
            pages.append(page)
            page_dims.append((float(page.mediabox.width), float(page.mediabox.height)))
            print(f"Successfully processed: {pdf_to_read} with dimensions {page_dims[-1]}")
    # (中略ここまで)

    if not page_dims or len(page_dims) != 8:
        print("Error: Did not successfully process exactly eight pages.")
        exit()

    max_width = max(w for w, h in page_dims)
    max_height = max(h for w, h in page_dims)
    print(f"Max detected dimensions for grid cells: Width={max_width:.2f}, Height={max_height:.2f}")

    output_writer = PdfWriter()

    total_width = max_width * 2
    total_height = max_height * 4
    merged_page = output_writer.add_blank_page(width=total_width, height=total_height)
    print(f"Created blank output page: Width={total_width:.2f}, Height={total_height:.2f}")

    # (0,0) は左下隅
    transformations = [
        # Row 1 (Top)
        Transformation().translate(tx=0, ty=max_height * 3),  # (a)
        Transformation().translate(tx=max_width, ty=max_height * 3),  # (b)
        # Row 2
        Transformation().translate(tx=0, ty=max_height * 2),  # (c)
        Transformation().translate(tx=max_width, ty=max_height * 2),  # (d)
        # Row 3
        Transformation().translate(tx=0, ty=max_height * 1),  # (e)
        Transformation().translate(tx=max_width, ty=max_height * 1),  # (f)
        # Row 4 (Bottom)
        Transformation().translate(tx=0, ty=0),  # (g)
        Transformation().translate(tx=max_width, ty=0)  # (h)
    ]

    # --- 修正箇所 1 (ここから) ---
    # ラベル描画用に、各グラフの左下隅の座標を別途リストとして定義
    # (Transformationオブジェクトから .tx, .ty で座標は取得できないため)
    label_positions = [
        # Row 1 (Top)
        (0, max_height * 3),  # (a)
        (max_width, max_height * 3),  # (b)
        # Row 2
        (0, max_height * 2),  # (c)
        (max_width, max_height * 2),  # (d)
        # Row 3
        (0, max_height * 1),  # (e)
        (max_width, max_height * 1),  # (f)
        # Row 4 (Bottom)
        (0, 0),  # (g)
        (max_width, 0)  # (h)
    ]
    # --- 修正箇所 1 (ここまで) ---

    # グラフのページを結合
    for i in range(8):
        print(f"Merging page {i} from {input_files[i]}...")
        page_to_merge = pages[i]
        final_transform = transformations[i]
        merged_page.merge_transformed_page(page_to_merge, final_transform)

    # ★★★ ラベル追加セクション (ここからが追加部分) ★★★
    print("Creating labels overlay...")
    packet = io.BytesIO()  # メモリ上に一時PDFバッファを作成
    c = canvas.Canvas(packet, pagesize=(total_width, total_height))
    c.setFont(label_font, label_font_size)  # フォントとサイズを設定

    # (a) から (h) までのラベルを生成
    labels = [f"({chr(ord('a') + i)})" for i in range(8)]

    # --- 修正箇所 2 (ここから) ---
    # 各グラフの配置場所 (label_positions) に基づいてラベルを描画
    # (イテレートする対象を transformations から label_positions に変更)
    for i, (tx, ty) in enumerate(label_positions):
        # tx = transform.tx # <-- Error
        # ty = transform.ty # <-- Error

        # ラベルを各グラフの「左上隅」に配置
        # Y座標: ty (左下) + max_height (グラフの高さ) + オフセット
        label_x = tx + label_x_offset
        label_y = ty + max_height + label_y_offset

        c.drawString(label_x, label_y, labels[i])
        print(f"Drawing label '{labels[i]}' at ({label_x:.0f}, {label_y:.0f})")
    # --- 修正箇所 2 (ここまで) ---

    c.save()  # ラベルPDFをメモリに保存

    # メモリ上のラベルPDFを読み込む
    packet.seek(0)
    labels_reader = PdfReader(packet)

    # グラフが結合されたページ(merged_page)の上に、ラベルのページを重ねる
    merged_page.merge_page(labels_reader.pages[0])
    print("Successfully added labels (a)-(h) overlay.")
    # ★★★ ラベル追加セクション (ここまで) ★★★

    # Write the output file
    with open(output_pdf, "wb") as fp:
        output_writer.write(fp)
    print(f"Successfully created merged PDF: {output_pdf}")

except Exception as e:
    print(f"An error occurred during merging: {e}")

finally:
    # (以下、クリーンアップ処理 - 変更なし)
    print("Closing PDF readers...")
    for reader in readers:
        try:
            reader.stream.close()
        except Exception as e:
            print(f"Error closing a reader stream: {e}")

    print("Cleaning up temporary files...")
    for temp_file in temp_pdfs:
        if os.path.exists(temp_file):
            try:
                os.remove(temp_file)
                print(f"Removed temporary file: {temp_file}")
            except Exception as del_err:
                print(f"Error removing temporary file {temp_file}: {del_err}")

print("Process finished.")