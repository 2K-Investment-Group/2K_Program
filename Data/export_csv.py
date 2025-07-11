import sqlite3
import csv

def export_db_table_to_csv(db_name, table_name, csv_filename):
    conn = None
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        # 컬럼 이름(스키마) 가져오기
        cursor.execute(f"PRAGMA table_info({table_name});")
        column_info = cursor.fetchall()
        column_names = [col[1] for col in column_info] # col[1]은 컬럼 이름

        # 데이터 가져오기
        cursor.execute(f"SELECT * FROM {table_name};")
        rows = cursor.fetchall()

        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)

            # 헤더 작성
            csv_writer.writerow(column_names)

            # 데이터 행 작성
            csv_writer.writerows(rows)

        print(f"테이블 '{table_name}'의 데이터가 '{csv_filename}'으로 성공적으로 내보내졌습니다.")

    except sqlite3.Error as e:
        print(f"데이터베이스 오류: {e}")
    except IOError as e:
        print(f"파일 오류: {e}")
    finally:
        if conn:
            conn.close()

# 사용 예시 (실제 데이터베이스 및 테이블 이름으로 대체하세요)
# export_db_table_to_csv('당신의_데이터베이스.db', '당신의_테이블', '당신의_출력파일.csv')