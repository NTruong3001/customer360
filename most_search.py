import os
from datetime import datetime, timedelta
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame

# Tạo Spark session và khai báo các địa chỉ
spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.executor.cores", 8).getOrCreate()

def read_files(list_date, directory_path):
    """Đọc các tệp Parquet từ các thư mục và lưu DataFrame đã lọc."""
    all_filtered_dfs = []  # Danh sách để lưu các DataFrame đã lọc

    for folder in list_date:
        parquet_file_path = os.path.join(directory_path, folder)
        print(f"Đang đọc tệp Parquet từ thư mục: {parquet_file_path}")
        try:
            # Đọc tệp Parquet
            df = spark.read.parquet(parquet_file_path)
            all_filtered_dfs.append(df)  # Thêm DataFrame vào danh sách
        except Exception as e:
            print(f"Không thể đọc tệp trong thư mục {parquet_file_path}: {str(e)}")

    # Kết hợp tất cả DataFrame đã đọc
    if all_filtered_dfs:
        combined_df = all_filtered_dfs[0]  # Bắt đầu từ DataFrame đầu tiên
        for df in all_filtered_dfs[1:]:  # Duyệt qua các DataFrame còn lại
            combined_df = combined_df.union(df)  # Kết hợp với DataFrame hiện tại
        return combined_df
    else:
        return None
    
def list_date(start_date, end_date):
    """Tạo danh sách các ngày từ start_date đến end_date."""
    list_d = []
    a = datetime.strptime(start_date, '%Y%m%d')

    while a <= datetime.strptime(end_date, '%Y%m%d'):  # Bao gồm cả end_date
        list_d.append(a.strftime('%Y%m%d'))  # Chuyển đổi về định dạng chuỗi 'yyyymmdd'
        a += timedelta(days=1)  # Cộng thêm 1 ngày

    return list_d

def main():
    folder_path = 'C:\\Users\\Ber\\Desktop\\de_thay LOng\\Dataset\\log_search'
    save_path = 'C:\\Users\\Ber\\Desktop\\de_thay LOng\\lab\\final_project_BigData\\save_lab2'
    
    # Tạo danh sách các ngày cho tháng 7 và tháng 6
    folders_thang7 = list_date('20220701', '20220714')
    folders_thang6 = list_date('20220601', '20220614')

    # Gọi hàm để đọc các tệp Parquet cho tháng 7
    combined_filtered_df_thang7 = read_files(folders_thang7, folder_path)
    if combined_filtered_df_thang7 is None:
        print("Không có dữ liệu cho tháng 7.")
        return

    # Gọi hàm để đọc các tệp Parquet cho tháng 6
    combined_filtered_df_thang6 = read_files(folders_thang6, folder_path)
    if combined_filtered_df_thang6 is None:
        print("Không có dữ liệu cho tháng 6.")
        return

    # Lấy danh sách 100 user_id khác nhau và thông tin keyword của họ cho tháng 6 và tháng 7
    distinct_user_data_thang6 = get_distinct_user_keywords(combined_filtered_df_thang6)
    distinct_user_data_thang7 = get_distinct_user_keywords(combined_filtered_df_thang7)

    # Lấy danh sách user_id từ tháng 6 để lọc dữ liệu tháng 7
    user_ids_thang6 = distinct_user_data_thang6.select("user_id").rdd.flatMap(lambda x: x).collect()

    # Lấy thông tin từ khóa cho các user_id của tháng 6 từ dữ liệu tháng 7
    user_keywords_thang7 = select_user_keywords(combined_filtered_df_thang7, user_ids_thang6)

    # Kết hợp dữ liệu của tháng 6 và tháng 7
    final_result = join_user_data(distinct_user_data_thang6, user_keywords_thang7)
    final_result.show(200)

if __name__ == "__main__":
    main()
