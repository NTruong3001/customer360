import os
from datetime import datetime, timedelta
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame

spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.executor.cores", 8).getOrCreate()
folder_path = 'C:\\Users\\Ber\\Desktop\\de_thay LOng\\Dataset\\log_search'
save_path = 'C:\\Users\\Ber\\Desktop\\de_thay LOng\\lab\\final_project_BigData\\save_lab2'
from pyspark.sql.functions import col

def get_poppular_cagories(dataframe):
    # Lọc các từ khóa không rỗng
    filtered_df = dataframe.filter(col('Keyword').isNotNull())
    
    # Đếm tổng số từ khóa khác nhau (distinct)
    distinct_keyword_count = filtered_df.select('Keyword').distinct().count()
    
    # Đếm số lần xuất hiện của từng Keyword và sắp xếp giảm dần
    keyword_counts = filtered_df.groupBy('Keyword').count().orderBy(col('count').desc())
    
    return distinct_keyword_count, keyword_counts


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
def save_data(result,save_path):
	result.repartition(1).write.csv(save_path,header=True)
	return print("Data Saved Successfully")

def main():
    # Thiết lập các ngày bắt đầu và kết thúc để tạo danh sách ngày cần đọc
    start_date = '20220601'
    end_date = '20220614'

    # Tạo danh sách các ngày cần đọc dữ liệu
    dates = list_date(start_date, end_date)
    
    # Đọc các tệp từ các thư mục theo danh sách ngày đã tạo
    dataframe = read_files(dates, folder_path)
    
    distinct_keyword_count, popular_categories = get_poppular_cagories(dataframe)
    save_path = 'popular_keywords.xlsx'
    save_data(popular_categories, save_path)
# Chạy hàm main
if __name__ == "__main__":
    main()



