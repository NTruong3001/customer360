import os
from datetime import datetime, timedelta
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame

# Tạo Spark session và khai báo các địa chỉ
spark = SparkSession.builder.config("spark.driver.memory", "8g").config("spark.executor.cores", 8).getOrCreate()

def get_distinct_user_keywords(dataframe):
    """
    Lấy danh sách 100 user_id khác nhau và thông tin keyword của họ.
    """
    distinct_user_keywords = dataframe.select("user_id", "keyword") \
                                       .distinct() \
                                       .limit(100)  # Lấy 100 user_id khác nhau

    return distinct_user_keywords

def select_user_keywords(dataframe, user_ids):
    """
    Lấy user_id và keyword cho các user_id đã cho.
    """
    filtered_data = dataframe.filter(col("user_id").isin(user_ids)) \
        .select("user_id", "keyword")  # Chọn các cột user_id và keyword

    return filtered_data

def filter_user_data(df: DataFrame) -> DataFrame:
    """Lọc dữ liệu để lấy 100 user_id xuất hiện nhiều nhất và các từ khóa tìm kiếm tương ứng."""
    
    # Bước 1: Lọc dữ liệu để lấy user_id và keyword, bỏ qua các giá trị null
    filtered_df = df.select("user_id", "keyword").filter(col("user_id").isNotNull() & col("keyword").isNotNull())

    # Bước 2: Đếm số lần xuất hiện của từng user_id
    user_count_df = filtered_df.groupBy("user_id").agg(count("user_id").alias("count"))

    # Bước 3: Lấy 100 user_id xuất hiện nhiều nhất
    top_users_df = user_count_df.orderBy(col("count").desc()).limit(100)

    # Bước 4: Lấy thông tin keyword của các user_id đó trên toàn bộ dữ liệu
    result_df = filtered_df.join(top_users_df.select("user_id"), on="user_id", how="inner")

    return result_df

def join_user_data(df_june, df_july):
    """
    Kết hợp hai DataFrame cho tháng 6 và tháng 7, tạo ra các cột user_id_t6, keyword_t6, categories,
    user_id_t7, keyword_t7, categories và status.
    """
    # Đổi tên cột cho rõ ràng
    df_june_renamed = df_june.withColumnRenamed("user_id", "user_id_t6") \
                               .withColumnRenamed("keyword", "keyword_t6") \
                               .withColumn("categories_t6", lit(""))  # Cột categories để trống

    df_july_renamed = df_july.withColumnRenamed("user_id", "user_id_t7") \
                               .withColumnRenamed("keyword", "keyword_t7") \
                               .withColumn("categories_t7", lit(""))  # Cột categories để trống

    # Kết hợp hai DataFrame dựa trên user_id bằng inner join
    joined_df = df_june_renamed.join(df_july_renamed,
                                      df_june_renamed.user_id_t6 == df_july_renamed.user_id_t7,
                                      "inner")  # Sử dụng inner join để chỉ giữ lại các user_id có mặt trong cả hai DataFrame

    # Xác định giá trị cho cột status
    joined_df = joined_df.withColumn("status",
                                      when(col("keyword_t6") == col("keyword_t7"), "unchange")
                                      .otherwise(""))  # Để trống nếu khác nhau

    # Chọn cột đầu ra cần thiết
    final_df = joined_df.select("user_id_t6", "keyword_t6", "categories_t6",
                                 "user_id_t7", "keyword_t7", "categories_t7", "status")

    return final_df

def list_date(start_date, end_date):
    """Tạo danh sách các ngày từ start_date đến end_date."""
    list_d = []
    a = datetime.strptime(start_date, '%Y%m%d')

    while a <= datetime.strptime(end_date, '%Y%m%d'):  # Bao gồm cả end_date
        list_d.append(a.strftime('%Y%m%d'))  # Chuyển đổi về định dạng chuỗi 'yyyymmdd'
        a += timedelta(days=1)  # Cộng thêm 1 ngày

    return list_d

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

  

