import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
import os
import gc
import psutil
from dask.distributed import Client

def process_large_csv(input_file, output_file, chunksize=10**8, max_memory_usage=30*1024**3):
    # 初始化进度条和 Dask Client
    pbar = ProgressBar()
    pbar.register()

    client = Client(n_workers=10, threads_per_worker=4, memory_limit='4GB')

    temp_files = []
    concatenated_chunk = pd.DataFrame()

    # 读取大文件并进行分块处理
    reader = pd.read_csv(input_file, chunksize=chunksize, names=['qq_number', 'phone_number'], low_memory=False)

    for i, chunk in enumerate(reader):
        # 对每个分块处理QQ号并排序
        chunk['padded_qq_number'] = chunk['qq_number'].apply(lambda x: str(x).zfill(15))
        sorted_chunk = chunk.sort_values(by='padded_qq_number')
        sorted_chunk.drop(columns=['padded_qq_number'], inplace=True)

        # 合并多个块后再写入临时文件
        if i % 10 == 0 and not concatenated_chunk.empty:  # 每处理10个块写入一次
            temp_file = f'temp_sorted_chunk_{i // 10}.csv'
            concatenated_chunk.to_csv(temp_file, index=False, header=False, mode='a')
            temp_files.append(temp_file)
            concatenated_chunk = pd.DataFrame()  # 清空缓存的DataFrame
            gc.collect()
        else:
            # 使用 pd.concat 合并到现有 DataFrame 中
            concatenated_chunk = pd.concat([concatenated_chunk, sorted_chunk])

        # 监控内存使用情况
        process = psutil.Process(os.getpid())
        memory_usage = process.memory_info().rss

        # 如果内存使用超过30GB或已用内存超过预设阈值，写入并清空当前数据块
        if memory_usage > max_memory_usage * 0.9:
            print(f"Memory usage high: {memory_usage / (1024 ** 3):.2f} GB. Writing to temp file and clearing memory.")
            temp_file = f'temp_sorted_chunk_mem_cleanup_{i}.csv'
            concatenated_chunk.to_csv(temp_file, index=False, header=False, mode='a')
            temp_files.append(temp_file)
            concatenated_chunk = pd.DataFrame()  # 清空缓存的DataFrame
            gc.collect()

    # 处理最后剩余的数据块
    if not concatenated_chunk.empty:
        temp_file = f'temp_sorted_chunk_final.csv'
        concatenated_chunk.to_csv(temp_file, index=False, header=False, mode='a')
        temp_files.append(temp_file)
    
    # 使用dask合并所有分块排序结果并保存最终输出
    ddf = dd.read_csv(temp_files, header=None, names=['qq_number', 'phone_number'])
    ddf = ddf.repartition(npartitions=500)  # 增加分区数量，提高并行度
    ddf = ddf.map_partitions(lambda df: df.sort_values(by='qq_number'))

    # 将最终结果写入文件
    ddf.to_csv(output_file, index=False, header=False, single_file=True)

    # 删除临时文件
    for temp_file in temp_files:
        os.remove(temp_file)

if __name__ == "__main__":
    input_file = r"H:\Data\extracted_qq_phone.csv"
    output_file = r"H:\Data\sorted_qq_phone.csv"
    process_large_csv(input_file, output_file)
