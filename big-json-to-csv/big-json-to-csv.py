import json
import threading
import time

# 初始化行计数器
line_count = 0
lock = threading.Lock()

def extract_qq_phone_from_json(json_data):
    qq_number = json_data.get('qq')
    phone_number = json_data.get('phone')

    if qq_number and phone_number:
        return qq_number, phone_number
    return None

def print_line_count():
    global line_count
    while True:
        with lock:
            print(f"Lines processed: {line_count}")
        time.sleep(1)

def process_large_file(file_path, output_file):
    global line_count
    # 启动打印行计数器的线程
    threading.Thread(target=print_line_count, daemon=True).start()
    
    with open(file_path, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        # 写入CSV文件头
        outfile.write('qq_number,phone_number\n')

        for line in infile:
            try:
                json_data = json.loads(line.strip(',\n'))  # 处理JSON对象可能以逗号结束的情况
                result = extract_qq_phone_from_json(json_data['_source'])
                if result:
                    outfile.write(f'{result[0]},{result[1]}\n')
                with lock:
                    line_count += 1
            except (json.JSONDecodeError, KeyError):
                continue  # 跳过无法解析的行或缺少关键字段的行

# 使用示例
file_path = "H:\\Data\\tencent.com1289489189489189489148989314893189478\\tencent.com1289489189489189489148989314893189478"
output_file = "H:\\Data\\extracted_qq_phone.csv"
process_large_file(file_path, output_file)
