import json
import os
import gzip  # Import the gzip module for handling .json.gz files
from multiprocessing import Pool, Value, Lock, Manager
from typing import Tuple
import argparse

def initialize_counter(lock, start_value):
    """
    Initializes a global counter and lock for use in the worker function.
    """
    global counter
    global counter_lock
    counter = start_value
    counter_lock = lock

def process_file(args: Tuple[str, str, str, Value, Lock]):
    """
    Processes a single .json.gz file by adding a unique ID to each JSON object (as a string) and writes it to a new folder.
    Receives a tuple containing file_path, dest_folder, and a shared counter with a lock.
    """
    file_path, dest_folder, id_name = args
    try:
        updated_json_lines = []
        file_index =0
        if file_path.endswith('.json.gz') or file_path.endswith('.jsonl.gz'):

            # Use gzip.open for reading .json.gz files
            with gzip.open(file_path, 'rt', encoding='utf-8') as file:
                for t, line in enumerate(file):
                    json_obj = json.loads(line)

                    with counter_lock:
                        # Convert the counter value to a string before assigning it as an ID
                        json_obj['id'] = str(id_name) + str(counter.value)
                        json_obj['source'] = str(id_name)
                        counter.value += 1

                    updated_json_lines.append(json_obj)
                    # if t % 250000 == 0:
                    #     save_to_gz(updated_json_lines, file_index, dest_folder)
                    #     file_index += 1
                    #     updated_json_lines = []

        else:
            with open(file_path, 'rt', encoding='utf-8') as file:
                for line in file:
                    json_obj = json.loads(line)

                    with counter_lock:
                        # Convert the counter value to a string before assigning it as an ID
                        json_obj['id'] = str(id_name) + str(counter.value)
                        json_obj['source'] = str(id_name)
                        counter.value += 1

                    updated_json_lines.append(json_obj)

        # Adjust the destination file path to also be .json.gz
        dest_file_name = os.path.basename(file_path)
        dest_file_path = os.path.join(dest_folder, dest_file_name)
        if not dest_file_path.endswith('.gz'):
            dest_file_path = dest_file_path + '.gz'

        # Use gzip.open with 'wt' to write compressed JSON lines
        with gzip.open(dest_file_path, 'wt', encoding='utf-8') as file:
            for obj in updated_json_lines:
                file.write(json.dumps(obj) + '\n')



    except Exception as e:
        print(f"Error processing {file_path}: {e}")

# def process_files_in_folder(src_folder_path: str, dest_folder_path: str, num_processes: int = 32):
#     """
#     Processes all .json.gz files in a source folder and writes them to a destination folder using multiprocessing Pool.
#     """
#     if not os.path.exists(dest_folder_path):
#         os.makedirs(dest_folder_path)  # Create the destination folder if it doesn't exist

#     files = [os.path.join(src_folder_path, f) for f in os.listdir(src_folder_path) if f.endswith('.json.gz')]

#     # Prepare shared counter and lock
#     manager = Manager()
#     start_id = manager.Value('i', 0)
#     lock = manager.Lock()

#     pool_args = [(file_path, dest_folder_path, start_id, lock) for file_path in files]

#     # Initialize Pool with the counter and lock
#     with Pool(processes=num_processes, initializer=initialize_counter, initargs=(lock, start_id)) as pool:
#         pool.map(process_file, pool_args)

def save_to_gz(data_chunk, file_index, dest_folder):
    """将数据块保存到.gz文件"""
    file_name = f"output_{file_index}.json.gz"
    with gzip.open(os.path.join(dest_folder, file_name), 'wt', encoding='utf-8') as file:
        for item in data_chunk:
            file.write(json.dumps(item) + '\n')
    print(f"File {file_name} saved.")

def split_save(data, dest_folder, num_files=59):
    #分块数据
    chunk_size = len(data) // num_files
    data_chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

    # 并行保存文件
    with ThreadPoolExecutor(max_workers=num_files) as executor:
        for i, data_chunk in enumerate(data_chunks):
            executor.submit(save_to_gz, data_chunk, i, dest_folder)



def process_files_in_folder(src_folder_path: str, dest_folder_path: str, id_name: str, num_processes: int = 16):
    """
    Processes all .json.gz files in a source folder and writes them to a destination folder using multiprocessing Pool.
    """
    print('------starting--------')
    if not os.path.exists(dest_folder_path):
        os.makedirs(dest_folder_path)  # Create the destination folder if it doesn't exist

    files = [os.path.join(src_folder_path, f) for f in os.listdir(src_folder_path) if f.endswith('.json.gz') or f.endswith('.jsonl.gz') or f.endswith('.jsonl') or f.endswith('.json') ]
    print('will deal this file', files)

    # Prepare shared counter and lock
    manager = Manager()
    start_id = manager.Value('i', 0)
    lock = manager.Lock()

    # Update here: Ensure pool_args only contains tuples of (file_path, dest_folder)
    pool_args = [(file_path, dest_folder_path, id_name) for file_path in files]

    # Initialize Pool with the counter and lock
    with Pool(processes=num_processes, initializer=initialize_counter, initargs=(lock, start_id)) as pool:
        pool.map(process_file, pool_args)
    print('------finish-----')

def get_arguments():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", help="src_folder_path", default='data/')
    ap.add_argument("--dest", help="dest_folder_path", default='output/')
    ap.add_argument("--id_name", help="add a special name before id number,like proofwiki,arXiv,,", default='baidu')
    ap.add_argument("--process", help="Number of processes", type=int, default=9)
    return ap.parse_args()


if __name__ == "__main__":
    args = get_arguments()
    process_files_in_folder(args.src, args.dest, args.id_name, args.process)
