import os
import random
import shutil
import zstandard as zstd
import gzip
import argparse
def decompress_zst(source_file, unzip_folder, target_folder):
    if source_file.endswith('.zst'):
        # 创建Zstandard解压缩器
        dctx = zstd.ZstdDecompressor()
        with open(os.path.join(target_folder, source_file), 'rb') as compressed:
            with open(os.path.join(unzip_folder,source_file[:-4]), 'wb') as decompressed:
                dctx.copy_stream(compressed, decompressed)
    elif source_file.endswith('.gz'):
        with gzip.open(os.path.join(target_folder, source_file), 'rb') as compressed:
            with open(os.path.join(unzip_folder,source_file[:-3]), 'wb') as destination:
                shutil.copyfileobj(compressed, destination)
    else:
        raise ValueError("Unsupported file extension")

def random_copy_and_decompress(source_folder, target_folder, max_files, max_size):
    copied_files = 0
    total_size = 0

    # 获取所有.zst文件的名称
    zst_files = [ f for f in os.listdir(source_folder) if f.endswith('.zst') or f.endswith('.gz')]

    for source_file in zst_files:
# ##TODO:修改 当数据量超过使，陷入死循环
#     while copied_files < max_files and total_size < max_size:
#
#         # 从列表中随机选择一个.zst文件
#         source_file = random.choice(zst_files)
#
#         #复制该文件到目标文件夹
#         shutil.copy(os.path.join(source_folder, source_file), target_folder)


        #目标文件夹下创建unzip文件夹
        unzip_folder = os.path.join(target_folder, 'unzip')
        if not os.path.exists(unzip_folder):
            os.makedirs(unzip_folder)

        # 解压文件
        decompress_zst(source_file, unzip_folder, target_folder)

        # 更新统计数据
        copied_files += 1
        if source_file.endswith('.zst'):
            total_size += os.path.getsize(os.path.join(unzip_folder,source_file[:-4]))
        elif source_file.endswith('.gz'):
            total_size += os.path.getsize(os.path.join(unzip_folder, source_file[:-3]))
        else:
            ValueError('Unsupported file extension')

        print(f"Copied and decompressed: {source_file}")

        # # 检查是否达到了文件数量或大小的限制
        # if copied_files >= max_files or total_size >= max_size:
        #     print("Reached the limit.")
        #     print('number of documents is ',copied_files)
        #     print('size of choose document after decompressing is,', total_size)
        #     break





def get_arguments():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", help="source_folder_path", default='public_data/RedPajamaBook/')
    ap.add_argument("--tar", help="target_folder_path", default='100G_data/RedPajamaBook/')
    ap.add_argument("--max_num", help="how many documents you need", type=int, default=10000)
    ap.add_argument("--max_size", help="how much size doucments you need,eg: 1M ", default='1M')
    return ap.parse_args()


def parse_size(max_size):
    if max_size[-1]=='G':
        return float(max_size[:-1])*1024*1024*1024
    elif max_size[-1]=='M':
        return float(max_size[:-1])*1024*1024
    elif max_size[-1]=='K':
        return float(max_size[:-1])*1024
    elif max_size[-1]=='T':
        return float(max_size[:-1])*1024*1024*1024*1024
    else:
        ValueError('pleas end with G , K, M, T')

if __name__ == "__main__":
    args = get_arguments()

    source_folder = args.src
    target_folder = args.tar
    max_files = args.max_num  # 举例：最多复制解压文件数量
    max_size = args.max_size  # 举例：目标文件夹大小上限（例如，这里是1GB）

    # 确保目标文件夹存在
    if not os.path.exists(target_folder):
        os.makedirs(target_folder, exist_ok=True)
    max_size=parse_size(max_size)

    random_copy_and_decompress(source_folder, target_folder, max_files, max_size)


