import os
import random

import argparse
def get_save_path(source_folder):

    # 获取所有.zst文件的名称
    all_npy_path = [ os.path.join(source_folder, f) for f in os.listdir(source_folder) if f.endswith('.npy')]
    target_file = os.path.join(source_folder, 'all-npy-path.txt')

    with open(target_file,'w') as file:
        for item in all_npy_path:
            # 写入每个元素，并添加换行符
            file.write('-'+ ' ' +item + '\n')



def get_arguments():
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", help="source_folder_path", default='public_data/RedPajamaBook/')
    return ap.parse_args()


if __name__ == "__main__":
    args = get_arguments()

    source_folder = args.src


    get_save_path(source_folder)


