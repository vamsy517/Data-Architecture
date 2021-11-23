import shutil
import os

def replace_files_and_folders(lst_entities: list, excl_entities: list, src_folder: str, dst_folder: str) -> bool:
    """
    Replaces files and folders from source directory in a destination directory
    :param lst_entities: List, containing files and folders
    :param excl_entities: List, containing files, or folders, which should not be moved
    :param src_folder: Source folder, from which we copy the files and folders
    :param dst_folder: Destination folder, where we move the files and folders
    :return:
    """
    # for each item in the list
    for entity in lst_entities:
        # check if entity is in excluded entities list
        if entity not in excl_entities:
            src = src_folder + '/' + str(entity)
            dst = dst_folder + str(entity)
            if os.path.isdir(src):
                # check if folder already exists
                if os.path.exists(dst):
                    # remove folder
                    shutil.rmtree(dst)
                shutil.copytree(src, dst)
                print(f'Finished copying {entity} folder to {dst}.')
            else:
                shutil.copyfile(src, dst)
                print(f'Finished copying {entity} file to {dst}.')
    return True