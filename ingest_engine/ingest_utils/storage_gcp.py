from google.cloud import storage


def download_file_from_storage(project_id: str, filename: str, bucket: str, download_dir: str) -> bool:
    """
    Download file from BIG Bucket to download_dir
    :param project_id: Project ID
    :param filename: name of the file in the bucket
    :param bucket: bucket name
    :param download_dir: dir to download the file
    :return: True when file is downloaded
    """
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket)
    blob = bucket.get_blob(filename)
    blob.download_to_filename(download_dir + filename)
    return True


def upload_file_to_storage(project_id: str, bucket: str, source_file: str, target_file: str) -> bool:
    """
    Upload file to BIG Bucket
    :param project_id: Project ID
    :param bucket: bucket name
    :param source_file: path to the source file + file name
    :param target_file: name of the file in the bucket
    :return: True when file is uploaded
    """
    # upload the files to bucket
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket)
    blob = bucket.blob(target_file)
    with open(source_file, 'rb') as my_file:
        blob.upload_from_file(my_file)
    return True
