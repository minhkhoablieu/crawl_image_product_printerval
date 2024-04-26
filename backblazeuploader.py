from b2sdk.v2 import *


#
# info = InMemoryAccountInfo()
# b2bl_api = B2Api(info)
# application_key_id = '409fc80853a7'
# application_key = '0052712824187f52d94db27e8177d600cf77a8b0b6'
# b2_api.authorize_account("production", application_key_id, application_key)
# b2_api.list_buckets()
# for b in b2_api.list_buckets():
#     print('%s  %-10s  %s' % (b.id_, b.type_, b.name))

class BackblazeUploader:
    def __init__(self, key_id, application_key, bucket_name):
        info = InMemoryAccountInfo()
        self.b2_api = B2Api(info)
        self.b2_api.authorize_account("production", key_id, application_key)
        self.bucket = self.b2_api.get_bucket_by_name(bucket_name)

    def upload_file(self, file_path, file_name):
        metadata = {"key": "value"}

        uploaded_file = self.bucket.upload_local_file(
            local_file=file_path,
            file_name=file_name,
            file_info=metadata,
        )

        print(self.b2_api.get_download_url_for_fileid(uploaded_file.id_))
