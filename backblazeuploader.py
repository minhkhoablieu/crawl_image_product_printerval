from b2sdk.v2 import *


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

        return self.b2_api.get_download_url_for_fileid(uploaded_file.id_)
