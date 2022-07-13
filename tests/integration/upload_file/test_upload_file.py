import base64
import hashlib
import os

import yaml

from tests.integration.base import DBTIntegrationTest, use_profile


class TestUploadFile(DBTIntegrationTest):
    local_file_path = "./files/run/manifest.json"

    @property
    def schema(self):
        return "upload_file"

    @property
    def models(self):
        return "models"

    def upload_file(self, overwrite, expect_pass=True):
        upload_args = yaml.safe_dump(
            {
                "local_file_path": self.local_file_path,
                "dbfs_file_path": f"{self.test_dbfs_root}/manifest.json",
                "overwrite": overwrite,
                "contents": None,
                "headers": None,
            }
        )

        result = self.run_dbt(
            ["run-operation", "upload_file", "--args", upload_args], expect_pass=expect_pass
        )
        return result

    def test_upload_file_no_overwrite(self):
        try:
            dbt_run_result = self.upload_file(overwrite=True)
            self.assertTrue(dbt_run_result.success)
            dbt_run_result = self.upload_file(overwrite=False, expect_pass=False)
            self.assertFalse(dbt_run_result.success)
        finally:
            self.dbapi_client.DbfsService.delete(f"{self.test_dbfs_root}/manifest.json")

    def check_uploaded_file_size(self):
        local_file_size = os.path.getsize(self.local_file_path)
        dbfs_file_status = self.dbapi_client.DbfsService.get_status(
            f"{self.test_dbfs_root}/manifest.json"
        )

        self.assertEqual(dbfs_file_status["file_size"], local_file_size)

    def check_uploaded_file_contents(self):
        read_bytes: int = 0
        file_contents_b64: str = ""
        while read_bytes < os.path.getsize(self.local_file_path):
            chunk = self.dbapi_client.DbfsService.read(
                f"{self.test_dbfs_root}/manifest.json", read_bytes, 1000000
            )
            file_contents_b64 += chunk["data"]
            read_bytes += chunk["bytes_read"]

        file_contents_decoded = base64.b64decode(file_contents_b64)

        with open(self.local_file_path, "rb") as f:
            local_file_contents_hashed = hashlib.sha256(f.read()).hexdigest()
            dbfs_file_contents_hashed = hashlib.sha256(file_contents_decoded).hexdigest()

        self.assertEqual(local_file_contents_hashed, dbfs_file_contents_hashed)

    def test_upload_file(self):
        try:
            dbt_run_result = self.upload_file(overwrite=True)
            self.assertTrue(dbt_run_result.success)

            self.check_uploaded_file_size()
            self.check_uploaded_file_contents()
        finally:
            self.dbapi_client.DbfsService.delete(f"{self.test_dbfs_root}/manifest.json")

    @use_profile("databricks_sql_endpoint")
    def test_upload_file_databricks_sql_endpoint(self):
        self.test_upload_file()

    @use_profile("databricks_cluster")
    def test_upload_file_databricks_cluster(self):
        self.test_upload_file()

    @use_profile("databricks_uc_cluster")
    def test_upload_file_databricks_uc_cluster(self):
        self.test_upload_file()

    @use_profile("databricks_uc_sql_endpoint")
    def test_upload_file_databricks_uc_sql_endpoint(self):
        self.test_upload_file()

    @use_profile("databricks_sql_endpoint")
    def test_upload_file_no_overwrite_databricks_sql_endpoint(self):
        self.test_upload_file_no_overwrite()

    @use_profile("databricks_cluster")
    def test_upload_file_no_overwrite_databricks_cluster(self):
        self.test_upload_file_no_overwrite()

    @use_profile("databricks_uc_cluster")
    def test_upload_file_no_overwrite_databricks_uc_cluster(self):
        self.test_upload_file_no_overwrite()

    @use_profile("databricks_uc_sql_endpoint")
    def test_upload_file_no_overwrite_databricks_uc_sql_endpoint(self):
        self.test_upload_file_no_overwrite()
