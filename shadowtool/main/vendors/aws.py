from typing import Optional, Any

import boto3
import base64
import json

from dataclasses import dataclass
from botocore.exceptions import ClientError

from shadowtool.interfaces.base_hook import BaseHook
from shadowtool.main.general.logging_utils import LoggingMixin
from shadowtool.main.general.shell_utils import run_commands


@dataclass
class BaseAWSHook(LoggingMixin, BaseHook):

    client: Any = None
    session: boto3.session.Session = boto3.session.Session()


@dataclass
class SecretManagerHook(BaseAWSHook):

    region_name: Optional[str] = 'ap-southeast-1'

    def __post_init__(self):
        self.client = self.session.client(service_name="secretsmanager", region_name=self.region_name)

    def get_secret(self, secret_name: str) -> str:
        """
        this function will attempt to get secrets from AWS secret manager based on
        secret name

        It inherits the credential finding strategy as the boto3 library
        """

        try:
            get_secret_value_response = self.client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "DecryptionFailureException":
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InternalServiceErrorException":
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidParameterException":
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidRequestException":
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "ResourceNotFoundException":
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            else:
                raise e

        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
        else:
            secret = base64.b64decode(get_secret_value_response["SecretBinary"])

        try:
            secret_kv = json.loads(secret)
            return secret_kv
        except json.decoder.JSONDecodeError as e:
            # Unable to load the secret string as JSON, inferring it as plain text
            return secret


@dataclass
class S3Hook(BaseAWSHook):
    """
    handles the interaction between machine and S3 buckets
    """
    bucket_name: str = None

    def __post_init__(self):
        assert self.bucket_name, "You will need to specify the bucket name to instantiate the S3 Hook. "
        self.log.debug(
            f"Instantiating S3 hooks for bucket {self.bucket_name}, please "
            f"check you have valid AWS credentials access in the "
            f"`~/.aws/credentials` file."
        )
        self.client = boto3.resource("s3")
        self.bucket_obj = self.client.Bucket(self.bucket_name)

    def list_files_in_bucket(self, prefix: str = "") -> list:
        return [el.key for el in self.bucket_obj.objects.filter(Prefix=prefix)]

    def download_file(self, target_key: str, target_file_path: str) -> None:
        """
        download a file from a bucket

        :param target_key: the complete key to the bucket object
        :param target_file_path: the local file path, preferably absolute path, or relative
                to the current working directory
        :return:
        """
        with open(target_file_path, "wb") as data:
            try:
                self.client.meta.client.download_fileobj(self.bucket_name, target_key, data)
            except self.client.meta.client.exceptions.ClientError:
                raise Exception(f"There is no data stored in s3")

    def bulk_download_files(
        self, s3_prefix: str, local_path: str, quiet: bool = True, delete: bool = True
    ):

        option_str = ""

        if quiet:
            option_str += " --quiet "

        if delete:
            option_str += " --delete "

        run_commands(
            f"aws s3 sync s3://{self.bucket_name}/{s3_prefix} {local_path} {option_str}"
        )

    def upload_file(
            self, target_key: str, target_file_path: str = None, target_binary: bytes = None
    ) -> None:

        assert (
                target_file_path or target_binary
        ), "Either path or binary object need to be past as input. "

        if target_binary:
            data = target_binary
        else:
            data = open(target_file_path, "rb")

        try:
            self.client.meta.client.upload_fileobj(data, self.bucket_name, target_key)
        except self.client.meta.client.exceptions.ClientError:
            raise Exception(f"Fail to upload data in s3")

        if target_file_path:
            data.close()

    def bulk_upload_files(self, s3_prefix: str, local_path: str, quiet: bool = True):
        option_str = ""

        if quiet:
            option_str += " --quiet "

        run_commands(
            f"aws s3 sync {local_path} s3://{self.bucket_name}/{s3_prefix} {option_str}"
        )

    def delete_folder(self, s3_prefix: str):
        run_commands(f"aws s3 rm s3://{self.bucket_name}/{s3_prefix} --recursive")

    def delete_file(self, target_key: str = None, prefix: str = None) -> None:
        """ delete file by exact s3 key or s3 prefix """
        if target_key and not prefix:
            self.client.Object(self.bucket_name, target_key).delete()
        elif prefix and not target_key:
            s3_files = self.list_files_in_bucket(prefix=prefix)

            self.log.warning(
                f"{len(s3_files)} files located in s3, waiting to be deleted. "
            )
            for file in s3_files:
                self.client.Object(self.bucket_name, file).delete()
        else:
            raise Exception(
                f"You need to provide with either `target_key` or `prefix`, but not both. "
            )

    def create_file(self, target_key: str, data: bytes):
        """
        create a file directory in the s3 destination
        :param target_key:
        :param data:
        :return:
        """
        obj = self.client.Object(self.bucket_name, target_key)
        obj.put(Body=data)

    def update_metadata(self, metadata: dict, s3_key: str) -> None:
        """
        Update the metadata of specified S3 object
        """
        obj = self.client.Object(self.bucket_name, s3_key)
        obj.metadata.update(metadata)
        # Replace is to indicate it's replacing the original metadata
        obj.copy_from(
            CopySource={"Bucket": self.bucket_name, "Key": s3_key},
            Metadata=obj.metadata,
            MetadataDirective="REPLACE",
        )
        self.log.debug(f"Finished updating {s3_key}")

    def get_metadata(self, s3_key: str) -> dict:
        """
        Get the metadata of a specified S3 object as a dictionary
        """
        obj = self.client.Object(self.bucket_name, s3_key)
        obj.load()
        return obj.metadata


class ECSHook:
    def __init__(self, cluster: str):
        self.client = boto3.client("ecs")
        self.cluster = cluster

    def stop_tasks_by_family(self, family_name: str):
        """
        stop RUNNING ECS task(s) based on their Task definition name

        This omits quite a lot customization
        """
        response = self.client.list_tasks(
            cluster=self.cluster, family=family_name, desiredStatus="RUNNING"
        )

        assert (
            response["ResponseMetadata"]["HTTPStatusCode"] == 200
        ), f"Non 200 status code while listing tasks"

        tasks = response["taskArns"]

        counter = 0
        if len(tasks):
            for task in tasks:
                response = self.client.stop_task(cluster=self.cluster, task=task)

                assert (
                    response["ResponseMetadata"]["HTTPStatusCode"] == 200
                ), f"Non 200 status code while stopping tasks"
                counter += 1
