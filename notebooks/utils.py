#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

import tarfile
import os
import urllib.parse
import sagemaker
import sagemaker.jumpstart.model
import tempfile


def pack_model_data_source(path: str, filename: str = "model.tar.gz"):
    """

    :param filename:
    :param path:
    :return:
    """
    # recursively list the files under the path

    ls_files = []
    for root, dirs, files in os.walk(path):
        for file in files:
            f = os.path.join(root, file)
            ls_files.append(f)
            print(f"Found file {f}")

    # create a tar.gz file and pack the files into it
    with tarfile.open(filename, 'w:gz') as tar:
        for file in ls_files:
            # set arcname to remove the path from the file name
            arcname = os.path.relpath(file, path)
            tar.add(file, arcname=arcname)
            print(f"Added {file} to the tar.gz file")


def parse_uri(uri):
    parsed = urllib.parse.urlparse(uri)
    bucket = parsed.hostname
    prefix = parsed.path[1:]
    return bucket, prefix


def pack(model_data_source: str, uri: str):
    """
    Download model source data from an S3 URI and pack it into a model.tar.gz, cleaning up the downloaded files
    :param model_data_source:
    :param uri:
    :return:
    """

    sagemaker_session = sagemaker.Session()

    # create a temporary directory
    with tempfile.TemporaryDirectory() as d:
        path_for_downloaded_files = f"{d}/model"
        path_for_model_data = f"{d}/model.tar.gz"
        bucket, prefix = parse_uri(model_data_source)
        # download the files
        files = sagemaker_session.download_data(path=path_for_downloaded_files, bucket=bucket, key_prefix=prefix)
        pack_model_data_source(path_for_downloaded_files, filename=path_for_model_data)
        # upload the model data to S3
        bucket, prefix = parse_uri(uri)
        model_data = sagemaker_session.upload_data(path_for_model_data, bucket=bucket, key_prefix=prefix)
    return model_data


def register_jumpstart_model(model_package_group_name, model_id, model_version):
    """

    :param model_package_group_name:
    :param model_id:
    :param model_version:
    :return: model_package_arn
    """
    sagemaker_session = sagemaker.Session()
    boto_session = sagemaker_session.boto_session
    default_bucket = sagemaker_session.default_bucket()

    model = sagemaker.jumpstart.model.JumpStartModel(model_id=model_id, model_version=model_version)

    image_uri = model.image_uri

    # prepack the model
    prefix = f"s3://{default_bucket}/{model_id}/{model_version}"
    model_data = pack(model.model_data["S3DataSource"]["S3Uri"], prefix)

    # retrieve hosting information
    specs = sagemaker.jumpstart.model.JumpStartModelsAccessor.get_model_specs(boto_session.region_name, model_id,
                                                                              model_version)

    supported_response_types = specs.predictor_specs.supported_accept_types
    supported_content_types = specs.predictor_specs.supported_content_types
    instances = specs.supported_inference_instance_types

    # register the model
    response = sagemaker_session.sagemaker_client.create_model_package(
        ModelPackageGroupName=model_package_group_name,
        InferenceSpecification={
            "Containers": [
                {
                    "Image": image_uri,
                    "ModelDataUrl": model_data
                }
            ],

            "SupportedContentTypes": supported_content_types,
            "SupportedResponseMIMETypes": supported_response_types,
            "SupportedRealtimeInferenceInstanceTypes": instances,

        }
    )
    model_package_arn = response["ModelPackageArn"]
    return model_package_arn
