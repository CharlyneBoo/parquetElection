import os
import boto3
from botocore.client import Config


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=f"https://{os.environ['CELLAR_ADDON_HOST']}",
        aws_access_key_id=os.environ["CELLAR_ADDON_KEY_ID"],
        aws_secret_access_key=os.environ["CELLAR_ADDON_KEY_SECRET"],
        region_name="default",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            request_checksum_calculation="WHEN_REQUIRED",
            response_checksum_validation="WHEN_REQUIRED",
        ),
    )


def main():
    bucket = os.environ["S3_BUCKET"]
    client = get_s3_client()

    print(f"📦 Listing bucket: {bucket}\n")

    response = client.list_objects_v2(Bucket=bucket)

    if "Contents" not in response:
        print("❌ Bucket vide")
        return

    for obj in response["Contents"]:
        print(f"📄 {obj['Key']}  ({obj['Size']} bytes)")


if __name__ == "__main__":
    main()
