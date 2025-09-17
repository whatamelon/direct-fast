import boto3
from ..core.config import settings
from datetime import datetime, timedelta

s3 = boto3.client('s3',
    aws_access_key_id=settings.aws_access_key_id,
    aws_secret_access_key=settings.aws_secret_access_key,
    region_name=settings.aws_region,
    endpoint_url=settings.aws_endpoint_url,
    bucket_name=settings.aws_bucket_name
)

def upload_image(file_content: str, file_name: str, image_name: str):

  content_type = image_name.split('.')[-1]

  s3.put_object(
      Bucket=settings.aws_bucket_name,
      Key=file_name,
      Body=file_content,
      CacheControl='no-cache, no-store, must-revalidate, max-age=0',
      Expires=datetime.now(add=timedelta(seconds=1)),
      ACL='public-read',
      ContentType=content_type
  )

  