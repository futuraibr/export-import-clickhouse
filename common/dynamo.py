import boto3

from common import config


def get_dynamodb_resource():
    """Abre o resource do DynamoDB na região/profile configurados (profile de dev, se setado)."""
    session = boto3.Session(
        profile_name=config.AWS_PROFILE or None,
        region_name=config.AWS_REGION,
    )
    return session.resource("dynamodb")
