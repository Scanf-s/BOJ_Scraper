import asyncio
import dotenv
import logging
from scraper import Scraper
from dto.response import Response
import aioboto3
import os

MAX_SEMAPHORE = 10
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class DynamoDBConnector:
    def __init__(self, dynamodb=None, user_table=None, scrap_data_table=None):
        self.dynamodb = dynamodb
        self.user_table = user_table
        self.scrap_data_table = scrap_data_table

async def main():
    session = aioboto3.Session()
    async with session.resource(
            service_name="dynamodb",
            region_name=os.getenv("AWS_REGION_NAME")
    ) as dynamodb_resource:
        user_table = await dynamodb_resource.Table(os.getenv("USER_TABLE_NAME"))
        scrap_data_table = await dynamodb_resource.Table(os.getenv("SCRAP_DATA_TABLE_NAME"))

        db_connector = DynamoDBConnector(
            dynamodb=dynamodb_resource,
            user_table=user_table,
            scrap_data_table=scrap_data_table
        )
        scraper = Scraper(logger=logger, database=db_connector, semaphore=MAX_SEMAPHORE)

        results = await scraper.scrap()
        results = results if results else []
        logger.info(f"Scrap completed with results : {results}")

        await scraper.close_session()
        return Response(
            status_code=200,
            body=f"Successfully processed {len(results)} submissions"
        ).to_dict()

def lambda_handler(event, context):
    logger.info("Start lambda function")
    return asyncio.run(main())


if __name__ == "__main__":
    dotenv.load_dotenv(".env")
    response = lambda_handler(None, None)
    print(response)
