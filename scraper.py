import asyncio
import time
from typing import Any, Dict, List

from boto3.dynamodb.conditions import Key, Attr
import aiohttp
import logging
import os

from bs4 import BeautifulSoup
from datetime import datetime


class Scraper:

    def __init__(
        self, logger: logging.Logger = None, database: Any = None, semaphore: int = 5
    ):
        self.logger = logger
        self.database = database
        self.semaphore = asyncio.Semaphore(semaphore)
        self.http_session = aiohttp.ClientSession()

    async def close_session(self):
        await self.http_session.close()

    async def scrap(self):
        try:
            users: List[str] = await self.fetch_user_list(
                table=self.database.user_table
            )
            self.logger.info(f"Fetched users : {users}")

            base_url = os.getenv("SCRAP_TARGET_BASE_URL")
            if not base_url:
                raise ValueError(
                    "SCRAP_TARGET_BASE_URL environment variable is not set"
                )
            submissions: List = []

            tasks = []
            for user in users:
                task = asyncio.create_task(self.scrap_task(user, base_url))
                tasks.append(task)

            # 모든 태스크 동시 실행
            result = await asyncio.gather(*tasks)
            for data in result:
                submissions.extend(data)

            # DB에 저장
            await self.save_db(db=self.database, submissions=submissions)
            return submissions
        except Exception as e:
            self.logger.error(f"An error occurred: {str(e)}")

    async def fetch_user_list(self, table) -> List[str]:
        try:
            self.logger.info("Fetch user list from DynamoDB")
            response = await table.scan(
                ProjectionExpression="boj_name"
            )
            if "Items" not in response:
                self.logger.info("There are no users in DynamoDB")
                return []

            users = [item["boj_name"] for item in response["Items"]]
            self.logger.info(f"Found {len(users)} users")
            return users
        except Exception as e:
            self.logger.error(f"An error occurred: {str(e)}")
            return []

    async def is_solved(self, table, username: str, problem_id: str) -> bool:
        """
        parse_html에서 추출한 problem_id를 사용해서,
        해당 사용자가 이전에 문제를 풀어서 디비에 해당 문제 데이터가 존재하는 경우,
        True를 리턴해서 중복된 데이터가 DB에 써지지 않도록 방지한다.
        :param table:
        :param username:
        :param problem_id:
        :return:
        """
        try:
            self.logger.info("Fetch user list from DynamoDB")
            response = await table.query(
                KeyConditionExpression=(
                    Key("username").eq(f"USER#{username}") &
                    Key("problem_id").eq(problem_id)
                ),
                ProjectionExpression="username, problem_id",
            )

            return True if response.get("Count", 0) > 0 else False
        except Exception as e:
            self.logger.error(f"An error occurred: {str(e)}")
            return False

    async def parse_html(self, html: str, username: str, base_url: str) -> List:
        container: List = []
        seen_keys: set = set() # DynamoDB 키 중복 방지 집합
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", id="status-table")

        if not table:  # 테이블이 없는 경우 처리
            self.logger.info(f"사용자 {username}의 데이터를 찾을 수 없습니다.")
            return container

        for row in table.find_all("tr"):
            submitted_result = row.find(
                "span", attrs={"class": "result-text result-ac"}
            )
            if submitted_result is not None:
                submitted_time = row.find("a", class_="real-time-update")["title"]
                submitted_datetime = datetime.strptime(
                    submitted_time, "%Y-%m-%d %H:%M:%S"
                )
                submitted_timestamp = submitted_datetime.timestamp()
                if (
                    submitted_time is not None
                    and time.time() - submitted_timestamp < 60 * 60 * 24
                ):
                    self.logger.debug(f"사용자: {username}")
                    self.logger.debug(f"제출 번호 : {row.find('td').text}")
                    self.logger.debug(
                        f"문제 번호 : {row.find('a', class_='problem_title').text}"
                    )
                    self.logger.debug(
                        f"문제 링크 : {base_url + row.find('a', attrs={'class': 'problem_title'}).get('href')}"
                    )
                    self.logger.debug(f"제출 시간: {submitted_time}")
                    self.logger.debug(
                        f"메모리 : {row.find('td', class_='memory').text}KB"
                    )
                    self.logger.debug(f"시간 : {row.find('td', class_='time').text}ms")

                    problem_id = row.find("a", class_="problem_title").text.strip()

                    # 수집한 데이터가 중복된 게 있는지 확인 -> 있다면 무시
                    key_tuple = (username, problem_id)
                    if key_tuple in seen_keys:
                        continue
                    seen_keys.add(key_tuple)

                    # DB에도 이미 존재하는 데이터인지 확인 -> 있다면 무시
                    if await self.is_solved(
                        table=self.database.scrap_data_table,
                        username=username,
                        problem_id=problem_id,
                    ):
                        continue

                    scrap_data = {
                        "username": username,  # 파티션 키
                        "problem_id": row.find("a", class_="problem_title").text,  # 정렬 키
                        "submission_id": row.find("td").text,  # 제출 번호
                        "problem_url": base_url
                        + row.find("a", attrs={"class": "problem_title"}).get(
                            "href"
                        ),  # 문제 링크
                        "submitted_time": submitted_time,  # 제출 시간
                        "memory_used": row.find(
                            "td", class_="memory"
                        ).text,  # 사용 메모리
                        "time_spent": row.find("td", class_="time").text,  # 실행 시간
                    }
                    container.append(scrap_data)
        return container

    async def scrap_task(self, username: str, base_url: str) -> List[Dict[str, Any]]:
        """
        Username을 사용해서 ACMICPC를 스크래핑하는 작업을 수행하는 비동기 함수
        :param username: 사용자 이름
        :param base_url: 스크랩 타겟 Base URL
        :return: 스크랩한 데이터 리스트
        """
        async with self.semaphore:
            request_url: str = base_url + f"/status?user_id={username}"
            request_user_agent: str = (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
            )
            data: List = []

            try:
                self.logger.info("Fetching ACMICPC data")
                async with self.http_session.get(
                    url=request_url, headers={"User-Agent": request_user_agent}
                ) as resp:
                    if resp.status != 200:
                        self.logger.error(f"request error {resp.status}")
                        return data

                    html: str = await resp.text()
                    data = await self.parse_html(
                        html=html, username=username, base_url=base_url
                    )

                return data
            except Exception as e:
                self.logger.error(f"Exception : {str(e)}")

    async def save_db(self, db, submissions: List[Dict[str, Any]]) -> None:
        table = db.scrap_data_table
        async with table.batch_writer() as writer:
            for submission in submissions:
                await writer.put_item(Item=submission)

        self.logger.info(f"Saved {len(submissions)} submissions into DynamoDB")
