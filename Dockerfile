# AWS가 제공하는 Lambda Python 베이스 이미지
FROM public.ecr.aws/lambda/python:3.12

# 필요 라이브러리 설치
COPY requirements.txt  .
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# 소스 코드 복사
COPY . ${LAMBDA_TASK_ROOT}

# entrypont.py 안에 있는 lambda_handler 함수를 진입점으로 사용
CMD [ "entrypoint.lambda_handler" ]