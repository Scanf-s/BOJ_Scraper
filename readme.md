# 이 모듈은 람다 함수 전용 모듈입니다.

## 1. scraper 모듈에서
```shell
python -m venv .venv
source .venv/bin/activate
```

## 2. requirements.txt 설치
```shell
pip install -r requirements.txt
```

## 3. 배포용 패키지 디렉토리 생성, 패키지 설치
```shell
mkdir distribution
pip install --target ./distribution -r requirements.txt
```

## 4. 소스코드 복사
```shell
cp lambda_database.py scraper.py lambda_function.py ./distribution 
```

## 5. zip 파일 생성
```shell
cd distribution
zip -r ../deploy_lambda.zip .
```

## 6. zip 파일 lambda에 업로드