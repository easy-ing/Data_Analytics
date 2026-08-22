Great Expectations (skeleton)

이 디렉토리는 Great Expectations를 사용한 데이터 품질 검사 도입을 위한 기본 안내와 예제 스크립트를 포함합니다.

빠른 시작:
1. 개발 의존성 설치
   pip install -r requirements-dev.txt
2. 샘플 CSV에 대해 기본 체크 실행
   python data_quality/run_quality_checks.py data/raw/events_sample.csv

더 확장하려면:
- great_expectations init을 실행해 full configuration을 생성하세요.
- expectation suites를 만들고 checkpoints를 구성해 자동화하세요.
