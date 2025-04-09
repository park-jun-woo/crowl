# 🌐 crowl

> ⚠️ **주의**  
> 본 오픈소스 프로젝트는 아직 개발중입니다.

**crowl**은 [Common Crawl](https://commoncrawl.org/)에서 제공하는 데이터를 쉽고 빠르게 다운받아 전처리하는 Go 기반의 데이터 전처리 도구입니다.


> 🛠️ **언어**: Go  
> 📄 **라이센스**: MIT License

---

## 📌 주요 기능

- Common Crawl의 WARC 파일 자동 다운로드
- gzip 압축 해제 및 스트리밍 처리
- HTML 문서 파싱 및 불필요한 태그/속성 제거
- 뉴스 페이지와 비정상 페이지를 인공지능 모델로 판별 가능한 형태로 정제하여 저장

---

## 🚀 빠르게 시작하기

### 설치 방법

```bash
git clone https://github.com/park-jun-woo/crowl.git
cd crowl
go mod tidy
go build -o crowl
```

### 사용 예시

```bash
./crowl
```

혹은 특정 연도와 월을 지정하여 실행:

```bash
./crowl -year 2025 -month 4
```

### 예제 코드 (Go)

```go
package main

import (
	"fmt"
	"parkjunwoo.com/crowl/pkg/commoncrawl"
)

func main() {
	cc := commoncrawl.NewCommonCrowl(0, 0)
	if err := cc.GetNews(); err != nil {
		fmt.Println("처리 중 오류 발생:", err)
	}
}
```

---

## 🧩 디렉토리 구조

```
crowl/
├── cmd/               # 실행 가능한 코드 및 진입점
├── pkg/               # 라이브러리 코드
│   └── commoncrawl/   # Common Crawl 관련 기능 구현
├── tmp/               # 임시 파일 저장소 (자동 생성됨)
├── data/              # 처리된 데이터 저장소 (자동 생성됨)
├── go.mod
├── go.sum
├── LICENSE
├── NOTICE
└── README.md
```

---

## ⚠️ 라이센스

**crowl**은 [MIT 라이센스](LICENSE)로 배포됩니다.

이 프로젝트는 다음과 같은 오픈소스를 사용하고 있습니다:

- [goquery](https://github.com/PuerkitoBio/goquery) (BSD-3-Clause)
- [gopsutil](https://github.com/shirou/gopsutil) (BSD-3-Clause)
- [Common Crawl Dataset](https://commoncrawl.org/) (CC0 1.0 Public Domain)

자세한 사항은 [NOTICE](NOTICE)를 참조하세요.

---

## 📚 참고 자료

- [Common Crawl 공식 사이트](https://commoncrawl.org/)
- [WARC 파일 포맷 설명](https://iipc.github.io/warc-specifications/)
- [goquery 사용법](https://github.com/PuerkitoBio/goquery)
- [gopsutil 사용법](https://github.com/shirou/gopsutil)

---

## 🙌 기여하기

기능 추가, 버그 수정 등 프로젝트에 기여를 원하신다면 아래의 과정을 따르세요.

1. Fork 후 본인의 레포지토리에 복제
2. 브랜치를 생성하여 기능 추가 또는 수정
3. 변경사항에 대한 커밋 후 풀 리퀘스트(PR) 제출

---

## 📞 연락처

- **이슈**: GitHub Issue를 통해 버그나 개선 제안 등록
- **이메일**: mail@parkjunwoo.com  

---

**🚩 crowl**을 활용하여 더 나은 데이터 처리 환경을 구축하세요!