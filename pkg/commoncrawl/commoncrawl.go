package commoncrawl

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/shirou/gopsutil/v3/cpu"
	"gopkg.in/yaml.v3"
)

type CommonCrawl struct {
	Workers         int    `yaml:"workers"`
	Predowns        int    `yaml:"predowns"`
	BaseURL         string `yaml:"base_url"`
	TempDir         string `yaml:"temp_dir"`
	RemoveSelectors struct {
		Tags          []string `yaml:"tags"`
		Classes       []string `yaml:"classes"`
		ClassKeywords []string `yaml:"class_keywords"`
		Attributes    []string `yaml:"attributes"`
	} `yaml:"remove_selectors"`
}

func LoadCommonCrawl(path string) (*CommonCrawl, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg CommonCrawl
	if err := yaml.Unmarshal(file, &cfg); err != nil {
		return nil, err
	}

	if cfg.Workers == 0 {
		var workerNums int
		workerNums, err = cpu.Counts(false) // 물리적 코어 수 반환 (logical=false)
		if err != nil {
			panic(err)
		}
		cfg.Workers = workerNums
	}

	if cfg.Predowns == 0 {
		cfg.Predowns = 10
	}

	return &cfg, nil
}

func NewCommonCrawl(workers int, predowns int) *CommonCrawl {
	var workerNums int
	var err error

	if workers == 0 {
		workerNums, err = cpu.Counts(false) // 물리적 코어 수 반환 (logical=false)
		if err != nil {
			panic(err)
		}
	} else {
		workerNums = workers
	}

	if predowns == 0 {
		predowns = 10
	}

	return &CommonCrawl{
		Workers:  workerNums,
		Predowns: predowns,
		BaseURL:  "https://data.commoncrawl.org/",
		TempDir:  "../tmp/commoncrawl/",
	}
}

func (cc *CommonCrawl) GetNews() error {
	/*
		// 지정한 연도와 월에 해당하는 warc.paths.gz 파일 다운로드
		paths, err := cc.getNewsWarcPaths()
		if err != nil {
			return err
		}
	*/

	warcPath := fmt.Sprintf("%sCC-NEWS-20250407010645-01509.warc.gz", cc.TempDir)
	err := cc.parseWarc(warcPath)
	if err != nil {
		fmt.Println("WARC 파일 파싱 오류:", err)
		return err
	}

	return nil
}

// GetWarcPaths는 지정한 연도(y), 월(m)의 warc.paths.gz 파일을 다운로드하여 압축 해제 후,
// 그 내용을 파싱하여 WARC 파일 경로 목록을 반환합니다.
func (cc *CommonCrawl) getNewsWarcPaths(year int, month int) ([]string, error) {
	// URL 생성 (월을 항상 두 자리로 맞춤)
	url := fmt.Sprintf("%scrawl-data/CC-NEWS/%d/%02d/warc.paths.gz", cc.BaseURL, year, month)

	// 다운로드할 파일 경로 설정
	if err := os.MkdirAll(cc.TempDir, os.ModePerm); err != nil {
		return nil, err
	}

	gzipFilePath := filepath.Join(cc.TempDir, "warc.paths.gz")
	outFile, err := os.Create(gzipFilePath)
	if err != nil {
		return nil, err
	}
	defer outFile.Close()

	// 파일 다운로드
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("failed to download file: %s", resp.Status)
	}

	// 응답을 파일에 쓰기
	if _, err = io.Copy(outFile, resp.Body); err != nil {
		return nil, err
	}

	// 압축 해제하고 내용 읽기
	gzFile, err := os.Open(gzipFilePath)
	if err != nil {
		return nil, err
	}
	defer gzFile.Close()

	gzReader, err := gzip.NewReader(gzFile)
	if err != nil {
		return nil, err
	}
	defer gzReader.Close()

	// 압축 해제된 내용을 메모리에 읽기
	data, err := io.ReadAll(gzReader)
	if err != nil {
		return nil, err
	}

	// 파일 내용을 줄 단위로 파싱하여 slice로 반환
	paths := strings.Split(strings.TrimSpace(string(data)), "\n")

	// 압축 파일 삭제 (임시 파일 정리)
	if err := os.Remove(gzipFilePath); err != nil {
		// 삭제 실패 시 경고만 표시하고 계속 진행
		fmt.Printf("임시 파일 삭제 실패: %v\n", err)
	}

	return paths, nil
}

func (cc *CommonCrawl) parseWarc(filePath string) error {
	iu := 0
	// WARC 파일을 열고 압축 해제
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("[워커] 파일 읽기 오류")
		return err
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		fmt.Println("[워커] gzReader 오류")
		return err
	}
	defer gzReader.Close()

	reader := bufio.NewReader(gzReader)

	for {
		headerLines := []string{}
		for {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				goto NEXT_FILE
			}
			if err != nil {
				fmt.Println("[워커] 헤더 읽기 오류:", err)
				goto NEXT_FILE
			}

			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				break
			}
			headerLines = append(headerLines, line)
		}

		header := parseHeader(headerLines)

		warcType := header["WARC-Type"]
		if warcType != "response" {
			skipBytes(reader, header["Content-Length"])
			continue
		}

		url := header["WARC-Target-URI"]
		contentLength, _ := strconv.Atoi(header["Content-Length"])
		content := make([]byte, contentLength)
		if _, err = io.ReadFull(reader, content); err != nil {
			fmt.Println("[워커] 본문 읽기 오류:", err)
			continue
		}
		fmt.Println("[워커] ", warcType, ":", url, contentLength)
		cc.parseBody(url, content)
		iu++
		if iu > 10 {
			fmt.Println("[워커] 10개 이상 읽음")
			return nil
		}
	}

NEXT_FILE:
	gzReader.Close()
	file.Close()
	return nil
}

func (cc *CommonCrawl) parseBody(rawurl string, content []byte) error {
	// 헤더와 본문 분리
	headerEnd := bytes.Index(content, []byte("\r\n\r\n"))
	if headerEnd == -1 {
		headerEnd = bytes.Index(content, []byte("\n\n"))
		if headerEnd == -1 {
			return fmt.Errorf("본문 구분자(\\n\\n)를 찾지 못함")
		}
	}

	// 실제 HTML 본문 시작 위치 (+4는 \r\n\r\n 길이)
	htmlContent := content[headerEnd+4:]

	cleaned, err := cc.cleanHTML(htmlContent)
	if err != nil {
		return err
	}

	// 파일명 처리
	parsedURL, err := url.Parse(rawurl)
	if err != nil {
		return err
	}
	filename := fmt.Sprintf("%s_%s.html",
		parsedURL.Hostname(),
		filepath.Base(parsedURL.Path),
	)

	if err := os.MkdirAll(cc.TempDir, os.ModePerm); err != nil {
		return err
	}
	filePath := filepath.Join(cc.TempDir, filename)

	// HTML 본문만 저장
	if err := os.WriteFile(filePath, cleaned, 0644); err != nil {
		return err
	}

	fmt.Println("HTTP 헤더 제거 후 저장 완료:", filePath)
	return nil
}

// CleanHTML은 불필요한 태그들을 제거한 HTML 본문을 반환합니다.
func (cc *CommonCrawl) cleanHTML(rawHTML []byte) ([]byte, error) {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(rawHTML))
	if err != nil {
		return nil, err
	}

	// 중복 방지를 위해 map 사용
	removeClassMap := make(map[string]struct{})
	attrMap := make(map[string]struct{})
	for _, attr := range cc.RemoveSelectors.Attributes {
		attrMap[attr] = struct{}{}
	}

	// 클래스 키워드를 빠르게 확인하기 위한 함수
	containsAnyKeyword := func(className string, keywords []string) bool {
		for _, keyword := range keywords {
			switch {
			case strings.HasPrefix(keyword, "^"):
				prefix := strings.TrimPrefix(keyword, "^")
				if strings.HasPrefix(className, prefix) {
					return true
				}
			case strings.HasSuffix(keyword, "$"):
				suffix := strings.TrimSuffix(keyword, "$")
				if strings.HasSuffix(className, suffix) {
					return true
				}
			default:
				if strings.Contains(className, keyword) {
					return true
				}
			}
		}
		return false
	}

	// 모든 요소를 한 번만 순회하여 추가 속성 정리
	doc.Find("*").Each(func(i int, sel *goquery.Selection) {
		classAttr, exists := sel.Attr("class")
		if exists {
			classes := strings.Fields(classAttr)
			for _, className := range classes {
				lowerClass := strings.ToLower(className)
				if containsAnyKeyword(lowerClass, cc.RemoveSelectors.ClassKeywords) {
					selector := "." + className
					removeClassMap[selector] = struct{}{}
				}
			}
		}
		node := sel.Get(0)
		// 제거할 동적 속성 (data-, area-, on*, item*)
		for _, attr := range node.Attr {
			if strings.HasPrefix(attr.Key, "data-") || strings.HasPrefix(attr.Key, "area-") || strings.HasPrefix(attr.Key, "on") || strings.HasPrefix(attr.Key, "item") {
				attrMap[attr.Key] = struct{}{}
			}
		}
	})

	// 선택한 요소 제거
	for _, sel := range cc.RemoveSelectors.Tags {
		doc.Find(sel).Remove()
	}

	// 클래스 선택자 삭제
	for sel := range removeClassMap {
		cc.RemoveSelectors.Classes = append(cc.RemoveSelectors.Classes, sel)
	}

	// 클래스 선택자 제거
	for _, sel := range cc.RemoveSelectors.Classes {
		doc.Find(sel).Each(func(i int, s *goquery.Selection) {
			if goquery.NodeName(s) != "body" {
				s.Remove()
			}
		})
	}

	// 속성 최종 제거 (중복 없이 처리)
	for attr := range attrMap {
		doc.Find("*").RemoveAttr(attr)
	}

	// HTML 주석 제거 후 공백 압축
	html, err := doc.Html()
	if err != nil {
		return nil, err
	}

	cleanedHTML := cleanSpaces(removeComments(html))

	return []byte(cleanedHTML), nil
}

// CleanSpaces는 HTML 문자열에서 불필요한 공백과 개행을 압축하여 정리합니다.
func cleanSpaces(html string) string {
	// 모든 공백(스페이스, 탭, 개행)을 하나의 스페이스로 변경
	re := regexp.MustCompile(`\s+`)
	cleaned := re.ReplaceAllString(html, " ")

	// 앞뒤 공백 제거 (trim)
	return strings.TrimSpace(cleaned)
}

// HTML 주석 제거 함수
func removeComments(html string) string {
	for {
		start := strings.Index(html, "<!--")
		if start == -1 {
			break
		}
		end := strings.Index(html[start:], "-->")
		if end == -1 {
			break
		}
		html = html[:start] + html[start+end+3:]
	}
	return html
}

// skipBytes는 지정된 길이만큼 바이트를 건너뜁니다.
func skipBytes(reader *bufio.Reader, lenStr string) {
	n, _ := strconv.Atoi(lenStr)
	io.CopyN(io.Discard, reader, int64(n))
}

// 헤더 파싱
func parseHeader(headerLines []string) map[string]string {
	header := make(map[string]string)
	for _, line := range headerLines {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			header[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return header
}

// DownloadedWarc는 warcPath 파일을 다운로드합니다.
func (cc *CommonCrawl) downloadedWarc(warcPath string) (string, error) {
	baseURL := "https://data.commoncrawl.org/"

	re := regexp.MustCompile(`CC-NEWS-(\d{4})(\d{2})\d{8}-\d{5}\.warc\.gz`)
	match := re.FindStringSubmatch(warcPath)

	if len(match) != 3 {
		return "", fmt.Errorf("invalid warcPath format: %s", warcPath)
	}

	// 다운로드할 임시 폴더 생성
	if err := os.MkdirAll(cc.TempDir, os.ModePerm); err != nil {
		return "", err
	}

	destPath := filepath.Join(cc.TempDir, filepath.Base(warcPath))

	// 파일 다운로드
	resp, err := http.Get(baseURL + warcPath)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("파일 다운로드 실패: %s", resp.Status)
	}

	outFile, err := os.Create(destPath)
	if err != nil {
		return "", err
	}
	defer outFile.Close()

	if _, err := io.Copy(outFile, resp.Body); err != nil {
		return "", err
	}

	return destPath, nil
}
