package commoncrawl

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/PuerkitoBio/goquery"
	"github.com/shirou/gopsutil/v3/cpu"
	"gopkg.in/yaml.v3"
)

type CommonCrawl struct {
	Workers         int    `yaml:"workers"`
	Predowns        int    `yaml:"predowns"`
	BaseURL         string `yaml:"base_url"`
	TempDir         string `yaml:"temp_dir"`
	DataDir         string `yaml:"data_dir"`
	RemoveSelectors struct {
		Tags          []string `yaml:"tags"`
		Classes       []string `yaml:"classes"`
		ClassKeywords []string `yaml:"class_keywords"`
		Attributes    []string `yaml:"attributes"`
	} `yaml:"remove_selectors"`
}

type warcTask struct {
	warcLocalPath string
	savePath      string
}

// 작업 단위 구조체
type parseJob struct {
	URL     string
	Content []byte
}

var (
	reComments = regexp.MustCompile(`<!--[\s\S]*?-->`)
	reSpaces   = regexp.MustCompile(`\s+`)
)

func NewCommonCrawl(path string) (*CommonCrawl, error) {
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

func (cc *CommonCrawl) GetNews(year int, month int) error {
	paths, err := cc.getNewsWarcPaths(year, month)
	if err != nil {
		return err
	}

	taskChan := make(chan warcTask, cc.Predowns) // 미리 다운로드 받을 버퍼링 채널
	var wg sync.WaitGroup
	var dlErr error

	// 다운로드 고루틴 (직렬로 다운로드 수행)
	go func() {
		for _, path := range paths {
			warcLocalPath, err := cc.downloadedWarc(path)
			if err != nil {
				fmt.Printf("다운로드 실패 (%s): %v\n", path, err)
				dlErr = err
				break
			}

			saveFileName := strings.Replace(filepath.Base(path), ".warc.gz", ".wrc.gz", 1)
			savePath := filepath.Join(cc.DataDir, saveFileName)

			taskChan <- warcTask{warcLocalPath, savePath} // 다운로드 완료 후 채널에 넣기
		}
		close(taskChan) // 모든 파일 다운로드 완료 시 채널 닫기
	}()

	// 파싱 워커 (병렬로 처리)
	for i := 0; i < cc.Workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range taskChan {
				fmt.Printf("[파싱 워커 %d] 파싱 시작: %s\n", workerID, task.warcLocalPath)
				err := cc.parseWarc(task.warcLocalPath, task.savePath)
				if err != nil {
					fmt.Printf("[파싱 워커 %d] 파싱 실패(%s): %v\n", workerID, task.warcLocalPath, err)
				} else {
					fmt.Printf("[파싱 워커 %d] 파싱 완료: %s\n", workerID, task.warcLocalPath)
				}

				if err := os.Remove(task.warcLocalPath); err != nil {
					fmt.Printf("[파싱 워커 %d] 임시파일 삭제 오류(%s): %v\n", workerID, task.warcLocalPath, err)
				}
			}
		}(i)
	}

	wg.Wait()

	return dlErr
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

func (cc *CommonCrawl) parseWarc(filePath string, savePath string) error {
	// WARC 파일을 열고 압축 해제
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("파일 열기 오류(%s): %w", filePath, err)
	}
	defer file.Close()

	// gzip Reader 생성
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("[워커] gzReader 오류: %w", err)
	}
	defer gzReader.Close()

	// bufio.Reader를 사용하여 파일을 줄 단위로 읽기
	reader := bufio.NewReader(gzReader)
	if err := os.MkdirAll(cc.DataDir, os.ModePerm); err != nil {
		return err
	}

	// 저장할 파일 열기
	f, err := os.OpenFile(savePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("[워커] 파일 열기 오류")
		return err
	}
	defer f.Close()

	// gzip Writer 생성
	gw := gzip.NewWriter(f)
	defer func() {
		if err := gw.Close(); err != nil {
			fmt.Println("gzip.Writer 닫기 오류:", err)
		}
	}()

	var wg sync.WaitGroup
	var mu sync.Mutex

	jobChan := make(chan parseJob, cc.Workers*2)

	var processedCount int64 = 0 // 처리된 개수 카운터

	// 워커 생성
	for w := 0; w < cc.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobChan {
				// 헤더와 본문 분리
				headerEnd := bytes.Index(job.Content, []byte("\r\n\r\n"))
				if headerEnd == -1 {
					headerEnd = bytes.Index(job.Content, []byte("\n\n"))
					if headerEnd == -1 {
						fmt.Printf("[워커 %d] 오류: 본문 구분자(\\n\\n)를 찾지 못함\n", workerID)
						continue
					}
				}

				// 실제 HTML 본문 시작 위치 (+4는 \r\n\r\n 길이)
				htmlContent := job.Content[headerEnd+4:]

				cleaned, err := cc.CleanHTML(htmlContent)
				if err != nil {
					fmt.Printf("[워커 %d] cleanHTML 오류: %v\n", workerID, err)
					continue
				}

				mu.Lock()
				err = writeWRC(gw, job.URL, cleaned)
				if err != nil {
					fmt.Printf("[워커 %d] writeWRC 오류: %v\n", workerID, err)
				} else {
				}

				// 처리 개수 증가 및 100개 단위 로그 출력
				newCount := atomic.AddInt64(&processedCount, 1)
				if newCount%1000 == 0 {
					fmt.Printf("[진행 상황] 현재까지 %d개 처리 완료.\n", newCount)
					if err := gw.Flush(); err != nil {
						fmt.Printf("[워커 %d] gzip.Flush 오류: %v\n", workerID, err)
					}
				}
				mu.Unlock()
			}
		}(w)
	}

	// 읽고 작업 전달
	for {
		headerLines := []string{}
		errorCount := 0
		const maxErrors = 10
		for {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				goto FINISH
			}
			if err != nil {
				fmt.Println("[워커] 헤더 읽기 오류:", err)
				goto FINISH
			}

			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				break
			}
			headerLines = append(headerLines, line)
		}

		header := cc.ParseHeader(headerLines)

		if header["WARC-Type"] != "response" {
			skipBytes(reader, header["Content-Length"])
			continue
		}

		url := header["WARC-Target-URI"]
		contentLength, _ := strconv.Atoi(header["Content-Length"])
		content := make([]byte, contentLength)
		if _, err = io.ReadFull(reader, content); err != nil {
			fmt.Println("[워커] 본문 읽기 오류:", err)
			errorCount++
			if errorCount > maxErrors {
				fmt.Println("[워커] 최대 에러 횟수 초과로 종료")
				break
			}
			continue
		} else {
			errorCount = 0 // 정상 처리 시 초기화
		}

		// job 채널에 작업 전달
		jobChan <- parseJob{URL: url, Content: content}
	}

FINISH:
	close(jobChan) // 작업 완료 신호
	wg.Wait()      // 모든 워커가 끝날 때까지 대기

	return nil
}

// 헤더 파싱
func (cc *CommonCrawl) ParseHeader(headerLines []string) map[string]string {
	header := make(map[string]string)
	for _, line := range headerLines {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 2 {
			header[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}
	return header
}

// CleanHTML은 불필요한 태그들을 제거한 HTML 본문을 반환합니다.
func (cc *CommonCrawl) CleanHTML(rawHTML []byte) ([]byte, error) {
	// HTML 주석 제거
	htmlWithoutComments := reComments.ReplaceAll(rawHTML, []byte(""))

	// HTML 파싱
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(htmlWithoutComments))
	if err != nil {
		return nil, err
	}

	// 클래스 제거를 위한 집합(set) 구성
	removeClassSet := make(map[string]struct{})
	for _, class := range cc.RemoveSelectors.Classes {
		removeClassSet[strings.ToLower(class)] = struct{}{}
	}

	// 태그 제거를 위한 집합(set) 구성
	removeTagSet := make(map[string]struct{})
	for _, tag := range cc.RemoveSelectors.Tags {
		removeTagSet[strings.ToLower(tag)] = struct{}{}
	}

	// DOM 요소 한 번만 탐색하며 제거 작업 수행
	doc.Find("*").Each(func(i int, sel *goquery.Selection) {
		nodeName := goquery.NodeName(sel)

		// 태그 제거
		if _, removeTag := removeTagSet[strings.ToLower(nodeName)]; removeTag {
			sel.Remove()
			return // 이미 삭제된 요소이므로 하위 처리 중단
		}

		// 클래스 기반 제거
		if classAttr, exists := sel.Attr("class"); exists && nodeName != "body" {
			classes := strings.Fields(classAttr)
			for _, className := range classes {
				lowerClass := strings.ToLower(className)
				if _, removeExactClass := removeClassSet[lowerClass]; removeExactClass ||
					containsAnyKeyword(lowerClass, cc.RemoveSelectors.ClassKeywords) {
					sel.Remove() // GoQuery의 API를 이용하여 안전하게 삭제
					return       // 이미 삭제된 요소이므로 하위 처리 중단
				}
			}
		}

		// 속성 제거 (data-, area-, on*, item* 및 설정된 속성들)
		attrs := sel.Nodes[0].Attr
		for _, attr := range attrs {
			keyLower := strings.ToLower(attr.Key)
			remove := false

			// 기본 삭제 대상 속성 검사
			if strings.HasPrefix(keyLower, "data-") ||
				strings.HasPrefix(keyLower, "area-") ||
				strings.HasPrefix(keyLower, "on") ||
				strings.HasPrefix(keyLower, "item") {
				remove = true
			} else {
				// 설정된 속성들 검사
				for _, removeAttr := range cc.RemoveSelectors.Attributes {
					if keyLower == strings.ToLower(removeAttr) {
						remove = true
						break
					}
				}
			}

			if remove {
				sel.RemoveAttr(attr.Key) // GoQuery의 API를 이용하여 안전하게 삭제
			}
		}
	})

	// HTML 최종 결과물 생성
	html, err := doc.Html()
	if err != nil {
		return nil, err
	}

	// 최종 공백 정리 후 반환
	cleanedHTML := cleanSpaces(html)

	return []byte(cleanedHTML), nil
}

// cleanSpaces는 HTML 문자열에서 불필요한 공백과 개행을 압축하여 정리합니다.
func cleanSpaces(html string) string {
	// 모든 공백(스페이스, 탭, 개행)을 하나의 스페이스로 변경
	cleaned := reSpaces.ReplaceAllString(html, " ")

	// 앞뒤 공백 제거 (trim)
	return strings.TrimSpace(cleaned)
}

// skipBytes는 지정된 길이만큼 바이트를 건너뜁니다.
func skipBytes(reader *bufio.Reader, lenStr string) {
	n, _ := strconv.Atoi(lenStr)
	io.CopyN(io.Discard, reader, int64(n))
}

// 클래스 확인 함수
func containsAnyKeyword(className string, keywords []string) bool {
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

// 파일 핸들을 전달받아 wrc.gz 형식으로 데이터를 추가하는 함수
func writeWRC(gw *gzip.Writer, url string, content []byte) error {
	entry := fmt.Sprintf("%s\n%d\n%s\n\n", url, len(content), content)

	if _, err := gw.Write([]byte(entry)); err != nil {
		return fmt.Errorf("writeWRC 오류(URL: %s): %w", url, err)
	}

	return nil
}
