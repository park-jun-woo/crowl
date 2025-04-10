package crowl

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
	reWarc     = regexp.MustCompile(`CC-NEWS-(\d{4})(\d{2})\d{8}-\d{5}\.warc\.gz`)
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
		cfg.Predowns = cfg.Workers / 4
	}

	return &cfg, nil
}

// GetNews는 지정한 연도(year)와 월(month)의 뉴스 데이터를 다운로드하고 파싱합니다.
func (cc *CommonCrawl) GetNews(year int, month int) error {
	paths, err := cc.getNewsWarcPaths(year, month)
	if err != nil {
		return err
	}

	saveDir := filepath.Join(cc.DataDir, fmt.Sprintf("%04d", year), fmt.Sprintf("%02d", month))
	if err := os.MkdirAll(saveDir, os.ModePerm); err != nil {
		return err
	}

	logFilePath := filepath.Join(saveDir, "completed")

	downloadSem := make(chan struct{}, cc.Predowns) // 병렬 다운로드 제한 세마포어
	taskChan := make(chan warcTask, cc.Predowns)    // 파싱 작업 채널

	var downloadWg sync.WaitGroup
	var parseWg sync.WaitGroup

	// ✅ 파싱 워커를 미리 시작 (문제 2 해결)
	for i := 0; i < cc.Workers; i++ {
		parseWg.Add(1)
		go func(workerID int) {
			defer parseWg.Done()
			for task := range taskChan {
				fmt.Printf("[워커 %d] 파싱 시작: %s\n", workerID, task.warcLocalPath)
				err := cc.parseWarc(task.warcLocalPath, task.savePath, logFilePath)
				if err != nil {
					fmt.Printf("[워커 %d] 파싱 실패(%s): %v\n", workerID, task.warcLocalPath, err)
				} else {
					fmt.Printf("[워커 %d] 파싱 완료: %s\n", workerID, task.warcLocalPath)
				}
				os.Remove(task.warcLocalPath)
			}
		}(i)
	}

	// ✅ 다운로드 워커 시작
	for _, path := range paths {
		saveFileName := strings.Replace(filepath.Base(path), ".warc.gz", ".wrc.gz", 1)
		savePath := filepath.Join(saveDir, saveFileName)

		completed, err := isCompletedWarc(logFilePath, saveFileName)
		if err != nil {
			return fmt.Errorf("로그 확인 오류: %w", err)
		}
		if completed {
			fmt.Printf("[스킵] 이미 완료된 파일: %s\n", saveFileName)
			continue
		}

		downloadWg.Add(1)
		downloadSem <- struct{}{} // 병렬 다운로드 제한

		go func(p, sp string) {
			defer downloadWg.Done()
			defer func() { <-downloadSem }()

			fmt.Printf("[다운로드 시작] %s\n", p)
			warcLocalPath, err := cc.downloadedWarc(p)
			if err != nil {
				fmt.Printf("[다운로드 실패] %s: %v\n", p, err)
				return
			}
			taskChan <- warcTask{warcLocalPath, sp}
		}(path, savePath)
	}

	// ✅ 다운로드가 끝나면 taskChan 닫기
	go func() {
		downloadWg.Wait()
		close(taskChan)
	}()

	// ✅ 파싱 워커 작업이 모두 끝날 때까지 기다림
	parseWg.Wait()

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

// DownloadedWarc는 warcPath 파일을 다운로드합니다.
func (cc *CommonCrawl) downloadedWarc(warcPath string) (string, error) {
	// 정규 표현식으로 WARC 파일 경로에서 연도와 월 추출
	match := reWarc.FindStringSubmatch(warcPath)

	if len(match) != 3 {
		return "", fmt.Errorf("invalid warcPath format: %s", warcPath)
	}

	// 다운로드할 임시 폴더 생성
	if err := os.MkdirAll(cc.TempDir, os.ModePerm); err != nil {
		return "", err
	}

	destPath := filepath.Join(cc.TempDir, filepath.Base(warcPath))

	// HEAD 요청으로 Content-Length 확인
	headResp, err := http.Head(cc.BaseURL + warcPath)
	if err != nil {
		return "", err
	}
	if headResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("HEAD 요청 실패: %s", headResp.Status)
	}

	expectedSize, err := strconv.ParseInt(headResp.Header.Get("Content-Length"), 10, 64)
	if err != nil || expectedSize <= 0 {
		return "", fmt.Errorf("유효하지 않은 Content-Length: %v", err)
	}

	// 이미 임시 파일이 있다면 크기 검사 후 스킵 여부 결정
	if fi, err := os.Stat(destPath); err == nil {
		if fi.Size() == expectedSize {
			fmt.Printf("[스킵] 임시 파일 이미 정상 다운로드됨: %s\n", destPath)
			return destPath, nil
		}
		fmt.Printf("[재다운로드] 임시 파일 크기 불일치: %s\n", destPath)
		// 불완전한 파일 삭제
		os.Remove(destPath)
	}

	// 파일 다운로드
	resp, err := http.Get(cc.BaseURL + warcPath)
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

func (cc *CommonCrawl) parseWarc(filePath, savePath, logPath string) error {
	saveFileName := filepath.Base(savePath)

	// 완료 여부 확인
	completed, err := isCompletedWarc(logPath, saveFileName)
	if err != nil {
		return fmt.Errorf("로그 확인 오류: %w", err)
	}
	if completed {
		fmt.Printf("[파싱 스킵] 완료된 파일: %s\n", saveFileName)
		return nil
	}

	// 중단된 파일 있으면 삭제 후 재생성
	if _, err := os.Stat(savePath); err == nil {
		if err := os.Remove(savePath); err != nil {
			return fmt.Errorf("중단된 파일 삭제 실패: %w", err)
		}
	}

	// WARC 파일 열기
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("파일 열기 오류(%s): %w", filePath, err)
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		return fmt.Errorf("[워커] gzReader 오류: %w", err)
	}
	defer gzReader.Close()

	reader := bufio.NewReader(gzReader)

	// 저장할 디렉토리 정확히 생성
	if err := os.MkdirAll(filepath.Dir(savePath), os.ModePerm); err != nil {
		return err
	}

	// 저장할 파일 생성
	f, err := os.OpenFile(savePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	gw := gzip.NewWriter(f)
	defer func() {
		if err := gw.Close(); err != nil {
			fmt.Printf("gzip.Writer 닫기 오류: %v\n", err)
		}
	}()

	var wg sync.WaitGroup
	var mu sync.Mutex

	jobChan := make(chan parseJob, cc.Workers*2)
	var processedCount int64

	for w := 0; w < cc.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobChan {
				headerEnd := bytes.Index(job.Content, []byte("\r\n\r\n"))
				if headerEnd == -1 {
					headerEnd = bytes.Index(job.Content, []byte("\n\n"))
					if headerEnd == -1 {
						continue
					}
				}
				htmlContent := job.Content[headerEnd+4:]

				cleaned, err := cc.CleanHTML(htmlContent)
				if err != nil {
					continue
				}

				mu.Lock()
				if err := writeWRC(gw, job.URL, cleaned); err != nil {
					fmt.Printf("[워커 %d] writeWRC 오류: %v\n", workerID, err)
				}
				atomic.AddInt64(&processedCount, 1)
				if processedCount%1000 == 0 {
					fmt.Printf("[진행 상황] %d개 처리 완료\n", processedCount)
					gw.Flush()
				}
				mu.Unlock()
			}
		}(w)
	}

	for {
		headerLines := []string{}
		for {
			line, err := reader.ReadString('\n')
			if err == io.EOF {
				goto FINISH
			}
			if err != nil {
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
			continue
		}

		jobChan <- parseJob{URL: url, Content: content}
	}

FINISH:
	close(jobChan)
	wg.Wait()

	// 완료 기록 로그 작성
	return logCompletedWarc(logPath, saveFileName)
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

var logMu sync.Mutex
var completedSet sync.Map // 메모리 기반 세트 추가

// logCompletedWarc는 완료된 WARC 파일 이름을 로그 파일에 기록합니다.
func logCompletedWarc(logFilePath string, warcFileName string) error {
	logMu.Lock()
	defer logMu.Unlock()

	// 로그 파일에 완료된 WARC 파일 이름 기록
	f, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// 파일에 WARC 파일 이름 추가
	if _, err := f.WriteString(warcFileName + "\n"); err != nil {
		fmt.Printf("로그 파일 기록 오류: %v\n", err)
		return err
	}

	return nil
}

// isCompletedWarc는 로그 파일에서 WARC 파일 이름을 검색하여 완료 여부를 확인합니다.

func isCompletedWarc(logFilePath, warcFileName string) (bool, error) {
	if _, exists := completedSet.Load(warcFileName); exists {
		return true, nil
	}
	// 기존 파일 기반 검사 로직 이후, 발견하면 메모리에 저장
	found, err := isCompletedWarcFile(logFilePath, warcFileName)
	if err != nil {
		return false, err
	}
	if found {
		completedSet.Store(warcFileName, struct{}{})
	}
	return found, nil
}

func isCompletedWarcFile(logFilePath string, warcFileName string) (bool, error) {
	file, err := os.Open(logFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == warcFileName {
			return true, nil
		}
	}
	return false, scanner.Err()
}
