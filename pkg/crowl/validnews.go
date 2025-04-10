package crowl

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"gopkg.in/yaml.v3"
)

type ValidNews struct {
	BatchSize int    `yaml:"batch_size"`
	PyPath    string `yaml:"py_path"`
	TempDir   string `yaml:"temp_dir"`
	DataDir   string `yaml:"data_dir"`
}

type newsItem struct {
	url         string
	htmlContent string
	cleanText   string
}

type inferRequest struct {
	Texts []string `json:"texts"`
}

type inferResponse struct {
	Answers []string `json:"answers"`
}

func NewValidNews(path string) (*ValidNews, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg ValidNews
	if err := yaml.Unmarshal(file, &cfg); err != nil {
		return nil, err
	}

	if cfg.BatchSize == 0 {
		cfg.BatchSize = 4
	}

	return &cfg, nil
}

func cleanHTML(html string) string {
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader([]byte(html)))
	if err != nil {
		return ""
	}
	doc.Find("script, style, noscript").Remove()
	text := doc.Text()
	text = strings.Join(strings.Fields(text), " ")

	if len(text) > 2000 {
		return text[:2000]
	}

	return text
}

func (vn *ValidNews) ProcessWRC(inputPath, outputPath string) error {
	// Python 서버 시작
	pyCmd := exec.Command("python3", vn.PyPath)
	if err := pyCmd.Start(); err != nil {
		return fmt.Errorf("start python server error: %v", err)
	}

	fmt.Println("🚀 starting python server...")

	// FastAPI 서버 준비될 때까지 대기
	if err := waitForPythonServer("http://127.0.0.1:8000", 120*time.Second); err != nil {
		return fmt.Errorf("python server not ready: %v", err)
	}

	defer func() {
		time.Sleep(3 * time.Second)
		if err := pyCmd.Process.Kill(); err != nil {
			fmt.Printf("⚠️ failed python server close : %v\n", err)
		} else {
			fmt.Println("✅ closed python server.")
		}
	}()

	inFile, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer inFile.Close()

	gzReader, err := gzip.NewReader(inFile)
	if err != nil {
		return err
	}
	defer gzReader.Close()

	reader := bufio.NewReader(gzReader)

	outFile, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	//gzWriter := gzip.NewWriter(outFile)
	//defer gzWriter.Close()

	var wg sync.WaitGroup
	preprocessChan := make(chan newsItem, vn.BatchSize*2)
	inferChan := make(chan newsItem, vn.BatchSize*2)

	// 1️⃣ 전처리 워커
	wg.Add(1)
	go func() {
		defer wg.Done()
		for item := range preprocessChan {
			cleaned := cleanHTML(item.htmlContent)
			if len(cleaned) == 0 {
				continue
			}
			item.cleanText = cleaned
			inferChan <- item
		}
		close(inferChan)
	}()

	// 2️⃣ 추론 워커
	wg.Add(1)
	go func() {
		defer wg.Done()
		batch := []newsItem{}

		for item := range inferChan {
			batch = append(batch, item)

			if len(batch) >= vn.BatchSize {
				//vn.flushBatch(batch, gzWriter)
				vn.flushBatch(batch, outFile)
				batch = []newsItem{}
			}
		}

		if len(batch) > 0 {
			//vn.flushBatch(batch, gzWriter)
			vn.flushBatch(batch, outFile)
		}
	}()

	mainLoopCount := 0

	// 데이터 읽기 및 전처리 워커로 전달
	for {
		url, err := reader.ReadString('\n')
		if err == io.EOF {
			break
		}
		url = strings.TrimSpace(url)

		sizeLine, err := reader.ReadString('\n')
		if err != nil {
			fmt.Printf("size read error (%s): %v\n", url, err)
			continue
		}
		size, err := strconv.Atoi(strings.TrimSpace(sizeLine))
		if err != nil {
			fmt.Printf("invalid size (%s): %v\n", url, err)
			continue
		}

		htmlContent := make([]byte, size)
		if _, err := io.ReadFull(reader, htmlContent); err != nil {
			fmt.Printf("content read error (%s): %v\n", url, err)
			continue
		}

		reader.ReadString('\n')
		reader.ReadString('\n')

		preprocessChan <- newsItem{url: url, htmlContent: string(htmlContent)}

		mainLoopCount++
		if mainLoopCount >= 100 {
			fmt.Println("🚩 테스트를 위해 100개 항목 처리 후 종료.")
			break
		}
	}

	close(preprocessChan)
	wg.Wait()

	return nil
}

func (vn *ValidNews) flushBatch(items []newsItem, gzWriter *os.File) {
	texts := make([]string, len(items))
	for i, item := range items {
		texts[i] = item.cleanText
	}

	resp, err := http.Post("http://127.0.0.1:8000/infer",
		"application/json",
		bytes.NewBuffer(jsonEncode(inferRequest{Texts: texts})))
	if err != nil {
		fmt.Printf("HTTP batch error: %v\n", err)
		return
	}

	var inferRes inferResponse
	if err := json.NewDecoder(resp.Body).Decode(&inferRes); err != nil {
		fmt.Printf("JSON Decode Error: %v\n", err)
		resp.Body.Close()
		return
	}
	resp.Body.Close()

	// ⚠️ 여기서 응답의 길이를 확인
	if len(inferRes.Answers) != len(items) {
		fmt.Printf("⚠️ 응답 개수 불일치: 요청 %d개, 응답 %d개\n", len(items), len(inferRes.Answers))
		return
	}

	for i, item := range items {
		entry := fmt.Sprintf("%s\n%s\n%d\n%s\n\n",
			item.url, inferRes.Answers[i], len(item.htmlContent), item.htmlContent)

		if _, err := gzWriter.Write([]byte(entry)); err != nil {
			fmt.Printf("Write Error (%s): %v\n", item.url, err)
		}
	}
}

func jsonEncode(v interface{}) []byte {
	data, _ := json.Marshal(v)
	return data
}

// waitForPythonServer가 FastAPI 서버 준비 완료될 때까지 기다림
func waitForPythonServer(url string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout: Python server not ready within %v", timeout)
		}
		resp, err := http.Get(url + "/health")
		if err == nil && resp.StatusCode == 200 {
			resp.Body.Close()
			return nil // 서버 준비 완료
		}
		time.Sleep(2 * time.Second)
	}
}
