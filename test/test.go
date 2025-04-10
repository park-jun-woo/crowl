package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"parkjunwoo.com/crowl/pkg/commoncrawl"
)

func main() {
	cc, err := commoncrawl.NewCommonCrawl("../config/commoncrawl.yaml")
	if err != nil {
		panic(err)
	}

	warcPath := fmt.Sprintf("%sCC-NEWS-20250407010645-01509.warc.gz", cc.TempDir)
	savePath := fmt.Sprintf("%sCC-NEWS-20250407010645-01509.wrc", cc.DataDir)
	err = parseWarcTest(cc, warcPath, savePath)
	if err != nil {
		fmt.Println("WARC 파일 파싱 오류:", err)
	}
}

func parseWarcTest(cc *commoncrawl.CommonCrawl, filePath string, savePath string) error {
	// WARC 파일을 열고 압축 해제
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Println("[워커] 파일 읽기 오류")
		return err
	}
	defer file.Close()

	// gzip Reader 생성
	gzReader, err := gzip.NewReader(file)
	if err != nil {
		fmt.Println("[워커] gzReader 오류")
		return err
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
	}
	defer f.Close()

	// 읽고 작업 전달
	iu := 0
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

		// 헤더와 본문 분리
		headerEnd := bytes.Index(content, []byte("\r\n\r\n"))
		if headerEnd == -1 {
			headerEnd = bytes.Index(content, []byte("\n\n"))
			if headerEnd == -1 {
				fmt.Printf("[워커] 오류: 본문 구분자(\\n\\n)를 찾지 못함\n")
				continue
			}
		}

		// 실제 HTML 본문 시작 위치 (+4는 \r\n\r\n 길이)
		htmlContent := content[headerEnd+4:]

		cleaned, err := cc.CleanHTML(htmlContent)
		if err != nil {
			fmt.Printf("[워커] cleanHTML 오류: %v\n", err)
			continue
		}

		entry := fmt.Sprintf("%s\n%d\n%s\n\n", url, len(cleaned), cleaned)

		if _, err := f.Write([]byte(entry)); err != nil {
			return err
		}

		iu++
		if iu >= 10 {
			fmt.Printf("[워커] %d개 처리 완료\n", iu)
			goto FINISH
		}
	}

FINISH:
	return nil
}

// skipBytes는 지정된 길이만큼 바이트를 건너뜁니다.
func skipBytes(reader *bufio.Reader, lenStr string) {
	n, _ := strconv.Atoi(lenStr)
	io.CopyN(io.Discard, reader, int64(n))
}
