package main

import "parkjunwoo.com/crowl/pkg/commoncrawl"

func main() {
	/*
		warcPath := "crawl-data/CC-NEWS/2025/04/CC-NEWS-20250407010645-01509.warc.gz"

		// overwrite를 true 또는 false로 선택 가능
		destPath, err := commoncrawl.DownloadedWarc(warcPath, false)
		if err != nil {
			fmt.Println("Error:", err)
		}
		fmt.Printf("다운로드된 파일 경로: %s\n", destPath)
		urls, err := commoncrawl.ExtractResponseURLs(destPath)
		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		fmt.Printf("총 %d개의 URL이 추출되었습니다.\n", len(urls))
		for i, url := range urls {
			fmt.Printf("%d: %s\n", i+1, url)
			if i >= 4 {
				break // 처음 5개만 출력 예시
			}
		}
	*/
	cc := commoncrawl.NewCommonCrowl(0, 0)
	cc.GetNews()
}
