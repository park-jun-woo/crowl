package main

import "parkjunwoo.com/crowl/pkg/crowl"

func main() {
	cc, err := crowl.NewCommonCrawl("../../config/crowl.yaml")
	if err != nil {
		panic(err)
	}

	cc.GetNews(2025, 3)
}
