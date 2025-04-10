package main

import "parkjunwoo.com/crowl/pkg/commoncrawl"

func main() {
	cc, err := commoncrawl.NewCommonCrawl("../config/commoncrawl.yaml")
	if err != nil {
		panic(err)
	}

	cc.GetNews(2025, 3)
}
