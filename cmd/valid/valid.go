package main

import "parkjunwoo.com/crowl/pkg/crowl"

func main() {

	vn, err := crowl.NewValidNews("../../config/crowl.yaml")
	if err != nil {
		panic(err)
	}

	vn.ProcessWRC("../../data/commoncrawl/2025/03/CC-NEWS-20250301004532-00945.wrc.gz", "../../tmp/commoncrawl/CC-NEWS-20250301004532-00945.txt")
}
