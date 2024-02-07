package indexer

type IndexBatchData struct {
	TotalIndexed int `json:"totalIndexed"`
	LastIndex    int `json:"lastIndex"`
}
