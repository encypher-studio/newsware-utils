package indexer

type UploadBatchData struct {
	TotalIndexed int `json:"totalIndexed"`
	LastIndex    int `json:"lastIndex"`
}
