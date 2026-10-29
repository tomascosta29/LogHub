package source

func resolveFetchSize(fetchSize, legacyBatchSize int) int {
	if fetchSize != 0 {
		return fetchSize
	}
	return legacyBatchSize
}
