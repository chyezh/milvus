package collector

// QueryNodeMetrics is the metrics of query node.
type QueryNodeMetrics struct {
	NodeID  string `json:"nodeID"`
	Address string `json:"address"`
	// Global metrics.
	Global         QueryNodeMetricsGlobal               `json:"global"`
	HotCollections map[int64]QueryNodeMetricsCollection `json:"hotCollections"` // collectionID -> metrics.
}

type QueryNodeMetricsGlobal struct {
	Cache  StorageMetrics `json:"cache"`
	System SystemMetric   `json:"system"`
}

type QueryNodeMetricsCollection struct {
	DatabaseID   int64          `json:"databaseID"`
	CollectionID int64          `json:"collectionID"`
	Cache        StorageMetrics `json:"cache"`
}

type StorageMetrics struct {
	Memory StorageMetricInner `json:"mem"`
	Disk   StorageMetricInner `json:"disk"`
	OSS    StorageMetricInner `json:"oss"`
}

// StorageMetricInner is the metrics of storage.
type StorageMetricInner struct {
	Hit   StorageCacheHitMetrics `json:"hit"`
	IO    StorageCacheIOMetrics  `json:"io"`
	Usage StorageUsageMetrics    `json:"usage"`
}

// StorageCacheHitMetrics is the metrics of cache hit.
// 1. When a query is executed if the data is already loaded on memory, it's a memory hit.
// 2. If the data is not loaded on memory but loaded on disk, it's a disk hit.
// 3. If the data is not loaded on memory and disk, it's a oss hit.
type StorageCacheHitMetrics struct {
	RequestPerSecond          int64 `json:"rps"`
	NQPerSecond               int64 `json:"vps"`
	ResultRowPerSecond        int64 `json:"rrps"`
	ResultBytesPerSecond      int64 `json:"rbps"`
	AverageLatencyMS          int64 `json:"avgLatencyMs"`
	AverageLatencyPerVectorMS int64 `json:"avgLatencyPvMs"`
}

// StorageCacheIOMetrics is the metrics of storage cache in or out operations.
// Evict can be seen at memory or disk cache.
// A Evict operation from memory will be seen at memory evict.
// A Evict operation from disk will be seen at disk evict.
// Load can be seen at disk or oss cache.
// A Load operation from disk will be seen at disk load.
// A Load operation from oss will be seen at disk load and oss load.
type StorageCacheIOMetrics struct {
	LoadPerSecond       int64 `json:"lps"`
	LoadBytesPerSecond  int64 `json:"lbps"`
	AverageLoadMS       int64 `json:"avgLoadMs"`
	EvictPerSecond      int64 `json:"eps"`
	EvictBytesPerSecond int64 `json:"ebps"`
}

// StorageUsageMetrics is the metrics of storage.
// Active is the size of data which is queried by user at last x times.
type StorageUsageMetrics struct {
	Active int64 `json:"active"`
	Used   int64 `json:"used"`
	Total  int64 `json:"total"`
}

// SystemMetric is the metrics of system.
type SystemMetric struct {
	CPU  CPUMetric    `json:"cpu"`
	Mem  MemoryMetric `json:"mem"`
	Disk DiskMetric   `json:"disk"`
	OSS  OSSMetric    `json:"oss"`
}

// CPU is the metrics of cpu.
type CPUMetric struct {
	Used  int64 `json:"used"`
	Total int64 `json:"total"`
}

// MemoryMetric is the metrics of memory.
type MemoryMetric struct {
	Used  int64 `json:"used"`
	Total int64 `json:"total"`
}

// DiskMetric is the metrics of disk.
type DiskMetric struct {
	RPS    int64 `json:"rps"`
	WPS    int64 `json:"wps"`
	RBPS   int64 `json:"rbps"`
	WBPS   int64 `json:"wbps"`
	RAwait int64 `json:"rawait"`
	WAwait int64 `json:"wawait"`
	Await  int64 `json:"await"`
	SVCTM  int64 `json:"svctm"`
	Util   int64 `json:"util"`
	Used   int64 `json:"used"`
	Total  int64 `json:"total"`
}

// OSSMetric is the metrics of oss.
// Used is the size of oss which is assigned to this query node.
type OSSMetric struct {
	// Performance.
	RPS  int64 `json:"rps"`
	WPS  int64 `json:"wps"`
	RBPS int64 `json:"rbps"`
	WBPS int64 `json:"wbps"`
	Used int64 `json:"used"`
}
