package main

import (
	"flag"
	"io"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/version"
)

const (
	namespace = "spark"
)

var (
	executorLabelNames    = []string{"executor_id"}
	applicationLabelNames = []string{"app_id"}
)

func newGaugeExecutorMetrics(metricName string, docString string, constLabels prometheus.Labels) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "executor_" + metricName,
			Help:        docString,
			ConstLabels: constLabels,
		},
		executorLabelNames,
	)
}

func newCounterExecutorMetrics(metricName string, docString string, constLabels prometheus.Labels) *prometheus.CounterVec {
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace:   namespace,
			Name:        "executor_" + metricName,
			Help:        docString,
			ConstLabels: constLabels,
		},
		executorLabelNames,
	)
}

func newApplicationMetrics(metricName string, docString string, constLabels prometheus.Labels) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "application_" + metricName,
			Help:        docString,
			ConstLabels: constLabels,
		},
		applicationLabelNames,
	)
}

var (
	executorGaugeMetrics = []*prometheus.GaugeVec{
		newGaugeExecutorMetrics("active_tasks", "Current number of active tasks", nil),
	}
	executorCounterMetrics = []*prometheus.CounterVec{
		newCounterExecutorMetrics("completedTasks", "Current number of active tasks", nil),
	}
)

// Exporter collects Spark stats from the given URI and exports them using
// the prometheus metrics package.
type Exporter struct {
	URI   string
	mutex sync.RWMutex
	fetch func() (io.ReadCloser, error)

	up prometheus.Gauge
}

func NewExporter(uri string, timeout time.Duration) (*Exporter, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}

	var fetch func() (io.ReadCloser, error)
	fetch = fetchHTTPApi(uri, timeout)

	return &Exporter{
		URI:   uri,
		fetch: fetch,
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "up",
			Help:      "Was the last scrape to Spark successful.",
		}),
	}, nil
}

func fetchHTTPApi(uri string, timeout time.Duration) {

}

// ClusterApplicationsInfo holds all applications metrics
type ClusterApplicationsInfo struct {
	Applications []ApplicationMetrics
}

// ApplicationInfo holds all application metrics including executors information
type ApplicationInfo struct {
	Attempts []struct {
		Completed bool   `json:"completed"`
		EndTime   string `json:"endTime"`
		SparkUser string `json:"sparkUser"`
		StartTime string `json:"startTime"`
	} `json:"attempts"`
	ID        string `json:"id"`
	Name      string `json:"name"`
	Executors []ExecutorMetrics
}

// ExecutorInfo holds all executor metrics it's used on each application
type ExecutorInfo struct {
	ActiveTasks    int `json:"activeTasks"`
	CompletedTasks int `json:"completedTasks"`
	DiskUsed       int `json:"diskUsed"`
	ExecutorLogs   struct {
		Stderr string `json:"stderr"`
		Stdout string `json:"stdout"`
	} `json:"executorLogs"`
	FailedTasks       int    `json:"failedTasks"`
	HostPort          string `json:"hostPort"`
	ID                string `json:"id"`
	MaxMemory         int64  `json:"maxMemory"`
	MemoryUsed        int    `json:"memoryUsed"`
	RddBlocks         int    `json:"rddBlocks"`
	TotalDuration     int    `json:"totalDuration"`
	TotalInputBytes   int    `json:"totalInputBytes"`
	TotalShuffleRead  int    `json:"totalShuffleRead"`
	TotalShuffleWrite int    `json:"totalShuffleWrite"`
	TotalTasks        int    `json:"totalTasks"`
}

func main() {
	var (
		listenAddress       = flag.String("web.listen-address", ":9110", "Address to listen on for web interface and telemetry.")
		metricsPath         = flag.String("web.telemetry-path", "/metrics", "Path under which to expose metrics.")
		sparkApplicationURI = flag.String("spark.application-uri", "http://localhost:4040", "URI on which to scrape Spark application metrics")
		sparkTimeout        = flag.Duration("spark.timeout", 5*time.Second, "Timeout for trying to get stats from Spark application")
	)
	flag.Parse()

	log.Infoln("Starting spark_exporter", version.Info())
	log.Infoln("Build context", version.BuildContext())

	exporter, err := NewExporter(*sparkApplicationURI, *sparkTimeout)
	if err != nil {
		log.Fatal(err)
	}
	prometheus.MustRegister(exporter)
	prometheus.MustRegister(version.NewCollector("spark_exporter"))

	log.Infoln("Listening on", *listenAddress)
	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
             <head><title>Spark Exporter</title></head>
             <body>
             <h1>Spark Exporter</h1>
             <p><a href='` + *metricsPath + `'>Metrics</a></p>
             </body>
             </html>`))
	})
	log.Fatal(http.ListenAndServe(*listenAddress, nil))

}
