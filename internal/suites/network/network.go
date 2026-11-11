package network

import (
	"context"
	"fmt"
	"net/netip"
	"path/filepath"
	"time"

	"github.com/harvester/hvperf/internal/suites/options"
	"github.com/harvester/hvperf/pkg/k8s"
	"github.com/harvester/hvperf/pkg/suites"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

func init() {
	suites.Register(NewNetworkSuite())
}

var _ suites.Suite = &NetworkSuite{}

type NetworkSuite struct {
	suites.SuiteMarshaler
	*suites.ProgressReporter
	*suites.Clients
}

func NewNetworkSuite() *NetworkSuite {
	n := &NetworkSuite{}
	n.Marshal = n
	return n
}

func (s *NetworkSuite) Name() string {
	return "network-benchmark"
}

func (s *NetworkSuite) Description() string {
	return "run netperf and iperf3 to benchmark the in-cluster network latency and throughput"
}

func (s *NetworkSuite) IsExclusive() bool {
	return true
}

func (s *NetworkSuite) OptionsSchema() suites.Options {
	return options.DefaultNetworkOptions()
}

func (s *NetworkSuite) SetClients(clients *suites.Clients) {
	s.Clients = clients
}

func (s *NetworkSuite) SetProgressReporter(reporter *suites.ProgressReporter) {
	s.ProgressReporter = reporter
}

func (s *NetworkSuite) RunE(
	ctx context.Context,
	runID string,
	namespace string,
	opts suites.Options,
) suites.SuiteResult {
	o, err := NewNetworkOptions(opts)
	if err != nil {
		return suites.NewSuiteResult(s.Name(), runID).WithError(err)
	}

	if _, err := k8s.EnsureNamespace(ctx, s.Clients, namespace, o.NamespaceReadyTimeout); err != nil {
		return suites.SuiteResult{
			Name:  s.Name(),
			RunID: runID,
			Err:   fmt.Sprintf("namespace %s not ready: %v", namespace, err.Error()),
		}
	}
	klog.V(3).InfoS("namespace is ready\n", "name", namespace)

	testSuites := []struct {
		name    string
		profile ProfileName
		fn      BenchmarkFunc
	}{
		{
			name:    "netperf",
			profile: ProfileTCPStream,
			fn:      s.netperf,
		},
		{
			name:    "netperf",
			profile: ProfileTCPCRR,
			fn:      s.netperf,
		},
		{
			name:    "netperf",
			profile: ProfileTCPRR,
			fn:      s.netperf,
		},
		{
			name:    "netperf",
			profile: ProfileUDPStream,
			fn:      s.netperf,
		},
		{
			name:    "netperf",
			profile: ProfileUDPRR,
			fn:      s.netperf,
		},
		{
			name:    "iperf3",
			profile: ProfileTCPStream,
			fn:      s.iperf3,
		},
		{
			name:    "iperf3",
			profile: ProfileUDPStream,
			fn:      s.iperf3,
		},
	}

	caseResults := []*suites.CaseResult{}
	for _, testCase := range testSuites {
		tcName := fmt.Sprintf("%s %s", testCase.name, testCase.profile)
		s.CaseStart(s.Name(), tcName)
		caseResult := testCase.fn(ctx, tcName, runID, namespace, testCase.profile, o)
		caseResults = append(caseResults, caseResult)
		s.CaseDone(s.Name(), tcName, caseResult.State, time.Since(caseResult.DateTimeStart))
	}

	suiteParams, err := suites.ToSuiteParams(o)
	if err != nil {
		// just log the params conversion error, don't fail the suite. the only effect
		// is that the suite params won't be reported in the results.
		klog.V(3).ErrorS(err, "failed to convert suite options to suite params\n")
	}

	return suites.NewSuiteResult(s.Name(), runID).WithParams(suiteParams).WithResults(caseResults)
}

type NetworkOptions struct {
	JobActiveDeadline      time.Duration
	JobPodImageName        string
	JobPodImageTag         string
	JobPodNode             string
	JobPodTTLAfterFinished time.Duration
	JobPodReadyTimeout     time.Duration
	JobSuspend             bool
	NamespaceReadyTimeout  time.Duration

	IPerfBinLocalPath           string
	IPerfClientMessageSizeBytes int
	IPerfClientParallelStreams  int
	IPerfIPFamily               IPFamilyOption
	IPerfOutputFormat           string
	IPerfRemoteCopyTargetDir    string
	IPerfServerPort             int
	IPerfTestDurationSeconds    int

	NetPerfIPFamily             IPFamilyOption
	NetPerfClientLocalPath      string
	NetPerfOutputFormat         string
	NetPerfServerLocalPath      string
	NetPerfRemoteCopyTargetDir  string
	NetPerfTestStartWaitSeconds int
	NetPerfTestLengthSeconds    int
}

func NewNetworkOptions(override suites.Options) (*NetworkOptions, error) {
	merged := options.DefaultGlobalOptions().
		Merge(options.DefaultNetworkOptions()).
		Merge(override)

	if err := merged.SetToolPaths(func(toolsDir string) {
		merged["netPerfClientLocalPath"] = filepath.Join(toolsDir, "netperf")
		merged["netPerfServerLocalPath"] = filepath.Join(toolsDir, "netserver")
		merged["iPerfBinLocalPath"] = filepath.Join(toolsDir, "iperf3")
	}); err != nil {
		return nil, fmt.Errorf("failed to set tool paths: %w", err)
	}

	defaultOutputOpts := func() {
		merged["netPerfOutputFormat"] = "-O"
	}
	jsonOutputOpts := func() {
		merged["iPerfOutputFormat"] = "-J"
		merged["netPerfOutputFormat"] = "-k"
	}
	if err := merged.SetOutputFormat(defaultOutputOpts, jsonOutputOpts); err != nil {
		return nil, fmt.Errorf("failed to set output format: %w", err)
	}

	return options.FromOptions[*NetworkOptions](merged)
}

func podIPByIPFamily(pod *corev1.Pod, ipFamily IPFamilyOption) (string, error) {
	var serverIP string
LOOP:
	for _, ip := range pod.Status.PodIPs {
		addr, err := netip.ParseAddr(ip.IP)
		if err != nil {
			return "", fmt.Errorf("failed to parse server pod IP '%s': %w", ip.IP, err)
		}

		switch {
		case ipFamily == IPv4 && addr.Is4():
			serverIP = ip.IP
			break LOOP
		case ipFamily == IPv6 && addr.Is6():
			serverIP = ip.IP
			break LOOP
		}
	}

	if serverIP == "" {
		return "", fmt.Errorf("no server pod IP found for requested IP family '%s'", ipFamily)
	}

	return serverIP, nil
}

type (
	BenchmarkFunc         func(context.Context, string, string, string, ProfileName, *NetworkOptions) *suites.CaseResult
	BenchmarkWorkloadType string

	// IPFamilyOption corresponds to the -4 or -6 option in netperf, which specifies
	// whether to use IPv4 or IPv6.
	IPFamilyOption string

	// ProfileName is the name of a netperf test profiles as defined in
	// https://hewlettpackard.github.io/netperf/doc/netperf.html#Options-common-to-TCP-UDP-and-SCTP-tests
	ProfileName string
)

const (
	BenchmarkClient BenchmarkWorkloadType = "client"
	BenchmarkServer BenchmarkWorkloadType = "server"

	IPv4 IPFamilyOption = "-4"
	IPv6 IPFamilyOption = "-6"

	ProfileTCPCRR    ProfileName = "TCP_CRR"
	ProfileTCPRR     ProfileName = "TCP_RR"
	ProfileTCPStream ProfileName = "TCP_STREAM"
	ProfileUDPRR     ProfileName = "UDP_RR"
	ProfileUDPStream ProfileName = "UDP_STREAM"
)
