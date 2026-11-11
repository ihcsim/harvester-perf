package network

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	"github.com/harvester/hvperf/pkg/k8s"
	"github.com/harvester/hvperf/pkg/suites"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

func (s *NetworkSuite) netperf(
	ctx context.Context,
	caseName string,
	runID string,
	namespace string,
	profile ProfileName,
	o *NetworkOptions,
) *suites.CaseResult {
	var (
		cmdResults    []*suites.CmdResult
		metricResults []*suites.MetricResult
		objs          []runtime.Object
	)
	start := time.Now()
	serverCmdResults, serverJob, serverPod, err := s.netperfServer(
		ctx,
		runID,
		namespace,
		profile,
		o,
	)
	if err != nil {
		return suites.NewCaseResultErrored(caseName, start, time.Now(), fmt.Errorf("failed to setup netperf server job: %w", err))
	}
	cmdResults = append(cmdResults, serverCmdResults...)
	objs = append(objs, serverJob, serverPod)

	// if the server job failed, we don't need to run the client job, so we can
	// return early with all the results we have so far.
	for _, r := range serverCmdResults {
		if r.Err != "" {
			return suites.NewCaseResult(caseName, start, time.Now(), cmdResults, nil, objs...)
		}
	}

	// look for the pod IP address that matches the requested IP family. if we can't
	// find one, return an error.
	serverIP, err := podIPByIPFamily(serverPod, o.NetPerfIPFamily)
	if err != nil {
		return suites.NewCaseResult(caseName, start, time.Now(), cmdResults, nil, objs...).
			WithError(err)
	}

	clientCmdResults, clientJob, clientPod, err := s.netperfClient(
		ctx,
		runID,
		namespace,
		serverIP,
		profile,
		o,
	)
	if err != nil {
		return suites.NewCaseResult(caseName, start, time.Now(), cmdResults, nil, objs...).
			WithError(fmt.Errorf("failed to setup netperf client job: %w", err))
	}
	cmdResults = append(cmdResults, clientCmdResults...)
	objs = append(objs, clientJob, clientPod)
	return suites.NewCaseResult(caseName, start, time.Now(), cmdResults, metricResults, objs...)
}

func (s *NetworkSuite) netperfServer(
	ctx context.Context,
	runID string,
	namespace string,
	profile ProfileName,
	o *NetworkOptions,
) (
	[]*suites.CmdResult,
	*batchv1.Job,
	*corev1.Pod,
	error,
) {
	job, pod, err := s.netperfSetup(ctx, runID, namespace, profile, BenchmarkServer, o)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to setup netperf server job: %w", err)
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(3*(o.NetPerfTestLengthSeconds+o.NetPerfTestStartWaitSeconds))*time.Second)
	defer cancel()

	var cmdResult *suites.CmdResult
	cmd := []string{"netserver", "-d", string(o.NetPerfIPFamily)}
	out, err := k8s.ExecPod(ctxWithTimeout, s.Clients, pod, o.DefaultPrimaryContainerName, cmd)
	if err != nil {
		// netperf server's stdout and stderr are interesting only if there is an error
		cmdResult = suites.NewCmdResult(cmd, pod.Spec.NodeName, out.Stdout, out.Stderr, err)
	} else {
		cmdResult = suites.NewCmdResult(cmd, pod.Spec.NodeName, "", "", nil)
	}

	return []*suites.CmdResult{cmdResult}, job, pod, nil
}

func (s *NetworkSuite) netperfClient(
	ctx context.Context,
	runID string,
	namespace string,
	serverIP string,
	profile ProfileName,
	o *NetworkOptions,
) (
	[]*suites.CmdResult,
	*batchv1.Job,
	*corev1.Pod,
	error,
) {
	job, pod, err := s.netperfSetup(ctx, runID, namespace, profile, BenchmarkClient, o)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to setup netperf client job: %w", err)
	}

	timeout := 3 * (o.NetPerfTestLengthSeconds + o.NetPerfTestStartWaitSeconds)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()

	cmd := []string{
		"netperf",
		"-s", strconv.Itoa(o.NetPerfTestStartWaitSeconds),
		"-H", serverIP,
		"-l", strconv.Itoa(o.NetPerfTestLengthSeconds),
		"-t", string(profile),
		"--",
	}
	cmd = append(cmd, o.NetPerfOutputFormat)
	cmd = append(cmd, NetPerfProfileParams[profile]...)
	out, err := k8s.ExecPod(ctxWithTimeout, s.Clients, pod, o.DefaultPrimaryContainerName, cmd)

	// if the test output is rendered as json, we need to replace the newlines with
	// | so that the output can be parsed as a value of a json object.
	stdout := out.Stdout
	if stdout != "" && o.NetPerfOutputFormat == "-k" {
		stdout = strings.ReplaceAll(stdout, "\n", "|")
		stdout = fmt.Sprintf(`{"out":"%s"}`, stdout)
	}

	cmdResult := suites.NewCmdResult(
		cmd,
		pod.Spec.NodeName,
		stdout,
		out.Stderr,
		err,
	)
	return []*suites.CmdResult{cmdResult}, job, pod, nil
}

func (s *NetworkSuite) netperfSetup(
	ctx context.Context,
	runID string,
	namespace string,
	profile ProfileName,
	workloadType BenchmarkWorkloadType,
	o *NetworkOptions,
) (
	*batchv1.Job,
	*corev1.Pod,
	error,
) {
	var netperfBin string
	switch workloadType {
	case BenchmarkServer:
		netperfBin = o.NetPerfServerLocalPath
	case BenchmarkClient:
		netperfBin = o.NetPerfClientLocalPath
	}

	overrides := func(job *batchv1.Job) error {
		job.Labels["harvesterhci.io/netperf-workload-type"] = string(workloadType)
		job.Labels["harvesterhci.io/netperf-profile"] = string(profile)
		job.Spec.Template.Spec.HostNetwork = false
		if job.Spec.Template.Labels == nil {
			job.Spec.Template.Labels = map[string]string{}
		}
		job.Spec.Template.Labels["harvesterhci.io/netperf-workload-type"] = string(workloadType)
		job.Spec.Template.Labels["harvesterhci.io/netperf-profile"] = string(profile)
		return nil
	}

	job, pod, err := k8s.EnsureJobReady(
		ctx,
		s.Clients,
		s.Name(),
		runID,
		namespace,
		o.JobPodNode,
		o.JobPodImageName+":"+o.JobPodImageTag,
		o.JobActiveDeadline,
		o.JobPodReadyTimeout,
		o.JobPodTTLAfterFinished,
		o.JobSuspend,
		overrides,
	)
	if err != nil {
		return nil, nil, err
	}
	klog.V(3).Infof("pod:'%s/%s' is now ready, phase:'%s'\n", pod.GetNamespace(), pod.GetName(), pod.Status.Phase)

	klog.V(3).Infof("copying '%s' to pod '%s/%s'\n", netperfBin, pod.GetNamespace(), pod.GetName())
	if err := k8s.CopyToJobPod(
		ctx,
		s.Clients,
		pod,
		o.DefaultPrimaryContainerName,
		o.NetPerfRemoteCopyTargetDir,
		netperfBin,
	); err != nil {
		return nil, nil, err
	}
	return job, pod, nil
}

// NetPerfProfileParams are parameters for each test profile derived from k8s-netperf's
// implementation
var NetPerfProfileParams = map[ProfileName][]string{
	ProfileTCPCRR:    {"THROUGHPUT,THROUGHPUT_UNITS,MIN_LATENCY,MEAN_LATENCY,P50_LATENCY,P90_LATENCY,P99_LATENCY,MAX_LATENCY,STDDEV_LATENCY,LOCAL_TRANSPORT_RETRANS", "-r", "1024,1024", "-P", "42424"},
	ProfileTCPRR:     {"THROUGHPUT,THROUGHPUT_UNITS,MIN_LATENCY,MEAN_LATENCY,P50_LATENCY,P90_LATENCY,P99_LATENCY,MAX_LATENCY,STDDEV_LATENCY,LOCAL_TRANSPORT_RETRANS", "-r", "1024,1024", "-P", "42424"},
	ProfileTCPStream: {"THROUGHPUT,THROUGHPUT_UNITS,LOCAL_SEND_CALLS,REMOTE_RECV_CALLS,LOCAL_TRANSPORT_RETRANS", "-m", "1024", "-P", "42424"},
	ProfileUDPRR:     {"THROUGHPUT,THROUGHPUT_UNITS,MIN_LATENCY,MEAN_LATENCY,P50_LATENCY,P90_LATENCY,P99_LATENCY,MAX_LATENCY,STDDEV_LATENCY,LOCAL_TRANSPORT_RETRANS", "-r", "1024,1024", "-P", "42424"},
	ProfileUDPStream: {"THROUGHPUT,THROUGHPUT_UNITS,LOCAL_SEND_CALLS,REMOTE_RECV_CALLS,LOCAL_TRANSPORT_RETRANS", "-m", "1024", "-R", "1", "-P", "42424"},
}
