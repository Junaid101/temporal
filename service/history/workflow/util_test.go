package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
)

func TestUpdateCallbackRequestIDEventTypeUsesLegacyStateWithoutCHASMRoot(t *testing.T) {
	ctrl := gomock.NewController(t)
	mutableState := historyi.NewMockMutableState(ctrl)
	const requestID = "legacy-request-id"
	mutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RequestIds: map[string]*persistencespb.RequestIDInfo{
			requestID: {
				EventId:   42,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
			},
		},
	})
	mutableState.EXPECT().ChasmEnabled().Return(false)

	eventType, found, err := (MutableStateWithEffects{MutableState: mutableState}).UpdateCallbackRequestIDEventType(
		"update-id",
		requestID,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, eventType)
}

func TestForceTerminateWorkflowRecordsMetric(t *testing.T) {
	ctrl := gomock.NewController(t)
	mutableState := historyi.NewMockMutableState(ctrl)
	mutableState.EXPECT().GetStartedWorkflowTask().Return(nil)
	mutableState.EXPECT().AddWorkflowExecutionTerminatedEvent(
		"force terminate",
		nil,
		consts.IdentityHistoryService,
		false,
		nil,
	).Return(&historypb.HistoryEvent{}, nil)
	mutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		nil,
		"active",
	))

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	err := ForceTerminateWorkflow(
		mutableState,
		"force terminate",
		nil,
		consts.IdentityHistoryService,
		false,
		nil,
		metricsHandler,
		chasm.ExecutionForceTerminationReasonEventBatchSizeExceedsLimit,
	)
	require.NoError(t, err)

	recordings := capture.Snapshot()[metrics.ExecutionForceTerminations.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, int64(1), recordings[0].Value)
	require.Equal(t, "test-namespace", recordings[0].Tags["namespace"])
	require.Equal(t, string(chasm.WorkflowArchetype), recordings[0].Tags["archetype"])
	require.Equal(t, string(chasm.ExecutionForceTerminationReasonEventBatchSizeExceedsLimit), recordings[0].Tags["reason"])
}
