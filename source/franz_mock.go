// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/conduitio/conduit-connector-kafka/source (interfaces: Client)
//
// Generated by this command:
//
//	mockgen -destination franz_mock.go -package source -mock_names=Client=MockClient . Client
//

// Package source is a generated GoMock package.
package source

import (
	context "context"
	reflect "reflect"

	kgo "github.com/twmb/franz-go/pkg/kgo"
	gomock "go.uber.org/mock/gomock"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockClient) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockClientMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockClient)(nil).Close))
}

// CommitRecords mocks base method.
func (m *MockClient) CommitRecords(arg0 context.Context, arg1 ...*kgo.Record) error {
	m.ctrl.T.Helper()
	varargs := []any{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "CommitRecords", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// CommitRecords indicates an expected call of CommitRecords.
func (mr *MockClientMockRecorder) CommitRecords(arg0 any, arg1 ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CommitRecords", reflect.TypeOf((*MockClient)(nil).CommitRecords), varargs...)
}

// OptValue mocks base method.
func (m *MockClient) OptValue(arg0 any) any {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "OptValue", arg0)
	ret0, _ := ret[0].(any)
	return ret0
}

// OptValue indicates an expected call of OptValue.
func (mr *MockClientMockRecorder) OptValue(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "OptValue", reflect.TypeOf((*MockClient)(nil).OptValue), arg0)
}

// PollFetches mocks base method.
func (m *MockClient) PollFetches(arg0 context.Context) kgo.Fetches {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PollFetches", arg0)
	ret0, _ := ret[0].(kgo.Fetches)
	return ret0
}

// PollFetches indicates an expected call of PollFetches.
func (mr *MockClientMockRecorder) PollFetches(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PollFetches", reflect.TypeOf((*MockClient)(nil).PollFetches), arg0)
}