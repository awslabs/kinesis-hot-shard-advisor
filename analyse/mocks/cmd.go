// Code generated by MockGen. DO NOT EDIT.
// Source: analyse/cmd.go

// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"

	service "github.com/awslabs/kinesis-hot-shard-advisor/analyse/service"
	gomock "github.com/golang/mock/gomock"
)

// Mockdiscover is a mock of discover interface.
type Mockdiscover struct {
	ctrl     *gomock.Controller
	recorder *MockdiscoverMockRecorder
}

// MockdiscoverMockRecorder is the mock recorder for Mockdiscover.
type MockdiscoverMockRecorder struct {
	mock *Mockdiscover
}

// NewMockdiscover creates a new mock instance.
func NewMockdiscover(ctrl *gomock.Controller) *Mockdiscover {
	mock := &Mockdiscover{ctrl: ctrl}
	mock.recorder = &MockdiscoverMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Mockdiscover) EXPECT() *MockdiscoverMockRecorder {
	return m.recorder
}

// ParentShards mocks base method.
func (m *Mockdiscover) ParentShards(ctx context.Context) ([]string, int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ParentShards", ctx)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(int)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// ParentShards indicates an expected call of ParentShards.
func (mr *MockdiscoverMockRecorder) ParentShards(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ParentShards", reflect.TypeOf((*Mockdiscover)(nil).ParentShards), ctx)
}

// Mockefo is a mock of efo interface.
type Mockefo struct {
	ctrl     *gomock.Controller
	recorder *MockefoMockRecorder
}

// MockefoMockRecorder is the mock recorder for Mockefo.
type MockefoMockRecorder struct {
	mock *Mockefo
}

// NewMockefo creates a new mock instance.
func NewMockefo(ctrl *gomock.Controller) *Mockefo {
	mock := &Mockefo{ctrl: ctrl}
	mock.recorder = &MockefoMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Mockefo) EXPECT() *MockefoMockRecorder {
	return m.recorder
}

// DeregisterConsumer mocks base method.
func (m *Mockefo) DeregisterConsumer(streamArn, consumerArn *string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeregisterConsumer", streamArn, consumerArn)
	ret0, _ := ret[0].(error)
	return ret0
}

// DeregisterConsumer indicates an expected call of DeregisterConsumer.
func (mr *MockefoMockRecorder) DeregisterConsumer(streamArn, consumerArn interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeregisterConsumer", reflect.TypeOf((*Mockefo)(nil).DeregisterConsumer), streamArn, consumerArn)
}

// EnsureEFOConsumer mocks base method.
func (m *Mockefo) EnsureEFOConsumer(ctx context.Context) (*string, *string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "EnsureEFOConsumer", ctx)
	ret0, _ := ret[0].(*string)
	ret1, _ := ret[1].(*string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// EnsureEFOConsumer indicates an expected call of EnsureEFOConsumer.
func (mr *MockefoMockRecorder) EnsureEFOConsumer(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "EnsureEFOConsumer", reflect.TypeOf((*Mockefo)(nil).EnsureEFOConsumer), ctx)
}

// Mockoutput is a mock of output interface.
type Mockoutput struct {
	ctrl     *gomock.Controller
	recorder *MockoutputMockRecorder
}

// MockoutputMockRecorder is the mock recorder for Mockoutput.
type MockoutputMockRecorder struct {
	mock *Mockoutput
}

// NewMockoutput creates a new mock instance.
func NewMockoutput(ctrl *gomock.Controller) *Mockoutput {
	mock := &Mockoutput{ctrl: ctrl}
	mock.recorder = &MockoutputMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Mockoutput) EXPECT() *MockoutputMockRecorder {
	return m.recorder
}

// Write mocks base method.
func (m *Mockoutput) Write(aggregatedShards []*service.ProcessOutput) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", aggregatedShards)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write.
func (mr *MockoutputMockRecorder) Write(aggregatedShards interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*Mockoutput)(nil).Write), aggregatedShards)
}

// MockshardProcessor is a mock of shardProcessor interface.
type MockshardProcessor struct {
	ctrl     *gomock.Controller
	recorder *MockshardProcessorMockRecorder
}

// MockshardProcessorMockRecorder is the mock recorder for MockshardProcessor.
type MockshardProcessorMockRecorder struct {
	mock *MockshardProcessor
}

// NewMockshardProcessor creates a new mock instance.
func NewMockshardProcessor(ctrl *gomock.Controller) *MockshardProcessor {
	mock := &MockshardProcessor{ctrl: ctrl}
	mock.recorder = &MockshardProcessorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockshardProcessor) EXPECT() *MockshardProcessorMockRecorder {
	return m.recorder
}

// Process mocks base method.
func (m *MockshardProcessor) Process(ctx context.Context, consumerArn string, parentShardIDs []string, children bool, progress func()) ([]*service.ProcessOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Process", ctx, consumerArn, parentShardIDs, children, progress)
	ret0, _ := ret[0].([]*service.ProcessOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Process indicates an expected call of Process.
func (mr *MockshardProcessorMockRecorder) Process(ctx, consumerArn, parentShardIDs, children, progress interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Process", reflect.TypeOf((*MockshardProcessor)(nil).Process), ctx, consumerArn, parentShardIDs, children, progress)
}
