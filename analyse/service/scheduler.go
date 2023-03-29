// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

package service

type schedulerCallback func()
type scheduler struct {
	workItems chan struct{}
}

func (s *scheduler) Go(callback schedulerCallback) {
	go func() {
		s.workItems <- struct{}{}
		callback()
		<-s.workItems
	}()
}

func newAsyncScheduler(max int) *scheduler {
	return &scheduler{
		workItems: make(chan struct{}, max),
	}
}
