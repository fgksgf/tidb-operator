// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/pingcap/tidb-operator/apis/core/v1alpha1"
	"github.com/pingcap/tidb-operator/pkg/client"
	"github.com/pingcap/tidb-operator/pkg/utils/fake"
	"github.com/pingcap/tidb-operator/pkg/utils/task/v3"
)

const (
	newRevision = "new"
	oldRevision = "old"

	fakePDName = "aaa-xxx"
)

func TestTaskStatus(t *testing.T) {
	now := metav1.Now()
	cases := []struct {
		desc          string
		state         *ReconcileContext
		unexpectedErr bool

		expectedStatus task.Status
		expectedObj    *v1alpha1.PD
	}{
		{
			desc: "no pod but healthy",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj(fakePDName, func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						obj.Status.CurrentRevision = "keep"
						return obj
					}),
				},
				Healthy:     true,
				Initialized: true,
				IsLeader:    true,
				MemberID:    fakePDName,
			},

			expectedStatus: task.SWait,
			expectedObj: fake.FakeObj(fakePDName, func(obj *v1alpha1.PD) *v1alpha1.PD {
				obj.Generation = 3
				obj.Labels = map[string]string{
					v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
				}

				obj.Status.ObservedGeneration = 3
				obj.Status.ID = fakePDName
				obj.Status.IsLeader = true
				obj.Status.UpdateRevision = newRevision
				obj.Status.CurrentRevision = "keep"
				obj.Status.Conditions = []metav1.Condition{
					{
						Type:               v1alpha1.PDCondInitialized,
						Status:             metav1.ConditionTrue,
						ObservedGeneration: 3,
						Reason:             "Initialized",
						Message:            "instance is initialized",
					},
					{
						Type:               v1alpha1.CondHealth,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             "Unhealthy",
						Message:            "instance is not healthy",
					},
					{
						Type:               v1alpha1.CondSuspended,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             v1alpha1.ReasonUnsuspended,
						Message:            "instace is not suspended",
					},
				}

				return obj
			}),
		},
		{
			desc: "pod is healthy",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj(fakePDName, func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						return obj
					}),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: oldRevision,
						}
						obj.Status.Phase = corev1.PodRunning
						obj.Status.Conditions = append(obj.Status.Conditions, corev1.PodCondition{
							Type:   corev1.PodReady,
							Status: corev1.ConditionTrue,
						})
						return obj
					}),
				},
				Healthy:     true,
				Initialized: true,
				IsLeader:    true,
				MemberID:    fakePDName,
			},

			expectedStatus: task.SComplete,
			expectedObj: fake.FakeObj(fakePDName, func(obj *v1alpha1.PD) *v1alpha1.PD {
				obj.Generation = 3
				obj.Labels = map[string]string{
					v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
				}

				obj.Status.ObservedGeneration = 3
				obj.Status.ID = fakePDName
				obj.Status.IsLeader = true
				obj.Status.UpdateRevision = newRevision
				obj.Status.CurrentRevision = oldRevision
				obj.Status.Conditions = []metav1.Condition{
					{
						Type:               v1alpha1.PDCondInitialized,
						Status:             metav1.ConditionTrue,
						ObservedGeneration: 3,
						Reason:             "Initialized",
						Message:            "instance is initialized",
					},
					{
						Type:               v1alpha1.CondHealth,
						Status:             metav1.ConditionTrue,
						ObservedGeneration: 3,
						Reason:             "Healthy",
						Message:            "instance is healthy",
					},
					{
						Type:               v1alpha1.CondSuspended,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             v1alpha1.ReasonUnsuspended,
						Message:            "instace is not suspended",
					},
				}

				return obj
			}),
		},
		{
			desc: "pod is deleting",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj(fakePDName, func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						return obj
					}),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						obj.SetDeletionTimestamp(&now)
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: oldRevision,
						}
						obj.Status.Phase = corev1.PodRunning
						obj.Status.Conditions = append(obj.Status.Conditions, corev1.PodCondition{
							Type:   corev1.PodReady,
							Status: corev1.ConditionTrue,
						})
						return obj
					}),
				},
				PodIsTerminating: true,
				Healthy:          true,
				Initialized:      true,
				IsLeader:         true,
				MemberID:         fakePDName,
			},

			expectedStatus: task.SRetry,
			expectedObj: fake.FakeObj(fakePDName, func(obj *v1alpha1.PD) *v1alpha1.PD {
				obj.Generation = 3
				obj.Labels = map[string]string{
					v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
				}

				obj.Status.ObservedGeneration = 3
				obj.Status.ID = fakePDName
				obj.Status.IsLeader = true
				obj.Status.UpdateRevision = newRevision
				obj.Status.Conditions = []metav1.Condition{
					{
						Type:               v1alpha1.PDCondInitialized,
						Status:             metav1.ConditionTrue,
						ObservedGeneration: 3,
						Reason:             "Initialized",
						Message:            "instance is initialized",
					},
					{
						Type:               v1alpha1.CondHealth,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             "Unhealthy",
						Message:            "instance is not healthy",
					},
					{
						Type:               v1alpha1.CondSuspended,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             v1alpha1.ReasonUnsuspended,
						Message:            "instace is not suspended",
					},
				}

				return obj
			}),
		},
		{
			desc: "not init",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj(fakePDName, func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						return obj
					}),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						obj.SetDeletionTimestamp(&now)
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: oldRevision,
						}
						obj.Status.Phase = corev1.PodRunning
						obj.Status.Conditions = append(obj.Status.Conditions, corev1.PodCondition{
							Type:   corev1.PodReady,
							Status: corev1.ConditionTrue,
						})
						return obj
					}),
				},
				Healthy:  true,
				IsLeader: true,
				MemberID: fakePDName,
			},

			expectedStatus: task.SWait,
			expectedObj: fake.FakeObj(fakePDName, func(obj *v1alpha1.PD) *v1alpha1.PD {
				obj.Generation = 3
				obj.Labels = map[string]string{
					v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
				}

				obj.Status.ObservedGeneration = 3
				obj.Status.ID = fakePDName
				obj.Status.IsLeader = true
				obj.Status.CurrentRevision = oldRevision
				obj.Status.UpdateRevision = newRevision
				obj.Status.Conditions = []metav1.Condition{
					{
						Type:               v1alpha1.PDCondInitialized,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             "Uninitialized",
						Message:            "instance has not been initialized yet",
					},
					{
						Type:               v1alpha1.CondHealth,
						Status:             metav1.ConditionTrue,
						ObservedGeneration: 3,
						Reason:             "Healthy",
						Message:            "instance is healthy",
					},
					{
						Type:               v1alpha1.CondSuspended,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             v1alpha1.ReasonUnsuspended,
						Message:            "instace is not suspended",
					},
				}

				return obj
			}),
		},
		{
			desc: "not init and not healthy",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj(fakePDName, func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						return obj
					}),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						obj.SetDeletionTimestamp(&now)
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: oldRevision,
						}
						obj.Status.Phase = corev1.PodRunning
						obj.Status.Conditions = append(obj.Status.Conditions, corev1.PodCondition{
							Type:   corev1.PodReady,
							Status: corev1.ConditionTrue,
						})
						return obj
					}),
				},
				IsLeader: true,
				MemberID: fakePDName,
			},

			expectedStatus: task.SWait,
			expectedObj: fake.FakeObj(fakePDName, func(obj *v1alpha1.PD) *v1alpha1.PD {
				obj.Generation = 3
				obj.Labels = map[string]string{
					v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
				}

				obj.Status.ObservedGeneration = 3
				obj.Status.ID = fakePDName
				obj.Status.IsLeader = true
				obj.Status.UpdateRevision = newRevision
				obj.Status.Conditions = []metav1.Condition{
					{
						Type:               v1alpha1.PDCondInitialized,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             "Uninitialized",
						Message:            "instance has not been initialized yet",
					},
					{
						Type:               v1alpha1.CondHealth,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             "Unhealthy",
						Message:            "instance is not healthy",
					},
					{
						Type:               v1alpha1.CondSuspended,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             v1alpha1.ReasonUnsuspended,
						Message:            "instace is not suspended",
					},
				}

				return obj
			}),
		},
		{
			desc: "failed to update status",
			state: &ReconcileContext{
				State: &state{
					pd: fake.FakeObj(fakePDName, func(obj *v1alpha1.PD) *v1alpha1.PD {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						return obj
					}),
					pod: fake.FakeObj("aaa-pd-xxx", func(obj *corev1.Pod) *corev1.Pod {
						obj.SetDeletionTimestamp(&now)
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: oldRevision,
						}
						obj.Status.Phase = corev1.PodRunning
						obj.Status.Conditions = append(obj.Status.Conditions, corev1.PodCondition{
							Type:   corev1.PodReady,
							Status: corev1.ConditionTrue,
						})
						return obj
					}),
				},
				IsLeader: true,
				MemberID: fakePDName,
			},
			unexpectedErr: true,

			expectedStatus: task.SFail,
		},
	}

	for i := range cases {
		c := &cases[i]
		t.Run(c.desc, func(tt *testing.T) {
			tt.Parallel()

			var objs []client.Object
			objs = append(objs, c.state.PD())
			if c.state.Pod() != nil {
				objs = append(objs, c.state.Pod())
			}
			fc := client.NewFakeClient(objs...)
			if c.unexpectedErr {
				fc.WithError("*", "*", errors.NewInternalError(fmt.Errorf("fake internal err")))
			}

			ctx := context.Background()
			res, done := task.RunTask(ctx, TaskStatus(c.state, fc))
			assert.Equal(tt, c.expectedStatus.String(), res.Status().String(), c.desc)
			assert.False(tt, done, c.desc)

			// no need to check update result
			if c.unexpectedErr {
				return
			}

			obj := &v1alpha1.PD{}
			require.NoError(tt, fc.Get(ctx, client.ObjectKey{Name: fakePDName}, obj), c.desc)
			conds := obj.Status.Conditions
			for i := range conds {
				cond := &conds[i]
				cond.LastTransitionTime = metav1.Time{}
			}
			assert.Equal(tt, c.expectedObj, obj, c.desc)
		})
	}
}