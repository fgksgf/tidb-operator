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
	pdv1 "github.com/pingcap/tidb-operator/pkg/timanager/apis/pd/v1"
	"github.com/pingcap/tidb-operator/pkg/utils/fake"
	"github.com/pingcap/tidb-operator/pkg/utils/task/v3"
)

const (
	newRevision = "new"
	oldRevision = "old"

	fakeTiKVName = "aaa-xxx"
)

func TestTaskStatus(t *testing.T) {
	now := metav1.Now()
	cases := []struct {
		desc          string
		state         *ReconcileContext
		unexpectedErr bool

		expectedStatus task.Status
		expectedObj    *v1alpha1.TiKV
	}{
		{
			desc: "no pod but healthy",
			state: &ReconcileContext{
				State: &state{
					tikv: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						obj.Status.CurrentRevision = "keep"
						obj.Status.ObservedGeneration = 3
						return obj
					}),
				},
				IsPDAvailable: true,
				Store:         &pdv1.Store{},
				StoreID:       fakeTiKVName,
				StoreState:    v1alpha1.StoreStateServing,
			},

			expectedStatus: task.SWait,
			expectedObj: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
				obj.Generation = 3
				obj.Labels = map[string]string{
					v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
				}

				obj.Status.ObservedGeneration = 3
				obj.Status.ID = fakeTiKVName
				obj.Status.State = v1alpha1.StoreStateServing
				obj.Status.UpdateRevision = newRevision
				obj.Status.CurrentRevision = "keep"
				obj.Status.Conditions = []metav1.Condition{
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
					{
						Type:               v1alpha1.TiKVCondLeadersEvicted,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             "NotEvicted",
						Message:            "leaders are not all evicted",
					},
				}

				return obj
			}),
		},
		{
			desc: "pod is healthy",
			state: &ReconcileContext{
				State: &state{
					tikv: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						return obj
					}),
					pod: fake.FakeObj("aaa-tikv-xxx", func(obj *corev1.Pod) *corev1.Pod {
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
				IsPDAvailable: true,
				Store:         &pdv1.Store{},
				StoreID:       fakeTiKVName,
				StoreState:    v1alpha1.StoreStateServing,
			},

			expectedStatus: task.SComplete,
			expectedObj: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
				obj.Generation = 3
				obj.Labels = map[string]string{
					v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
				}

				obj.Status.ObservedGeneration = 3
				obj.Status.ID = fakeTiKVName
				obj.Status.State = v1alpha1.StoreStateServing
				obj.Status.UpdateRevision = newRevision
				obj.Status.CurrentRevision = oldRevision
				obj.Status.Conditions = []metav1.Condition{
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
					{
						Type:               v1alpha1.TiKVCondLeadersEvicted,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             "NotEvicted",
						Message:            "leaders are not all evicted",
					},
				}

				return obj
			}),
		},
		{
			desc: "pod is deleting",
			state: &ReconcileContext{
				State: &state{
					tikv: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						return obj
					}),
					pod: fake.FakeObj("aaa-tikv-xxx", func(obj *corev1.Pod) *corev1.Pod {
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
				IsPDAvailable:    true,
				Store:            &pdv1.Store{},
				StoreID:          fakeTiKVName,
				PodIsTerminating: true,
			},

			expectedStatus: task.SRetry,
			expectedObj: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
				obj.Generation = 3
				obj.Labels = map[string]string{
					v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
				}

				obj.Status.ObservedGeneration = 3
				obj.Status.ID = fakeTiKVName
				obj.Status.UpdateRevision = newRevision
				obj.Status.Conditions = []metav1.Condition{
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
					{
						Type:               v1alpha1.TiKVCondLeadersEvicted,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             "NotEvicted",
						Message:            "leaders are not all evicted",
					},
				}

				return obj
			}),
		},
		{
			desc: "not serving",
			state: &ReconcileContext{
				State: &state{
					tikv: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						return obj
					}),
					pod: fake.FakeObj("aaa-tikv-xxx", func(obj *corev1.Pod) *corev1.Pod {
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
				IsPDAvailable: true,
				Store:         &pdv1.Store{},
				StoreID:       fakeTiKVName,
			},

			expectedStatus: task.SWait,
			expectedObj: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
				obj.Generation = 3
				obj.Labels = map[string]string{
					v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
				}

				obj.Status.ObservedGeneration = 3
				obj.Status.ID = fakeTiKVName
				obj.Status.UpdateRevision = newRevision
				obj.Status.Conditions = []metav1.Condition{
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
					{
						Type:               v1alpha1.TiKVCondLeadersEvicted,
						Status:             metav1.ConditionFalse,
						ObservedGeneration: 3,
						Reason:             "NotEvicted",
						Message:            "leaders are not all evicted",
					},
				}

				return obj
			}),
		},
		{
			desc: "evicting and pd is avail",
			state: &ReconcileContext{
				State: &state{
					tikv: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						return obj
					}),
					pod: fake.FakeObj("aaa-tikv-xxx", func(obj *corev1.Pod) *corev1.Pod {
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
				IsPDAvailable:  true,
				LeaderEvicting: true,
				Store:          &pdv1.Store{},
				StoreID:        fakeTiKVName,
				StoreState:     v1alpha1.StoreStateServing,
			},

			expectedStatus: task.SWait,
			expectedObj: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
				obj.Generation = 3
				obj.Labels = map[string]string{
					v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
				}

				obj.Status.ObservedGeneration = 3
				obj.Status.ID = fakeTiKVName
				obj.Status.State = v1alpha1.StoreStateServing
				obj.Status.CurrentRevision = oldRevision
				obj.Status.UpdateRevision = newRevision
				obj.Status.Conditions = []metav1.Condition{
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
					{
						Type:               v1alpha1.TiKVCondLeadersEvicted,
						Status:             metav1.ConditionTrue,
						ObservedGeneration: 3,
						Reason:             "Evicted",
						Message:            "all leaders are evicted",
					},
				}

				return obj
			}),
		},
		{
			desc: "pd is avail and no store",
			state: &ReconcileContext{
				State: &state{
					tikv: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						return obj
					}),
					pod: fake.FakeObj("aaa-tikv-xxx", func(obj *corev1.Pod) *corev1.Pod {
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
				IsPDAvailable: true,
				StoreID:       fakeTiKVName,
			},

			expectedStatus: task.SWait,
			expectedObj: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
				obj.Generation = 3
				obj.Labels = map[string]string{
					v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
				}

				obj.Status.ObservedGeneration = 3
				obj.Status.ID = fakeTiKVName
				obj.Status.UpdateRevision = newRevision
				obj.Status.Conditions = []metav1.Condition{
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
					{
						Type:               v1alpha1.TiKVCondLeadersEvicted,
						Status:             metav1.ConditionTrue,
						ObservedGeneration: 3,
						Reason:             "StoreIsRemoved",
						Message:            "store does not exist",
					},
				}

				return obj
			}),
		},
		{
			desc: "failed to update status",
			state: &ReconcileContext{
				State: &state{
					tikv: fake.FakeObj(fakeTiKVName, func(obj *v1alpha1.TiKV) *v1alpha1.TiKV {
						obj.Generation = 3
						obj.Labels = map[string]string{
							v1alpha1.LabelKeyInstanceRevisionHash: newRevision,
						}
						return obj
					}),
					pod: fake.FakeObj("aaa-tikv-xxx", func(obj *corev1.Pod) *corev1.Pod {
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
			objs = append(objs, c.state.TiKV())
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

			obj := &v1alpha1.TiKV{}
			require.NoError(tt, fc.Get(ctx, client.ObjectKey{Name: fakeTiKVName}, obj), c.desc)
			conds := obj.Status.Conditions
			for i := range conds {
				cond := &conds[i]
				cond.LastTransitionTime = metav1.Time{}
			}
			assert.Equal(tt, c.expectedObj, obj, c.desc)
		})
	}
}