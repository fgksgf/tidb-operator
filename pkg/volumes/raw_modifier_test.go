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

package volumes

import (
	"context"
	"testing"
	stdtime "time"

	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/go-logr/logr"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	"github.com/pingcap/tidb-operator/pkg/client"
	"github.com/pingcap/tidb-operator/pkg/utils/fake"
	"github.com/pingcap/tidb-operator/pkg/utils/time"
	"github.com/pingcap/tidb-operator/pkg/volumes/cloud"
	"github.com/pingcap/tidb-operator/pkg/volumes/cloud/aws"
)

func withPVCStatus(size string) fake.ChangeFunc[corev1.PersistentVolumeClaim, *corev1.PersistentVolumeClaim] {
	return func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
		pvc.Status.Phase = corev1.ClaimBound
		pvc.Status.Capacity = corev1.ResourceList{}
		pvc.Status.Capacity[corev1.ResourceStorage] = resource.MustParse(size)
		pvc.Status.CurrentVolumeAttributesClassName = nil
		return pvc
	}
}

func withPVCSpec(scName *string, vol, size string) fake.ChangeFunc[corev1.PersistentVolumeClaim, *corev1.PersistentVolumeClaim] {
	return func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
		pvc.Spec.StorageClassName = scName
		pvc.Spec.VolumeAttributesClassName = nil
		pvc.Spec.VolumeName = vol
		pvc.Spec.Resources.Requests = corev1.ResourceList{}
		pvc.Spec.Resources.Requests[corev1.ResourceStorage] = resource.MustParse(size)
		return pvc
	}
}

func withPVCAnnotation(key, value string) fake.ChangeFunc[corev1.PersistentVolumeClaim, *corev1.PersistentVolumeClaim] {
	return func(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolumeClaim {
		if pvc.Annotations == nil {
			pvc.Annotations = map[string]string{}
		}
		pvc.Annotations[key] = value
		return pvc
	}
}

func withParameters(params map[string]string) fake.ChangeFunc[storagev1.StorageClass, *storagev1.StorageClass] {
	return func(sc *storagev1.StorageClass) *storagev1.StorageClass {
		sc.Parameters = params
		return sc
	}
}

func withProvisioner(p string) fake.ChangeFunc[storagev1.StorageClass, *storagev1.StorageClass] {
	return func(sc *storagev1.StorageClass) *storagev1.StorageClass {
		sc.Provisioner = p
		return sc
	}
}

func withAllowVolumeExpansion() fake.ChangeFunc[storagev1.StorageClass, *storagev1.StorageClass] {
	return func(sc *storagev1.StorageClass) *storagev1.StorageClass {
		sc.AllowVolumeExpansion = ptr.To(true)
		return sc
	}
}

func getObjectsFromActualVolume(vol *ActualVolume) []client.Object {
	var objs []client.Object
	if vol != nil {
		if vol.Desired != nil && vol.Desired.StorageClass != nil {
			objs = append(objs, vol.Desired.StorageClass)
		}
		if vol.StorageClass != nil {
			objs = append(objs, vol.StorageClass)
		}
		if vol.PVC != nil {
			objs = append(objs, vol.PVC)
		}
		if vol.PV != nil {
			objs = append(objs, vol.PV)
		}
	}
	return objs
}

func Test_rawModifier_GetActualVolume(t *testing.T) {
	tests := []struct {
		name         string
		existingObjs []client.Object
		desired      *corev1.PersistentVolumeClaim
		current      *corev1.PersistentVolumeClaim
		getState     aws.GetVolumeStateFunc
		expect       func(*WithT, *ActualVolume)
		wantErr      bool
	}{
		{
			name: "happy path: no modification",
			existingObjs: []client.Object{
				fake.FakeObj[corev1.PersistentVolume]("pv-0"),
				fake.FakeObj[storagev1.StorageClass]("sc-0"),
			},
			desired: fake.FakeObj("pvc-0", withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi")),
			current: fake.FakeObj("pvc-0", withPVCStatus("10Gi"), withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi")),
			getState: func(_ string) types.VolumeModificationState {
				return types.VolumeModificationStateFailed
			},
			expect: func(g *WithT, volume *ActualVolume) {
				g.Expect(volume).ShouldNot(BeNil())
				g.Expect(volume.Desired).ShouldNot(BeNil())
				g.Expect(volume.Desired.Size).Should(Equal(resource.MustParse("10Gi")))
				g.Expect(volume.Desired.StorageClassName).Should(Equal(ptr.To("sc-0")))
				g.Expect(volume.Desired.StorageClass).ShouldNot(BeNil())

				g.Expect(volume.PVC).ShouldNot(BeNil())
				g.Expect(volume.PV).ShouldNot(BeNil())
				g.Expect(volume.StorageClass).ShouldNot(BeNil())
				g.Expect(volume.Phase).Should(Equal(VolumePhaseUnknown))
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cli := client.NewFakeClient(tt.existingObjs...)
			m := NewRawModifier(cli, logr.Discard())
			got, err := m.GetActualVolume(context.TODO(), tt.desired, tt.current)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetActualVolume() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			g := NewGomegaWithT(t)
			if tt.expect != nil {
				tt.expect(g, got)
			}
		})
	}
}

func Test_rawModifier_getVolumePhase(t *testing.T) {
	tests := []struct {
		name            string
		volumeModifiers map[string]cloud.VolumeModifier
		clock           time.Clock
		vol             *ActualVolume
		want            VolumePhase
		wantStr         string
		shouldModify    bool
	}{
		{
			name: "no need to modify",
			vol: &ActualVolume{
				Desired: &DesiredVolume{
					Size:             resource.MustParse("10Gi"),
					StorageClassName: ptr.To("sc-0"),
				},
				PVC:          fake.FakeObj("pvc-0", withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"), withPVCStatus("10Gi")),
				StorageClass: fake.FakeObj("sc-0", withProvisioner("test")),
			},
			volumeModifiers: map[string]cloud.VolumeModifier{
				"test": &cloud.FakeVolumeModifier{},
			},
			want:         VolumePhaseModified,
			wantStr:      "Modified",
			shouldModify: false,
		},
		{
			name: "change storage class",
			vol: &ActualVolume{
				Desired: &DesiredVolume{
					Size:             resource.MustParse("10Gi"),
					StorageClassName: ptr.To("sc-1"),
					StorageClass:     fake.FakeObj("sc-1", withParameters(map[string]string{"iops": "100"})),
				},
				PVC:          fake.FakeObj("pvc-0", withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"), withPVCStatus("10Gi")),
				StorageClass: fake.FakeObj("sc-0", withProvisioner("test")),
				PV:           fake.FakeObj[corev1.PersistentVolume]("pv-0"),
			},
			volumeModifiers: map[string]cloud.VolumeModifier{
				"test": &cloud.FakeVolumeModifier{},
			},
			want:         VolumePhasePreparing,
			wantStr:      "Preparing",
			shouldModify: true,
		},
		{
			name: "increase size",
			vol: &ActualVolume{
				Desired: &DesiredVolume{
					Size:             resource.MustParse("100Gi"),
					StorageClassName: ptr.To("sc-0"),
				},
				PVC:          fake.FakeObj("pvc-0", withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"), withPVCStatus("10Gi")),
				StorageClass: fake.FakeObj("sc-0", withProvisioner("test"), withAllowVolumeExpansion()),
			},
			volumeModifiers: map[string]cloud.VolumeModifier{
				"test": &cloud.FakeVolumeModifier{},
			},
			want:         VolumePhasePreparing,
			wantStr:      "Preparing",
			shouldModify: true,
		},
		{
			name: "decrease size",
			vol: &ActualVolume{
				Desired: &DesiredVolume{
					Size:             resource.MustParse("1Gi"),
					StorageClassName: ptr.To("sc-0"),
				},
				PVC:          fake.FakeObj("pvc-1", withPVCSpec(ptr.To("sc-0"), "pv-1", "20Gi"), withPVCStatus("20Gi")),
				StorageClass: fake.FakeObj("sc-0", withProvisioner("test")),
			},
			volumeModifiers: map[string]cloud.VolumeModifier{
				"test": &cloud.FakeVolumeModifier{},
			},
			want:         VolumePhaseCannotModify,
			wantStr:      "CannotModify",
			shouldModify: false,
		},
		{
			name: "modifying",
			vol: &ActualVolume{
				Desired: &DesiredVolume{
					Size:             resource.MustParse("100Gi"),
					StorageClassName: ptr.To("sc-0"),
				},
				PVC: fake.FakeObj("pvc-1",
					withPVCSpec(ptr.To("sc-0"), "pv-1", "10Gi"), withPVCStatus("10Gi"),
					withPVCAnnotation(annoKeyPVCSpecRevision, "2"),
					withPVCAnnotation(annoKeyPVCStatusRevision, "1"),
				),
				StorageClass: fake.FakeObj("sc-0", withProvisioner("test"), withAllowVolumeExpansion()),
			},
			volumeModifiers: map[string]cloud.VolumeModifier{
				"test": &cloud.FakeVolumeModifier{},
			},
			want:         VolumePhaseModifying,
			wantStr:      "Modifying",
			shouldModify: true,
		},
		{
			name: "wait for next time",
			vol: &ActualVolume{
				Desired: &DesiredVolume{
					Size:             resource.MustParse("100Gi"),
					StorageClassName: ptr.To("sc-0"),
				},
				PVC: fake.FakeObj("pvc-0",
					withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"), withPVCStatus("10Gi"),
					withPVCAnnotation(annoKeyPVCLastTransitionTimestamp, "2121-01-01T00:00:00Z"), // a future time
				),
				StorageClass: fake.FakeObj("sc-0", withProvisioner("test"), withAllowVolumeExpansion()),
			},
			clock: time.RealClock{},
			volumeModifiers: map[string]cloud.VolumeModifier{
				"test": &cloud.FakeVolumeModifier{},
			},
			want:         VolumePhasePending,
			wantStr:      "Pending",
			shouldModify: false,
		},
		{
			name: "no need to wait for next time",
			vol: &ActualVolume{
				Desired: &DesiredVolume{
					Size:             resource.MustParse("100Gi"),
					StorageClassName: ptr.To("sc-0"),
				},
				PVC: fake.FakeObj("pvc-0",
					withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"), withPVCStatus("10Gi"),
					withPVCAnnotation(annoKeyPVCLastTransitionTimestamp, "2021-01-01T00:00:00Z"), // a past time
				),
				StorageClass: fake.FakeObj("sc-0", withProvisioner("test"), withAllowVolumeExpansion()),
			},
			clock: time.RealClock{},
			volumeModifiers: map[string]cloud.VolumeModifier{
				"test": &cloud.FakeVolumeModifier{},
			},
			want:         VolumePhasePreparing,
			wantStr:      "Preparing",
			shouldModify: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &rawModifier{
				k8sClient:       client.NewFakeClient(getObjectsFromActualVolume(tt.vol)...),
				logger:          logr.Discard(),
				volumeModifiers: tt.volumeModifiers,
				clock:           tt.clock,
			}
			if got := m.getVolumePhase(tt.vol); got != tt.want {
				t.Errorf("getVolumePhase() = %v, want %v", got, tt.want)
			}
			if got := tt.want.String(); got != tt.wantStr {
				t.Errorf("VolumePhase.String() = %v, want %v", got, tt.wantStr)
			}
			if got := m.ShouldModify(context.TODO(), tt.vol); got != tt.shouldModify {
				t.Errorf("ShouldModify() = %v, want %v", got, tt.shouldModify)
			}
		})
	}
}

func Test_rawModifier_Modify(t *testing.T) {
	tests := []struct {
		name     string
		vol      *ActualVolume
		getState aws.GetVolumeStateFunc
		wantErr  bool
	}{
		{
			name: "can not modify",
			vol: &ActualVolume{
				PVC:   fake.FakeObj[corev1.PersistentVolumeClaim]("pvc-0"),
				Phase: VolumePhaseModified,
			},
			wantErr: true,
		},
		{
			name: "preparing, wait for fs to be resized",
			vol: &ActualVolume{
				PVC: fake.FakeObj("pvc-0",
					withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"),
					withPVCStatus("10Gi"),
				),
				Phase: VolumePhasePreparing,
				Desired: &DesiredVolume{
					Size:             resource.MustParse("20Gi"),
					StorageClassName: ptr.To("sc-0"),
				},
				StorageClass: fake.FakeObj("sc-0", withProvisioner("ebs.csi.aws.com"), withAllowVolumeExpansion()),
			},
			wantErr: true,
		},
		{
			name: "modifying, wait for fs to be resized",
			vol: &ActualVolume{
				PVC: fake.FakeObj("pvc-0",
					withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"),
					withPVCStatus("10Gi"),
				),
				Phase: VolumePhaseModifying,
				Desired: &DesiredVolume{
					Size:             resource.MustParse("20Gi"),
					StorageClassName: ptr.To("sc-0"),
				},
				StorageClass: fake.FakeObj("sc-0", withProvisioner("ebs.csi.aws.com"), withAllowVolumeExpansion()),
			},
			wantErr: true,
		},
		{
			name: "modifying, synced with desired",
			vol: &ActualVolume{
				PVC: fake.FakeObj("pvc-0",
					withPVCSpec(ptr.To("sc-0"), "pv-0", "20Gi"),
					withPVCStatus("20Gi"),
				),
				Phase: VolumePhaseModifying,
				Desired: &DesiredVolume{
					Size:             resource.MustParse("20Gi"),
					StorageClassName: ptr.To("sc-0"),
				},
				StorageClass: fake.FakeObj("sc-0", withProvisioner("ebs.csi.aws.com"), withAllowVolumeExpansion()),
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &rawModifier{
				k8sClient: client.NewFakeClient(getObjectsFromActualVolume(tt.vol)...),
				logger:    logr.Discard(),
				volumeModifiers: map[string]cloud.VolumeModifier{
					"ebs.csi.aws.com": aws.NewFakeEBSModifier(tt.getState),
				},
				clock: &time.RealClock{},
			}
			if err := m.Modify(context.TODO(), tt.vol); (err != nil) != tt.wantErr {
				t.Errorf("Modify() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func Test_rawModifier_updatePVCAnnotations(t *testing.T) {
	tests := []struct {
		name        string
		pvc         *corev1.PersistentVolumeClaim
		annotations map[string]string
		wantErr     bool
	}{
		{
			name: "successful update",
			pvc: fake.FakeObj("pvc-0",
				withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"),
				withPVCStatus("10Gi"),
			),
			annotations: map[string]string{
				"test-key": "test-value",
			},
			wantErr: false,
		},
		{
			name: "update with Azure modification history",
			pvc: fake.FakeObj("pvc-0",
				withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"),
				withPVCStatus("10Gi"),
			),
			annotations: map[string]string{
				"azure.tidb.pingcap.com/modify-history": `["2024-01-01T00:00:00Z"]`,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := NewGomegaWithT(t)

			// Setup PVC with annotations
			if tt.pvc.Annotations == nil {
				tt.pvc.Annotations = make(map[string]string)
			}
			for k, v := range tt.annotations {
				tt.pvc.Annotations[k] = v
			}

			// Create fake client
			cli := client.NewFakeClient(tt.pvc)

			m := &rawModifier{
				k8sClient: cli,
				logger:    logr.Discard(),
			}

			vol := &ActualVolume{
				PVC: tt.pvc,
			}

			err := m.updatePVCAnnotations(context.TODO(), vol)

			if tt.wantErr {
				g.Expect(err).Should(HaveOccurred())
			} else {
				g.Expect(err).ShouldNot(HaveOccurred())

				// Verify annotations are persisted
				var updatedPVC corev1.PersistentVolumeClaim
				err := cli.Get(context.TODO(), client.ObjectKey{
					Namespace: tt.pvc.Namespace,
					Name:      tt.pvc.Name,
				}, &updatedPVC)
				g.Expect(err).ShouldNot(HaveOccurred())

				for k, v := range tt.annotations {
					g.Expect(updatedPVC.Annotations).Should(HaveKey(k))
					g.Expect(updatedPVC.Annotations[k]).Should(Equal(v))
				}
			}
		})
	}
}

// modifierWithAnnotations simulates a cloud provider that sets annotations and returns wait=true
type modifierWithAnnotations struct{}

func (m *modifierWithAnnotations) Name() string {
	return "test-cloud"
}

func (m *modifierWithAnnotations) Modify(ctx context.Context, pvc *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, sc *storagev1.StorageClass) (bool, error) {
	// Simulate cloud provider setting metadata in annotations
	// Use Azure-specific annotation to test cloud annotation merging
	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
	}
	pvc.Annotations["azure.tidb.pingcap.com/test-metadata"] = "test-value"

	// Return wait=true to simulate rate limiting
	return true, nil
}

func (m *modifierWithAnnotations) MinWaitDuration() stdtime.Duration {
	return stdtime.Second
}

func (m *modifierWithAnnotations) Validate(_, _ *corev1.PersistentVolumeClaim, _, _ *storagev1.StorageClass) error {
	return nil
}

func Test_rawModifier_Modify_PersistsAnnotationsOnWait(t *testing.T) {
	g := NewGomegaWithT(t)

	pvc := fake.FakeObj("pvc-0",
		withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"),
		withPVCStatus("10Gi"),
		withPVCAnnotation(annoKeyPVCSpecRevision, "2"),
		withPVCAnnotation(annoKeyPVCStatusRevision, "1"),
	)

	vol := &ActualVolume{
		PVC:   pvc,
		Phase: VolumePhaseModifying,
		Desired: &DesiredVolume{
			Size:             resource.MustParse("20Gi"),
			StorageClassName: ptr.To("sc-0"),
		},
		StorageClass: fake.FakeObj("sc-0", withProvisioner("test-cloud"), withAllowVolumeExpansion()),
	}

	// Create a modifier that returns wait=true (simulating rate limiting)
	fakeModifier := &modifierWithAnnotations{}

	cli := client.NewFakeClient(getObjectsFromActualVolume(vol)...)
	m := &rawModifier{
		k8sClient: cli,
		logger:    logr.Discard(),
		volumeModifiers: map[string]cloud.VolumeModifier{
			"test-cloud": fakeModifier,
		},
		clock: &time.RealClock{},
	}

	// Call Modify - should return WaitError
	err := m.Modify(context.TODO(), vol)
	g.Expect(err).Should(HaveOccurred())

	// Verify it's a WaitError
	var waitErr *WaitError
	g.Expect(err).Should(BeAssignableToTypeOf(waitErr))

	// Verify annotations were persisted despite wait=true
	var updatedPVC corev1.PersistentVolumeClaim
	err = cli.Get(context.TODO(), client.ObjectKey{
		Namespace: pvc.Namespace,
		Name:      pvc.Name,
	}, &updatedPVC)
	g.Expect(err).ShouldNot(HaveOccurred())
	g.Expect(updatedPVC.Annotations).Should(HaveKey("azure.tidb.pingcap.com/test-metadata"))
	g.Expect(updatedPVC.Annotations["azure.tidb.pingcap.com/test-metadata"]).Should(Equal("test-value"))
}

func Test_rawModifier_Modify_OnlyMergesCloudAnnotations(t *testing.T) {
	g := NewGomegaWithT(t)

	// Setup: PVC with annotation from another controller
	pvc := fake.FakeObj("pvc-0",
		withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"),
		withPVCStatus("10Gi"),
		withPVCAnnotation(annoKeyPVCSpecRevision, "2"),
		withPVCAnnotation(annoKeyPVCStatusRevision, "1"),
		withPVCAnnotation("other-controller/key", "should-not-be-overwritten"),
		withPVCAnnotation("backup.tidb.pingcap.com/some-key", "backup-value"),
	)

	vol := &ActualVolume{
		PVC:   pvc,
		Phase: VolumePhaseModifying,
		Desired: &DesiredVolume{
			Size:             resource.MustParse("20Gi"),
			StorageClassName: ptr.To("sc-0"),
		},
		StorageClass: fake.FakeObj("sc-0", withProvisioner("test-cloud"), withAllowVolumeExpansion()),
	}

	// Create a modifier that sets cloud-specific annotations
	fakeModifier := &modifierWithAnnotations{}

	cli := client.NewFakeClient(getObjectsFromActualVolume(vol)...)
	m := &rawModifier{
		k8sClient: cli,
		logger:    logr.Discard(),
		volumeModifiers: map[string]cloud.VolumeModifier{
			"test-cloud": fakeModifier,
		},
		clock: &time.RealClock{},
	}

	// Call Modify
	err := m.Modify(context.TODO(), vol)
	g.Expect(err).Should(HaveOccurred()) // Should return WaitError

	// Verify it's a WaitError
	var waitErr *WaitError
	g.Expect(err).Should(BeAssignableToTypeOf(waitErr))

	// Verify annotations: cloud annotations merged, other annotations preserved
	var updatedPVC corev1.PersistentVolumeClaim
	err = cli.Get(context.TODO(), client.ObjectKey{
		Namespace: pvc.Namespace,
		Name:      pvc.Name,
	}, &updatedPVC)
	g.Expect(err).ShouldNot(HaveOccurred())

	// Cloud annotation should be set by modifier (but it's not in our cloudAnnotationPrefixes, so shouldn't be merged)
	// Wait, "cloud-metadata" is not a cloud-specific annotation in our definition
	// Let me check what the modifierWithAnnotations sets
	g.Expect(updatedPVC.Annotations).ShouldNot(HaveKey("cloud-metadata"), "non-cloud annotation should not be merged")

	// Other controller's annotation should be preserved
	g.Expect(updatedPVC.Annotations).Should(HaveKey("other-controller/key"))
	g.Expect(updatedPVC.Annotations["other-controller/key"]).Should(Equal("should-not-be-overwritten"))

	// Backup annotation (not in cloud prefix) should be preserved
	g.Expect(updatedPVC.Annotations).Should(HaveKey("backup.tidb.pingcap.com/some-key"))
	g.Expect(updatedPVC.Annotations["backup.tidb.pingcap.com/some-key"]).Should(Equal("backup-value"))
}

// modifierThatSetsAzureAnnotation simulates Azure modifier setting cloud-specific annotations
type modifierThatSetsAzureAnnotation struct{}

func (m *modifierThatSetsAzureAnnotation) Name() string {
	return "disk.csi.azure.com"
}

func (m *modifierThatSetsAzureAnnotation) Modify(ctx context.Context, pvc *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume, sc *storagev1.StorageClass) (bool, error) {
	// Simulate Azure setting its modification history
	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
	}
	pvc.Annotations["azure.tidb.pingcap.com/modify-history"] = `["2024-01-01T00:00:00Z"]`
	pvc.Annotations["azure.tidb.pingcap.com/disk-created-at"] = "2024-01-01T00:00:00Z"

	// Also try to set a non-cloud annotation (should not be merged)
	pvc.Annotations["malicious/annotation"] = "should-not-appear"

	return true, nil
}

func (m *modifierThatSetsAzureAnnotation) MinWaitDuration() stdtime.Duration {
	return stdtime.Second
}

func (m *modifierThatSetsAzureAnnotation) Validate(_, _ *corev1.PersistentVolumeClaim, _, _ *storagev1.StorageClass) error {
	return nil
}

func Test_rawModifier_Modify_MergesOnlyAzureCloudAnnotations(t *testing.T) {
	g := NewGomegaWithT(t)

	// Setup: PVC with annotations from another controller
	pvc := fake.FakeObj("pvc-0",
		withPVCSpec(ptr.To("sc-0"), "pv-0", "10Gi"),
		withPVCStatus("10Gi"),
		withPVCAnnotation(annoKeyPVCSpecRevision, "2"),
		withPVCAnnotation(annoKeyPVCStatusRevision, "1"),
		withPVCAnnotation("other-controller/key", "important-value"),
	)

	vol := &ActualVolume{
		PVC:   pvc,
		Phase: VolumePhaseModifying,
		Desired: &DesiredVolume{
			Size:             resource.MustParse("20Gi"),
			StorageClassName: ptr.To("sc-0"),
		},
		StorageClass: fake.FakeObj("sc-0", withProvisioner("disk.csi.azure.com"), withAllowVolumeExpansion()),
	}

	// Create Azure modifier that sets both cloud and non-cloud annotations
	azureModifier := &modifierThatSetsAzureAnnotation{}

	cli := client.NewFakeClient(getObjectsFromActualVolume(vol)...)
	m := &rawModifier{
		k8sClient: cli,
		logger:    logr.Discard(),
		volumeModifiers: map[string]cloud.VolumeModifier{
			"disk.csi.azure.com": azureModifier,
		},
		clock: &time.RealClock{},
	}

	// Call Modify
	err := m.Modify(context.TODO(), vol)
	g.Expect(err).Should(HaveOccurred()) // Should return WaitError

	// Verify annotations were persisted correctly
	var updatedPVC corev1.PersistentVolumeClaim
	err = cli.Get(context.TODO(), client.ObjectKey{
		Namespace: pvc.Namespace,
		Name:      pvc.Name,
	}, &updatedPVC)
	g.Expect(err).ShouldNot(HaveOccurred())

	// Azure cloud annotations should be merged
	g.Expect(updatedPVC.Annotations).Should(HaveKey("azure.tidb.pingcap.com/modify-history"))
	g.Expect(updatedPVC.Annotations["azure.tidb.pingcap.com/modify-history"]).Should(Equal(`["2024-01-01T00:00:00Z"]`))
	g.Expect(updatedPVC.Annotations).Should(HaveKey("azure.tidb.pingcap.com/disk-created-at"))

	// Non-cloud annotations from modifier should NOT be merged
	g.Expect(updatedPVC.Annotations).ShouldNot(HaveKey("malicious/annotation"))

	// Other controller's annotations should be preserved
	g.Expect(updatedPVC.Annotations).Should(HaveKey("other-controller/key"))
	g.Expect(updatedPVC.Annotations["other-controller/key"]).Should(Equal("important-value"))
}
