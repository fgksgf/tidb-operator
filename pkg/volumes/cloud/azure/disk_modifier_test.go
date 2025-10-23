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

package azure

import (
	"context"
	"errors"
	"testing"
	"time"

	azruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute"
	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/utils/ptr"

	"github.com/pingcap/tidb-operator/pkg/volumes/cloud"
)

// FakeClock implements Clock interface for testing with a fixed time
type FakeClock struct {
	now time.Time
}

func (f *FakeClock) Now() time.Time {
	return f.now
}

func (f *FakeClock) Since(t time.Time) time.Duration {
	return f.now.Sub(t)
}

func TestModifyVolume(t *testing.T) {
	sc := &storagev1.StorageClass{
		Parameters: map[string]string{
			paramKeyThroughput: "100",
			paramKeyIOPS:       "500",
			paramKeyType:       "Premium_LRS",
		},
	}

	tests := []struct {
		name           string
		pvc            *corev1.PersistentVolumeClaim
		pv             *corev1.PersistentVolume
		FakeDiskClient *FakeDiskClient
		expectedWait   bool
		expectedError  error
	}{
		{
			name: "successful modification",
			pvc:  cloud.NewTestPVC("10Gi"),
			pv:   cloud.NewTestPV("/subscriptions/123/resourceGroups/rg1/providers/Microsoft.Compute/disks/disk1"),
			FakeDiskClient: &FakeDiskClient{
				GetFunc: func(_ context.Context, _, _ string, _ *armcompute.DisksClientGetOptions) (armcompute.DisksClientGetResponse, error) {
					// Use fixed base time for testing
					baseTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
					createdAt := baseTime.Add(-48 * time.Hour) // disk created 2 days ago
					return armcompute.DisksClientGetResponse{
						Disk: armcompute.Disk{
							ID: to.Ptr("/subscriptions/123/resourceGroups/rg1/providers/Microsoft.Compute/disks/disk1"),
							Properties: &armcompute.DiskProperties{
								DiskSizeGB:        ptr.To[int32](5), // current size is 5Gi, desired is 10Gi
								DiskIOPSReadWrite: ptr.To[int64](500),
								DiskMBpsReadWrite: ptr.To[int64](100),
								TimeCreated:       &createdAt,
							},
							SKU: &armcompute.DiskSKU{
								Name: to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS),
							},
						},
					}, nil
				},
				BeginUpdateFunc: func(_ context.Context, _, _ string, _ armcompute.DiskUpdate, _ *armcompute.DisksClientBeginUpdateOptions) (*azruntime.Poller[armcompute.DisksClientUpdateResponse], error) {
					return nil, nil
				},
			},
			expectedWait:  true, // should wait after modification
			expectedError: nil,
		},
		{
			name:           "failed to get disk info",
			pvc:            cloud.NewTestPVC("10Gi"),
			pv:             cloud.NewTestPV("invalid/volume/handle"),
			FakeDiskClient: &FakeDiskClient{},
			expectedWait:   false,
			expectedError:  errors.New("failed to get Azure disk info from PV: invalid volumeHandle format"),
		},
		{
			name: "volume modification is failed, modify again",
			pvc:  cloud.NewTestPVC("10Gi"),
			pv:   cloud.NewTestPV("/subscriptions/123/resourceGroups/rg1/providers/Microsoft.Compute/disks/disk1"),
			FakeDiskClient: &FakeDiskClient{
				GetFunc: func(_ context.Context, _, _ string, _ *armcompute.DisksClientGetOptions) (armcompute.DisksClientGetResponse, error) {
					baseTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
					createdAt := baseTime.Add(-48 * time.Hour)
					return armcompute.DisksClientGetResponse{
						Disk: armcompute.Disk{
							ID: to.Ptr("/subscriptions/123/resourceGroups/rg1/providers/Microsoft.Compute/disks/disk1"),
							Properties: &armcompute.DiskProperties{
								DiskSizeGB:        ptr.To[int32](20),
								DiskIOPSReadWrite: ptr.To[int64](500),
								DiskMBpsReadWrite: ptr.To[int64](100),
								TimeCreated:       &createdAt,
							},
							SKU: &armcompute.DiskSKU{
								Name: to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS),
							},
						},
					}, nil
				},
				BeginUpdateFunc: func(_ context.Context, _, _ string, _ armcompute.DiskUpdate, _ *armcompute.DisksClientBeginUpdateOptions) (*azruntime.Poller[armcompute.DisksClientUpdateResponse], error) {
					return nil, errors.New("begin update failed, please try again")
				},
			},
			expectedWait:  false,
			expectedError: errors.New("failed to update disk: begin update failed, please try again"),
		},
		{
			name: "volume modification is completed, no need to wait",
			pvc:  cloud.NewTestPVC("10Gi"),
			pv:   cloud.NewTestPV("/subscriptions/123/resourceGroups/rg1/providers/Microsoft.Compute/disks/disk1"),
			FakeDiskClient: &FakeDiskClient{
				GetFunc: func(_ context.Context, _, _ string, _ *armcompute.DisksClientGetOptions) (armcompute.DisksClientGetResponse, error) {
					baseTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
					createdAt := baseTime.Add(-48 * time.Hour)
					return armcompute.DisksClientGetResponse{
						Disk: armcompute.Disk{
							ID: to.Ptr("/subscriptions/123/resourceGroups/rg1/providers/Microsoft.Compute/disks/disk1"),
							Properties: &armcompute.DiskProperties{
								DiskSizeGB:        ptr.To[int32](10),
								DiskIOPSReadWrite: ptr.To[int64](500),
								DiskMBpsReadWrite: ptr.To[int64](100),
								TimeCreated:       &createdAt,
							},
							SKU: &armcompute.DiskSKU{
								Name: to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS),
							},
						},
					}, nil
				},
			},
			expectedWait:  false,
			expectedError: nil,
		},
		{
			name: "volume has not been modified, try to modify",
			pvc:  cloud.NewTestPVC("10Gi"),
			pv:   cloud.NewTestPV("/subscriptions/123/resourceGroups/rg1/providers/Microsoft.Compute/disks/disk1"),
			FakeDiskClient: &FakeDiskClient{
				GetFunc: func(_ context.Context, _, _ string, _ *armcompute.DisksClientGetOptions) (armcompute.DisksClientGetResponse, error) {
					baseTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
					createdAt := baseTime.Add(-48 * time.Hour)
					return armcompute.DisksClientGetResponse{
						Disk: armcompute.Disk{
							ID: to.Ptr("/subscriptions/123/resourceGroups/rg1/providers/Microsoft.Compute/disks/disk1"),
							Properties: &armcompute.DiskProperties{
								DiskSizeGB:        ptr.To[int32](5),
								DiskIOPSReadWrite: ptr.To[int64](300),
								DiskMBpsReadWrite: ptr.To[int64](50),
								TimeCreated:       &createdAt,
							},
							SKU: &armcompute.DiskSKU{
								Name: to.Ptr(armcompute.DiskStorageAccountTypesPremiumLRS),
							},
						},
					}, nil
				},
				BeginUpdateFunc: func(_ context.Context, _, _ string, _ armcompute.DiskUpdate, _ *armcompute.DisksClientBeginUpdateOptions) (*azruntime.Poller[armcompute.DisksClientUpdateResponse], error) {
					return nil, nil
				},
			},
			expectedWait:  true,
			expectedError: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use a fixed time for testing
			baseTime := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
			modifier := &DiskModifier{
				DiskClient: tt.FakeDiskClient,
				Logger:     logr.Discard(),
				Clock: &FakeClock{
					now: baseTime,
				},
			}
			wait, err := modifier.Modify(context.TODO(), tt.pvc, tt.pv, sc)
			if wait != tt.expectedWait {
				t.Errorf("expected wait %v, got %v", tt.expectedWait, wait)
			}
			if err != nil && tt.expectedError == nil {
				t.Errorf("unexpected error: %v", err)
			}
			if err == nil && tt.expectedError != nil {
				t.Errorf("expected error: %v, got nil", tt.expectedError)
			}
			if err != nil && tt.expectedError != nil {
				if diff := cmp.Diff(tt.expectedError.Error(), err.Error()); diff != "" {
					t.Errorf("error mismatch (-expected +got):\n%s", diff)
				} else {
					t.Logf("error messages match: %v", err)
				}
			}
		})
	}
}

func TestGetDiskInfoFromVolumeID(t *testing.T) {
	tests := []struct {
		name             string
		volumeID         string
		expectedDiskName string
		expectedSubID    string
		expectedRGName   string
		wantError        bool
	}{
		{
			name:             "valid volume ID",
			volumeID:         "/subscriptions/123/resourceGroups/rg1/providers/Microsoft.Compute/disks/disk1",
			expectedDiskName: "disk1",
			expectedSubID:    "123",
			expectedRGName:   "rg1",
		},
		{
			name:             "invalid volume ID format",
			volumeID:         "invalid/volume/handle",
			expectedDiskName: "",
			expectedSubID:    "",
			expectedRGName:   "",
			wantError:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			diskName, subID, rgName, err := getDiskInfoFromVolumeID(tt.volumeID)
			if (err != nil) != tt.wantError {
				t.Errorf("expected error %v, got %v", tt.wantError, err)
			}
			if diskName != tt.expectedDiskName {
				t.Errorf("expected diskName %v, got %v", tt.expectedDiskName, diskName)
			}
			if subID != tt.expectedSubID {
				t.Errorf("expected subscriptionID %v, got %v", tt.expectedSubID, subID)
			}
			if rgName != tt.expectedRGName {
				t.Errorf("expected resourceGroupName %v, got %v", tt.expectedRGName, rgName)
			}
		})
	}
}

func timePtr(t time.Time) *time.Time {
	return &t
}

func TestModifyHistoryCanModify(t *testing.T) {
	baseTime := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name              string
		history           ModifyHistory
		now               time.Time
		expectedCanModify bool
		expectedReason    string
	}{
		{
			name:              "no history, can modify",
			history:           ModifyHistory{Timestamps: []time.Time{}},
			now:               baseTime,
			expectedCanModify: true,
		},
		{
			name: "3 modifications within 24h for regular disk, can modify",
			history: ModifyHistory{
				Timestamps: []time.Time{
					baseTime.Add(-23 * time.Hour),
					baseTime.Add(-12 * time.Hour),
					baseTime.Add(-1 * time.Hour),
				},
			},
			now:               baseTime,
			expectedCanModify: true,
		},
		{
			name: "4 modifications within 24h for regular disk, cannot modify",
			history: ModifyHistory{
				Timestamps: []time.Time{
					baseTime.Add(-23 * time.Hour),
					baseTime.Add(-12 * time.Hour),
					baseTime.Add(-6 * time.Hour),
					baseTime.Add(-1 * time.Hour),
				},
			},
			now:               baseTime,
			expectedCanModify: false,
			expectedReason:    "reached max 4 modifications within 24h",
		},
		{
			name: "new disk (<24h), 2 modifications, can modify",
			history: ModifyHistory{
				CreatedAt: timePtr(baseTime.Add(-20 * time.Hour)),
				Timestamps: []time.Time{
					baseTime.Add(-10 * time.Hour),
					baseTime.Add(-5 * time.Hour),
				},
			},
			now:               baseTime,
			expectedCanModify: true,
		},
		{
			name: "new disk (<24h), 3 modifications, cannot modify",
			history: ModifyHistory{
				CreatedAt: timePtr(baseTime.Add(-20 * time.Hour)),
				Timestamps: []time.Time{
					baseTime.Add(-15 * time.Hour),
					baseTime.Add(-10 * time.Hour),
					baseTime.Add(-5 * time.Hour),
				},
			},
			now:               baseTime,
			expectedCanModify: false,
			expectedReason:    "new disk can only be modified 3 times in first 24h",
		},
		{
			name: "old modifications (>24h) should be ignored",
			history: ModifyHistory{
				Timestamps: []time.Time{
					baseTime.Add(-30 * time.Hour), // expired
					baseTime.Add(-25 * time.Hour), // expired
					baseTime.Add(-20 * time.Hour),
					baseTime.Add(-10 * time.Hour),
				},
			},
			now:               baseTime,
			expectedCanModify: true, // only 2 valid modifications
		},
		{
			name: "disk older than 24h treated as regular disk",
			history: ModifyHistory{
				CreatedAt: timePtr(baseTime.Add(-30 * time.Hour)),
				Timestamps: []time.Time{
					baseTime.Add(-10 * time.Hour),
					baseTime.Add(-5 * time.Hour),
					baseTime.Add(-1 * time.Hour),
				},
			},
			now:               baseTime,
			expectedCanModify: true, // 3 modifications ok for regular disk
		},
		{
			name: "exactly at 24h boundary for regular disk",
			history: ModifyHistory{
				Timestamps: []time.Time{
					baseTime.Add(-24 * time.Hour), // exactly 24h ago, should be excluded
					baseTime.Add(-23 * time.Hour),
					baseTime.Add(-12 * time.Hour),
					baseTime.Add(-1 * time.Hour),
				},
			},
			now:               baseTime,
			expectedCanModify: true, // only 3 valid modifications (first one expired)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			canModify, reason := tt.history.CanModify(tt.now)
			if canModify != tt.expectedCanModify {
				t.Errorf("expected canModify=%v, got %v", tt.expectedCanModify, canModify)
			}
			if !tt.expectedCanModify && reason != tt.expectedReason {
				t.Errorf("expected reason=%q, got %q", tt.expectedReason, reason)
			}
		})
	}
}

func TestModifyHistoryNextAvailableTime(t *testing.T) {
	baseTime := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		history      ModifyHistory
		now          time.Time
		expectedTime time.Time
	}{
		{
			name:         "no history, available immediately",
			history:      ModifyHistory{Timestamps: []time.Time{}},
			now:          baseTime,
			expectedTime: baseTime,
		},
		{
			name: "oldest modification expires first",
			history: ModifyHistory{
				Timestamps: []time.Time{
					baseTime.Add(-20 * time.Hour), // oldest, expires at baseTime + 4h
					baseTime.Add(-10 * time.Hour),
					baseTime.Add(-5 * time.Hour),
				},
			},
			now:          baseTime,
			expectedTime: baseTime.Add(4 * time.Hour), // 24h - 20h = 4h
		},
		{
			name: "multiple modifications, return earliest expiry",
			history: ModifyHistory{
				Timestamps: []time.Time{
					baseTime.Add(-23 * time.Hour), // expires at baseTime + 1h
					baseTime.Add(-15 * time.Hour),
					baseTime.Add(-8 * time.Hour),
					baseTime.Add(-2 * time.Hour),
				},
			},
			now:          baseTime,
			expectedTime: baseTime.Add(1 * time.Hour),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nextTime := tt.history.NextAvailableTime(tt.now)
			if !nextTime.Equal(tt.expectedTime) {
				t.Errorf("expected nextTime=%v, got %v", tt.expectedTime, nextTime)
			}
		})
	}
}

//nolint:gocyclo // Test function with multiple subtests
func TestGetAndSetModifyHistory(t *testing.T) {
	baseTime := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)

	t.Run("initially no history", func(t *testing.T) {
		pvc := cloud.NewTestPVC("10Gi")

		history, err := getModifyHistory(pvc)
		if err != nil {
			t.Fatalf("failed to get history: %v", err)
		}
		if len(history.Timestamps) != 0 {
			t.Errorf("expected empty history, got %d timestamps", len(history.Timestamps))
		}
		if history.CreatedAt != nil {
			t.Errorf("expected nil CreatedAt, got %v", history.CreatedAt)
		}
	})

	t.Run("record single modification", func(t *testing.T) {
		pvc := cloud.NewTestPVC("10Gi")

		if err := recordModification(pvc, baseTime); err != nil {
			t.Fatalf("failed to record: %v", err)
		}

		history, err := getModifyHistory(pvc)
		if err != nil {
			t.Fatalf("failed to get history: %v", err)
		}
		if len(history.Timestamps) != 1 {
			t.Errorf("expected 1 timestamp, got %d", len(history.Timestamps))
		}
		if !history.Timestamps[0].Equal(baseTime) {
			t.Errorf("expected timestamp %v, got %v", baseTime, history.Timestamps[0])
		}
	})

	t.Run("record multiple modifications", func(t *testing.T) {
		pvc := cloud.NewTestPVC("10Gi")

		times := []time.Time{
			baseTime.Add(-10 * time.Hour),
			baseTime.Add(-5 * time.Hour),
			baseTime,
		}

		for _, ts := range times {
			if err := recordModification(pvc, ts); err != nil {
				t.Fatalf("failed to record: %v", err)
			}
		}

		history, err := getModifyHistory(pvc)
		if err != nil {
			t.Fatalf("failed to get history: %v", err)
		}
		if len(history.Timestamps) != 3 {
			t.Errorf("expected 3 timestamps, got %d", len(history.Timestamps))
		}
	})

	t.Run("clean up expired timestamps", func(t *testing.T) {
		pvc := cloud.NewTestPVC("10Gi")

		// Record some old and new timestamps
		oldTime := baseTime.Add(-30 * time.Hour) // >24h, should be cleaned
		recentTime := baseTime.Add(-10 * time.Hour)

		if err := recordModification(pvc, oldTime); err != nil {
			t.Fatalf("failed to record old time: %v", err)
		}
		if err := recordModification(pvc, recentTime); err != nil {
			t.Fatalf("failed to record recent time: %v", err)
		}
		if err := recordModification(pvc, baseTime); err != nil { // This should trigger cleanup
			t.Fatalf("failed to record base time: %v", err)
		}

		history, err := getModifyHistory(pvc)
		if err != nil {
			t.Fatalf("failed to get history: %v", err)
		}

		// Only recent timestamps should remain
		for _, ts := range history.Timestamps {
			if baseTime.Sub(ts) > modifyWindowDuration {
				t.Errorf("found expired timestamp %v that should have been cleaned", ts)
			}
		}
	})

	t.Run("set and get disk creation time", func(t *testing.T) {
		pvc := cloud.NewTestPVC("10Gi")
		createdAt := baseTime.Add(-20 * time.Hour)

		setDiskCreatedTime(pvc, createdAt)

		history, err := getModifyHistory(pvc)
		if err != nil {
			t.Fatalf("failed to get history: %v", err)
		}
		if history.CreatedAt == nil {
			t.Fatal("expected CreatedAt to be set")
		}
		if !history.CreatedAt.Equal(createdAt) {
			t.Errorf("expected CreatedAt %v, got %v", createdAt, history.CreatedAt)
		}
	})

	t.Run("creation time should not be overwritten", func(t *testing.T) {
		pvc := cloud.NewTestPVC("10Gi")
		firstTime := baseTime.Add(-20 * time.Hour)
		secondTime := baseTime.Add(-10 * time.Hour)

		setDiskCreatedTime(pvc, firstTime)
		setDiskCreatedTime(pvc, secondTime) // should not overwrite

		history, err := getModifyHistory(pvc)
		if err != nil {
			t.Fatalf("failed to get history: %v", err)
		}
		if !history.CreatedAt.Equal(firstTime) {
			t.Errorf("expected CreatedAt to remain %v, got %v", firstTime, history.CreatedAt)
		}
	})

	t.Run("handle corrupted annotation gracefully", func(t *testing.T) {
		pvc := cloud.NewTestPVC("10Gi")
		pvc.Annotations = map[string]string{
			annoKeyAzureDiskModifyHistory: "invalid-json",
		}

		_, err := getModifyHistory(pvc)
		if err == nil {
			t.Error("expected error for invalid JSON, got nil")
		}
	})
}
