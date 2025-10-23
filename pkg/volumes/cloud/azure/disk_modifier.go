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
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	azruntime "github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/utils/ptr"

	"github.com/pingcap/tidb-operator/pkg/utils"
	"github.com/pingcap/tidb-operator/pkg/volumes/cloud"
)

const (
	paramKeyThroughput = "DiskMBpsReadWrite"
	paramKeyIOPS       = "DiskIOPSReadWrite"
	paramKeyType       = "skuName"

	maxSize = 32767 // Azure Disk max size in GiB
	minSize = 1

	volumeIDPartsLength = 9

	// Azure official rate limits per documentation:
	// "Within every 24 hours, you can adjust the performance of these disks up to four times.
	// If you just created one of these disks, for the first 24 hours you can only adjust
	// its performance up to three times."
	maxModificationsRegularDisk = 4 // within 24h rolling window
	maxModificationsNewDisk     = 3 // within first 24h after creation
	modifyWindowDuration        = 24 * time.Hour
	defaultWaitDuration         = 1 * time.Minute // polling interval for rate limit checks

	// PVC annotations for tracking Azure disk modification history
	annoKeyAzureDiskCreatedAt     = "azure.tidb.pingcap.com/disk-created-at"
	annoKeyAzureDiskModifyHistory = "azure.tidb.pingcap.com/modify-history"
)

type DiskModifier struct {
	// for unit test, add switch for fake client
	// because we need to get subscription id from volume ID, so we cannot initialize it in constructor
	DiskClient DiskClient
	Logger     logr.Logger
	// Clock provides time-related operations, can be injected for testing
	Clock Clock
}

// Clock interface for time operations, enables time mocking in tests
type Clock interface {
	Now() time.Time
	Since(time.Time) time.Duration
}

// RealClock implements Clock using real time
type RealClock struct{}

func (RealClock) Now() time.Time {
	return time.Now()
}

func (RealClock) Since(t time.Time) time.Duration {
	return time.Since(t)
}

type DiskClient interface {
	Get(
		ctx context.Context,
		resourceGroupName, diskName string,
		opts *armcompute.DisksClientGetOptions,
	) (armcompute.DisksClientGetResponse, error)

	BeginUpdate(ctx context.Context,
		resourceGroupName, diskName string,
		parameters armcompute.DiskUpdate,
		options *armcompute.DisksClientBeginUpdateOptions,
	) (*azruntime.Poller[armcompute.DisksClientUpdateResponse], error)
}

type Volume struct {
	VolumeID   string
	Size       *int32
	IOPS       *int64
	Throughput *int64
	Type       string
	CreatedAt  *time.Time // disk creation time from Azure API
}

// ModifyHistory tracks modification timestamps for Azure disk rate limiting.
// Azure allows up to 4 modifications within 24h for regular disks,
// and up to 3 modifications within first 24h for newly created disks.
type ModifyHistory struct {
	// Timestamps of all modifications within the 24h rolling window
	Timestamps []time.Time `json:"timestamps"`
	// CreatedAt is the disk creation time from Azure API
	CreatedAt *time.Time `json:"created_at,omitempty"`
}

// CanModify checks if modification is allowed based on Azure rate limits.
// Returns (canModify bool, reason string).
func (h *ModifyHistory) CanModify(now time.Time) (canModify bool, reason string) {
	// Filter modifications within 24h window
	validMods := h.filterValidModifications(now)

	// Check if disk is new (created within 24h)
	isNewDisk := h.isNewDisk(now)

	maxAllowed := maxModificationsRegularDisk
	if isNewDisk {
		maxAllowed = maxModificationsNewDisk
	}

	if len(validMods) >= maxAllowed {
		if isNewDisk {
			return false, fmt.Sprintf("new disk can only be modified %d times in first 24h", maxModificationsNewDisk)
		}
		return false, fmt.Sprintf("reached max %d modifications within 24h", maxModificationsRegularDisk)
	}

	return true, ""
}

// filterValidModifications returns modifications within 24h window from now.
func (h *ModifyHistory) filterValidModifications(now time.Time) []time.Time {
	var valid []time.Time
	cutoff := now.Add(-modifyWindowDuration)
	for _, ts := range h.Timestamps {
		if ts.After(cutoff) {
			valid = append(valid, ts)
		}
	}
	return valid
}

// isNewDisk checks if disk was created within 24h from now.
func (h *ModifyHistory) isNewDisk(now time.Time) bool {
	if h.CreatedAt == nil {
		return false
	}
	diff := now.Sub(*h.CreatedAt)
	// Handle clock skew or invalid data (CreatedAt > now)
	if diff < 0 {
		return false
	}
	return diff < modifyWindowDuration
}

// NextAvailableTime calculates when next modification will be allowed.
// Returns the earliest time when oldest modification expires from the 24h window.
func (h *ModifyHistory) NextAvailableTime(now time.Time) time.Time {
	validMods := h.filterValidModifications(now)
	if len(validMods) == 0 {
		return now // can modify immediately
	}

	// Sort timestamps to find the oldest
	sort.Slice(validMods, func(i, j int) bool {
		return validMods[i].Before(validMods[j])
	})

	// Oldest modification will expire first
	oldestMod := validMods[0]
	return oldestMod.Add(modifyWindowDuration)
}

func NewDiskModifier(logger logr.Logger) cloud.VolumeModifier {
	return &DiskModifier{
		Logger: logger,
		Clock:  RealClock{}, // Use real clock by default
	}
}

func (*DiskModifier) Name() string {
	return "disk.csi.azure.com"
}

func (m *DiskModifier) Validate(_, _ *corev1.PersistentVolumeClaim, ssc, dsc *storagev1.StorageClass) error {
	if ssc != nil && dsc != nil {
		if ssc.Provisioner != dsc.Provisioner {
			return fmt.Errorf("provisioner should not be changed, now from %s to %s", ssc.Provisioner, dsc.Provisioner)
		}
		if ssc.Provisioner != m.Name() {
			return fmt.Errorf("provisioner should be %s, now is %s", m.Name(), ssc.Provisioner)
		}
	} else {
		m.Logger.Info("storage class is nil, skip validation")
	}
	return nil
}

func (m *DiskModifier) Modify(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
	pv *corev1.PersistentVolume,
	sc *storagev1.StorageClass,
) ( /*wait*/ bool, error) {
	logger := m.Logger.WithValues("namespace", pvc.Namespace, "pvc", pvc.Name)
	logger.Info("Starting Modify for PVC")

	if pv == nil {
		m.Logger.Info("Persistent volume is nil, skip modifying PV. This may be caused by no relevant permissions", "pv", pvc.Spec.VolumeName)
		return false, nil
	}

	// Check rate limiting BEFORE proceeding with modification
	// Use UTC time for consistency across different timezones
	now := m.Clock.Now().UTC()

	history, err := getModifyHistory(pvc)
	if err != nil {
		// Distinguish between missing history (backward compatible) and corrupted data
		if pvc.Annotations != nil && pvc.Annotations[annoKeyAzureDiskModifyHistory] != "" {
			// History exists but is corrupted - reject to prevent data loss
			return false, fmt.Errorf("corrupted modify history, refusing modification to maintain safety: %w", err)
		}
		// No history annotation - backward compatibility, allow modification
		logger.Info("no modify history found, allowing modification (backward compatibility)")
		history = &ModifyHistory{Timestamps: []time.Time{}}
	}

	canModify, reason := history.CanModify(now)
	if !canModify {
		nextTime := history.NextAvailableTime(now)
		waitDur := nextTime.Sub(now)
		validMods := history.filterValidModifications(now)
		logger.Info("modification rate limited",
			"reason", reason,
			"nextAvailableTime", nextTime,
			"waitDuration", waitDur,
			"currentModifications", len(validMods),
			"isNewDisk", history.isNewDisk(now))
		return true, nil // return wait=true to trigger retry later
	}

	// Reserve modification slot immediately to prevent race conditions
	// This is crucial for concurrent safety: record intent before actual modification
	if err := recordModification(pvc, now); err != nil {
		return false, fmt.Errorf("failed to reserve modification slot: %w", err)
	}

	// Getting expected volume for PVC
	desired, err := getExpectedVolume(pvc, pv, sc)
	if err != nil {
		return false, fmt.Errorf("error getting expected volume: %w", err)
	}

	diskName, subscriptionID, resourceGroupName, err := getDiskInfoFromVolumeID(desired.VolumeID)
	if err != nil {
		return false, fmt.Errorf("failed to get Azure disk info from PV: %w", err)
	}

	if err = m.setDiskClientForPV(subscriptionID); err != nil {
		return false, fmt.Errorf("failed to create disk client: %w", err)
	}

	// Getting current volume status for PVC
	actual, err := m.getCurrentVolumeStatus(ctx, desired.VolumeID)
	if err != nil {
		return false, fmt.Errorf("getting current volume status: %w", err)
	}

	// Save disk creation time if available (for rate limiting new disks)
	if actual.CreatedAt != nil {
		setDiskCreatedTime(pvc, *actual.CreatedAt)
	}

	if actual != nil {
		if !diffVolume(actual, desired) {
			// if actual volume is equal to desired volume, return completed
			logger.Info("Volume modification is already completed for PVC")
			return false, nil
		}
	}

	logger.Info("Begin disk update for PVC")
	if _, err = m.DiskClient.BeginUpdate(ctx, resourceGroupName, diskName, armcompute.DiskUpdate{
		Properties: &armcompute.DiskUpdateProperties{
			DiskIOPSReadWrite: desired.IOPS,
			DiskSizeGB:        desired.Size,
			DiskMBpsReadWrite: desired.Throughput,
		},
	}, nil); err != nil {
		// Azure API call failed - remove the reserved modification slot
		if removeErr := removeLastModification(pvc); removeErr != nil {
			logger.Error(removeErr, "failed to remove reserved modification slot after Azure API failure")
		}
		return false, fmt.Errorf("failed to update disk: %w", err)
	}

	// Modification timestamp already recorded at line 235 (before API call)
	// This ensures concurrent safety by reserving the slot before actual operation
	logger.Info("Successfully modified volume for PVC")
	return true, nil
}

// diffVolume checks if the actual volume is equal to the desired volume.
// If actual volume is equal to desired volume, return false.
func diffVolume(actual, desired *Volume) bool {
	return utils.ValuesDiffer(actual.IOPS, desired.IOPS) ||
		utils.ValuesDiffer(actual.Throughput, desired.Throughput) ||
		utils.ValuesDiffer(actual.Size, desired.Size) ||
		(actual.Type != "" && desired.Type != "" && actual.Type != desired.Type)
}

func (m *DiskModifier) getCurrentVolumeStatus(ctx context.Context, volumeID string) (*Volume, error) {
	diskName, _, resourceGroupName, err := getDiskInfoFromVolumeID(volumeID)
	if err != nil {
		return nil, fmt.Errorf("failed to get Azure disk info from PV: %w", err)
	}

	diskResponse, err := m.DiskClient.Get(ctx, resourceGroupName, diskName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get disk: %w", err)
	}

	disk := diskResponse.Disk
	return &Volume{
		VolumeID:   *disk.ID,
		Size:       disk.Properties.DiskSizeGB,
		IOPS:       disk.Properties.DiskIOPSReadWrite,
		Throughput: disk.Properties.DiskMBpsReadWrite,
		Type:       string(*disk.SKU.Name),
		CreatedAt:  disk.Properties.TimeCreated, // Extract disk creation time from Azure API
	}, nil
}

func getExpectedVolume(
	pvc *corev1.PersistentVolumeClaim,
	pv *corev1.PersistentVolume,
	sc *storagev1.StorageClass,
) (*Volume, error) {
	v := Volume{
		VolumeID: pv.Spec.CSI.VolumeHandle,
	}
	if err := utilerrors.NewAggregate([]error{
		setArgsFromPVC(&v, pvc),
		setArgsFromStorageClass(&v, sc),
	}); err != nil {
		return nil, err
	}
	return &v, nil
}

func (*DiskModifier) MinWaitDuration() time.Duration {
	// Return a short polling interval since we track rate limits internally.
	// This allows the controller to check frequently if rate limit window has expired.
	return defaultWaitDuration
}

func setArgsFromPVC(v *Volume, pvc *corev1.PersistentVolumeClaim) error {
	size, err := getSizeFromPVC(pvc)
	if err != nil {
		return err
	}
	v.Size = ptr.To(int32(size)) //nolint: gosec // by design
	return nil
}

func getSizeFromPVC(pvc *corev1.PersistentVolumeClaim) (int64, error) {
	quantity := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
	sizeBytes := quantity.ScaledValue(0)
	size := sizeBytes / 1024 / 1024 / 1024

	if size < minSize || size > maxSize {
		return 0, fmt.Errorf("invalid storage size: %v", quantity)
	}
	return size, nil
}

func setArgsFromStorageClass(v *Volume, sc *storagev1.StorageClass) error {
	if sc == nil {
		return nil
	}
	throughput, err := getParamInt64(sc.Parameters, paramKeyThroughput)
	if err != nil {
		return err
	}
	v.Throughput = throughput

	iops, err := getParamInt64(sc.Parameters, paramKeyIOPS)
	if err != nil {
		return err
	}
	v.IOPS = iops

	v.Type = sc.Parameters[paramKeyType]
	return nil
}

func getParamInt64(params map[string]string, key string) (*int64, error) {
	str, ok := params[key]
	if !ok || str == "" {
		return nil, nil
	}
	param, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("can't parse %v param in storage class: %w", key, err)
	}

	return ptr.To(param), nil
}

func getDiskInfoFromVolumeID(volumeID string) (diskName, subscriptionID, resourceGroupName string, err error) {
	// get diskName, subscriptionID, resourceGroupName from volumeHandle
	// example: /subscriptions/xxxx/resourceGroups/xxxx/providers/Microsoft.Compute/disks/xxxx
	parts := strings.Split(volumeID, "/")
	if len(parts) != volumeIDPartsLength {
		return "", "", "", fmt.Errorf("invalid volumeHandle format")
	}
	subscriptionID = parts[2]
	resourceGroupName = parts[4]
	diskName = parts[len(parts)-1]

	return diskName, subscriptionID, resourceGroupName, nil
}

func (m *DiskModifier) setDiskClientForPV(subscriptionID string) error {
	if m.DiskClient != nil {
		return nil
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to obtain a credential: %w", err)
	}

	diskClient, err := armcompute.NewDisksClient(subscriptionID, cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create disk client: %w", err)
	}

	m.DiskClient = diskClient

	return nil
}

// getModifyHistory retrieves modification history from PVC annotations.
// Returns empty history if annotations are not set (backward compatibility).
func getModifyHistory(pvc *corev1.PersistentVolumeClaim) (*ModifyHistory, error) {
	history := &ModifyHistory{Timestamps: []time.Time{}}

	if pvc.Annotations == nil {
		return history, nil
	}

	// Load disk creation time
	if createdStr, ok := pvc.Annotations[annoKeyAzureDiskCreatedAt]; ok && createdStr != "" {
		created, err := time.Parse(time.RFC3339, createdStr)
		if err == nil {
			history.CreatedAt = &created
		}
	}

	// Load modification history
	if historyStr, ok := pvc.Annotations[annoKeyAzureDiskModifyHistory]; ok && historyStr != "" {
		var timestampStrs []string
		if err := json.Unmarshal([]byte(historyStr), &timestampStrs); err != nil {
			return nil, fmt.Errorf("failed to parse modify history: %w", err)
		}

		for _, ts := range timestampStrs {
			t, err := time.Parse(time.RFC3339, ts)
			if err == nil {
				history.Timestamps = append(history.Timestamps, t)
			}
		}
	}

	return history, nil
}

// recordModification adds a new modification timestamp to PVC annotations.
// Automatically cleans up timestamps older than 24h to keep annotation size small.
//
// Note: This function only updates the in-memory PVC object. Annotation persistence
// to Kubernetes API Server is handled by raw_modifier to ensure atomicity using
// optimistic locking (resourceVersion). The caller is responsible for persisting
// these changes to prevent data loss in concurrent scenarios.
func recordModification(pvc *corev1.PersistentVolumeClaim, modTime time.Time) error {
	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
	}

	history, err := getModifyHistory(pvc)
	if err != nil {
		// If we can't parse existing history, start fresh to avoid blocking operations
		history = &ModifyHistory{Timestamps: []time.Time{}}
	}

	// Add new timestamp (in UTC for consistency)
	history.Timestamps = append(history.Timestamps, modTime.UTC())

	// Clean up old timestamps to prevent annotation from growing indefinitely
	history.Timestamps = history.filterValidModifications(modTime)

	// Serialize timestamps as array of RFC3339 strings
	var timestampStrs []string
	for _, ts := range history.Timestamps {
		timestampStrs = append(timestampStrs, ts.Format(time.RFC3339))
	}

	data, err := json.Marshal(timestampStrs)
	if err != nil {
		return fmt.Errorf("failed to marshal timestamps: %w", err)
	}

	pvc.Annotations[annoKeyAzureDiskModifyHistory] = string(data)
	return nil
}

// setDiskCreatedTime saves disk creation time to PVC annotation.
// Only sets the value if not already present (immutable after first set).
func setDiskCreatedTime(pvc *corev1.PersistentVolumeClaim, createdAt time.Time) {
	if pvc.Annotations == nil {
		pvc.Annotations = make(map[string]string)
	}

	// Only set if not already set (creation time is immutable)
	if _, exists := pvc.Annotations[annoKeyAzureDiskCreatedAt]; !exists {
		pvc.Annotations[annoKeyAzureDiskCreatedAt] = createdAt.UTC().Format(time.RFC3339)
	}
}

// removeLastModification removes the most recent modification timestamp from PVC annotations.
// This is used to rollback a reserved modification slot when Azure API call fails.
//
// Note: This function only updates the in-memory PVC object. The caller (raw_modifier)
// is responsible for persisting these changes to Kubernetes API Server.
func removeLastModification(pvc *corev1.PersistentVolumeClaim) error {
	if pvc.Annotations == nil {
		return nil // Nothing to remove
	}

	history, err := getModifyHistory(pvc)
	if err != nil {
		return fmt.Errorf("failed to get modify history for removal: %w", err)
	}

	if len(history.Timestamps) == 0 {
		return nil // No timestamps to remove
	}

	// Remove the last timestamp (most recent)
	history.Timestamps = history.Timestamps[:len(history.Timestamps)-1]

	// Serialize remaining timestamps
	var timestampStrs []string
	for _, ts := range history.Timestamps {
		timestampStrs = append(timestampStrs, ts.Format(time.RFC3339))
	}

	data, err := json.Marshal(timestampStrs)
	if err != nil {
		return fmt.Errorf("failed to marshal timestamps after removal: %w", err)
	}

	pvc.Annotations[annoKeyAzureDiskModifyHistory] = string(data)
	return nil
}
