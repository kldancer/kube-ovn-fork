package ovs

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/klog/v2"

	"github.com/kubeovn/kube-ovn/pkg/util"
)

const (
	ovsTableQos          = "qos"
	ovsTableQueue        = "queue"
	ovsColumnOtherConfig = "other_config"
	ovsColumnExternalIDs = "external-ids"
	ovsKeyMaxRate        = "max-rate"
	ovsKeyMinRate        = "min-rate"
	ovsKeyIfaceID        = "iface-id" // Key within external-ids

	defaultQueueIndexSuffix = "-0"
	clusterQueueIndexSuffix = "-1"
)

// SetInterfaceBandwidth set ingress/egress qos for given pod, annotation values are for node/pod
// but ingress/egress parameters here are from the point of ovs port/interface view, so reverse input parameters when call func SetInterfaceBandwidth
func SetInterfaceBandwidth(podName, podNamespace, iface, ingress, egress string) error {
	ingressMPS, _ := strconv.Atoi(ingress)
	ingressKPS := ingressMPS * 1000
	interfaceList, err := ovsFind("interface", "name", fmt.Sprintf("external-ids:iface-id=%s", iface))
	if err != nil {
		klog.Error(err)
		return err
	}

	qosIfaceUIDMap, err := ListExternalIDs("qos")
	if err != nil {
		klog.Error(err)
		return err
	}

	queueIfaceUIDMap, err := ListExternalIDs("queue")
	if err != nil {
		klog.Error(err)
		return err
	}

	for _, ifName := range interfaceList {
		// ingress_policing_rate is in Kbps
		err := ovsSet("interface", ifName, fmt.Sprintf("ingress_policing_rate=%d", ingressKPS), fmt.Sprintf("ingress_policing_burst=%d", ingressKPS*8/10))
		if err != nil {
			klog.Error(err)
			return err
		}

		egressMPS, _ := strconv.Atoi(egress)
		egressBPS := egressMPS * 1000 * 1000

		if egressBPS > 0 {
			queueUID, err := SetHtbQosQueueRecord(podName, podNamespace, iface, egressBPS, queueIfaceUIDMap)
			if err != nil {
				klog.Error(err)
				return err
			}

			if err = SetQosQueueBinding(podName, podNamespace, ifName, iface, queueUID, qosIfaceUIDMap); err != nil {
				return err
			}
		} else {
			if qosUID, ok := qosIfaceUIDMap[iface]; ok {
				qosType, err := ovsGet("qos", qosUID, "type", "")
				if err != nil {
					klog.Error(err)
					return err
				}
				if qosType != util.HtbQos {
					continue
				}
				queueID, err := ovsGet("qos", qosUID, "queues", "0")
				if err != nil {
					klog.Error(err)
					return err
				}

				if _, err := Exec("remove", "queue", queueID, "other_config", "max-rate"); err != nil {
					return fmt.Errorf("failed to remove rate limit for queue in pod %v/%v, %v", podNamespace, podName, err)
				}
			}
		}

		// Delete Qos and Queue record if both bandwidth and priority do not exist
		if err = CheckAndUpdateHtbQos(podName, podNamespace, iface, queueIfaceUIDMap); err != nil {
			klog.Errorf("failed to check htb qos: %v", err)
			return err
		}
	}
	return nil
}

func SetInterfaceEgressBandwidth(iface string, clusterNetworkBandwidth, maxBandwidth, mark int) error {
	klog.Infof("Set interface %s egress clusterNetwork bandwidth [%d] maxBandwidth [%d]", iface, clusterNetworkBandwidth, maxBandwidth)

	qosIfaceUIDMap, err := ListExternalIDs(ovsTableQos)
	if err != nil {
		klog.Error(err)
		return err
	}

	queueIfaceUIDMap, err := ListExternalIDs(ovsTableQueue)
	if err != nil {
		klog.Error(err)
		return err
	}

	queueIdList := make([]string, 0)

	clusterNetworkBandwidthBPS := clusterNetworkBandwidth * 1000 * 1000
	maxBandwidthBPS := maxBandwidth * 1000 * 1000

	if clusterNetworkBandwidthBPS > maxBandwidthBPS {
		klog.Warningf("clusterNetworkBandwidthBPS %d can not  greater than maxBandwidthBPS %d, skip...", clusterNetworkBandwidthBPS, maxBandwidthBPS)
		clusterNetworkBandwidthBPS = maxBandwidthBPS
	}

	defaultBandwidthBPS := 0
	if clusterNetworkBandwidthBPS > 0 {
		defaultBandwidthBPS = maxBandwidthBPS - clusterNetworkBandwidthBPS
	}

	if defaultBandwidthBPS > 0 {
		queueIdList, err = SetClusterNetworkHtbQosQueueRecord(iface, defaultBandwidthBPS, clusterNetworkBandwidthBPS, queueIfaceUIDMap)
		if err != nil {
			klog.Error(err)
			return err
		}
		if err = SetQosMultipleQueueBinding(iface, queueIdList, qosIfaceUIDMap, maxBandwidthBPS); err != nil {
			return err
		}

		if err := util.AddIfTcFilter(iface, uint32(mark)); err != nil {
			klog.Errorf("failed to add tc filter: %v", err)
		}

	}

	qosUID, qosExists := qosIfaceUIDMap[iface]
	if defaultBandwidthBPS <= 0 {
		if qosExists {
			klog.Infof("The default network bandwidth cannot be less than or equal to 0! clear the existing queue")
			if err := removeQueueRateLimit(qosUID, "0"); err != nil {
				return err
			}

			if err := removeQueueRateLimit(qosUID, "1"); err != nil { // Pass index "1"
				return err
			}
			// 如果带宽不存在，则删除 Qos 和 Queue 记录
			if err = CheckAndUpdateClusterNetworkHtbQos(iface, queueIfaceUIDMap); err != nil {
				klog.Errorf("failed to check/update htb qos for iface %s: %v", iface, err)
				return err
			}
		}
	}

	return nil
}

func removeQueueRateLimit(qosUID string, queueIndex string) error {
	qosType, err := ovsGet(ovsTableQos, qosUID, "type", "")
	if err != nil {
		klog.Error(err)
		return err // Or wrap error
	}
	// 假设只有 HTB 类型需要此清理
	if qosType != util.HtbQos {
		klog.Infof("QoS type is %s, not HTB. Skipping rate removal for queue index %s.", qosType, queueIndex)
		return nil
	}

	queueID, err := ovsGet(ovsTableQos, qosUID, ovsTableQueue, queueIndex)
	if err != nil {
		klog.Errorf("Failed to get queue ID for index %s on qos %s: %v", queueIndex, qosUID, err)
		return err
	}

	// 在尝试删除之前检查 queueID 是否有效
	if queueID == "" || queueID == "0" { // Example check, adjust based on ovsGet behavior
		klog.Warningf("No valid queue found for index %s on qos %s .", queueIndex, qosUID)
		return nil
	}

	_, err = Exec("remove", ovsTableQueue, queueID, ovsColumnOtherConfig, ovsKeyMaxRate)
	if err != nil {
		return fmt.Errorf("failed to remove max-rate limit for queue %s (index %s) : %w", queueID, queueIndex, err)
	}

	_, err = Exec("remove", ovsTableQueue, queueID, ovsColumnOtherConfig, ovsKeyMinRate)
	if err != nil {
		return fmt.Errorf("failed to remove min-rate limit for queue %s (index %s): %w", queueID, queueIndex, err)
	}

	klog.Infof("Successfully removed max-rate、min-rate for queue %s (index %s)", queueID, queueIndex)
	return nil
}

func ClearHtbQosQueue(podName, podNamespace, iface string) error {
	var queueList []string
	var err error
	if iface != "" {
		queueList, err = ovsFind("queue", "_uuid", fmt.Sprintf(`external-ids:iface-id="%s"`, iface))
		if err != nil {
			klog.Error(err)
			return err
		}
	} else {
		queueList, err = ovsFind("queue", "_uuid", fmt.Sprintf(`external-ids:pod="%s/%s"`, podNamespace, podName))
		if err != nil {
			klog.Error(err)
			return err
		}
	}

	// https://github.com/kubeovn/kube-ovn/issues/1191
	qosQueueMap, err := ListQosQueueIDs()
	if err != nil {
		klog.Error(err)
		return err
	}

	for _, queueID := range queueList {
		found := false
		for _, usedQueueID := range qosQueueMap {
			if queueID == usedQueueID {
				found = true
				break
			}
		}
		if found {
			continue
		}

		if err := ovsDestroy("queue", queueID); err != nil {
			return err
		}
	}
	return nil
}

func IsHtbQos(iface string) (bool, error) {
	qosType, err := ovsFind("qos", "type", fmt.Sprintf(`external-ids:iface-id="%s"`, iface))
	if err != nil {
		klog.Error(err)
		return false, err
	}

	if len(qosType) != 0 && qosType[0] == util.HtbQos {
		return true, nil
	}
	return false, nil
}

func SetHtbQosQueueRecord(podName, podNamespace, iface string, maxRateBPS int, queueIfaceUIDMap map[string]string) (string, error) {
	var queueCommandValues []string
	var err error
	if maxRateBPS > 0 {
		queueCommandValues = append(queueCommandValues, fmt.Sprintf("other_config:max-rate=%d", maxRateBPS))
	}

	if queueUID, ok := queueIfaceUIDMap[iface]; ok {
		if err := ovsSet("queue", queueUID, queueCommandValues...); err != nil {
			return queueUID, err
		}
	} else {
		queueCommandValues = append(queueCommandValues, fmt.Sprintf("external-ids:iface-id=%s", iface))
		if podNamespace != "" && podName != "" {
			queueCommandValues = append(queueCommandValues, fmt.Sprintf("external-ids:pod=%s/%s", podNamespace, podName))
		}

		var queueID string
		if queueID, err = ovsCreate("queue", queueCommandValues...); err != nil {
			return queueUID, err
		}
		queueIfaceUIDMap[iface] = queueID
	}

	return queueIfaceUIDMap[iface], nil
}

// SetClusterNetworkHtbQosQueueRecord 确保接口存在两条 OVS 队列记录，一条用于默认流量，一条用于集群网络流量，配置了指定的带宽.
//
// 利用缓存 （queueIfaceUIDMap） 来查找现有记录，并在创建新记录时更新它。
// 返回包含两个队列 [defaultUUID， clusterUUID] 的 UUID 的切片。
func SetClusterNetworkHtbQosQueueRecord(iface string, defaultBandwidthBPS, clusterNetworkBandwidthBPS int, queueIfaceUIDMap map[string]string) ([]string, error) {
	queueIdList := make([]string, 2) // Pre-allocate slice for 2 UUIDs
	var err error

	queueIdList[0], err = ensureOvsQueueRecord(iface, defaultQueueIndexSuffix, defaultBandwidthBPS, queueIfaceUIDMap)
	if err != nil {
		return nil, err // Return immediately on first error
	}

	queueIdList[1], err = ensureOvsQueueRecord(iface, clusterQueueIndexSuffix, clusterNetworkBandwidthBPS, queueIfaceUIDMap)
	if err != nil {
		return nil, err // Return immediately on second error
	}

	// Both queues ensured successfully
	return queueIdList, nil
}

// ensureOvsQueueRecord 为接口和索引创建或更新特定的 OVS 队列记录.
//
// 如果 bandwidthBPS > 0，则设置 max-rate 和 min-rate。
// 确保 external-ids：iface-id 标签存在。
// 使用队列的 UUID 更新 queueCache 映射。
// 返回队列 UUID 和错误（如果发生）。
func ensureOvsQueueRecord(iface, queueIndexSuffix string, bandwidthBPS int, queueCache map[string]string) (string, error) {
	queueKey := iface + queueIndexSuffix
	ovsArgs := []string{} // Arguments for ovsSet or ovsCreate

	if bandwidthBPS > 0 {
		maxRateStr := strconv.Itoa(bandwidthBPS)
		minRateStr := strconv.Itoa(bandwidthBPS)
		ovsArgs = append(ovsArgs, fmt.Sprintf("%s:%s=%s", ovsColumnOtherConfig, ovsKeyMaxRate, maxRateStr))
		ovsArgs = append(ovsArgs, fmt.Sprintf("%s:%s=%s", ovsColumnOtherConfig, ovsKeyMinRate, minRateStr))
	}

	if queueUID, ok := queueCache[queueKey]; ok {
		klog.V(4).Infof("Queue record found for key %s (UUID: %s). Updating...", queueKey, queueUID)
		if len(ovsArgs) > 0 {
			if err := ovsSet(ovsTableQueue, queueUID, ovsArgs...); err != nil {
				return "", fmt.Errorf("failed to set OVS queue config for existing record %s (key: %s): %w", queueUID, queueKey, err)
			}
			klog.V(4).Infof("Successfully updated OVS queue %s for key %s with args: %v", queueUID, queueKey, ovsArgs)
		} else {
			klog.V(4).Infof("No rate updates required for existing OVS queue %s (key: %s) as bandwidth is <= 0.", queueUID, queueKey)
		}
		return queueUID, nil // Return existing UUID
	} else {
		klog.V(4).Infof("No queue record found for key %s. Creating...", queueKey)
		ovsArgs = append(ovsArgs, fmt.Sprintf("%s:%s=%s", ovsColumnExternalIDs, ovsKeyIfaceID, queueKey))

		queueUUID, err := ovsCreate(ovsTableQueue, ovsArgs...)
		if err != nil {
			return "", fmt.Errorf("failed to create OVS queue for key %s with args %v: %w", queueKey, ovsArgs, err)
		}
		queueCache[queueKey] = queueUUID
		klog.V(4).Infof("Successfully created OVS queue for key %s (UUID: %s) with args: %v", queueKey, queueUUID, ovsArgs)
		return queueUUID, nil
	}
}

// SetQosQueueBinding set qos related to queue record.
func SetQosQueueBinding(podName, podNamespace, ifName, iface, queueUID string, qosIfaceUIDMap map[string]string) error {
	var qosCommandValues []string
	qosCommandValues = append(qosCommandValues, fmt.Sprintf("queues:0=%s", queueUID))

	if qosUID, ok := qosIfaceUIDMap[iface]; !ok {
		qosCommandValues = append(qosCommandValues, "type=linux-htb", fmt.Sprintf(`external-ids:iface-id="%s"`, iface))
		if podNamespace != "" && podName != "" {
			qosCommandValues = append(qosCommandValues, fmt.Sprintf("external-ids:pod=%s/%s", podNamespace, podName))
		}
		qos, err := ovsCreate("qos", qosCommandValues...)
		if err != nil {
			klog.Error(err)
			return err
		}
		err = ovsSet("port", ifName, fmt.Sprintf("qos=%s", qos))
		if err != nil {
			klog.Error(err)
			return err
		}
		qosIfaceUIDMap[iface] = qos
	} else {
		qosType, err := ovsGet("qos", qosUID, "type", "")
		if err != nil {
			klog.Error(err)
			return err
		}
		if qosType != util.HtbQos {
			klog.Errorf("netem qos exists for pod %s/%s, conflict with current qos, will be changed to htb qos", podNamespace, podName)
			qosCommandValues = append(qosCommandValues, "type=linux-htb")
		}

		if qosType == util.HtbQos {
			queueID, err := ovsGet("qos", qosUID, "queues", "0")
			if err != nil {
				klog.Error(err)
				return err
			}
			if queueID == queueUID {
				return nil
			}
		}

		if err := ovsSet("qos", qosUID, qosCommandValues...); err != nil {
			return err
		}
	}
	return nil
}

func SetQosMultipleQueueBinding(iface string, queueUIDs []string, qosIfaceUIDMap map[string]string, maxBandWidth int) error {
	var qosCommandValues []string
	for i, q := range queueUIDs {
		qosCommandValues = append(qosCommandValues, fmt.Sprintf("queues:%d=%s", i, q))
	}

	if qosUID, ok := qosIfaceUIDMap[iface]; !ok {
		qosCommandValues = append(qosCommandValues, "type=linux-htb", fmt.Sprintf(`external-ids:iface-id="%s"`, iface))
		qosCommandValues = append(qosCommandValues, fmt.Sprintf("other-config:max-rate=%d", maxBandWidth))
		qos, err := ovsCreate("qos", qosCommandValues...)
		if err != nil {
			klog.Error(err)
			return err
		}
		err = ovsSet("port", iface, fmt.Sprintf("qos=%s", qos))
		if err != nil {
			klog.Error(err)
			return err
		}
		qosIfaceUIDMap[iface] = qos
	} else {
		qosType, err := ovsGet("qos", qosUID, "type", "")
		if err != nil {
			klog.Error(err)
			return err
		}
		if qosType != util.HtbQos {
			klog.Errorf("netem qos exists for iface %s, conflict with current qos, will be changed to htb qos", iface)
			qosCommandValues = append(qosCommandValues, "type=linux-htb")
		}

		if qosType == util.HtbQos {
			for i, q := range queueUIDs {
				queueID, err := ovsGet("qos", qosUID, "queues", strconv.Itoa(i))
				if err != nil {
					klog.Error(err)
					return err
				}
				if queueID != q {
					break
				}
				return nil
			}
		}

		if err := ovsSet("qos", qosUID, qosCommandValues...); err != nil {
			return err
		}
	}
	return nil
}

// The latency value expressed in us.
func SetNetemQos(podName, podNamespace, iface, latency, limit, loss, jitter string) error {
	latencyMs, _ := strconv.Atoi(latency)
	latencyUs := latencyMs * 1000
	jitterMs, _ := strconv.Atoi(jitter)
	jitterUs := jitterMs * 1000
	limitPkts, _ := strconv.Atoi(limit)
	lossPercent, _ := strconv.ParseFloat(loss, 64)

	interfaceList, err := ovsFind("interface", "name", fmt.Sprintf("external-ids:iface-id=%s", iface))
	if err != nil {
		klog.Error(err)
		return err
	}

	for _, ifName := range interfaceList {
		qosList, err := GetQosList(podName, podNamespace, iface)
		if err != nil {
			klog.Error(err)
			return err
		}

		var qosCommandValues []string
		if latencyMs > 0 {
			qosCommandValues = append(qosCommandValues, fmt.Sprintf("other_config:latency=%d", latencyUs))
		}
		if jitterMs > 0 {
			qosCommandValues = append(qosCommandValues, fmt.Sprintf("other_config:jitter=%d", jitterUs))
		}
		if limitPkts > 0 {
			qosCommandValues = append(qosCommandValues, fmt.Sprintf("other_config:limit=%d", limitPkts))
		}
		if lossPercent > 0 {
			qosCommandValues = append(qosCommandValues, fmt.Sprintf("other_config:loss=%v", lossPercent))
		}
		if latencyMs > 0 || limitPkts > 0 || lossPercent > 0 || jitterMs > 0 {
			if len(qosList) == 0 {
				qosCommandValues = append(qosCommandValues, "type=linux-netem", fmt.Sprintf(`external-ids:iface-id="%s"`, iface))
				if podNamespace != "" && podName != "" {
					qosCommandValues = append(qosCommandValues, fmt.Sprintf("external-ids:pod=%s/%s", podNamespace, podName))
				}

				qos, err := ovsCreate("qos", qosCommandValues...)
				if err != nil {
					klog.Error(err)
					return err
				}

				if err = ovsSet("port", ifName, fmt.Sprintf("qos=%s", qos)); err != nil {
					klog.Error(err)
					return err
				}
			} else {
				for _, qos := range qosList {
					qosType, err := ovsGet("qos", qos, "type", "")
					if err != nil {
						klog.Error(err)
						return err
					}
					if qosType != util.NetemQos {
						klog.Errorf("htb qos with higher priority exists for pod %v/%v, conflict with netem qos config, please delete htb qos first", podNamespace, podName)
						return nil
					}

					latencyVal, lossVal, limitVal, jitterVal, err := getNetemQosConfig(qos)
					if err != nil {
						klog.Error(err)
						klog.Errorf("failed to get other_config for qos %s: %v", qos, err)
						return err
					}

					if latencyVal == strconv.Itoa(latencyUs) && limitVal == limit && lossVal == loss && jitterVal == strconv.Itoa(jitterUs) {
						klog.Info("no value changed for netem qos, ignore")
						continue
					}

					if err = deleteNetemQosByID(qos, iface, podName, podNamespace); err != nil {
						klog.Errorf("failed to delete netem qos: %v", err)
						return err
					}

					qosCommandValues = append(qosCommandValues, "type=linux-netem", fmt.Sprintf(`external-ids:iface-id="%s"`, iface))
					if podNamespace != "" && podName != "" {
						qosCommandValues = append(qosCommandValues, fmt.Sprintf("external-ids:pod=%s/%s", podNamespace, podName))
					}

					qos, err := ovsCreate("qos", qosCommandValues...)
					if err != nil {
						klog.Errorf("failed to create netem qos: %v", err)
						return err
					}

					if err = ovsSet("port", ifName, fmt.Sprintf("qos=%s", qos)); err != nil {
						klog.Errorf("failed to set netem qos to port: %v", err)
						return err
					}
				}
			}
		} else {
			for _, qos := range qosList {
				if err := deleteNetemQosByID(qos, iface, podName, podNamespace); err != nil {
					klog.Errorf("failed to delete netem qos: %v", err)
					return err
				}
			}
		}
	}
	return nil
}

func getNetemQosConfig(qosID string) (string, string, string, string, error) {
	var latency, loss, limit, jitter string

	config, err := ovsGet("qos", qosID, "other_config", "")
	if err != nil {
		klog.Errorf("failed to get other_config for qos %s: %v", qosID, err)
		return latency, loss, limit, jitter, err
	}
	if len(config) == 0 {
		return latency, loss, limit, jitter, nil
	}

	values := strings.Split(strings.Trim(config, "{}"), ",")
	for _, value := range values {
		records := strings.Split(value, "=")
		switch strings.TrimSpace(records[0]) {
		case "latency":
			latency = strings.TrimSpace(records[1])
		case "loss":
			loss = strings.TrimSpace(records[1])
		case "limit":
			limit = strings.TrimSpace(records[1])
		case "jitter":
			jitter = strings.TrimSpace(records[1])
		}
	}
	return latency, loss, limit, jitter, nil
}

func deleteNetemQosByID(qosID, iface, podName, podNamespace string) error {
	qosType, _ := ovsGet("qos", qosID, "type", "")
	if qosType != util.NetemQos {
		return nil
	}

	if err := ClearPortQosBinding(iface); err != nil {
		klog.Errorf("failed to delete qos bingding info for interface %s: %v", iface, err)
		return err
	}

	// reuse this function to delete qos record
	if err := ClearPodBandwidth(podName, podNamespace, iface); err != nil {
		klog.Errorf("failed to delete netemqos record for pod %s/%s: %v", podNamespace, podName, err)
		return err
	}
	return nil
}

func IsUserspaceDataPath() (is bool, err error) {
	dp, err := ovsFind("bridge", "datapath_type", "name=br-int")
	if err != nil {
		klog.Error(err)
		return false, err
	}
	return len(dp) > 0 && dp[0] == "netdev", nil
}

func CheckAndUpdateHtbQos(podName, podNamespace, ifaceID string, queueIfaceUIDMap map[string]string) error {
	var queueUID string
	var ok bool
	if queueUID, ok = queueIfaceUIDMap[ifaceID]; !ok {
		return nil
	}

	config, err := ovsGet("queue", queueUID, "other_config", "")
	if err != nil {
		klog.Errorf("failed to get other_config for queueID %s: %v", queueUID, err)
		return err
	}
	// bandwidth or priority exists, can not delete qos
	if config != "{}" {
		return nil
	}

	if htbQos, _ := IsHtbQos(ifaceID); !htbQos {
		return nil
	}

	if err := ClearPortQosBinding(ifaceID); err != nil {
		klog.Errorf("failed to delete qos binding info: %v", err)
		return err
	}

	if err := ClearPodBandwidth(podName, podNamespace, ifaceID); err != nil {
		klog.Errorf("failed to delete htbqos record: %v", err)
		return err
	}

	if err := ClearHtbQosQueue(podName, podNamespace, ifaceID); err != nil {
		klog.Errorf("failed to delete htbqos queue: %v", err)
		return err
	}
	return nil
}

func CheckAndUpdateClusterNetworkHtbQos(ifaceID string, queueIfaceUIDMap map[string]string) error {
	if htbQos, _ := IsHtbQos(ifaceID); !htbQos {
		return nil
	}

	if err := ClearPortQosBindingByName(ifaceID); err != nil {
		klog.Errorf("failed to delete qos binding info: %v", err)
		return err
	}

	if err := ClearPodBandwidth("", "", ifaceID); err != nil {
		klog.Errorf("failed to delete htbqos record: %v", err)
		return err
	}

	for i := 0; i < 2; i++ {
		queueKey := ifaceID + fmt.Sprintf("-%d", i)
		var queueUID string
		var ok bool
		if queueUID, ok = queueIfaceUIDMap[queueKey]; !ok {
			return nil
		}

		config, err := ovsGet(ovsTableQueue, queueUID, "other_config", "")
		if err != nil {
			klog.Errorf("failed to get other_config for queueID %s: %v", queueUID, err)
			return err
		}
		// 带宽或优先级存在，无法删除 QoS
		if config != "{}" {
			continue
		}

		if err := ClearHtbQosQueue("", "", queueKey); err != nil {
			klog.Errorf("failed to delete htbqos queue: %v", err)
			return err
		}
	}

	return nil
}
