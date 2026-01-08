package model

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
)

var group2model2channels map[string]map[string][]int // enabled channel
var channelsIDM map[int]*Channel                     // all channels include disabled
var channelSyncLock sync.RWMutex

// 粘性模式的渠道索引存储
// key: "group:model:priority" -> 当前使用的渠道在该优先级渠道列表中的索引
var stickyChannelIndex sync.Map

// 粘性模式预切换：时间窗口内的请求计数器
// key: "group:model:priority" -> *WindowCounter
var stickyWindowCounter sync.Map

// WindowCounter 记录时间窗口内的请求数
type WindowCounter struct {
	count       int
	windowStart time.Time
	mu          sync.Mutex
}

// getStickyKey 生成粘性索引的key
func getStickyKey(group, model string, priority int64) string {
	return fmt.Sprintf("%s:%s:%d", group, model, priority)
}

// GetStickyChannelIndex 获取当前粘性索引
func GetStickyChannelIndex(group, model string, priority int64) int {
	key := getStickyKey(group, model, priority)
	if val, ok := stickyChannelIndex.Load(key); ok {
		return val.(int)
	}
	return 0
}

// AdvanceStickyChannelIndex 切换到下一个渠道（当前渠道失败时调用）
func AdvanceStickyChannelIndex(group, model string, priority int64, totalChannels int) {
	if totalChannels <= 1 {
		return
	}
	key := getStickyKey(group, model, priority)
	for {
		oldVal, _ := stickyChannelIndex.LoadOrStore(key, 0)
		oldIdx := oldVal.(int)
		newIdx := (oldIdx + 1) % totalChannels
		if stickyChannelIndex.CompareAndSwap(key, oldIdx, newIdx) {
			common.SysLog(fmt.Sprintf("sticky channel switched: %s, from index %d to %d", key, oldIdx, newIdx))
			// 切换渠道后重置计数器
			resetStickyWindowCounter(key)
			break
		}
	}
}

// RecordStickyRequest 记录一次成功请求，返回是否应该主动切换渠道
// 仅在粘性模式且配置了预切换参数时生效
func RecordStickyRequest(group, model string, priority int64, totalChannels int) bool {
	// 只有多渠道才需要切换
	if totalChannels <= 1 {
		return false
	}
	// 检查是否启用了预切换功能
	// windowSeconds <= 0 或 maxRequests <= 1 都视为禁用（maxRequests=1 没有意义，等同于每次都切换）
	windowSeconds := common.StickyWindowSeconds
	maxRequests := common.StickyMaxRequestsInWindow
	if windowSeconds <= 0 || maxRequests <= 1 {
		return false
	}

	key := getStickyKey(group, model, priority)
	now := time.Now()
	windowDuration := time.Duration(windowSeconds) * time.Second

	// 获取或创建计数器
	val, _ := stickyWindowCounter.LoadOrStore(key, &WindowCounter{
		count:       0,
		windowStart: now,
	})
	counter := val.(*WindowCounter)

	counter.mu.Lock()
	defer counter.mu.Unlock()

	// 窗口过期，重置
	if now.Sub(counter.windowStart) > windowDuration {
		counter.count = 1
		counter.windowStart = now
		return false
	}

	counter.count++
	if counter.count >= maxRequests {
		// 达到阈值，需要切换
		return true
	}
	return false
}

// resetStickyWindowCounter 重置计数器（切换渠道后调用）
func resetStickyWindowCounter(key string) {
	if val, ok := stickyWindowCounter.Load(key); ok {
		counter := val.(*WindowCounter)
		counter.mu.Lock()
		counter.count = 0
		counter.windowStart = time.Now()
		counter.mu.Unlock()
	}
}

func InitChannelCache() {
	if !common.MemoryCacheEnabled {
		return
	}
	newChannelId2channel := make(map[int]*Channel)
	var channels []*Channel
	DB.Find(&channels)
	for _, channel := range channels {
		newChannelId2channel[channel.Id] = channel
	}
	var abilities []*Ability
	DB.Find(&abilities)
	groups := make(map[string]bool)
	for _, ability := range abilities {
		groups[ability.Group] = true
	}
	newGroup2model2channels := make(map[string]map[string][]int)
	for group := range groups {
		newGroup2model2channels[group] = make(map[string][]int)
	}
	for _, channel := range channels {
		if channel.Status != common.ChannelStatusEnabled {
			continue // skip disabled channels
		}
		groups := strings.Split(channel.Group, ",")
		for _, group := range groups {
			models := strings.Split(channel.Models, ",")
			for _, model := range models {
				if _, ok := newGroup2model2channels[group][model]; !ok {
					newGroup2model2channels[group][model] = make([]int, 0)
				}
				newGroup2model2channels[group][model] = append(newGroup2model2channels[group][model], channel.Id)
			}
		}
	}

	// sort by priority
	for group, model2channels := range newGroup2model2channels {
		for model, channels := range model2channels {
			sort.Slice(channels, func(i, j int) bool {
				return newChannelId2channel[channels[i]].GetPriority() > newChannelId2channel[channels[j]].GetPriority()
			})
			newGroup2model2channels[group][model] = channels
		}
	}

	channelSyncLock.Lock()
	group2model2channels = newGroup2model2channels
	//channelsIDM = newChannelId2channel
	for i, channel := range newChannelId2channel {
		if channel.ChannelInfo.IsMultiKey {
			channel.Keys = channel.GetKeys()
			if channel.ChannelInfo.MultiKeyMode == constant.MultiKeyModePolling {
				if oldChannel, ok := channelsIDM[i]; ok {
					// 存在旧的渠道，如果是多key且轮询，保留轮询索引信息
					if oldChannel.ChannelInfo.IsMultiKey && oldChannel.ChannelInfo.MultiKeyMode == constant.MultiKeyModePolling {
						channel.ChannelInfo.MultiKeyPollingIndex = oldChannel.ChannelInfo.MultiKeyPollingIndex
					}
				}
			}
		}
	}
	channelsIDM = newChannelId2channel
	channelSyncLock.Unlock()
	common.SysLog("channels synced from database")
}

func SyncChannelCache(frequency int) {
	for {
		time.Sleep(time.Duration(frequency) * time.Second)
		common.SysLog("syncing channels from database")
		InitChannelCache()
	}
}

func GetRandomSatisfiedChannel(group string, model string, retry int) (*Channel, error) {
	// if memory cache is disabled, get channel directly from database
	if !common.MemoryCacheEnabled {
		return GetChannel(group, model, retry)
	}

	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	// First, try to find channels with the exact model name.
	channels := group2model2channels[group][model]

	// If no channels found, try to find channels with the normalized model name.
	if len(channels) == 0 {
		normalizedModel := ratio_setting.FormatMatchingModelName(model)
		channels = group2model2channels[group][normalizedModel]
	}

	if len(channels) == 0 {
		return nil, nil
	}

	if len(channels) == 1 {
		if channel, ok := channelsIDM[channels[0]]; ok {
			return channel, nil
		}
		return nil, fmt.Errorf("数据库一致性错误，渠道# %d 不存在，请联系管理员修复", channels[0])
	}

	uniquePriorities := make(map[int]bool)
	for _, channelId := range channels {
		if channel, ok := channelsIDM[channelId]; ok {
			uniquePriorities[int(channel.GetPriority())] = true
		} else {
			return nil, fmt.Errorf("数据库一致性错误，渠道# %d 不存在，请联系管理员修复", channelId)
		}
	}
	var sortedUniquePriorities []int
	for priority := range uniquePriorities {
		sortedUniquePriorities = append(sortedUniquePriorities, priority)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(sortedUniquePriorities)))

	if retry >= len(uniquePriorities) {
		retry = len(uniquePriorities) - 1
	}
	targetPriority := int64(sortedUniquePriorities[retry])

	// get the priority for the given retry number
	var sumWeight = 0
	var targetChannels []*Channel
	for _, channelId := range channels {
		if channel, ok := channelsIDM[channelId]; ok {
			if channel.GetPriority() == targetPriority {
				sumWeight += channel.GetWeight()
				targetChannels = append(targetChannels, channel)
			}
		} else {
			return nil, fmt.Errorf("数据库一致性错误，渠道# %d 不存在，请联系管理员修复", channelId)
		}
	}

	if len(targetChannels) == 0 {
		return nil, errors.New(fmt.Sprintf("no channel found, group: %s, model: %s, priority: %d", group, model, targetPriority))
	}

	// 粘性模式：一直使用同一个渠道，直到失败时才切换
	if common.ChannelSelectMode == "sticky" {
		stickyIdx := GetStickyChannelIndex(group, model, targetPriority)
		// 确保索引在有效范围内
		if stickyIdx >= len(targetChannels) {
			stickyIdx = 0
		}
		return targetChannels[stickyIdx], nil
	}

	// 随机模式（默认）：加权随机选择

	// smoothing factor and adjustment
	smoothingFactor := 1
	smoothingAdjustment := 0

	if sumWeight == 0 {
		// when all channels have weight 0, set sumWeight to the number of channels and set smoothing adjustment to 100
		// each channel's effective weight = 100
		sumWeight = len(targetChannels) * 100
		smoothingAdjustment = 100
	} else if sumWeight/len(targetChannels) < 10 {
		// when the average weight is less than 10, set smoothing factor to 100
		smoothingFactor = 100
	}

	// Calculate the total weight of all channels up to endIdx
	totalWeight := sumWeight * smoothingFactor

	// Generate a random value in the range [0, totalWeight)
	randomWeight := rand.Intn(totalWeight)

	// Find a channel based on its weight
	for _, channel := range targetChannels {
		randomWeight -= channel.GetWeight()*smoothingFactor + smoothingAdjustment
		if randomWeight < 0 {
			return channel, nil
		}
	}
	// return null if no channel is not found
	return nil, errors.New("channel not found")
}

// GetChannelCountByGroupModelPriority 获取指定分组、模型、优先级下的渠道数量
// 用于粘性模式下计算可切换的渠道总数
func GetChannelCountByGroupModelPriority(group, model string, priority int64) int {
	if !common.MemoryCacheEnabled {
		return 0 // 非内存缓存模式暂不支持
	}

	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	channels := group2model2channels[group][model]
	// 如果没找到，尝试归一化模型名（与 GetRandomSatisfiedChannel 保持一致）
	if len(channels) == 0 {
		normalizedModel := ratio_setting.FormatMatchingModelName(model)
		channels = group2model2channels[group][normalizedModel]
	}
	if len(channels) == 0 {
		return 0
	}

	count := 0
	for _, channelId := range channels {
		if channel, ok := channelsIDM[channelId]; ok {
			if channel.GetPriority() == priority {
				count++
			}
		}
	}
	return count
}

func CacheGetChannel(id int) (*Channel, error) {
	if !common.MemoryCacheEnabled {
		return GetChannelById(id, true)
	}
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	c, ok := channelsIDM[id]
	if !ok {
		return nil, fmt.Errorf("渠道# %d，已不存在", id)
	}
	return c, nil
}

func CacheGetChannelInfo(id int) (*ChannelInfo, error) {
	if !common.MemoryCacheEnabled {
		channel, err := GetChannelById(id, true)
		if err != nil {
			return nil, err
		}
		return &channel.ChannelInfo, nil
	}
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()

	c, ok := channelsIDM[id]
	if !ok {
		return nil, fmt.Errorf("渠道# %d，已不存在", id)
	}
	return &c.ChannelInfo, nil
}

func CacheUpdateChannelStatus(id int, status int) {
	if !common.MemoryCacheEnabled {
		return
	}
	channelSyncLock.Lock()
	defer channelSyncLock.Unlock()
	if channel, ok := channelsIDM[id]; ok {
		channel.Status = status
	}
	if status != common.ChannelStatusEnabled {
		// delete the channel from group2model2channels
		for group, model2channels := range group2model2channels {
			for model, channels := range model2channels {
				for i, channelId := range channels {
					if channelId == id {
						// remove the channel from the slice
						group2model2channels[group][model] = append(channels[:i], channels[i+1:]...)
						break
					}
				}
			}
		}
	}
}

func CacheUpdateChannel(channel *Channel) {
	if !common.MemoryCacheEnabled {
		return
	}
	channelSyncLock.Lock()
	defer channelSyncLock.Unlock()
	if channel == nil {
		return
	}

	println("CacheUpdateChannel:", channel.Id, channel.Name, channel.Status, channel.ChannelInfo.MultiKeyPollingIndex)

	println("before:", channelsIDM[channel.Id].ChannelInfo.MultiKeyPollingIndex)
	channelsIDM[channel.Id] = channel
	println("after :", channelsIDM[channel.Id].ChannelInfo.MultiKeyPollingIndex)
}
