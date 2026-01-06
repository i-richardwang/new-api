package constant

type ChannelSelectMode string

const (
	ChannelSelectModeRandom ChannelSelectMode = "random" // 随机选择（默认）
	ChannelSelectModeSticky ChannelSelectMode = "sticky" // 粘性选择：一直用一个渠道直到失败才切换
)
