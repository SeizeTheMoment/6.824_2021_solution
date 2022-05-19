package shardkv
type CommandType int
const (
	Operation CommandType = iota
	ConfigChange
	Insert
	Erase
	StopWaiting
	Empty
)
type RaftLogCommand struct {
	CommandType
	Data interface{}
}

func newRaftLogCommand(commandType CommandType, data interface{}) RaftLogCommand  {
	return RaftLogCommand{
		CommandType: commandType,
		Data:        data,
	}
}