package messages

type ProduceRequest struct {
	ProduceAck chan ProduceAck
	BatchId    uint64
	Payload    []byte
}

type ProduceAck struct {
	BatchId       uint64
	PartitionName string
}
