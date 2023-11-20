package aviary

// corresponds to one document in MongoDB
type InputData struct {
	Tag       string
	Partition int
	Contents  string
}

type IntermediateData struct {
	Tag       string
	Partition int
}
