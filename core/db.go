package core

type LSMTStorage struct {
	memTableThreshold int // Max size of entries in the memtable before flushing to SSTables
	seqNumber         int
	sparseIndex       SparseIndex
	memTable          MemTable
	ssTableManager    *SSTableManager
	wal               *WAL
	metadata          *Metadata
}

func NewLSMTStorage(memTableThreshold int) *LSMTStorage {
	wal, err := NewWAL()
	if err != nil {
		panic("failed to create WAL")
	}

	return &LSMTStorage{
		memTableThreshold: memTableThreshold,
		seqNumber:         0,
		sparseIndex:       NewSparseIndex(),
		memTable:          NewRBMemTable(),
		ssTableManager:    NewSSTableManager(),
		wal:               wal,
		metadata:          NewMetadata(),
	}
}

func (s *LSMTStorage) updateSeq() {
	s.seqNumber++
}

func (s *LSMTStorage) Write(key string, value string) error {
	if err := s.wal.Log(key, value); err != nil {
		return err
	}

	if err := s.memTable.Write(key, []byte(value)); err != nil {
		return err
	}

	// s.sparseIndex.Update(key, )
	s.updateSeq()

	// TODO: Move as a background task
	if s.memTableThreshold < s.memTable.Size() {
		sstable := s.ssTableManager.AddSSTable()
		if err := s.memTable.Flush(sstable); err != nil {
			return err
		}

		memtableHead := s.memTable.First()
		memtableTail := s.memTable.Last()

		s.metadata.Set(sstable.Name, memtableHead.Key, memtableTail.Key)
	}

	return nil
}

func (s *LSMTStorage) Compact(key string) ([]byte, error) {
	return nil, nil
}
