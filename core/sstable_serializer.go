package core

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"

	"github.com/ogioldat/ttrunksdb/algo"
)

type SSTableFile []byte

type SSTableSerializer interface {
	Serialize(
		algo.BloomFilter,
		algo.SparseIndex,
		[]DBRecord,
	) (SSTableFile, error)
	RecordSize(DBRecordKey, DBRecordValue) int
	MetadataSize(algo.BloomFilter, algo.SparseIndex) int
}

type Deserialized struct {
	BloomFilter algo.BloomFilter
	SparseIndex algo.SparseIndex
	Records     []DBRecord
}

type SSTableDeserializer interface {
	Deserialize(io.Reader) (*Deserialized, error)
	DeserializeRecord(io.Reader) (*DBRecord, error)
}

type StandardSSTableSerializer struct{}

type StandardSSTableDeserializer struct{}

type BinarySSTableSerializer struct{}

type BinarySSTableDeserializer struct{}

var BYTES_ORDER = binary.LittleEndian

type BloomFilterSize int32
type SparseIndexSize int32
type DBRecordKeySize int32
type DBRecordValueSize int32
type DBRecordTimestampSize int64
type DBRecordTombstoneSize int32

const BLOOM_FILTER_SIZE_BYTES = 4
const SPARSE_INDEX_SIZE_BYTES = 4
const DB_RECORD_KEY_SIZE_BYTES = 4
const DB_RECORD_VALUE_SIZE_BYTES = 4
const DB_RECORD_TIMESTAMP_SIZE_BYTES = 8
const DB_RECORD_TIMESTAMP_BYTES = 8
const DB_RECORD_TOMBSTONE_SIZE_BYTES = 4
const DB_RECORD_TOMBSTONE_BYTES = 1

func (s *StandardSSTableSerializer) Serialize(
	bloomFilter algo.BloomFilter,
	sparseIndex algo.SparseIndex,
	ser []DBRecord,
) (SSTableFile, error) {
	bloomFilterStr := bloomFilter.String()
	sparseIndexStr := sparseIndex.String()

	var dataBlock []string

	for _, node := range ser {
		serializedNode := fmt.Sprintf(
			"%d %s %d %s %d %d %d %d",
			len([]byte(node.Key)), node.Key,
			len([]byte(node.Value)), node.Value,
			8, node.Timestamp,
			1, boolToInt(bool(node.Tombstone)))

		dataBlock = append(dataBlock, serializedNode)
	}

	return []byte(bloomFilterStr + "\n" + sparseIndexStr + "\n" + strings.Join(dataBlock, ",") + "\n"), nil
}

func (s *BinarySSTableSerializer) RecordSize(key DBRecordKey, value DBRecordValue) int {
	return DB_RECORD_KEY_SIZE_BYTES +
		len([]byte(key)) +
		DB_RECORD_VALUE_SIZE_BYTES +
		len([]byte(value)) +
		DB_RECORD_TIMESTAMP_SIZE_BYTES +
		DB_RECORD_TIMESTAMP_BYTES +
		DB_RECORD_TOMBSTONE_SIZE_BYTES +
		DB_RECORD_TOMBSTONE_BYTES
}

func (s *BinarySSTableSerializer) MetadataSize(
	bloomFilter algo.BloomFilter,
	sparseIndex algo.SparseIndex,
) int {
	return BLOOM_FILTER_SIZE_BYTES +
		len([]byte(bloomFilter.String())) +
		SPARSE_INDEX_SIZE_BYTES +
		len([]byte(sparseIndex.String()))
}

func (s *BinarySSTableSerializer) Serialize(
	bloomFilter algo.BloomFilter,
	sparseIndex algo.SparseIndex,
	records []DBRecord,
) (SSTableFile, error) {
	buf := new(bytes.Buffer)

	bloomFilterBits := bloomFilter.String()
	bloomFilterBitsSize := BloomFilterSize(len(bloomFilterBits))
	sparseIndexStr := sparseIndex.String()
	sparseIndexSize := SparseIndexSize(len(sparseIndexStr))

	if err := binary.Write(buf, BYTES_ORDER, bloomFilterBitsSize); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(bloomFilterBits); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, BYTES_ORDER, sparseIndexSize); err != nil {
		return nil, err
	}
	if _, err := buf.WriteString(sparseIndexStr); err != nil {
		return nil, err
	}

	for _, record := range records {
		key := DBRecordKey(record.Key)
		keySize := DBRecordKeySize(len(key))
		value := record.Value
		valueSize := DBRecordValueSize(len(value))
		timestamp := record.Timestamp
		timestampSize := DBRecordTimestampSize(8)
		tombstone := record.Tombstone
		tombstoneSize := DBRecordTombstoneSize(1)

		if err := binary.Write(buf, BYTES_ORDER, keySize); err != nil {
			return nil, err
		}
		if _, err := buf.WriteString(string(key)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, BYTES_ORDER, valueSize); err != nil {
			return nil, err
		}
		if _, err := buf.Write(value); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, BYTES_ORDER, timestampSize); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, BYTES_ORDER, timestamp); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, BYTES_ORDER, tombstoneSize); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, BYTES_ORDER, tombstone); err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

func (d *BinarySSTableDeserializer) DeserializeRecord(reader io.Reader) (*DBRecord, error) {
	var keySize DBRecordKeySize
	var valueSize DBRecordValueSize
	var timestampSize DBRecordTimestampSize
	var timestamp DBRecordTimestamp
	var tombstoneSize DBRecordTombstoneSize
	var tombstone DBRecordTombstone

	if err := binary.Read(reader, BYTES_ORDER, &keySize); err != nil {
		return nil, err
	}
	key := make([]byte, keySize)
	if err := binary.Read(reader, BYTES_ORDER, key); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, BYTES_ORDER, &valueSize); err != nil {
		return nil, err
	}
	if valueSize < 0 {
		return nil, fmt.Errorf("invalid value size: %d", valueSize)
	}
	value := make([]byte, valueSize)
	if err := binary.Read(reader, BYTES_ORDER, value); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, BYTES_ORDER, &timestampSize); err != nil {
		return nil, err
	}
	if timestampSize < 0 {
		return nil, fmt.Errorf("invalid timestamp size: %d", timestampSize)
	}
	if err := binary.Read(reader, BYTES_ORDER, &timestamp); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, BYTES_ORDER, &tombstoneSize); err != nil {
		return nil, err
	}
	if tombstoneSize < 0 {
		return nil, fmt.Errorf("invalid tombstone size: %d", tombstoneSize)
	}
	if err := binary.Read(reader, BYTES_ORDER, &tombstone); err != nil {
		return nil, err
	}

	return &DBRecord{
		Key:       DBRecordKey(key),
		Value:     DBRecordValue(value),
		Timestamp: DBRecordTimestamp(timestamp),
		Tombstone: DBRecordTombstone(tombstone),
	}, nil
}

// TODO: VALIDATE
func (d *BinarySSTableDeserializer) Deserialize(reader io.Reader) (*Deserialized, error) {
	var bloomFilterBitsSize BloomFilterSize
	var bloomFilterBits []byte
	var sparseIndexSize SparseIndexSize
	var sparseIndex []byte

	if err := binary.Read(reader, BYTES_ORDER, &bloomFilterBitsSize); err != nil {
		return nil, err
	}
	if bloomFilterBitsSize <= 0 {
		return nil, fmt.Errorf("invalid bloom filter size: %d", bloomFilterBitsSize)
	}

	bloomFilterBits = make([]byte, bloomFilterBitsSize)
	if err := binary.Read(reader, BYTES_ORDER, &bloomFilterBits); err != nil {
		return nil, err
	}
	if err := binary.Read(reader, BYTES_ORDER, &sparseIndexSize); err != nil {
		return nil, err
	}
	if sparseIndexSize < 0 {
		return nil, fmt.Errorf("invalid sparse index size: %d", sparseIndexSize)
	}
	sparseIndex = make([]byte, sparseIndexSize)
	if err := binary.Read(reader, BYTES_ORDER, &sparseIndex); err != nil {
		return nil, err
	}

	records := []DBRecord{}

	for {
		var keySize DBRecordKeySize
		var valueSize DBRecordValueSize
		var timestampSize DBRecordTimestampSize
		var timestamp DBRecordTimestamp
		var tombstoneSize DBRecordTombstoneSize
		var tombstone DBRecordTombstone

		if err := binary.Read(reader, BYTES_ORDER, &keySize); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		key := make([]byte, keySize)
		if err := binary.Read(reader, BYTES_ORDER, key); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, BYTES_ORDER, &valueSize); err != nil {
			return nil, err
		}
		if valueSize < 0 {
			return nil, fmt.Errorf("invalid value size: %d", valueSize)
		}
		value := make([]byte, valueSize)
		if err := binary.Read(reader, BYTES_ORDER, value); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, BYTES_ORDER, &timestampSize); err != nil {
			return nil, err
		}
		if timestampSize < 0 {
			return nil, fmt.Errorf("invalid timestamp size: %d", timestampSize)
		}
		if err := binary.Read(reader, BYTES_ORDER, &timestamp); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, BYTES_ORDER, &tombstoneSize); err != nil {
			return nil, err
		}
		if tombstoneSize < 0 {
			return nil, fmt.Errorf("invalid tombstone size: %d", tombstoneSize)
		}
		if err := binary.Read(reader, BYTES_ORDER, &tombstone); err != nil {
			return nil, err
		}

		records = append(records, DBRecord{
			Key:       DBRecordKey(key),
			Value:     DBRecordValue(value),
			Timestamp: DBRecordTimestamp(timestamp),
			Tombstone: DBRecordTombstone(tombstone),
		})

	}

	return &Deserialized{
		BloomFilter: *algo.NewBloomFilterFromString(string(bloomFilterBits)),
		SparseIndex: *algo.NewSparseIndexFromString(string(sparseIndex)),
		Records:     records,
	}, nil
}
