package core

import (
	"bytes"
	"testing"

	"github.com/ogioldat/olappie/algo"
	"github.com/stretchr/testify/assert"
)

// Helper function for tests
func emptyCallback(buf *bytes.Buffer) {
	// Empty callback for tests
}

func TestBinarySerializerSingleRecord(t *testing.T) {
	// Create SSTable using manager
	config := &LSMTStorageConfig{
		outputDir:              "../data/test",
		sstableBloomFilterSize: 1000,
	}
	manager := NewSSTableManager(config)
	sstable := manager.AddSSTable(config)

	// Use BinarySSTableSerializer directly
	serializer := &BinarySSTableSerializer{}

	record := DBRecord{
		Key:       DBRecordKey("test_key"),
		Value:     DBRecordValue("test_value"),
		Timestamp: DBRecordTimestamp(1751374012),
		Tombstone: DBRecordTombstone(false),
	}

	records := []DBRecord{record}

	// Use the new interface with separate bloom filter and sparse index
	result, err := serializer.Serialize(*sstable.BloomFilter, *sstable.SparseIndex, records)

	assert.NoError(t, err, "Serialization should not fail")
	assert.NotNil(t, result, "Result should not be nil")
	assert.Greater(t, len(result), 0, "Result should have content")
}

func TestBinarySerializerMultipleRecords(t *testing.T) {
	// Create SSTable using manager
	config := &LSMTStorageConfig{
		outputDir:              "../data/test",
		sstableBloomFilterSize: 1000,
	}
	manager := NewSSTableManager(config)
	sstable := manager.AddSSTable(config)

	serializer := &BinarySSTableSerializer{}

	records := []DBRecord{
		{
			Key:       DBRecordKey("key1"),
			Value:     DBRecordValue("value1"),
			Timestamp: DBRecordTimestamp(1758380683547),
			Tombstone: DBRecordTombstone(false),
		},
		{
			Key:       DBRecordKey("key2"),
			Value:     DBRecordValue(""),
			Timestamp: DBRecordTimestamp(1758380683547),
			Tombstone: DBRecordTombstone(true),
		},
		{
			Key:       DBRecordKey("longer_key_name"),
			Value:     DBRecordValue("longer value with more content"),
			Timestamp: DBRecordTimestamp(1758380683547),
			Tombstone: DBRecordTombstone(false),
		},
	}

	result, err := serializer.Serialize(*sstable.BloomFilter, *sstable.SparseIndex, records)

	assert.NoError(t, err, "Serialization should not fail")
	assert.NotNil(t, result, "Result should not be nil")
	assert.Greater(t, len(result), len(records)*20, "Result should be appropriately sized for multiple records")
}

func TestBinaryDeserializerSingleRecord(t *testing.T) {
	deserializer := &BinarySSTableDeserializer{}

	// First serialize a record to get valid binary data
	serializer := &BinarySSTableSerializer{}
	originalRecord := DBRecord{
		Key:       DBRecordKey("test"),
		Value:     DBRecordValue("data"),
		Timestamp: DBRecordTimestamp(1234567890),
		Tombstone: DBRecordTombstone(true),
	}

	records := []DBRecord{originalRecord}
	config := &LSMTStorageConfig{
		outputDir:              "../data/test",
		sstableBloomFilterSize: 10,
	}
	manager := NewSSTableManager(config)
	sstable := manager.AddSSTable(config)

	serializedData, err := serializer.Serialize(
		*algo.NewBloomFilterFromString("1001010101"),
		*sstable.SparseIndex,
		records,
	)

	assert.NoError(t, err, "Serialization should work for test setup")

	// Now test deserialization - use full Deserialize method
	deserialized, err := deserializer.Deserialize(bytes.NewReader(serializedData))
	assert.NoError(t, err, "Deserialization should not fail")
	assert.NotNil(t, deserialized, "Deserialized result should not be nil")
	assert.Len(t, deserialized.Records, 1, "Should have one record")

	result := &deserialized.Records[0]

	assert.NoError(t, err, "Deserialization should not fail")
	assert.NotNil(t, result, "Result should not be nil")
	assert.Equal(t, originalRecord.Key, result.Key, "Key should match")
	assert.Equal(t, originalRecord.Value, result.Value, "Value should match")
	assert.Equal(t, originalRecord.Timestamp, result.Timestamp, "Timestamp should match")
	assert.Equal(t, originalRecord.Tombstone, result.Tombstone, "Tombstone should match")
}

func TestBinaryRoundTripSingleRecord(t *testing.T) {
	serializer := &BinarySSTableSerializer{}
	deserializer := &BinarySSTableDeserializer{}

	testCases := []DBRecord{
		{
			Key:       DBRecordKey("simple"),
			Value:     DBRecordValue("value"),
			Timestamp: DBRecordTimestamp(1000000000),
			Tombstone: DBRecordTombstone(false),
		},
		{
			Key:       DBRecordKey("empty_value"),
			Value:     DBRecordValue(""),
			Timestamp: DBRecordTimestamp(2000000000),
			Tombstone: DBRecordTombstone(true),
		},
		{
			Key:       DBRecordKey("long_content_key_with_many_chars"),
			Value:     DBRecordValue("This is a much longer value with various characters !@#$%^&*()"),
			Timestamp: DBRecordTimestamp(9999999999),
			Tombstone: DBRecordTombstone(false),
		},
		{
			Key:       DBRecordKey("a"),
			Value:     DBRecordValue("b"),
			Timestamp: DBRecordTimestamp(1),
			Tombstone: DBRecordTombstone(true),
		},
	}

	for _, testCase := range testCases {
		t.Run("RoundTrip_"+string(testCase.Key), func(t *testing.T) {
			// Serialize
			records := []DBRecord{testCase}
			config := &LSMTStorageConfig{
				outputDir:              "../data/test",
				sstableBloomFilterSize: 1000,
			}
			manager := NewSSTableManager(config)
			sstable := manager.AddSSTable(config)
			serialized, err := serializer.Serialize(*sstable.BloomFilter, *sstable.SparseIndex, records)
			assert.NoError(t, err, "Serialization should not fail")

			// Deserialize
			deserialized, err := deserializer.Deserialize(bytes.NewReader(serialized))
			assert.NoError(t, err, "Deserialization should not fail")
			assert.NotNil(t, deserialized, "Deserialized result should not be nil")
			assert.Len(t, deserialized.Records, 1, "Should have one record")

			result := &deserialized.Records[0]

			// Compare
			assert.Equal(t, testCase.Key, result.Key, "Key should match after round trip")
			assert.Equal(t, testCase.Value, result.Value, "Value should match after round trip")
			assert.Equal(t, testCase.Timestamp, result.Timestamp, "Timestamp should match after round trip")
			assert.Equal(t, testCase.Tombstone, result.Tombstone, "Tombstone should match after round trip")
		})
	}
}

func TestBinaryDeserializerInvalidData(t *testing.T) {
	deserializer := &BinarySSTableDeserializer{}

	testCases := []struct {
		name string
		data []byte
	}{
		{
			name: "Empty data",
			data: []byte{},
		},
		{
			name: "Insufficient data",
			data: []byte{1, 2, 3},
		},
		{
			name: "Invalid size field",
			data: []byte{0xFF, 0xFF, 0xFF, 0xFF}, // Very large size
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			result, err := deserializer.Deserialize(bytes.NewReader(testCase.data))

			assert.Error(t, err, "Should return error for invalid data")
			assert.Nil(t, result, "Result should be nil on error")
		})
	}
}

func TestBinarySerializerEmptyRecords(t *testing.T) {
	serializer := &BinarySSTableSerializer{}

	records := []DBRecord{}
	config := &LSMTStorageConfig{
		outputDir:              "../data/test",
		sstableBloomFilterSize: 1000,
	}
	manager := NewSSTableManager(config)
	sstable := manager.AddSSTable(config)

	result, err := serializer.Serialize(*sstable.BloomFilter, *sstable.SparseIndex, records)

	assert.NoError(t, err, "Serialization should not fail for empty records")
	assert.NotNil(t, result, "Result should not be nil")
	assert.Equal(t, serializer.MetadataSize(*sstable.BloomFilter, *sstable.SparseIndex), len(result), "Result should be empty for no records")
}

func TestBinarySerializerWithSpecialCharacters(t *testing.T) {
	serializer := &BinarySSTableSerializer{}
	deserializer := &BinarySSTableDeserializer{}

	record := DBRecord{
		Key:       DBRecordKey("key_with_unicode_🔥"),
		Value:     DBRecordValue("value with\nnewlines\tand\x00null bytes"),
		Timestamp: DBRecordTimestamp(1751374012),
		Tombstone: DBRecordTombstone(false),
	}

	// Serialize
	records := []DBRecord{record}
	config := &LSMTStorageConfig{
		outputDir:              "../data/test",
		sstableBloomFilterSize: 1000,
	}
	manager := NewSSTableManager(config)
	sstable := manager.AddSSTable(config)
	serialized, err := serializer.Serialize(*sstable.BloomFilter, *sstable.SparseIndex, records)
	assert.NoError(t, err, "Serialization should handle special characters")

	// Deserialize
	deserialized, err := deserializer.Deserialize(bytes.NewReader(serialized))
	assert.NoError(t, err, "Deserialization should handle special characters")
	assert.NotNil(t, deserialized, "Deserialized result should not be nil")
	assert.Len(t, deserialized.Records, 1, "Should have one record")

	result := &deserialized.Records[0]

	// Compare
	assert.Equal(t, record.Key, result.Key, "Unicode key should survive round trip")
	assert.Equal(t, record.Value, result.Value, "Special characters in value should survive round trip")
	assert.Equal(t, record.Timestamp, result.Timestamp, "Timestamp should match")
	assert.Equal(t, record.Tombstone, result.Tombstone, "Tombstone should match")
}

func TestBinarySerializerDataSize(t *testing.T) {
	serializer := &BinarySSTableSerializer{}

	record := DBRecord{
		Key:       DBRecordKey("test"),
		Value:     DBRecordValue("data"),
		Timestamp: DBRecordTimestamp(1234567890),
		Tombstone: DBRecordTombstone(true),
	}

	records := []DBRecord{record}
	config := &LSMTStorageConfig{
		outputDir:              "../data/test",
		sstableBloomFilterSize: 1000,
	}
	manager := NewSSTableManager(config)
	sstable := manager.AddSSTable(config)
	result, err := serializer.Serialize(*sstable.BloomFilter, *sstable.SparseIndex, records)

	assert.NoError(t, err, "Serialization should not fail")

	// Calculate expected size:
	// key(4) + keySize(4) + value(4) + valueSize(4) + timestamp(8) + timestampSize(4) + tombstone(1) + tombstoneSize(4)
	expectedSize := serializer.MetadataSize(*sstable.BloomFilter, *sstable.SparseIndex) + serializer.RecordSize(
		DBRecordKey("test"),
		DBRecordValue("data"),
	)
	assert.Equal(t, expectedSize, len(result), "Serialized data should have expected size")
}
