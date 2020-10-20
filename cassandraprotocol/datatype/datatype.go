package datatype

import (
	"fmt"
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/primitives"
	"io"
)

type DataType interface {
	GetDataTypeCode() cassandraprotocol.DataTypeCode
}

type Encoder interface {
	Encode(t DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error)
	EncodedLength(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error)
}

type Decoder interface {
	Decode(source io.Reader, version cassandraprotocol.ProtocolVersion) (t DataType, err error)
}

type Codec interface {
	Encoder
	Decoder
}

func FindCodec(code cassandraprotocol.DataTypeCode) (Codec, error) {
	codec, ok := codecs[code]
	if !ok {
		return nil, fmt.Errorf("cannot find codec for data type code %v", code)
	}
	return codec, nil
}

func WriteDataType(t DataType, dest io.Writer, version cassandraprotocol.ProtocolVersion) (err error) {
	if err = primitives.WriteShort(t.GetDataTypeCode(), dest); err != nil {
		return fmt.Errorf("cannot write data type code %v: %w", t.GetDataTypeCode(), err)
	} else if codec, err := FindCodec(t.GetDataTypeCode()); err != nil {
		return err
	} else if err = codec.Encode(t, dest, version); err != nil {
		return fmt.Errorf("cannot write data type %v: %w", t, err)
	} else {
		return nil
	}
}

func LengthOfDataType(t DataType, version cassandraprotocol.ProtocolVersion) (length int, err error) {
	length += primitives.LengthOfShort // type code
	if codec, err := FindCodec(t.GetDataTypeCode()); err != nil {
		return -1, err
	} else if dataTypeLength, err := codec.EncodedLength(t, version); err != nil {
		return -1, fmt.Errorf("cannot compute length of data type %v: %w", t, err)
	} else {
		return length + dataTypeLength, nil
	}
}

func ReadDataType(source io.Reader, version cassandraprotocol.ProtocolVersion) (decoded DataType, err error) {
	var typeCode cassandraprotocol.DataTypeCode
	if typeCode, err = primitives.ReadShort(source); err != nil {
		return nil, fmt.Errorf("cannot read data type code: %w", err)
	} else if codec, err := FindCodec(typeCode); err != nil {
		return nil, err
	} else if decoded, err = codec.Decode(source, version); err != nil {
		return nil, fmt.Errorf("cannot read data type code %v: %w", typeCode, err)
	} else {
		return decoded, nil
	}
}

var codecs = map[cassandraprotocol.DataTypeCode]Codec{
	cassandraprotocol.DataTypeCodeAscii:     &primitiveTypeCodec{Ascii},
	cassandraprotocol.DataTypeCodeBigint:    &primitiveTypeCodec{Bigint},
	cassandraprotocol.DataTypeCodeBlob:      &primitiveTypeCodec{Blob},
	cassandraprotocol.DataTypeCodeBoolean:   &primitiveTypeCodec{Boolean},
	cassandraprotocol.DataTypeCodeCounter:   &primitiveTypeCodec{Counter},
	cassandraprotocol.DataTypeCodeDecimal:   &primitiveTypeCodec{Decimal},
	cassandraprotocol.DataTypeCodeDouble:    &primitiveTypeCodec{Double},
	cassandraprotocol.DataTypeCodeFloat:     &primitiveTypeCodec{Float},
	cassandraprotocol.DataTypeCodeInt:       &primitiveTypeCodec{Int},
	cassandraprotocol.DataTypeCodeTimestamp: &primitiveTypeCodec{Timestamp},
	cassandraprotocol.DataTypeCodeUuid:      &primitiveTypeCodec{Uuid},
	cassandraprotocol.DataTypeCodeVarchar:   &primitiveTypeCodec{Varchar},
	cassandraprotocol.DataTypeCodeVarint:    &primitiveTypeCodec{Varint},
	cassandraprotocol.DataTypeCodeTimeuuid:  &primitiveTypeCodec{Timeuuid},
	cassandraprotocol.DataTypeCodeInet:      &primitiveTypeCodec{Inet},
	cassandraprotocol.DataTypeCodeDate:      &primitiveTypeCodec{Date},
	cassandraprotocol.DataTypeCodeTime:      &primitiveTypeCodec{Time},
	cassandraprotocol.DataTypeCodeSmallint:  &primitiveTypeCodec{Smallint},
	cassandraprotocol.DataTypeCodeTinyint:   &primitiveTypeCodec{Tinyint},
	cassandraprotocol.DataTypeCodeDuration:  &primitiveTypeCodec{Duration},
	cassandraprotocol.DataTypeCodeList:      &listTypeCodec{},
	cassandraprotocol.DataTypeCodeSet:       &setTypeCodec{},
	cassandraprotocol.DataTypeCodeMap:       &mapTypeCodec{},
	cassandraprotocol.DataTypeCodeTuple:     &tupleTypeCodec{},
	cassandraprotocol.DataTypeCodeUdt:       &userDefinedTypeCodec{},
	cassandraprotocol.DataTypeCodeCustom:    &customTypeCodec{},
}
