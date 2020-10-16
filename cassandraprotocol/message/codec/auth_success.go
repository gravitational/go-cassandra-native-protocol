package codec

import (
	"go-cassandra-native-protocol/cassandraprotocol"
	"go-cassandra-native-protocol/cassandraprotocol/message"
	"go-cassandra-native-protocol/cassandraprotocol/primitive"
)

type AuthSuccessCodec struct{}

func (c AuthSuccessCodec) Encode(msg message.Message, dest []byte, version cassandraprotocol.ProtocolVersion) error {
	authSuccess := msg.(*message.AuthSuccess)
	_, err := primitive.WriteBytes(authSuccess.Token, dest)
	return err
}

func (c AuthSuccessCodec) EncodedSize(msg message.Message, version cassandraprotocol.ProtocolVersion) (int, error) {
	authSuccess := msg.(*message.AuthSuccess)
	return primitive.SizeOfBytes(authSuccess.Token), nil
}

func (c AuthSuccessCodec) Decode(source []byte, version cassandraprotocol.ProtocolVersion) (message.Message, error) {
	token, _, err := primitive.ReadBytes(source)
	if err != nil {
		return nil, err
	}
	return &message.AuthSuccess{Token: token}, nil
}
