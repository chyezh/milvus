package message

// CloneImmutableMessage creates an independent immutable message with copied
// payload and properties. Transaction children are cloned recursively.
func CloneImmutableMessage(msg ImmutableMessage) ImmutableMessage {
	if msg == nil {
		return nil
	}
	cloned := cloneImmutableMessageBase(msg)
	txn := AsImmutableTxnMessage(msg)
	if txn == nil {
		return cloned
	}
	clonedTxn := &immutableTxnMessageImpl{
		immutableMessageImpl: *cloned,
		begin:                CloneImmutableMessage(txn.Begin()),
		messages:             make([]ImmutableMessage, 0, txn.Size()),
		commit:               CloneImmutableMessage(txn.Commit()),
	}
	_ = txn.RangeOver(func(inner ImmutableMessage) error {
		clonedTxn.messages = append(clonedTxn.messages, CloneImmutableMessage(inner))
		return nil
	})
	return clonedTxn
}

func cloneImmutableMessageBase(msg ImmutableMessage) *immutableMessageImpl {
	serialized := msg.IntoMessageProto()
	properties := serialized.GetProperties()
	clonedProperties := make(propertiesImpl, len(properties))
	for key, value := range properties {
		clonedProperties[key] = value
	}
	return &immutableMessageImpl{
		id: msg.MessageID(),
		messageImpl: messageImpl{
			payload:    append([]byte(nil), serialized.GetPayload()...),
			properties: clonedProperties,
		},
	}
}
