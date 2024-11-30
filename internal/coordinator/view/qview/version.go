package qview

type QueryViewVersion struct {
	DataVersion  int64
	QueryVersion int64
}

// GTE returns true if qv is greater than or equal to qv2.
func (qv QueryViewVersion) GTE(qv2 QueryViewVersion) bool {
	return qv.DataVersion > qv2.DataVersion ||
		(qv.DataVersion == qv2.DataVersion && qv.QueryVersion >= qv2.QueryVersion)
}

// GT returns true if qv is greater than qv2.
func (qv QueryViewVersion) GT(qv2 QueryViewVersion) bool {
	return qv.DataVersion > qv2.DataVersion ||
		(qv.DataVersion == qv2.DataVersion && qv.QueryVersion > qv2.QueryVersion)
}

// EQ returns true if qv is equal to qv2.
func (qv QueryViewVersion) EQ(qv2 QueryViewVersion) bool {
	return qv.DataVersion == qv2.DataVersion && qv.QueryVersion == qv2.QueryVersion
}
