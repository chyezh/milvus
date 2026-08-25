package predicates

// PredicateTarget is enum for Predicate target type.
type PredicateTarget int32

const (
	// PredTargetValue is predicate target for key-value perid
	PredTargetValue PredicateTarget = iota + 1
	// PredTargetNotExist is the predicate target for an absent key: satisfied
	// only when the key is not present in the store. Backends translate it to
	// a native atomic condition (etcd version==0) or an explicit read check
	// (TiKV ErrNotExist).
	PredTargetNotExist
)

type PredicateType int32

const (
	PredTypeEqual PredicateType = iota + 1
)

// Predicate provides interface for kv predicate.
type Predicate interface {
	Target() PredicateTarget
	Type() PredicateType
	IsTrue(any) bool
	Key() string
	TargetValue() any
}

type valuePredicate struct {
	k, v string
	pt   PredicateType
}

func (p *valuePredicate) Target() PredicateTarget {
	return PredTargetValue
}

func (p *valuePredicate) Type() PredicateType {
	return p.pt
}

func (p *valuePredicate) IsTrue(target any) bool {
	switch v := target.(type) {
	case string:
		return predicateValue(p.pt, v, p.v)
	case []byte:
		return predicateValue(p.pt, string(v), p.v)
	default:
		return false
	}
}

func (p *valuePredicate) Key() string {
	return p.k
}

func (p *valuePredicate) TargetValue() any {
	return p.v
}

func predicateValue[T comparable](pt PredicateType, v1, v2 T) bool {
	switch pt {
	case PredTypeEqual:
		return v1 == v2
	default:
		return false
	}
}

func ValueEqual(k, v string) Predicate {
	return &valuePredicate{
		k:  k,
		v:  v,
		pt: PredTypeEqual,
	}
}

type predNotExist struct {
	k string
}

func (p *predNotExist) Target() PredicateTarget {
	return PredTargetNotExist
}

func (p *predNotExist) Type() PredicateType {
	return PredTypeEqual
}

// IsTrue is satisfied only by the not-found signal (nil): a caller that read
// the key and found it absent passes nil, any actual value fails the check.
func (p *predNotExist) IsTrue(target any) bool {
	return target == nil
}

func (p *predNotExist) Key() string {
	return p.k
}

func (p *predNotExist) TargetValue() any {
	return nil
}

// NotExist is satisfied when the key is absent from the store. It backs the
// atomic create-if-absent of a key (e.g. the first consume-checkpoint write):
// a backend applies it as a native txn condition (etcd version==0) or as a
// same-transaction read+write guarded by write-conflict detection (TiKV), so
// two concurrent creators cannot both succeed.
func NotExist(k string) Predicate {
	return &predNotExist{k: k}
}
