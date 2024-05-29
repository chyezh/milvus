package adaptor

import (
	"github.com/milvus-io/milvus/internal/lognode/server/wal"
	"github.com/milvus-io/milvus/internal/lognode/server/wal/walimpls"
)

var _ wal.OpenerBuilder = (*builderAdaptorImpl)(nil)

func AdaptImplsToBuilder(builder walimpls.OpenerBuilderImpls) wal.OpenerBuilder {
	return builderAdaptorImpl{
		builder: builder,
	}
}

type builderAdaptorImpl struct {
	builder walimpls.OpenerBuilderImpls
}

func (b builderAdaptorImpl) Name() string {
	return b.builder.Name()
}

func (b builderAdaptorImpl) Build() (wal.Opener, error) {
	o, err := b.builder.Build()
	if err != nil {
		return nil, err
	}
	return adaptImplsToOpener(o), nil
}
