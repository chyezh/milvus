package utils

import (
	"context"
	"crypto/x509"
	"os"
	"time"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func GracefulStopGRPCServer(s *grpc.Server) {
	if s == nil {
		return
	}
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		log.Info(context.TODO(), "try to graceful stop grpc server...")
		// will block until all rpc finished.
		s.GracefulStop()
	}()
	select {
	case <-ch:
	case <-time.After(paramtable.Get().ProxyGrpcServerCfg.GracefulStopTimeout.GetAsDuration(time.Second)):
		// took too long, manually close grpc server
		log.Info(context.TODO(), "force to stop grpc server...")
		s.Stop()
		// concurrent GracefulStop should be interrupted
		<-ch
	}
}

func getTLSCreds(certFile string, keyFile string, nodeType string) credentials.TransportCredentials {
	log.Info(context.TODO(), "TLS Server PEM Path", log.String("path", certFile))
	log.Info(context.TODO(), "TLS Server Key Path", log.String("path", keyFile))
	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		log.Warn(context.TODO(), nodeType+" can't create creds", log.Err(err))
		log.Warn(context.TODO(), nodeType+" can't create creds", log.Err(err))
	}
	return creds
}

func EnableInternalTLS(NodeType string) grpc.ServerOption {
	var Params *paramtable.ComponentParam = paramtable.Get()
	certFile := Params.InternalTLSCfg.InternalTLSServerPemPath.GetValue()
	keyFile := Params.InternalTLSCfg.InternalTLSServerKeyPath.GetValue()
	internaltlsEnabled := Params.InternalTLSCfg.InternalTLSEnabled.GetAsBool()

	log.Info(context.TODO(), "Internal TLS Enabled", log.Bool("value", internaltlsEnabled))

	if internaltlsEnabled {
		creds := getTLSCreds(certFile, keyFile, NodeType)
		return grpc.Creds(creds)
	}
	return grpc.Creds(nil)
}

func CreateCertPoolforClient(caFile string, nodeType string) (*x509.CertPool, error) {
	log.Info(context.TODO(), "Creating cert pool for " + nodeType)
	log.Info(context.TODO(), "Cert file path:", log.String("caFile", caFile))
	certPool := x509.NewCertPool()

	b, err := os.ReadFile(caFile)
	if err != nil {
		log.Error(context.TODO(), "Error reading cert file in client", log.Err(err))
		return nil, err
	}

	if !certPool.AppendCertsFromPEM(b) {
		log.Error(context.TODO(), "credentials: failed to append certificates")
		return nil, errors.New("failed to append certificates") // Cert pool is invalid, return nil and the error
	}
	return certPool, err
}
