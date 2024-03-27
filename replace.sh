#!/bin/bash

set -e

MDLOG_VERSION=$1
PULSAR_CLIENT_GO_VERSION=$2
PULSARCTL_VERSION=$3
# check arguments
if [ $# != 3 ]; then
    echo "Usage: $0 MDLOG_VERSION PULSAR_CLIENT_GO_VERSION PULSARCTL_VERSION"
    exit 1
fi
if [ -z $PULSAR_CLIENT_GO_VERSION ]; then
    echo "PULSAR_CLIENT_GO_VERSION is empty"
    exit 1
fi
if [ -z $PULSARCTL_VERSION ]; then
    echo "PULSARCTL_VERSION is empty"
    exit 1
fi
if [ -z $MDLOG_VERSION ]; then
    echo "MDLOG_VERSION is empty"
    exit 1
fi

ZILLIZTECH_REPO="github.com/zilliztech/"
ZILLIZTECH_LOGSTORE_REPO="github.com/zilliztech/logstore"
ZILLIZTECH_MDLOG=$ZILLIZTECH_LOGSTORE_REPO"/mdlog"
ZILLIZTECH_PULSAR_CLIENT_GO=$ZILLIZTECH_LOGSTORE_REPO"/pulsar-client-go"
ZILLIZTECH_PULSARCTL=$ZILLIZTECH_LOGSTORE_REPO"/pulsarctl"
ZILLIZTECH_MDLOG_WITH_VERSION=$ZILLIZTECH_MDLOG' '$MDLOG_VERSION
ZILLIZTECH_PULSAR_CLIENT_GO_WITH_VERSION=$ZILLIZTECH_PULSAR_CLIENT_GO' '$PULSAR_CLIENT_GO_VERSION
ZILLIZTECH_PULSARCTL_WITH_VERSION=$ZILLIZTECH_PULSARCTL' '$PULSARCTL_VERSION

SED_EXPR1='s#github.com/apache/pulsar-client-go =>.*#github.com/apache/pulsar-client-go => '$ZILLIZTECH_PULSAR_CLIENT_GO_WITH_VERSION'#'
SED_EXPR2='s#github.com/streamnative/pulsarctl =>.*#github.com/streamnative/pulsarctl => '$ZILLIZTECH_PULSARCTL_WITH_VERSION'#'
SED_EXPR3='/replace (/a\\t'$ZILLIZTECH_MDLOG' => '$ZILLIZTECH_MDLOG_WITH_VERSION
SED_EXPR4='/mkdir -p $(INSTALL_PATH) && go env -w CGO_ENABLED="1"/a\\t\t$(GO) env -w GOPRIVATE="'$ZILLIZTECH_REPO'" && $(GO) mod tidy && \\'

echo $SED_EXPR1
echo $SED_EXPR2
echo $SED_EXPR3
echo $SED_EXPR4

# Replace
sed -i "$SED_EXPR1" pkg/go.mod
sed -i "$SED_EXPR1" go.mod
sed -i "$SED_EXPR2" pkg/go.mod
sed -i "$SED_EXPR2" go.mod
sed -i "$SED_EXPR3" pkg/go.mod
sed -i "$SED_EXPR3" go.mod
sed -i "$SED_EXPR4" Makefile

# Check if replace is successful
if grep -q "$ZILLIZTECH_PULSAR_CLIENT_GO_WITH_VERSION" pkg/go.mod && grep -q "$ZILLIZTECH_PULSARCTL_WITH_VERSION" pkg/go.mod && grep -q "$ZILLIZTECH_MDLOG_WITH_VERSION" pkg/go.mod; then
    echo "Replace $ZILLIZTECH_REPO in pkg/go.mod successfully"
else
    echo "Replace $ZILLIZTECH_REPO in pkg/go.mod failed"
    exit 1
fi

if grep -q "$ZILLIZTECH_PULSAR_CLIENT_GO_WITH_VERSION" go.mod && grep -q "$ZILLIZTECH_PULSARCTL_WITH_VERSION" go.mod && grep -q "$ZILLIZTECH_MDLOG_WITH_VERSION" go.mod; then
    echo "Replace $ZILLIZTECH_REPO in go.mod successfully"
else
    echo "Replace $ZILLIZTECH_REPO in go.mod failed"
    exit 1
fi

if grep -q $ZILLIZTECH_REPO Makefile; then
    echo "Replace $ZILLIZTECH_REPO in Makefile successfully"
else
    echo "Replace $ZILLIZTECH_REPO in Makefile failed"
    exit 1
fi