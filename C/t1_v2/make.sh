#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT="${OUT:-}"
CXX="${CXX:-g++}"
REQUIRED_PROTOC_VERSION="libprotoc 3.21.12"

WITH_ZLIB=1
WITH_PROTOBUF=1
WITH_TDENGINE=1
WITH_REDIS=1
WITH_RABBITMQ=1
RUN_SELF_TEST=0

for arg in "$@"; do
    case "$arg" in
        --help|-h)
            cat <<'USAGE'
Usage: ./make.sh [options]

Default:
  Build the production binary with zlib/protobuf/TDengine/Redis/RabbitMQ enabled
  and write /tmp/t1_v2_all_live.

Options:
  --self-test        Build, then run built-in semantic checks
  --dev-minimal      Developer-only dependency-light build
  --out=<path>       Output binary path
USAGE
            exit 0
            ;;
        --dev-minimal|--minimal)
            WITH_ZLIB=0
            WITH_PROTOBUF=0
            WITH_TDENGINE=0
            WITH_REDIS=0
            WITH_RABBITMQ=0
            ;;
        --full)
            WITH_ZLIB=1
            WITH_PROTOBUF=1
            WITH_TDENGINE=1
            WITH_REDIS=1
            WITH_RABBITMQ=1
            ;;
        --with-zlib)
            WITH_ZLIB=1
            ;;
        --with-protobuf)
            WITH_PROTOBUF=1
            ;;
        --with-tdengine)
            WITH_TDENGINE=1
            ;;
        --with-redis)
            WITH_REDIS=1
            ;;
        --with-rabbitmq)
            WITH_RABBITMQ=1
            ;;
        --self-test)
            RUN_SELF_TEST=1
            ;;
        --no-self-test)
            RUN_SELF_TEST=0
            ;;
        --out=*)
            OUT="${arg#--out=}"
            ;;
        *)
            echo "unknown option: $arg" >&2
            exit 2
            ;;
    esac
done

if [[ -z "$OUT" ]]; then
    if [[ "$WITH_ZLIB" == "1" && "$WITH_PROTOBUF" == "1" && "$WITH_TDENGINE" == "1" && "$WITH_REDIS" == "1" && "$WITH_RABBITMQ" == "1" ]]; then
        OUT="/tmp/t1_v2_all_live"
    else
        OUT="/tmp/t1_v2"
    fi
fi

generate_protobuf_sources() {
    local protoc_version
    if ! command -v protoc >/dev/null 2>&1; then
        echo "required protoc is not installed: ${REQUIRED_PROTOC_VERSION}" >&2
        exit 1
    fi
    protoc_version="$(protoc --version)"
    if [[ "$protoc_version" != "$REQUIRED_PROTOC_VERSION" ]]; then
        echo "unsupported protoc version: ${protoc_version}; required ${REQUIRED_PROTOC_VERSION}" >&2
        exit 1
    fi
    local proto_root="$ROOT_DIR/.."
    local proto_file="$proto_root/schema.proto"
    protoc -I "$proto_root" --cpp_out="$proto_root" "$proto_file"
    local generated_cc="$proto_root/schema.pb.cc"
    local generated_h="$proto_root/schema.pb.h"
    if [[ ! -s "$generated_cc" || ! -s "$generated_h" ]]; then
        echo "protoc did not generate schema.pb.cc/schema.pb.h" >&2
        exit 1
    fi
    echo "protoc_version=${protoc_version}"
    echo "generation_command=protoc -I ${proto_root} --cpp_out=${proto_root} ${proto_file}"
    echo "generated_schema_pb_cc_sha256=$(sha256sum "$generated_cc" | awk '{print $1}')"
    echo "generated_schema_pb_h_sha256=$(sha256sum "$generated_h" | awk '{print $1}')"
}

CXXFLAGS_ARR=(-std=c++17 -O2 -Wall -Wextra -I "$ROOT_DIR")
LDFLAGS_ARR=()
SOURCES=("$ROOT_DIR"/*.cpp)

if [[ "$WITH_ZLIB" == "1" ]]; then
    CXXFLAGS_ARR+=(-DT1_V2_ENABLE_ZLIB)
    LDFLAGS_ARR+=(-lz)
fi

if [[ "$WITH_PROTOBUF" == "1" ]]; then
    generate_protobuf_sources
    CXXFLAGS_ARR+=(-DT1_V2_ENABLE_PROTOBUF -I /usr/local/protobuf/include -I "$ROOT_DIR/..")
    LDFLAGS_ARR+=(-L/usr/local/protobuf/lib -lprotobuf -Wl,-rpath,/usr/local/protobuf/lib)
    SOURCES+=("$ROOT_DIR/../schema.pb.cc")
fi

if [[ "$WITH_TDENGINE" == "1" ]]; then
    CXXFLAGS_ARR+=(-DT1_V2_ENABLE_TDENGINE -I /usr/local/taos/include)
    LDFLAGS_ARR+=(-ltaos)
fi

if [[ "$WITH_REDIS" == "1" ]]; then
    CXXFLAGS_ARR+=(-DT1_V2_ENABLE_REDIS -I /usr/local/include)
    LDFLAGS_ARR+=(-L/usr/local/lib -lhiredis -Wl,-rpath,/usr/local/lib)
fi

if [[ "$WITH_RABBITMQ" == "1" ]]; then
    CXXFLAGS_ARR+=(-DT1_V2_ENABLE_RABBITMQ -I /usr/local/include)
    LDFLAGS_ARR+=(-L/usr/local/lib64 -lrabbitmq -Wl,-rpath,/usr/local/lib64)
fi

"$CXX" "${CXXFLAGS_ARR[@]}" "${SOURCES[@]}" -o "$OUT" "${LDFLAGS_ARR[@]}"

if [[ "$RUN_SELF_TEST" == "1" ]]; then
    "$OUT" --self-test
fi

echo "built $OUT"
