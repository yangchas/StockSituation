#include "self_test.h"

#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "config_v2.h"
#include "engine_core.h"
#include "payload_decompressor.h"
#include "protobuf_tick_decoder.h"
#include "rabbitmq_batch_decoder.h"
#include "rabbitmq_tick_source.h"
#include "rabbitmq_wire_message.h"
#include "raw_tick_converter.h"
#include "redis_command_executor.h"
#include "redis_command_formatter.h"
#include "redis_v2_writer.h"
#include "replay_slice_scheduler.h"
#include "runtime_batch_stats.h"
#include "runtime_execution_coordinator.h"
#include "runtime_loop.h"
#include "runtime_pipeline.h"
#include "source_tick_batch_builder.h"
#include "snapshot_trigger.h"
#include "tdengine_command_executor.h"
#include "td_replay_query.h"
#include "td_replay_row_converter.h"
#include "tdengine_row_adapter.h"
#include "tdengine_v2_writer.h"

#if defined(T1_V2_ENABLE_ZLIB)
#include <zlib.h>
#endif

#if defined(T1_V2_ENABLE_PROTOBUF)
#include "schema.pb.h"
#endif

namespace t1_v2 {
namespace {

int64_t make_local_ts_ms(int year, int month, int day, int hour, int minute, int second) {
    std::tm tm{};
    tm.tm_year = year - 1900;
    tm.tm_mon = month - 1;
    tm.tm_mday = day;
    tm.tm_hour = hour;
    tm.tm_min = minute;
    tm.tm_sec = second;
    tm.tm_isdst = -1;
    return static_cast<int64_t>(std::mktime(&tm)) * 1000;
}

void set_symbol(RawTick& tick, const char* symbol) {
    std::memset(tick.symbol, 0, sizeof(tick.symbol));
    std::memcpy(tick.symbol, symbol, sizeof(tick.symbol) - 1);
}

void set_market(RawTick& tick, const char* market) {
    std::memset(tick.market, 0, sizeof(tick.market));
    if (market) {
        std::memcpy(tick.market, market, std::min<std::size_t>(std::strlen(market), sizeof(tick.market) - 1));
    }
}

// This builds a normalized RawTick, not a wire/protobuf StockData record.
// Source adapters must separately prove float/int64 feed fields are converted
// into these fixed-point units without unit drift.
RawTick make_tick(const char* symbol, int64_t ts_ms, int px_milli, int64_t amt_yuan, const char* market = "") {
    RawTick tick;
    set_symbol(tick, symbol);
    set_market(tick, market);
    tick.ts_ms = ts_ms;
    tick.px_milli = px_milli;
    tick.pc_milli = 10000;
    tick.o_milli = px_milli;
    tick.h_milli = px_milli;
    tick.l_milli = px_milli;
    tick.amt_yuan = amt_yuan;
    tick.vol_units = 10000;
    tick.bp_milli[0] = px_milli;
    tick.ap_milli[0] = px_milli;
    tick.bp_milli[1] = px_milli;
    tick.ap_milli[1] = px_milli;
    tick.bv[0] = 1000;
    tick.av[0] = 800;
    tick.bv[1] = 3000;
    tick.av[1] = 2000;
    return tick;
}

class FakeTickSource final : public ITickSource {
public:
    struct Counters {
        uint32_t ack_count = 0;
        uint32_t reject_count = 0;
        uint64_t last_delivery_tag = 0;
        bool last_requeue = false;
    };

    explicit FakeTickSource(std::vector<TickSourceResult> results, Counters* counters = nullptr)
        : results_(std::move(results)), counters_(counters) {}

    bool start() override {
        started_ = true;
        index_ = 0;
        return true;
    }

    void stop() override {
        started_ = false;
    }

    TickSourceResult next_batch() override {
        if (!started_) {
            TickSourceResult result;
            result.status = TickSourceStatus::Error;
            result.error_msg = "fake source not started";
            return result;
        }
        if (index_ >= results_.size()) {
            TickSourceResult result;
            result.status = TickSourceStatus::EndOfStream;
            return result;
        }
        return results_[index_++];
    }

    RuntimeMode mode() const override {
        return RuntimeMode::Replay;
    }

    bool ack(const TickSourceResult& result) override {
        if (!counters_) {
            return true;
        }
        ++counters_->ack_count;
        counters_->last_delivery_tag = result.delivery_tag;
        return true;
    }

    bool reject(const TickSourceResult& result, bool requeue) override {
        if (!counters_) {
            return true;
        }
        ++counters_->reject_count;
        counters_->last_delivery_tag = result.delivery_tag;
        counters_->last_requeue = requeue;
        return true;
    }

private:
    std::vector<TickSourceResult> results_;
    Counters* counters_ = nullptr;
    size_t index_ = 0;
    bool started_ = false;
};

class FailingRedisCommandExecutor final : public IRedisCommandExecutor {
public:
    RedisExecutionResult execute(const std::vector<RedisCommand>& commands) override {
        RedisExecutionResult result;
        result.command_count = static_cast<int>(commands.size());
        result.ok = commands.empty();
        if (!result.ok) {
            result.error = "intentional redis failure";
        }
        return result;
    }
};

bool expect(bool condition, const char* message) {
    if (!condition) {
        std::cerr << "self-test failed: " << message << std::endl;
        return false;
    }
    return true;
}

int count_dirty_quotes(const QuoteStateStore& store) {
    int count = 0;
    store.for_each_active([&](const QuoteState& state) {
        if (state.dirty_mask != DIRTY_NONE) {
            ++count;
        }
    });
    return count;
}

}  // namespace

bool run_self_test() {
    const std::string wire_header =
        "{\"record_count\":2,\"compression\":\"GZIP\",\"original_size\":100,"
        "\"compressed_size\":60,\"timestamp\":1777425845000,"
        "\"batch_id\":\"b1\",\"proto_version\":\"v1\"}";
    const std::string wire_payload = "protobuf-bytes";
    std::vector<char> wire_body;
    const uint32_t header_len = static_cast<uint32_t>(wire_header.size());
    wire_body.push_back(static_cast<char>((header_len >> 24) & 0xFF));
    wire_body.push_back(static_cast<char>((header_len >> 16) & 0xFF));
    wire_body.push_back(static_cast<char>((header_len >> 8) & 0xFF));
    wire_body.push_back(static_cast<char>(header_len & 0xFF));
    wire_body.insert(wire_body.end(), wire_header.begin(), wire_header.end());
    wire_body.insert(wire_body.end(), wire_payload.begin(), wire_payload.end());

    RabbitMqWireMessageView wire_view;
    std::string wire_error;
    if (!expect(RabbitMqWireMessageParser::parse(wire_body, wire_view, &wire_error), "rabbit wire parses")) return false;
    if (!expect(wire_view.header.record_count == 2, "rabbit wire record count")) return false;
    if (!expect(wire_view.header.compression == "GZIP", "rabbit wire compression")) return false;
    if (!expect(wire_view.payload_size == wire_payload.size(), "rabbit wire payload size")) return false;

    std::vector<char> decompressed;
    const PayloadDecompressResult none_decompress = PayloadDecompressor::decompress(
        PayloadCompression::None,
        wire_payload.data(),
        wire_payload.size(),
        decompressed
    );
    if (!expect(none_decompress.ok, "none payload decompress")) return false;
    if (!expect(std::string(decompressed.begin(), decompressed.end()) == wire_payload, "none payload passthrough")) return false;

    std::vector<char> gzip_out;
    const PayloadDecompressResult gzip_decompress = PayloadDecompressor::decompress(
        PayloadDecompressor::parse_compression(wire_view.header.compression),
        wire_view.payload,
        wire_view.payload_size,
        gzip_out
    );
#if defined(T1_V2_ENABLE_ZLIB)
    if (!expect(!gzip_decompress.ok, "invalid gzip payload rejected")) return false;

    const std::string zlib_plain = "decoded-protobuf-batch";
    std::vector<char> zlib_payload(compressBound(static_cast<uLong>(zlib_plain.size())));
    uLongf zlib_payload_size = static_cast<uLongf>(zlib_payload.size());
    const int compress_ret = compress2(
        reinterpret_cast<Bytef*>(zlib_payload.data()),
        &zlib_payload_size,
        reinterpret_cast<const Bytef*>(zlib_plain.data()),
        static_cast<uLong>(zlib_plain.size()),
        Z_BEST_SPEED
    );
    if (!expect(compress_ret == Z_OK, "zlib test compression")) return false;
    zlib_payload.resize(static_cast<std::size_t>(zlib_payload_size));
    std::vector<char> zlib_decoded;
    const PayloadDecompressResult zlib_result = PayloadDecompressor::decompress(
        PayloadCompression::Gzip,
        zlib_payload.data(),
        zlib_payload.size(),
        zlib_decoded
    );
    if (!expect(zlib_result.ok, "zlib payload decompress")) return false;
    if (!expect(std::string(zlib_decoded.begin(), zlib_decoded.end()) == zlib_plain, "zlib payload roundtrip")) return false;
#else
    if (!expect(!gzip_decompress.ok && gzip_decompress.unsupported, "gzip unsupported without zlib")) return false;
#endif

    ConfigV2 trigger_config;
    SnapshotTrigger trigger(trigger_config);
    const SnapshotTriggerState before_a25 =
        trigger.update(make_local_ts_ms(2026, 4, 29, 9, 24, 59), MarketPhase::Auction);
    if (!expect(!before_a25.emit_a25, "a25 not emitted before 09:25:00")) return false;
    const SnapshotTriggerState at_a25 =
        trigger.update(make_local_ts_ms(2026, 4, 29, 9, 25, 0), MarketPhase::Auction);
    if (!expect(at_a25.emit_a25, "a25 emitted at first 09:25 tick")) return false;
    const SnapshotTriggerState repeat_a25 =
        trigger.update(make_local_ts_ms(2026, 4, 29, 9, 25, 2), MarketPhase::Auction);
    if (!expect(!repeat_a25.emit_a25, "a25 emitted only once")) return false;

    ConfigV2 default_replay_config;
    if (!expect(default_replay_config.tdengine.replay_table == "stock_tick_v2", "default replay table is v2 ticks")) return false;
    if (!expect(default_replay_config.replay.write_redis, "replay writes redis by default")) return false;
    if (!expect(!default_replay_config.replay.write_tdengine, "replay suppresses tdengine by default")) return false;

    ConfigV2 replay_config;
    replay_config.tdengine.replay_table = "stock_data";
    replay_config.replay.start_time = "2026-04-29 09:20:00";
    replay_config.replay.end_time = "2026-04-29 09:20:06";
    replay_config.replay.tick_interval_ms = 3000;
    TdReplayQueryBuilder replay_query(replay_config);
    const int64_t aligned = replay_query.align_to_tick_interval(1777425845123);
    if (!expect(aligned == 1777425843000LL, "td replay interval align")) return false;
    const std::string replay_sql = replay_query.build_slice_query(aligned);
    if (!expect(replay_sql.find("FROM stock_data") != std::string::npos, "td replay table")) return false;
    if (!expect(replay_sql.find("inst_vol, inst_amt, large_net") != std::string::npos, "td replay derived fields")) return false;
    ConfigV2 replay_v2_config = replay_config;
    replay_v2_config.tdengine.replay_table = "stock_tick_v2";
    TdReplayQueryBuilder replay_v2_query(replay_v2_config);
    const std::string replay_v2_sql = replay_v2_query.build_slice_query(aligned);
    if (!expect(replay_v2_sql.find("FROM stock_tick_v2") != std::string::npos, "td replay v2 table")) return false;
    if (!expect(replay_v2_sql.find("px_milli/1000.0 AS lp") != std::string::npos, "td replay v2 price alias")) return false;
    if (!expect(replay_v2_sql.find("inst_amt_yuan AS inst_amt") != std::string::npos, "td replay v2 amount alias")) return false;
    ReplaySliceScheduler scheduler(replay_config);
    if (!expect(scheduler.start(), "replay scheduler start")) return false;
    const ReplaySlice slice1 = scheduler.next();
    const ReplaySlice slice2 = scheduler.next();
    const ReplaySlice slice3 = scheduler.next();
    if (!expect(slice1.valid && slice2.valid && !slice3.valid, "replay scheduler slices")) return false;
    if (!expect(slice2.start_ms - slice1.start_ms == 3000, "replay scheduler interval")) return false;

    SourceTickRecord source;
    source.tss = make_local_ts_ms(2026, 4, 29, 9, 24, 5);
    source.symbol = "000070";
    source.market = "sz";
    source.lp = 12.345;
    source.o = 12.010;
    source.h = 12.700;
    source.l = 11.980;
    source.lc = 11.220;
    source.a = 123456789.4;
    source.v = 9876543;
    source.ap[0] = 12.350;
    source.ap[1] = 12.350;
    source.bp[0] = 12.350;
    source.bp[1] = 12.350;
    source.av[0] = 3000;
    source.av[1] = 5000;
    source.bv[0] = 7000;
    source.bv[1] = 11000;
    source.inst_vol = 1000;
    source.inst_amt = 12345;
    source.large_net = -12345;
    source.limit_up = 12.340;
    source.limit_down = 10.100;
    source.limit_band_bp = 500;
    source.is_st = true;

    const std::vector<std::string> replay_fields{
        "ts", "symbol", "lp", "o", "h", "l", "lc", "a", "v",
        "ap1", "ap2", "bp1", "bp2", "av1", "av2", "bv1", "bv2",
        "inst_vol", "inst_amt", "large_net", "limit_up", "limit_down", "limit_band_bp", "is_st"
    };
    const TdReplayRowPlan replay_plan = TdReplayRowConverter::build_plan(replay_fields);
    const std::vector<TdReplayCell> replay_cells{
        TdReplayCell::i64(source.tss), TdReplayCell::string("000070.SZ"),
        TdReplayCell::f64(source.lp), TdReplayCell::f64(source.o), TdReplayCell::f64(source.h),
        TdReplayCell::f64(source.l), TdReplayCell::f64(source.lc), TdReplayCell::f64(source.a),
        TdReplayCell::i64(source.v), TdReplayCell::f64(source.ap[0]), TdReplayCell::f64(source.ap[1]),
        TdReplayCell::f64(source.bp[0]), TdReplayCell::f64(source.bp[1]),
        TdReplayCell::i64(source.av[0]), TdReplayCell::i64(source.av[1]),
        TdReplayCell::i64(source.bv[0]), TdReplayCell::i64(source.bv[1]),
        TdReplayCell::i64(source.inst_vol), TdReplayCell::i64(source.inst_amt), TdReplayCell::i64(source.large_net),
        TdReplayCell::f64(source.limit_up), TdReplayCell::f64(source.limit_down),
        TdReplayCell::i64(source.limit_band_bp), TdReplayCell::i64(1)
    };
    SourceTickRecord replay_record;
    if (!expect(TdReplayRowConverter::convert(replay_plan, replay_cells, replay_record), "td replay row converts")) return false;
    if (!expect(replay_record.symbol == "000070" && replay_record.market == "sz", "td replay symbol normalized")) return false;
    if (!expect(replay_record.inst_amt == source.inst_amt && replay_record.large_net == source.large_net, "td replay derived fields")) return false;
    if (!expect(replay_record.limit_band_bp == 500 && replay_record.is_st, "td replay limit metadata")) return false;
#if defined(T1_V2_ENABLE_TDENGINE)
    if (!expect(TdengineRowAdapter::enabled(), "tdengine adapter enabled")) return false;
#else
    if (!expect(!TdengineRowAdapter::enabled(), "tdengine adapter disabled")) return false;
#endif

    RawTick converted;
    if (!expect(RawTickConverter::from_source_record(source, converted), "source record converts")) return false;
    if (!expect(std::string(converted.symbol) == "000070", "source symbol")) return false;
    if (!expect(std::string(converted.market) == "sz", "source market")) return false;
    if (!expect(converted.px_milli == 12345, "source price milli")) return false;
    if (!expect(converted.pc_milli == 11220, "source previous close milli")) return false;
    if (!expect(converted.amt_yuan == 123456789, "source amount yuan")) return false;
    if (!expect(converted.vol_units == 9876543, "source volume shares")) return false;
    if (!expect(converted.ap_milli[1] == 12350 && converted.bv[1] == 11000, "source book levels")) return false;
    if (!expect(converted.inst_amt_yuan == 12345 && converted.large_net_yuan == -12345, "source derived fields")) return false;
    if (!expect(converted.limit_up_milli == 12340 && converted.limit_down_milli == 10100, "source limit prices")) return false;
    if (!expect(converted.limit_band_bp == 500 && converted.is_st, "source limit metadata")) return false;
    SourceTickRecord sh_index_source = source;
    sh_index_source.symbol = "000001";
    sh_index_source.market = "sh";
    sh_index_source.lp = 4179.952;
    RawTick rejected_index_tick;
    if (!expect(!RawTickConverter::from_source_record(sh_index_source, rejected_index_tick), "sh index rejected from stock tick path")) return false;
    SourceTickRecord sz_index_source = source;
    sz_index_source.symbol = "000170";
    sz_index_source.market = "sz";
    sz_index_source.lp = 6349.69;
    RawTick rejected_sz_index_tick;
    if (!expect(!RawTickConverter::from_source_record(sz_index_source, rejected_sz_index_tick), "sz index-like price rejected from stock tick path")) return false;

    SourceTickRecord bad_source = source;
    bad_source.symbol = "BAD";
    std::vector<SourceTickRecord> source_records{source, bad_source, sh_index_source, sz_index_source};
    TickBatch source_batch;
    const SourceTickBatchBuildStats build_stats = SourceTickBatchBuilder::build(
        source_records,
        RuntimeMode::Replay,
        0,
        7,
        source_batch
    );
    if (!expect(build_stats.input_count == 4, "source batch input count")) return false;
    if (!expect(build_stats.accepted_count == 1 && build_stats.rejected_count == 3, "source batch filtering")) return false;
    if (!expect(source_batch.mode == RuntimeMode::Replay, "source batch mode")) return false;
    if (!expect(source_batch.logical_ts_ms == source.tss, "source batch logical ts fallback")) return false;
    if (!expect(source_batch.ticks.size() == 1 && std::string(source_batch.ticks[0].symbol) == "000070", "source batch tick")) return false;

#if defined(T1_V2_ENABLE_PROTOBUF)
    dataservice::DataBatch proto_batch;
    dataservice::DataRecord* proto_record = proto_batch.add_records();
    proto_record->set_tss(source.tss);
    proto_record->set_symbol(source.symbol);
    proto_record->set_exchange("SZ");
    proto_record->set_market("sz");
    proto_record->set_lp(static_cast<float>(source.lp));
    proto_record->set_o(static_cast<float>(source.o));
    proto_record->set_h(static_cast<float>(source.h));
    proto_record->set_l(static_cast<float>(source.l));
    proto_record->set_lc(static_cast<float>(source.lc));
    proto_record->set_a(static_cast<float>(source.a));
    proto_record->set_v(source.v);
    proto_record->set_ap1(static_cast<float>(source.ap[0]));
    proto_record->set_ap2(static_cast<float>(source.ap[1]));
    proto_record->set_bp1(static_cast<float>(source.bp[0]));
    proto_record->set_bp2(static_cast<float>(source.bp[1]));
    proto_record->set_av1(source.av[0]);
    proto_record->set_av2(source.av[1]);
    proto_record->set_bv1(source.bv[0]);
    proto_record->set_bv2(source.bv[1]);
    std::string proto_batch_bytes;
    if (!expect(proto_batch.SerializeToString(&proto_batch_bytes), "protobuf batch serialize")) return false;

    dataservice::DataRequest proto_request;
    proto_request.set_compression(dataservice::NONE);
    proto_request.set_compressed_data(proto_batch_bytes);
    std::string proto_request_bytes;
    if (!expect(proto_request.SerializeToString(&proto_request_bytes), "protobuf request serialize")) return false;

    std::vector<SourceTickRecord> decoded_records;
    const ProtobufDecodeResult proto_decode = ProtobufTickDecoder::decode_data_request_payload(
        proto_request_bytes.data(),
        proto_request_bytes.size(),
        decoded_records
    );
    if (!expect(proto_decode.ok, "protobuf request decode")) return false;
    if (!expect(decoded_records.size() == 1 && decoded_records[0].symbol == "000070", "protobuf decoded symbol")) return false;

    const std::string live_header =
        "{\"record_count\":1,\"compression\":\"NONE\",\"original_size\":"
        + std::to_string(proto_request_bytes.size())
        + ",\"compressed_size\":" + std::to_string(proto_request_bytes.size())
        + ",\"timestamp\":1777425845000,\"batch_id\":\"live1\",\"proto_version\":\"v1\"}";
    std::vector<char> live_body;
    const uint32_t live_header_len = static_cast<uint32_t>(live_header.size());
    live_body.push_back(static_cast<char>((live_header_len >> 24) & 0xFF));
    live_body.push_back(static_cast<char>((live_header_len >> 16) & 0xFF));
    live_body.push_back(static_cast<char>((live_header_len >> 8) & 0xFF));
    live_body.push_back(static_cast<char>(live_header_len & 0xFF));
    live_body.insert(live_body.end(), live_header.begin(), live_header.end());
    live_body.insert(live_body.end(), proto_request_bytes.begin(), proto_request_bytes.end());
    TickBatch live_decoded_batch;
    const RabbitMqBatchDecodeResult live_decoded = RabbitMqBatchDecoder::decode_body(live_body, 99, live_decoded_batch);
    if (!expect(live_decoded.ok, "rabbit live body decode")) return false;
    if (!expect(live_decoded_batch.seq_no == 99 && live_decoded_batch.ticks.size() == 1, "rabbit live batch output")) return false;
#else
    std::vector<SourceTickRecord> decoded_records;
    const ProtobufDecodeResult proto_decode = ProtobufTickDecoder::decode_data_request_payload(
        wire_payload.data(),
        wire_payload.size(),
        decoded_records
    );
    if (!expect(!proto_decode.ok && proto_decode.unsupported, "protobuf unsupported without flag")) return false;
    TickBatch unsupported_live_batch;
    const RabbitMqBatchDecodeResult unsupported_live = RabbitMqBatchDecoder::decode_body(wire_body, 1, unsupported_live_batch);
    if (!expect(!unsupported_live.ok && unsupported_live.unsupported, "rabbit live decode unsupported without protobuf")) return false;
#endif

    ConfigV2 config;
    config.runtime_mode = RuntimeMode::Live;
    config.processing.dry_run = false;
    EngineCore engine(config);
    if (!expect(engine.initialize(), "engine initialize")) {
        return false;
    }

    const int64_t ts_0920 = make_local_ts_ms(2026, 4, 29, 9, 20, 5);
    const int64_t ts_0921 = make_local_ts_ms(2026, 4, 29, 9, 21, 5);
    const int64_t ts_0922 = make_local_ts_ms(2026, 4, 29, 9, 22, 5);

    TickBatch batch1;
    batch1.logical_ts_ms = ts_0920;
    batch1.ticks.push_back(make_tick("000001", ts_0920, 10000, 100000000));
    EngineProcessStats stats1 = engine.on_batch(batch1);
    if (!expect(stats1.phase == MarketPhase::Auction, "0920 phase auction")) return false;

    TickBatch batch2;
    batch2.logical_ts_ms = ts_0921;
    batch2.ticks.push_back(make_tick("000001", ts_0921, 10100, 130000000));
    engine.on_batch(batch2);

    TickBatch batch3;
    batch3.logical_ts_ms = ts_0922;
    batch3.ticks.push_back(make_tick("000001", ts_0922, 10200, 160000000));
    EngineProcessStats stats3 = engine.on_batch(batch3);

    const QuoteState* found = nullptr;
    engine.quote_store().for_each_active([&](const QuoteState& state) {
        if (std::string(state.symbol) == "000001") {
            found = &state;
        }
    });
    if (!expect(found != nullptr, "quote state exists")) return false;
    const QuoteState& state = *found;
    if (!expect(state.auction.a20_px_milli == 10000, "a20 captured")) return false;
    if (!expect(state.auction.match_amt_yuan == 816000, "auction matched amount uses l1 min lots")) return false;
    if (!expect(state.auction.rest_bid_amt_yuan == 3060000, "auction rest bid uses l2 lots")) return false;
    if (!expect(state.auction.rest_ask_amt_yuan == 2040000, "auction rest ask uses l2 lots")) return false;
    if (!expect(state.inst_amt_yuan == 30000000, "auction inst amount diff")) return false;
    if (!expect(state.large_net_yuan == 0, "auction large net stays zero")) return false;
    if (!expect(state.spd1m_bp == 99, "speed_1m minute bucket")) return false;
    if (!expect(state.amt2m_yuan == 60000000, "amount_2m cumulative difference")) return false;
    if (!expect(state.amt5m_yuan == 60000000, "amount_5m fallback inside window")) return false;

    EngineCore auction25_engine(config);
    if (!expect(auction25_engine.initialize(), "auction25 engine initialize")) return false;
    const int64_t ts_0925 = make_local_ts_ms(2026, 4, 29, 9, 25, 0);
    TickBatch auction25_batch;
    auction25_batch.logical_ts_ms = ts_0925;
    auction25_batch.ticks.push_back(make_tick("688820", ts_0925, 115000, 71536200, "kc"));
    auction25_engine.on_batch(auction25_batch);
    bool has_auction25_amount = false;
    auction25_engine.quote_store().for_each_active([&](const QuoteState& quote) {
        if (std::string(quote.symbol) == "688820") {
            has_auction25_amount = quote.auction.match_amt_yuan == 71536200;
        }
    });
    if (!expect(has_auction25_amount, "0925 auction matched amount uses cumulative amount")) return false;

    TickBatch limit_batch;
    limit_batch.logical_ts_ms = ts_0922;
    limit_batch.ticks.push_back(make_tick("300001", ts_0922, 12000, 50000000));
    RawTick no_buy_seal_limit = make_tick("300002", ts_0922, 12000, 50000000);
    no_buy_seal_limit.bv[1] = 0;
    limit_batch.ticks.push_back(no_buy_seal_limit);
    limit_batch.ticks.push_back(make_tick("000003", ts_0922, 9000, 50000000));
    RawTick no_sell_seal_limit = make_tick("000004", ts_0922, 9000, 50000000);
    no_sell_seal_limit.av[1] = 0;
    limit_batch.ticks.push_back(no_sell_seal_limit);
    RawTick st_limit = make_tick("600005", ts_0922, 10500, 50000000, "sh");
    st_limit.is_st = true;
    limit_batch.ticks.push_back(st_limit);
    RawTick bj_limit = make_tick("830001", ts_0922, 13000, 50000000, "bj");
    limit_batch.ticks.push_back(bj_limit);
    RawTick no_limit_day = make_tick("000006", ts_0922, 11000, 50000000, "sz");
    no_limit_day.no_price_limit = true;
    limit_batch.ticks.push_back(no_limit_day);
    engine.on_batch(limit_batch);
    bool has_computed_limit_up = false;
    bool no_buy_seal_is_normal = false;
    bool has_computed_limit_down = false;
    bool no_sell_seal_is_normal = false;
    bool st_limit_up = false;
    bool bj_limit_up = false;
    bool no_limit_is_normal = false;
    engine.quote_store().for_each_active([&](const QuoteState& quote) {
        if (std::string(quote.symbol) == "300001") {
            has_computed_limit_up = quote.limit_state == LimitState::Up;
        } else if (std::string(quote.symbol) == "300002") {
            no_buy_seal_is_normal = quote.limit_state == LimitState::Normal;
        } else if (std::string(quote.symbol) == "000003") {
            has_computed_limit_down = quote.limit_state == LimitState::Down;
        } else if (std::string(quote.symbol) == "000004") {
            no_sell_seal_is_normal = quote.limit_state == LimitState::Normal;
        } else if (std::string(quote.symbol) == "600005") {
            st_limit_up = quote.limit_state == LimitState::Up;
        } else if (std::string(quote.symbol) == "830001") {
            bj_limit_up = quote.limit_state == LimitState::Up;
        } else if (std::string(quote.symbol) == "000006") {
            no_limit_is_normal = quote.limit_state == LimitState::Normal;
        }
    });
    if (!expect(has_computed_limit_up, "limit up requires price and buy seal")) return false;
    if (!expect(no_buy_seal_is_normal, "limit up rejected without buy seal")) return false;
    if (!expect(has_computed_limit_down, "limit down requires price and sell seal")) return false;
    if (!expect(no_sell_seal_is_normal, "limit down rejected without sell seal")) return false;
    if (!expect(st_limit_up, "ST explicit metadata uses 5pct limit band")) return false;
    if (!expect(bj_limit_up, "BJ market uses 30pct limit band")) return false;
    if (!expect(no_limit_is_normal, "no price-limit day never marks limit")) return false;

    EngineCore stock_market_engine(config);
    if (!expect(stock_market_engine.initialize(), "stock market engine initialize")) return false;
    TickBatch stock_market_batch;
    stock_market_batch.logical_ts_ms = ts_0922;
    stock_market_batch.ticks.push_back(make_tick("000001", ts_0922, 11300, 200000000, "sz"));
    stock_market_engine.on_batch(stock_market_batch);
    bool has_sz_stock = false;
    stock_market_engine.quote_store().for_each_active([&](const QuoteState& quote) {
        if (std::string(quote.symbol) == "000001" && std::string(quote.market) == "sz" && quote.px_milli == 11300) {
            has_sz_stock = true;
        }
    });
    if (!expect(stock_market_engine.quote_store().size() == 1 && has_sz_stock, "sz stock remains in stock path")) return false;

    const int64_t ts_0930 = make_local_ts_ms(2026, 4, 29, 9, 30, 5);
    const int64_t ts_0931 = make_local_ts_ms(2026, 4, 29, 9, 31, 5);
    TickBatch trade_batch1;
    trade_batch1.logical_ts_ms = ts_0930;
    trade_batch1.ticks.push_back(make_tick("000002", ts_0930, 10000, 200000000));
    engine.on_batch(trade_batch1);
    TickBatch trade_batch2;
    trade_batch2.logical_ts_ms = ts_0931;
    trade_batch2.ticks.push_back(make_tick("000002", ts_0931, 10300, 203000000));
    engine.on_batch(trade_batch2);

    const QuoteState* trade_state = nullptr;
    engine.quote_store().for_each_active([&](const QuoteState& quote) {
        if (std::string(quote.symbol) == "000002") {
            trade_state = &quote;
        }
    });
    if (!expect(trade_state != nullptr, "trade quote state exists")) return false;
    if (!expect(trade_state->inst_amt_yuan == 3000000, "trade inst amount diff")) return false;
    if (!expect(trade_state->large_net_yuan == 3000000, "trade large net signed positive")) return false;

    RedisV2Writer redis_writer(config);
    if (!expect(redis_writer.initialize(), "redis writer initialize")) return false;
    const auto q2_commands = redis_writer.build_q2_commands(engine.quote_store(), ts_0922);
    if (!expect(!q2_commands.empty(), "q2 command generated")) return false;
    RedisArgvCommand first_redis_argv;
    if (!expect(RedisCommandFormatter::format(q2_commands.front(), first_redis_argv), "redis command formats")) return false;
    if (!expect(first_redis_argv.argv.size() == 50, "q2 hset argv count")) return false;
    bool has_active_symbol_index = false;
    for (const RedisCommand& command : q2_commands) {
        if (command.type == RedisCommandType::SAdd && command.key.find("q2:active:") == 0) {
            RedisArgvCommand sadd_argv;
            if (!expect(RedisCommandFormatter::format(command, sadd_argv), "q2 active SADD formats")) return false;
            has_active_symbol_index = sadd_argv.argv.size() >= 3;
        }
    }
    if (!expect(has_active_symbol_index, "q2 active symbol index generated")) return false;
    const RedisCommandFormatStats redis_stats = RedisCommandFormatter::estimate(q2_commands);
    if (!expect(redis_stats.ok && redis_stats.command_count == q2_commands.size(), "redis command estimate")) return false;
    const auto stock_market_q2_commands = redis_writer.build_q2_commands(stock_market_engine.quote_store(), ts_0922);
    bool has_stock_alias_key = false;
    for (const RedisCommand& command : stock_market_q2_commands) {
        if (command.key == "q2:000001") {
            has_stock_alias_key = true;
        }
    }
    if (!expect(has_stock_alias_key, "sz stock writes q2 alias")) return false;
    const RuntimeBatchStats runtime_stats = RuntimeBatchStatsBuilder::build(
        7,
        ts_0922,
        ts_0922 + 30,
        build_stats,
        stats3,
        redis_stats
    );
    if (!expect(runtime_stats.source_rejected == 3 && runtime_stats.redis_fields > 0, "runtime batch stats")) return false;
    if (!expect(runtime_stats.wall_ts_ms == ts_0922 + 30, "runtime wall timestamp stats")) return false;
    SnapshotTriggerState legacy_trigger;
    legacy_trigger.emit_a20 = true;
    const auto a2_commands_for_legacy = redis_writer.build_a2_commands(engine.quote_store(), legacy_trigger, ts_0922);
    bool has_legacy_auction_key = false;
    bool has_legacy_latest_key = false;
    for (const RedisCommand& command : a2_commands_for_legacy) {
        if (command.key.find("market:auction:") == 0 && command.key.find(":0920") != std::string::npos) {
            for (const auto& field : command.fields) {
                if (field.first == "top_amount" && field.second.find("\"symbol\"") != std::string::npos) {
                    has_legacy_auction_key = true;
                }
            }
        }
        if (command.key.find("market:auction:") == 0 && command.key.find(":latest") != std::string::npos) {
            has_legacy_latest_key = true;
        }
    }
    if (!expect(has_legacy_auction_key, "legacy market auction top_amount key")) return false;
    if (!expect(has_legacy_latest_key, "legacy market auction latest key")) return false;
    SnapshotTriggerState anchor_trigger;
    anchor_trigger.emit_a25 = true;
    const auto a2_commands_for_anchor = redis_writer.build_a2_commands(engine.quote_store(), anchor_trigger, ts_0922);
    bool has_anchor_archive = false;
    for (const RedisCommand& command : a2_commands_for_anchor) {
        if (command.type == RedisCommandType::SetString &&
            command.key.find("market:auction:anchor:") == 0 &&
            command.value.find("\"redis_0925\"") != std::string::npos) {
            RedisArgvCommand set_argv;
            if (!expect(RedisCommandFormatter::format(command, set_argv), "anchor archive SET formats")) return false;
            has_anchor_archive = set_argv.argv.size() == 3;
        }
    }
    if (!expect(has_anchor_archive, "market auction anchor archive key")) return false;
#if defined(T1_V2_ENABLE_REDIS)
    HiredisRedisCommandExecutor hiredis_executor(config);
    const RedisExecutionResult empty_redis_result = hiredis_executor.execute({});
    if (!expect(empty_redis_result.ok && empty_redis_result.command_count == 0, "hiredis empty execute")) return false;
    if (!expect(!hiredis_executor.is_connected(), "hiredis empty execute does not connect")) return false;
#endif
#if defined(T1_V2_ENABLE_RABBITMQ) && (!defined(T1_V2_ENABLE_PROTOBUF) || !defined(T1_V2_ENABLE_ZLIB))
    RabbitMqTickSource rabbit_without_decoder(config);
    if (!expect(!rabbit_without_decoder.start(), "rabbitmq source refuses to start without full live decoder")) return false;
#endif

    RuntimePipeline pipeline(config);
    if (!expect(pipeline.initialize(), "runtime pipeline initialize")) return false;
    RuntimePipelineResult pipeline_result = pipeline.process_batch(batch3, build_stats);
    if (!expect(pipeline_result.engine_stats.tick_count == 1, "runtime pipeline engine ticks")) return false;
    if (!expect(!pipeline_result.redis_commands.empty(), "runtime pipeline redis commands")) return false;
    if (!expect(pipeline_result.has_q2_commands, "runtime pipeline q2 command flag")) return false;
    bool has_runtime_command = false;
    for (const RedisCommand& command : pipeline_result.redis_commands) {
        if (command.key.find("m2:runtime:") == 0) {
            has_runtime_command = true;
            break;
        }
    }
    if (!expect(has_runtime_command, "runtime pipeline m2 command")) return false;
    if (!expect(pipeline_result.runtime_stats.source_rejected == 3, "runtime pipeline stats")) return false;
    if (!expect(count_dirty_quotes(pipeline.engine().quote_store()) > 0, "runtime pipeline dirty before commit")) return false;
    NullRedisCommandExecutor null_redis_executor;
    NullTDengineCommandExecutor null_td_executor_for_runtime;
    RuntimeExecutionCoordinator coordinator(null_redis_executor, null_td_executor_for_runtime);
    const RuntimeExecutionResult execution_result = coordinator.execute_and_commit(pipeline, pipeline_result);
    if (!expect(execution_result.ok, "runtime execution coordinator ok")) return false;
    if (!expect(execution_result.redis_committed_quotes > 0, "runtime execution coordinator commit count")) return false;
    if (!expect(count_dirty_quotes(pipeline.engine().quote_store()) == 0, "runtime pipeline dirty after commit")) return false;
    pipeline.shutdown();

    TickSourceResult loop_source_result;
    loop_source_result.status = TickSourceStatus::Ok;
    loop_source_result.batch = batch3;
    loop_source_result.source_stats = build_stats;
    loop_source_result.requires_ack = true;
    loop_source_result.delivery_tag = 123;
    std::vector<TickSourceResult> loop_results;
    loop_results.push_back(loop_source_result);
    TickSourceResult loop_eos_result;
    loop_eos_result.status = TickSourceStatus::EndOfStream;
    loop_results.push_back(loop_eos_result);
    RuntimeLoopOptions loop_options;
    loop_options.max_empty_polls = 2;
    FakeTickSource::Counters ack_counters;
    RuntimeLoop loop(
        config,
        std::make_unique<FakeTickSource>(std::move(loop_results), &ack_counters),
        null_redis_executor,
        null_td_executor_for_runtime,
        loop_options
    );
    const RuntimeLoopStats loop_stats = loop.run();
    if (!expect(loop_stats.ok, "runtime loop ok")) return false;
    if (!expect(loop_stats.batches == 1 && loop_stats.ticks == 1, "runtime loop batch stats")) return false;
    if (!expect(loop_stats.source_input == build_stats.input_count, "runtime loop source input stats")) return false;
    if (!expect(loop_stats.source_rejected == build_stats.rejected_count, "runtime loop source rejected stats")) return false;
    if (!expect(loop_stats.redis_committed_quotes > 0, "runtime loop commits dirty quotes")) return false;
    if (!expect(loop_stats.source_acks == 1 && loop_stats.source_rejects == 0, "runtime loop ack stats")) return false;
    if (!expect(ack_counters.ack_count == 1 && ack_counters.last_delivery_tag == 123, "runtime loop source ack")) return false;

    std::vector<TickSourceResult> reject_loop_results;
    reject_loop_results.push_back(loop_source_result);
    reject_loop_results.push_back(loop_eos_result);
    FakeTickSource::Counters reject_counters;
    FailingRedisCommandExecutor failing_redis_executor;
    RuntimeLoop reject_loop(
        config,
        std::make_unique<FakeTickSource>(std::move(reject_loop_results), &reject_counters),
        failing_redis_executor,
        null_td_executor_for_runtime,
        loop_options
    );
    const RuntimeLoopStats reject_loop_stats = reject_loop.run();
    if (!expect(!reject_loop_stats.ok, "runtime loop fails on redis failure")) return false;
    if (!expect(reject_loop_stats.source_acks == 0 && reject_loop_stats.source_rejects == 1, "runtime loop reject stats")) return false;
    if (!expect(reject_counters.reject_count == 1 && reject_counters.last_delivery_tag == 123, "runtime loop source reject")) return false;
    if (!expect(reject_counters.last_requeue, "runtime loop source reject requeue")) return false;

    ConfigV2 no_redis_replay_config = config;
    no_redis_replay_config.replay.write_redis = false;
    RuntimePipeline no_redis_replay_pipeline(no_redis_replay_config);
    if (!expect(no_redis_replay_pipeline.initialize(), "no-redis replay pipeline initialize")) return false;
    TickBatch replay_batch = batch3;
    replay_batch.mode = RuntimeMode::Replay;
    RuntimePipelineResult no_redis_result = no_redis_replay_pipeline.process_batch(replay_batch, build_stats);
    if (!expect(no_redis_result.engine_stats.tick_count == 1, "no-redis replay still processes ticks")) return false;
    if (!expect(no_redis_result.redis_commands.empty(), "no-redis replay suppresses redis commands")) return false;
    if (!expect(no_redis_result.tdengine_statements.empty(), "replay suppresses td statements by default")) return false;
    const RuntimeExecutionResult no_redis_execution = coordinator.execute_and_commit(
        no_redis_replay_pipeline,
        no_redis_result
    );
    if (!expect(no_redis_execution.ok, "no-redis replay execution ok")) return false;
    if (!expect(no_redis_execution.redis_committed_quotes == 0, "no-redis replay does not fake commit")) return false;
    no_redis_replay_pipeline.shutdown();

    ConfigV2 redis_only_replay_config = config;
    redis_only_replay_config.replay.write_redis = true;
    redis_only_replay_config.replay.write_tdengine = false;
    RuntimePipeline redis_only_replay_pipeline(redis_only_replay_config);
    if (!expect(redis_only_replay_pipeline.initialize(), "redis-only replay pipeline initialize")) return false;
    RuntimePipelineResult redis_only_replay_result = redis_only_replay_pipeline.process_batch(replay_batch, build_stats);
    if (!expect(!redis_only_replay_result.redis_commands.empty(), "redis-only replay emits redis commands")) return false;
    if (!expect(redis_only_replay_result.tdengine_statements.empty(), "redis-only replay suppresses td statements")) return false;
    redis_only_replay_pipeline.shutdown();

    ConfigV2 dry_run_config = config;
    dry_run_config.processing.dry_run = true;
    RuntimePipeline dry_run_pipeline(dry_run_config);
    if (!expect(dry_run_pipeline.initialize(), "dry-run pipeline initialize")) return false;
    RuntimePipelineResult dry_run_result = dry_run_pipeline.process_batch(batch3, build_stats);
    if (!expect(dry_run_result.engine_stats.tick_count == 1, "dry-run still processes ticks")) return false;
    if (!expect(dry_run_result.redis_commands.empty(), "dry-run suppresses redis commands")) return false;
    if (!expect(dry_run_result.tdengine_statements.empty(), "dry-run suppresses td statements")) return false;
    dry_run_pipeline.shutdown();

    std::vector<TickSourceResult> dry_run_loop_results;
    dry_run_loop_results.push_back(loop_source_result);
    dry_run_loop_results.push_back(loop_eos_result);
    FakeTickSource::Counters dry_run_ack_counters;
    RuntimeLoop dry_run_loop(
        dry_run_config,
        std::make_unique<FakeTickSource>(std::move(dry_run_loop_results), &dry_run_ack_counters),
        null_redis_executor,
        null_td_executor_for_runtime,
        loop_options
    );
    const RuntimeLoopStats dry_run_loop_stats = dry_run_loop.run();
    if (!expect(dry_run_loop_stats.ok, "dry-run runtime loop ok")) return false;
    if (!expect(dry_run_loop_stats.source_acks == 0, "dry-run runtime loop suppresses ack")) return false;
    if (!expect(dry_run_loop_stats.source_ack_skipped == 1, "dry-run runtime loop tracks skipped ack")) return false;
    if (!expect(dry_run_ack_counters.ack_count == 0, "dry-run source ack not called")) return false;

    ConfigV2 dry_run_ack_config = dry_run_config;
    dry_run_ack_config.processing.ack_in_dry_run = true;
    std::vector<TickSourceResult> dry_run_ack_loop_results;
    dry_run_ack_loop_results.push_back(loop_source_result);
    dry_run_ack_loop_results.push_back(loop_eos_result);
    FakeTickSource::Counters dry_run_ack_enabled_counters;
    RuntimeLoop dry_run_ack_loop(
        dry_run_ack_config,
        std::make_unique<FakeTickSource>(std::move(dry_run_ack_loop_results), &dry_run_ack_enabled_counters),
        null_redis_executor,
        null_td_executor_for_runtime,
        loop_options
    );
    const RuntimeLoopStats dry_run_ack_loop_stats = dry_run_ack_loop.run();
    if (!expect(dry_run_ack_loop_stats.ok, "dry-run ack-enabled runtime loop ok")) return false;
    if (!expect(dry_run_ack_loop_stats.source_acks == 1, "dry-run ack-enabled runtime loop ack")) return false;
    if (!expect(dry_run_ack_enabled_counters.ack_count == 1, "dry-run ack-enabled source ack called")) return false;

    std::vector<TickSourceResult> unbounded_dry_run_results;
    unbounded_dry_run_results.push_back(loop_source_result);
    RuntimeLoop unbounded_dry_run_loop(
        dry_run_config,
        std::make_unique<FakeTickSource>(std::move(unbounded_dry_run_results)),
        null_redis_executor,
        null_td_executor_for_runtime
    );
    const RuntimeLoopStats unbounded_dry_run_stats = unbounded_dry_run_loop.run();
    if (!expect(!unbounded_dry_run_stats.ok, "unbounded live dry-run is rejected")) return false;

    TDengineV2Writer td_writer(config);
    if (!expect(td_writer.initialize(), "td writer initialize")) return false;
    const std::string tick_sql = td_writer.build_stock_tick_insert_sql(batch3);
    if (!expect(tick_sql.find("stock_tick_v2") != std::string::npos, "stock tick sql")) return false;
    TickBatch td_filter_batch;
    td_filter_batch.logical_ts_ms = ts_0922;
    td_filter_batch.ticks.push_back(make_tick("000001", ts_0922, 4179952, 1000000000, "sh"));
    td_filter_batch.ticks.push_back(make_tick("000001", ts_0922, 11300, 200000000, "sz"));
    const std::string market_tick_sql = td_writer.build_stock_tick_insert_sql(td_filter_batch);
    if (!expect(market_tick_sql.find("t2_s_000001") != std::string::npos &&
                market_tick_sql.find("4179952") == std::string::npos,
                "td stock tick skips sh index and keeps sz stock")) return false;
    const auto schema_statements = td_writer.build_schema_statements();
    if (!expect(schema_statements.size() == 3, "td schema statements")) return false;
    NullTDengineCommandExecutor td_executor;
    if (!expect(td_executor.execute(schema_statements).statement_count == 3, "null td executor")) return false;
    const auto first_batch_td = td_writer.build_batch_statements(
        batch3,
        engine.quote_store(),
        stats3.snapshot_trigger,
        ts_0922
    );
    if (!expect(first_batch_td.size() >= 4, "first td batch prepends schema")) return false;
    if (!expect(first_batch_td.front().find("CREATE STABLE IF NOT EXISTS stock_tick_v2") != std::string::npos,
                "first td batch starts with stock schema")) return false;
    const auto second_batch_td = td_writer.build_batch_statements(
        batch3,
        engine.quote_store(),
        stats3.snapshot_trigger,
        ts_0922
    );
    if (!expect(!second_batch_td.empty() && second_batch_td.front().find("CREATE ") == std::string::npos,
                "second td batch skips schema")) return false;
#if defined(T1_V2_ENABLE_TDENGINE)
    TaosTDengineCommandExecutor taos_executor(config);
    const TDengineExecutionResult empty_td_result = taos_executor.execute({});
    if (!expect(empty_td_result.ok && empty_td_result.statement_count == 0, "taos empty execute")) return false;
    if (!expect(!taos_executor.is_connected(), "taos empty execute does not connect")) return false;
#endif
    const std::string summary_sql = td_writer.build_auction_summary_insert_sql(
        engine.quote_store(),
        stats3.snapshot_trigger,
        ts_0922
    );
    if (!expect(summary_sql.empty(), "no anchor summary before trigger")) return false;

    engine.shutdown();
    std::cout << "t1_v2 self-test passed" << std::endl;
    return true;
}

}  // namespace t1_v2
