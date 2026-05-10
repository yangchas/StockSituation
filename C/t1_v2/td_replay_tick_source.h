#pragma once

#include <vector>

#include "config_v2.h"
#include "raw_tick_converter.h"
#include "replay_slice_scheduler.h"
#include "td_replay_query.h"
#include "tick_source.h"

#if defined(T1_V2_ENABLE_TDENGINE)
#include <taos.h>
#endif

namespace t1_v2 {

class TdReplayTickSource final : public ITickSource {
public:
    explicit TdReplayTickSource(const ConfigV2& config);
    ~TdReplayTickSource() override = default;

    bool start() override;
    void stop() override;
    TickSourceResult next_batch() override;
    RuntimeMode mode() const override { return RuntimeMode::Replay; }

private:
    bool connect();
    void disconnect();
    TickSourceResult query_next_slice();
    TickSourceResult emit_pending_batch();
    void throttle_before_next_slice();

    ConfigV2 config_;
    ReplaySliceScheduler scheduler_;
    TdReplayQueryBuilder query_builder_;
    uint32_t seq_no_ = 0;
    uint32_t slice_no_ = 0;
    bool started_ = false;
    std::string last_error_;
    std::vector<SourceTickRecord> pending_records_;
    int64_t pending_logical_ts_ms_ = 0;
    uint32_t pending_raw_input_count_ = 0;
    uint32_t pending_total_accepted_count_ = 0;

#if defined(T1_V2_ENABLE_TDENGINE)
    TAOS* conn_ = nullptr;
#endif
};

}  // namespace t1_v2
