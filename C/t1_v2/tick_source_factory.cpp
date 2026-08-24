#include "tick_source_factory.h"

#include "rabbitmq_tick_source.h"
#include "td_replay_tick_source.h"
#include "tickpack_tick_source.h"

namespace t1_v2 {

std::unique_ptr<ITickSource> TickSourceFactory::create(const ConfigV2& config) {
    if (config.runtime_mode == RuntimeMode::Replay) {
        if (!config.replay.tickpack_path.empty()) {
            return std::make_unique<TickPackTickSource>(config);
        }
        return std::make_unique<TdReplayTickSource>(config);
    }
    return std::make_unique<RabbitMqTickSource>(config);
}

}  // namespace t1_v2
