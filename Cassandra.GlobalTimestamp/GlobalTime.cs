using System;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Connections;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace SkbKontur.Cassandra.GlobalTimestamp
{
    [PublicAPI]
    public class GlobalTime : IGlobalTime
    {
        public GlobalTime(IColumnFamilyConnection cfConnection)
        {
            maxTicksHolder = new MaxTicksHolder(cfConnection);
        }

        [NotNull]
        public Timestamp UpdateNowTimestamp()
        {
            var prevGlobalTicks = maxTicksHolder.GetMaxTicks(globalTicksKey) ?? 0;
            var newGlobalTicks = Math.Max(prevGlobalTicks + PreciseTimestampGenerator.TicksPerMicrosecond, Timestamp.Now.Ticks);
            maxTicksHolder.UpdateMaxTicks(globalTicksKey, newGlobalTicks);
            return new Timestamp(newGlobalTicks);
        }

        public void ResetInMemoryState()
        {
            maxTicksHolder.ResetInMemoryState();
        }

        private const string globalTicksKey = "global_ticks";
        private readonly MaxTicksHolder maxTicksHolder;
    }
}