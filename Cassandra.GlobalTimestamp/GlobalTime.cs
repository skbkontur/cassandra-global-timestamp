using System;

using GroBuf;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Connections;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace SkbKontur.Cassandra.GlobalTimestamp
{
    [PublicAPI]
    public class GlobalTime : IGlobalTime
    {
        public GlobalTime(ISerializer serializer, IColumnFamilyConnection cfConnection)
        {
            maxTicksHolder = new MaxTicksHolder(serializer, cfConnection);
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

        private const string globalTicksKey = "GlobalTicks";
        private readonly MaxTicksHolder maxTicksHolder;
    }
}