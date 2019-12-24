using System;
using System.Collections.Concurrent;

using GroBuf;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Connections;

namespace SkbKontur.Cassandra.GlobalTimestamp
{
    [PublicAPI]
    public class MinTicksHolder
    {
        public MinTicksHolder(ISerializer serializer, IColumnFamilyConnection minTicksConnection)
        {
            this.serializer = serializer;
            this.minTicksConnection = minTicksConnection;
        }

        public long? GetMinTicks([NotNull] string key)
        {
            if (!minTicksConnection.TryGetColumn(key, ticksColumnName, out var column))
                return null;
            return long.MaxValue - serializer.Deserialize<long>(column.Value);
        }

        public void UpdateMinTicks([NotNull] string key, long ticks)
        {
            if (persistedMinTicks.TryGetValue(key, out var minTicks) && ticks >= minTicks)
                return;
            minTicksConnection.AddColumn(key, new Column
                {
                    Name = ticksColumnName,
                    Timestamp = long.MaxValue - ticks,
                    Value = serializer.Serialize(long.MaxValue - ticks),
                    TTL = null
                });
            persistedMinTicks.AddOrUpdate(key, ticks, (k, oldMinTicks) => Math.Min(ticks, oldMinTicks));
        }

        public void ResetInMemoryState()
        {
            persistedMinTicks.Clear();
        }

        private const string ticksColumnName = "Ticks";
        private readonly ISerializer serializer;
        private readonly IColumnFamilyConnection minTicksConnection;
        private readonly ConcurrentDictionary<string, long> persistedMinTicks = new ConcurrentDictionary<string, long>();
    }
}