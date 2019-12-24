using System;
using System.Collections.Concurrent;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Connections;

namespace SkbKontur.Cassandra.GlobalTimestamp
{
    [PublicAPI]
    public class MinTicksHolder
    {
        public MinTicksHolder(IColumnFamilyConnection minTicksConnection)
        {
            this.minTicksConnection = minTicksConnection;
        }

        public long? GetMinTicks([NotNull] string key)
        {
            if (!minTicksConnection.TryGetColumn(key, ticksColumnName, out var column))
                return null;
            return Serializer.Deserialize(column.Value);
        }

        public void UpdateMinTicks([NotNull] string key, long ticks)
        {
            if (persistedMinTicks.TryGetValue(key, out var minTicks) && ticks >= minTicks)
                return;
            minTicksConnection.AddColumn(key, new Column
                {
                    Name = ticksColumnName,
                    Timestamp = long.MaxValue - ticks,
                    Value = Serializer.Serialize(ticks),
                    TTL = null
                });
            persistedMinTicks.AddOrUpdate(key, ticks, (k, oldMinTicks) => Math.Min(ticks, oldMinTicks));
        }

        public void ResetInMemoryState()
        {
            persistedMinTicks.Clear();
        }

        private const string ticksColumnName = "ticks";
        private readonly IColumnFamilyConnection minTicksConnection;
        private readonly ConcurrentDictionary<string, long> persistedMinTicks = new ConcurrentDictionary<string, long>();
    }
}