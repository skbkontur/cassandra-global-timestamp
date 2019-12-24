using System;
using System.Collections.Concurrent;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Connections;

namespace SkbKontur.Cassandra.GlobalTimestamp
{
    [PublicAPI]
    public class MaxTicksHolder
    {
        public MaxTicksHolder(IColumnFamilyConnection cfConnection)
        {
            this.cfConnection = cfConnection;
        }

        public long? GetMaxTicks([NotNull] string key)
        {
            if (!cfConnection.TryGetColumn(key, ticksColumnName, out var column))
                return null;
            return Serializer.Deserialize(column.Value);
        }

        public void UpdateMaxTicks([NotNull] string key, long ticks)
        {
            if (persistedMaxTicks.TryGetValue(key, out var maxTicks) && ticks <= maxTicks)
                return;
            cfConnection.AddColumn(key, new Column
                {
                    Name = ticksColumnName,
                    Timestamp = ticks,
                    Value = Serializer.Serialize(ticks),
                    TTL = null
                });
            persistedMaxTicks.AddOrUpdate(key, ticks, (k, oldMaxTicks) => Math.Max(ticks, oldMaxTicks));
        }

        public void ResetInMemoryState()
        {
            persistedMaxTicks.Clear();
        }

        private const string ticksColumnName = "ticks";
        private readonly IColumnFamilyConnection cfConnection;
        private readonly ConcurrentDictionary<string, long> persistedMaxTicks = new ConcurrentDictionary<string, long>();
    }
}