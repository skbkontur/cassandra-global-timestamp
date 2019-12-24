using System;
using System.Collections.Concurrent;

using GroBuf;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Connections;

namespace SkbKontur.Cassandra.GlobalTimestamp
{
    [PublicAPI]
    public class MaxTicksHolder
    {
        public MaxTicksHolder(ISerializer serializer, IColumnFamilyConnection cfConnection)
        {
            this.serializer = serializer;
            this.cfConnection = cfConnection;
        }

        public long? GetMaxTicks([NotNull] string key)
        {
            if (!cfConnection.TryGetColumn(key, ticksColumnName, out var column))
                return null;
            return serializer.Deserialize<long>(column.Value);
        }

        public void UpdateMaxTicks([NotNull] string key, long ticks)
        {
            if (persistedMaxTicks.TryGetValue(key, out var maxTicks) && ticks <= maxTicks)
                return;
            cfConnection.AddColumn(key, new Column
                {
                    Name = ticksColumnName,
                    Timestamp = ticks,
                    Value = serializer.Serialize(ticks),
                    TTL = null
                });
            persistedMaxTicks.AddOrUpdate(key, ticks, (k, oldMaxTicks) => Math.Max(ticks, oldMaxTicks));
        }

        public void ResetInMemoryState()
        {
            persistedMaxTicks.Clear();
        }

        private const string ticksColumnName = "Ticks";
        private readonly ISerializer serializer;
        private readonly IColumnFamilyConnection cfConnection;
        private readonly ConcurrentDictionary<string, long> persistedMaxTicks = new ConcurrentDictionary<string, long>();
    }
}