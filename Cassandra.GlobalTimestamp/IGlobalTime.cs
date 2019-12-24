using JetBrains.Annotations;

using SkbKontur.Cassandra.TimeBasedUuid;

namespace SkbKontur.Cassandra.GlobalTimestamp
{
    [PublicAPI]
    public interface IGlobalTime
    {
        [NotNull]
        Timestamp UpdateNowTimestamp();

        void ResetInMemoryState();
    }
}