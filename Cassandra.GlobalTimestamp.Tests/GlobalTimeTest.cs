using System;

using NUnit.Framework;

using SkbKontur.Cassandra.GlobalTimestamp;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace Cassandra.GlobalTimestamp.Tests
{
    public class GlobalTimeTest
    {
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            globalTime = new GlobalTime(SetUpFixture.GetMaxTicksConnection());
        }

        [SetUp]
        public void SetUp()
        {
            globalTime.ResetInMemoryState();
        }

        [Test]
        public void UpdateNowTimestamp()
        {
            Timestamp lastTimestamp = null;
            for (var i = 0; i < 1000; i++)
            {
                var nowTimestamp = globalTime.UpdateNowTimestamp();
                Assert.That(lastTimestamp <= nowTimestamp + TimeSpan.FromTicks(PreciseTimestampGenerator.TicksPerMicrosecond));
                lastTimestamp = nowTimestamp;
            }
        }

        private GlobalTime globalTime;
    }
}