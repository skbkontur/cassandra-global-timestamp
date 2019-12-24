using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using MoreLinq;

using NUnit.Framework;

using SkbKontur.Cassandra.GlobalTimestamp;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace Cassandra.GlobalTimestamp.Tests
{
    public class MinMaxTicksHolderTest
    {
        [OneTimeSetUp]
        public void OneTimeSetUp()
        {
            var minTicksConnection = SetUpFixture.GetMinTicksConnection();
            minTicksHolder1 = new MinTicksHolder(minTicksConnection);
            minTicksHolder2 = new MinTicksHolder(minTicksConnection);

            var maxTicksConnection = SetUpFixture.GetMaxTicksConnection();
            maxTicksHolder1 = new MaxTicksHolder(maxTicksConnection);
            maxTicksHolder2 = new MaxTicksHolder(maxTicksConnection);
        }

        [SetUp]
        public void SetUp()
        {
            minTicksHolder1.ResetInMemoryState();
            minTicksHolder2.ResetInMemoryState();
            maxTicksHolder1.ResetInMemoryState();
            maxTicksHolder2.ResetInMemoryState();
        }

        [Test]
        public void MinTicks()
        {
            var ticks = Timestamp.Now.Ticks;
            var otherKey = Guid.NewGuid().ToString();
            minTicksHolder1.UpdateMinTicks(otherKey, ticks - TimeSpan.FromHours(1).Ticks);

            var key = Guid.NewGuid().ToString();
            Assert.That(minTicksHolder1.GetMinTicks(key), Is.Null);
            Assert.That(minTicksHolder2.GetMinTicks(key), Is.Null);
            Assert.That(minTicksHolder1.UpdateAndGetMinTicks(key, ticks), Is.EqualTo(ticks));
            Assert.That(minTicksHolder2.UpdateAndGetMinTicks(key, ticks - 2), Is.EqualTo(ticks - 2));
            Assert.That(minTicksHolder1.UpdateAndGetMinTicks(key, ticks - 1), Is.EqualTo(ticks - 2));
            Assert.That(minTicksHolder1.GetMinTicks(key), Is.EqualTo(ticks - 2));
            Assert.That(minTicksHolder2.GetMinTicks(key), Is.EqualTo(ticks - 2));
        }

        [Test]
        public void MaxTicks()
        {
            var ticks = Timestamp.Now.Ticks;
            var otherKey = Guid.NewGuid().ToString();
            maxTicksHolder1.UpdateMaxTicks(otherKey, ticks + TimeSpan.FromHours(1).Ticks);

            var key = Guid.NewGuid().ToString();
            Assert.That(maxTicksHolder1.GetMaxTicks(key), Is.Null);
            Assert.That(maxTicksHolder2.GetMaxTicks(key), Is.Null);
            Assert.That(maxTicksHolder1.UpdateAndGetMaxTicks(key, ticks), Is.EqualTo(ticks));
            Assert.That(maxTicksHolder2.UpdateAndGetMaxTicks(key, ticks + 2), Is.EqualTo(ticks + 2));
            Assert.That(maxTicksHolder1.UpdateAndGetMaxTicks(key, ticks + 1), Is.EqualTo(ticks + 2));
            Assert.That(maxTicksHolder1.GetMaxTicks(key), Is.EqualTo(ticks + 2));
            Assert.That(maxTicksHolder2.GetMaxTicks(key), Is.EqualTo(ticks + 2));
        }

        [Test]
        public void ConcurrentUpdates()
        {
            var key = Guid.NewGuid().ToString();
            const int threadsCount = 8;
            const int countPerThread = 1000 * 1000;
            const int valuesCount = threadsCount * countPerThread;
            var values = Enumerable.Range(0, valuesCount).Select(x => ThreadLocalRandom.Instance.Next(valuesCount)).ToList();
            var valuesByThread = values.Batch(countPerThread, Enumerable.ToArray).ToArray();
            var threads = new List<Thread>();
            var startSignal = new ManualResetEvent(initialState : false);
            for (var i = 0; i < threadsCount; i++)
            {
                var threadIndex = i;
                var thread = new Thread(() =>
                    {
                        startSignal.WaitOne();
                        var minTicksHolder = threadIndex % 2 == 0 ? minTicksHolder1 : minTicksHolder2;
                        var maxTicksHolder = threadIndex % 2 == 0 ? maxTicksHolder1 : maxTicksHolder2;
                        foreach (var value in valuesByThread[threadIndex])
                        {
                            minTicksHolder.UpdateMinTicks(key, value);
                            maxTicksHolder.UpdateMaxTicks(key, value);
                        }
                    });
                thread.Start();
                threads.Add(thread);
            }
            startSignal.Set();
            threads.ForEach(thread => thread.Join());
            Assert.That(minTicksHolder1.GetMinTicks(key), Is.EqualTo(values.Min()));
            Assert.That(minTicksHolder2.GetMinTicks(key), Is.EqualTo(values.Min()));
            Assert.That(maxTicksHolder1.GetMaxTicks(key), Is.EqualTo(values.Max()));
            Assert.That(maxTicksHolder2.GetMaxTicks(key), Is.EqualTo(values.Max()));
        }

        private MinTicksHolder minTicksHolder1, minTicksHolder2;
        private MaxTicksHolder maxTicksHolder1, maxTicksHolder2;
    }
}