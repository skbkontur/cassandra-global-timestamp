using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Connections;
using SkbKontur.Cassandra.ThriftClient.Schema;

namespace Cassandra.GlobalTimestamp.Tests
{
    [SetUpFixture]
    public class SetUpFixture
    {
        public static IColumnFamilyConnection GetMinTicksConnection()
        {
            return cassandraCluster.RetrieveColumnFamilyConnection(ksName, minTicksCfName);
        }

        public static IColumnFamilyConnection GetMaxTicksConnection()
        {
            return cassandraCluster.RetrieveColumnFamilyConnection(ksName, maxTicksCfName);
        }

        [OneTimeSetUp]
        public static void SetUp()
        {
            cassandraCluster = new CassandraCluster(LocalCassandraSettingsFactory.CreateSettings(), Logger.Instance);
            var cassandraSchemaActualizer = new CassandraSchemaActualizer(cassandraCluster, null, Logger.Instance);
            cassandraSchemaActualizer.ActualizeKeyspaces(new[]
                {
                    new KeyspaceSchema
                        {
                            Name = ksName,
                            Configuration = new KeyspaceConfiguration
                                {
                                    ReplicationStrategy = SimpleReplicationStrategy.Create(replicationFactor : 1),
                                    ColumnFamilies = new[]
                                        {
                                            new ColumnFamily
                                                {
                                                    Name = minTicksCfName,
                                                    CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(sstableSizeInMb : 160),
                                                    Caching = ColumnFamilyCaching.All
                                                },
                                            new ColumnFamily
                                                {
                                                    Name = maxTicksCfName,
                                                    CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(sstableSizeInMb : 160),
                                                    Caching = ColumnFamilyCaching.KeysOnly
                                                }
                                        }
                                }
                        },
                }, changeExistingKeyspaceMetadata : false);
        }

        private const string ksName = "GlobalTimestampTests";
        private const string minTicksCfName = "MinTicks";
        private const string maxTicksCfName = "MaxTicks";

        private static CassandraCluster cassandraCluster;
    }
}