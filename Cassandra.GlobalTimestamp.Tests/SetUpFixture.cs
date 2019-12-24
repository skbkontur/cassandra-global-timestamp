using System;
using System.IO;

using NUnit.Framework;

using SkbKontur.Cassandra.Local;
using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Connections;
using SkbKontur.Cassandra.ThriftClient.Scheme;

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
            var templateDirectory = Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"v3.11.x");
            var deployDirectory = Path.Combine(Path.GetTempPath(), "deployed_cassandra_v3.11.x");
            cassandraNode = new LocalCassandraNode(templateDirectory, deployDirectory)
                {
                    RpcPort = 9360,
                    CqlPort = 9343,
                    JmxPort = 7399,
                    GossipPort = 7400,
                };
            cassandraNode.Restart(timeout : TimeSpan.FromMinutes(1));

            cassandraCluster = new CassandraCluster(cassandraNode.CreateSettings(), Logger.Instance);

            cassandraCluster.ActualizeKeyspaces(new[]
                {
                    new KeyspaceScheme
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
                });
        }

        [OneTimeTearDown]
        public static void TearDown()
        {
            cassandraNode.Stop();
        }

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
                throw new InvalidOperationException("Failed to find cassandra templates directory");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        private const string ksName = "GlobalTimestampTests";
        private const string minTicksCfName = "MinTicks";
        private const string maxTicksCfName = "MaxTicks";
        private const string cassandraTemplates = @"cassandra-local\cassandra";

        private static LocalCassandraNode cassandraNode;
        private static CassandraCluster cassandraCluster;
    }
}