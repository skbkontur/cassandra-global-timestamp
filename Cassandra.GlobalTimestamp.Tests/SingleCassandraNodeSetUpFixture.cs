using System;
using System.IO;

using NUnit.Framework;

using SkbKontur.Cassandra.Local;

namespace Cassandra.GlobalTimestamp.Tests
{
    [SetUpFixture]
    public class SingleCassandraNodeSetUpFixture
    {
        internal static LocalCassandraNode Node { get; private set; }

        [OneTimeSetUp]
        public static void SetUp()
        {
            var templateDirectory = Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"v3.11.x");
            var deployDirectory = Path.Combine(Path.GetTempPath(), "deployed_cassandra_v3.11.x");
            Node = new LocalCassandraNode(templateDirectory, deployDirectory)
                {
                    RpcPort = 9360,
                    CqlPort = 9343,
                    JmxPort = 7399,
                    GossipPort = 7400,
                };
            Node.Restart(timeout : TimeSpan.FromMinutes(1));
        }

        [OneTimeTearDown]
        public static void TearDown()
        {
            Node.Stop();
        }

        private static string FindCassandraTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
                throw new InvalidOperationException("Failed to find cassandra templates directory");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        private const string cassandraTemplates = @"cassandra-local\cassandra";
    }
}