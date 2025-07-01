package com.instaclustr.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.instaclustr.cassandra.TransformerOptions.TransformationStrategy.ONE_FILE_ALL_SSTABLES;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

public class RemoteTransformationTest extends SharedClusterSparkIntegrationTestBase
{
    static
    {
        System.setProperty("cassandra.sidecar.versions_to_test", "4.0");
        System.setProperty("cassandra.test.dtest_jar_path", "dtest-jars");
        System.setProperty("cassandra.integration.sidecar.test.enable_mtls", "false");
        System.setProperty("SKIP_STARTUP_VALIDATIONS", "true");
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteTransformationTest.class);

    private final List<Server> sidecarServerList = new ArrayList<>();

    @Test
    public void testRemoteTransformationOfPartition(@TempDir Path tmpDir)
    {
        TransformerOptions options = new TransformerOptions();
        options.sidecar = List.of("127.0.0.1:" + server.actualPort());
        options.keyspace = TEST_KEYSPACE;
        options.table = TEST_TABLE_PREFIX;
        options.transformationStrategy = ONE_FILE_ALL_SSTABLES;
        options.output = tmpDir.toAbsolutePath().toString();
        options.keepSnapshot = true;
        SSTableToParquetTransformer transformer = new SSTableToParquetTransformer(options);
        List<? extends AbstractOutputFile<?>> outputFiles = transformer.runTransformation();
        System.out.println(outputFiles);
    }

    @Test
    public void testRemoteTransformationOfRing()
    {
    }

    @Test
    public void testRemoteTransformationSortedOutput()
    {

    }

    @Test
    public void testRemoteTransformationMaxNumberOfRows()
    {

    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(new QualifiedName(TEST_KEYSPACE, TEST_TABLE_PREFIX),
                        "CREATE TABLE %s (id int primary key)");

        try (Cluster cluster = createDriverCluster(super.cluster))
        {
            Session session = cluster.connect();
            for (int i = 0; i < 10; i++)
            {
                session.execute(String.format("INSERT INTO %s.%s (id) values (%s)", TEST_KEYSPACE, TEST_TABLE_PREFIX, i));
            }
        }
    }

    @Override
    protected void startSidecar(ICluster<? extends IInstance> cluster) throws InterruptedException
    {
        for (IInstance instance : cluster)
        {
            LOGGER.info("Starting Sidecar instance for Cassandra instance {}",
                        instance.config().num());
            Server server = startSidecarWithInstances(Collections.singleton(instance));
            sidecarServerList.add(server);
        }

        assertThat(sidecarServerList.size())
                .as("Each Cassandra Instance will be managed by a single Sidecar instance")
                .isEqualTo(cluster.size());

        // assign the server to the first instance
        server = sidecarServerList.get(0);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                .tokenCount(16)
                .nodesPerDc(1);
    }
}
