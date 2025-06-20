package com.instaclustr.cassandra;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterIntegrationTestBase;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteTransformationTest.class);

    private final List<Server> sidecarServerList = new ArrayList<>();

    @Test
    public void testRemoteTransformationOfPartition()
    {
        SparkSession orCreateSparkSession = getOrCreateSparkSession();
        logger.info("DOING WORK");
        orCreateSparkSession.close();
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
        return super.testClusterConfiguration().nodesPerDc(3);
    }
}
