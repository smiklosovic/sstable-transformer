/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.instaclustr.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.vertx.core.http.HttpMethod;
import org.apache.cassandra.analytics.SharedClusterSparkIntegrationTestBase;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SchemaReportingConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.jetbrains.annotations.NotNull;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static com.instaclustr.cassandra.TransformationVerifier.verify;
import static com.instaclustr.cassandra.TransformerOptions.OutputFormat.AVRO;
import static com.instaclustr.cassandra.TransformerOptions.OutputFormat.PARQUET;
import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

@Ignore
public abstract class AbstractTransformationTest extends SharedClusterSparkIntegrationTestBase
{
    static
    {
        System.setProperty("cassandra.sidecar.versions_to_test", "4.1");
        System.setProperty("cassandra.test.dtest_jar_path", "dtest-jars");
        System.setProperty("cassandra.integration.sidecar.test.enable_mtls", "false");
        System.setProperty("SKIP_STARTUP_VALIDATIONS", "true");
    }

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractTransformationTest.class);

    protected final List<Server> sidecarServerList = new ArrayList<>();

    @Test
    public void testTransformation(@TempDir Path outputDir) throws Exception
    {
        TransformerOptions tmpOpts = getRemoteOptions(outputDir);
        RemoteDataLayerWrapper wrapper = new ArrayList<>(DataLayerWrapperBuilders.getRemoteDataLayers(tmpOpts)).get(0);
        DataLayer dataLayer = wrapper.getDataLayer();
        int i = dataLayer.partitionCount();
        String partitions = "0.." + (i - 1);

        TransformerOptions options = getOptions(outputDir);
        options.partitions = partitions;
        // unsorted
        options.sorted = false;
        verify(10_000, new SSTableToParquetTransformer(options).runTransformation(), tmpOpts);

        // sorted
        options.sorted = true;
        verify(10_000, new SSTableToParquetTransformer(options).runTransformation(), tmpOpts);
    }

    @Test
    public void testParquetCompressors(@TempDir Path outputDir) throws Exception
    {
        TransformerOptions options = getOptions(outputDir);
        options.outputFormat = PARQUET;

        for (TransformerOptions.Compression compression : TransformerOptions.Compression.getAllForParquet())
        {
            options.compression = compression;
            verify(10_000, new SSTableToParquetTransformer(options).runTransformation(), options);
        }
    }

    @Test
    public void testAvroCompressors(@TempDir Path outputDir) throws Exception
    {
        TransformerOptions options = getOptions(outputDir);
        options.outputFormat = AVRO;

        for (TransformerOptions.Compression compression : TransformerOptions.Compression.getAllForAvro())
        {
            options.compression = compression;
            verify(10_000, new SSTableToParquetTransformer(options).runTransformation(), options);
        }
    }

    @Test
    public void testMaxNumberOfRows(@TempDir Path outputDir) throws Exception
    {
        TransformerOptions options = getOptions(outputDir);
        options.maxRowsPerFile = 100;

        options.sorted = false;
        verify(10_000, new SSTableToParquetTransformer(options).runTransformation(), options);

        options.sorted = true;
        verify(10_000, new SSTableToParquetTransformer(options).runTransformation(), options);
    }

    @Override
    protected void afterClusterProvisioned()
    {
        cluster.get(1).nodetoolResult("disableautocompaction").asserts().success();
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
            for (int i = 0; i < 10_000; i++)
            {
                if (i % 1000 == 0)
                    super.cluster.get(1).flush(TEST_KEYSPACE);

                session.execute(String.format("INSERT INTO %s.%s (id) values (%s)", TEST_KEYSPACE, TEST_TABLE_PREFIX, i));
            }
        }

        cluster.get(1).flush(TEST_KEYSPACE);
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

    @Override
    protected Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides()
    {
        return builder ->
        {
            builder.schemaReportingConfiguration(new SchemaReportingConfiguration()
            {
                @Override
                public String endpoint()
                {
                    return null;
                }

                @Override
                public HttpMethod method()
                {
                    return null;
                }

                @Override
                public int retries()
                {
                    return 0;
                }

                @Override
                public boolean enabled()
                {
                    return false;
                }

                @Override
                public @NotNull MillisecondBoundConfiguration initialDelay()
                {
                    return new MillisecondBoundConfiguration("1s");
                }

                @Override
                public @NotNull MillisecondBoundConfiguration executeInterval()
                {
                    return new MillisecondBoundConfiguration("1s");
                }
            });
            return builder;
        };
    }

    public abstract TransformerOptions getOptions(Path outPutDir);

    public TransformerOptions getRemoteOptions(Path outPutDir)
    {
        TransformerOptions options = new TransformerOptions();
        options.sidecar = List.of("localhost:" + server.actualPort());
        options.keyspace = TEST_KEYSPACE;
        options.table = TEST_TABLE_PREFIX;
        options.transformationStrategy = TransformerOptions.TransformationStrategy.ONE_FILE_ALL_SSTABLES;
        options.output = outPutDir.toAbsolutePath().toString();
        options.keepSnapshot = true;

        return options;
    }
}
