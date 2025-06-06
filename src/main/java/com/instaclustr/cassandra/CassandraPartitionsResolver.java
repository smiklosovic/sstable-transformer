package com.instaclustr.cassandra;

import com.google.common.collect.ImmutableMap;
import o.a.c.sidecar.client.shaded.common.response.RingResponse;
import o.a.c.sidecar.client.shaded.io.vertx.core.Vertx;
import o.a.c.sidecar.client.shaded.io.vertx.core.VertxOptions;
import org.apache.cassandra.sidecar.client.HttpClientConfig;
import org.apache.cassandra.sidecar.client.SidecarClient;
import org.apache.cassandra.sidecar.client.SidecarClientConfig;
import org.apache.cassandra.sidecar.client.SidecarClientConfigImpl;
import org.apache.cassandra.sidecar.client.SidecarInstanceImpl;
import org.apache.cassandra.sidecar.client.SimpleSidecarInstancesProvider;
import org.apache.cassandra.sidecar.client.VertxHttpClient;
import org.apache.cassandra.sidecar.client.VertxRequestExecutor;
import org.apache.cassandra.sidecar.client.retry.ExponentialBackoffRetryPolicy;
import org.apache.cassandra.sidecar.client.retry.RetryPolicy;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Mixin;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Command(name = "partitions",
        description = "Realize how many Spark partitions your Cassandra ring consists of.",
        versionProvider = Transformer.class,
        subcommands = HelpCommand.class)
public class CassandraPartitionsResolver implements Runnable
{
    @Mixin
    public PartitionResolverOptions options;

    public String datacenter;
    public String keyspace;
    public String sidecar;

    // for picocli
    public CassandraPartitionsResolver()
    {
    }

    public CassandraPartitionsResolver(String datacenter, String keyspace, String sidecar)
    {
        this.datacenter = datacenter;
        this.keyspace = keyspace;
        this.sidecar = sidecar;
    }

    @Override
    public void run()
    {
        if (options != null)
        {
            this.datacenter = options.dc;
            this.keyspace = options.keyspace;
            this.sidecar = options.sidecar;
        }

        String collect = Arrays.stream(partitions(datacenter, keyspace, sidecar.split(":")[0], Integer.parseInt(sidecar.split(":")[1])))
                .boxed()
                .map(Object::toString)
                .collect(Collectors.joining(","));
        System.out.println(collect);
    }

    public static int[] partitions(String datacenter, String keyspace, String hostname, int port)
    {
        try (SidecarClient client = getSidecar(hostname, port))
        {
            RingResponse ring = client.ring(keyspace).get();

            Collection<CassandraInstance> instances = ring
                    .stream()
                    .filter(status -> datacenter == null || datacenter.equalsIgnoreCase(status.datacenter()))
                    .map(status -> new CassandraInstance(status.token(), status.fqdn(), status.datacenter()))
                    .collect(Collectors.toList());

            CassandraRing cassandraRing = new CassandraRing(Partitioner.Murmur3Partitioner,
                                                            keyspace,
                                                            new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                                                  ImmutableMap.of("dc1", 3)),
                                                            instances);

            TokenPartitioner tokenPartitioner = new TokenPartitioner(cassandraRing, 1, 1);
            return IntStream.rangeClosed(0, tokenPartitioner.numPartitions() - 1).toArray();
        } catch (Throwable t)
        {
            throw new RuntimeException("Unable to get partitions.");
        }
    }

    public static SidecarClient getSidecar(String hostname, int port)
    {
        Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(16));

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder<>()
                .ssl(false)
                .timeoutMillis(10_000)
                .idleTimeoutMillis(30_000)
                .receiveBufferSize(8196)
                .maxChunkSize(4092)
                .userAgent("sample-agent/0.0.1")
                .build();

        SidecarClientConfig clientConfig = SidecarClientConfigImpl.builder()
                .maxRetries(10)
                .retryDelayMillis(200)
                .maxRetryDelayMillis(10_000)
                .build();

        RetryPolicy defaultRetryPolicy = new ExponentialBackoffRetryPolicy(clientConfig.maxRetries(),
                                                                           clientConfig.retryDelayMillis(),
                                                                           clientConfig.maxRetryDelayMillis());

        VertxHttpClient vertxHttpClient = new VertxHttpClient(vertx, httpClientConfig);
        VertxRequestExecutor requestExecutor = new VertxRequestExecutor(vertxHttpClient);
        SimpleSidecarInstancesProvider instancesProvider = new SimpleSidecarInstancesProvider(List.of(new SidecarInstanceImpl(hostname, port)));
        return new SidecarClient(instancesProvider, requestExecutor, clientConfig, defaultRetryPolicy);
    }
}
