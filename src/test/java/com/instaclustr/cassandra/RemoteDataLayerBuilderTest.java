package com.instaclustr.cassandra;/*
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

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Future;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class RemoteDataLayerBuilderTest
{
    @Test
    @Disabled
    public void startSidecar()
    {
        Path sidecarConfig = Paths.get("src/test/resources/sidecar-test.yaml");
        Injector injector = Guice.createInjector(SidecarModules.all(sidecarConfig));
        Server sidecarServer = injector.getInstance(Server.class);
        Future<String> start = sidecarServer.start();
        start.onSuccess(s -> System.out.println(s + " started"));
        sidecarServer.stop(start.result());
    }

    @Test
    @Disabled
    public void testLayer() throws Exception
    {
        Map<String, String> client = Map.of("sidecar_contact_points", "spark-master-1:9043",
                                            "keyspace", "spark_test",
                                            "table", "test",
                                            "createsnapshot", "true");
        Map<String, String> sidecarClient = Map.of();
        Map<String, String> ssl = Map.of();
        List<RemoteDataLayerWrapper> build = new RemoteDataLayersBuilder(client, sidecarClient, ssl, "/tmp/file", -1, 1).build();
        CassandraDataLayer dataLayer = (CassandraDataLayer) build.get(0).getDataLayer();
        CassandraRing ring = dataLayer.ring();
        int partitionCount = dataLayer.partitionCount();
    }
}
