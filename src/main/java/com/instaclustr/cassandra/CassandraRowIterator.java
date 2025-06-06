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

import org.apache.avro.Schema;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.cassandra.spark.sparksql.SparkRowIterator;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Transforms stream of rows from Cassandra by {@link SparkRowIterator}.
 * <p>
 * As rows are being transformed and written to a respective file, if that file is considered to be full,
 * it will take care of switching the writer to the next output file.
 */
public class CassandraRowIterator implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraRowIterator.class);

    private final DataLayerWrapper dataLayerWrapper;
    private final DataLayer dataLayer;
    private SparkRowConsumer sparkRowConsumer;
    private final StructType structType;
    private final Schema avroSchema;
    private final TransformerOptions options;
    private final long maxRowsPerFile;

    public CassandraRowIterator(DataLayerWrapper dataLayerWrapper, TransformerOptions options)
    {
        this.dataLayerWrapper = dataLayerWrapper;
        this.dataLayer = dataLayerWrapper.getDataLayer();
        this.structType = dataLayer.structType();
        this.options = options;
        this.avroSchema = SchemaConverters.toAvroType(structType,
                                                      false,
                                                      "root",
                                                      SSTableToParquetTransformer.class.getCanonicalName());

        this.maxRowsPerFile = dataLayerWrapper.getMaxRowsPerParquetFile();
        this.sparkRowConsumer = switchConsumer(dataLayerWrapper.currentDestination());
    }

    public Collection<? extends AbstractOutputFile<?>> readAllRows()
    {
        List<AbstractOutputFile<?>> outputFiles = new ArrayList<>();
        outputFiles.add(dataLayerWrapper.currentDestination());

        try (SparkRowIterator iterator = new SparkRowIterator(dataLayerWrapper.getPartition(),
                                                              dataLayer,
                                                              structType,
                                                              Collections.emptyList()))
        {
            int count = 0;
            long start = System.currentTimeMillis();
            while (iterator.next())
            {
                if (count == maxRowsPerFile)
                {
                    start = printDuration(dataLayerWrapper.currentDestination(),
                                          count,
                                          start,
                                          System.currentTimeMillis());

                    AbstractOutputFile<?> nextDestination = dataLayerWrapper.getNextDestination();
                    sparkRowConsumer = switchConsumer(nextDestination);
                    outputFiles.add(nextDestination);
                    count = 0;
                }

                sparkRowConsumer.accept(iterator.get());
                count++;
            }

            if (count != 0)
                printDuration(dataLayerWrapper.currentDestination(), count, start, System.currentTimeMillis());

        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
        finally
        {
            try
            {
                if (sparkRowConsumer != null)
                    sparkRowConsumer.close();
            }
            catch (Exception ex)
            {
                throw new RuntimeException("Unable to close row consumer", ex);
            }
        }

        return outputFiles;
    }

    private long printDuration(AbstractOutputFile<?> outputFile, int count, long start, long end)
    {
        logger.info("Transformed {} rows to {} in {} seconds.",
                    count,
                    outputFile.getPath(),
                    (end - start) / 1000);

        // new start
        return System.currentTimeMillis();
    }

    private SparkRowConsumer switchConsumer(AbstractOutputFile<?> destination)
    {
        if (!destination.canWrite())
            throw new IllegalStateException("Can not write to " + destination.getPath());

        try
        {
            if (sparkRowConsumer != null)
                sparkRowConsumer.close();

            return new SparkRowConsumer(dataLayer.structType(),
                                        avroSchema,
                                        destination,
                                        options);
        } catch (Throwable t)
        {
            throw new TransformerException("Error while closing current Spark row consumer", t);
        }
    }

    @Override
    public void close() throws Exception
    {
        sparkRowConsumer.close();
    }
}
