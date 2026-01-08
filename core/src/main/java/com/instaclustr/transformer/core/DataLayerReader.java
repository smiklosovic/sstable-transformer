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
package com.instaclustr.transformer.core;

import com.instaclustr.transformer.api.OutputFormat;
import org.apache.avro.Schema;
import org.apache.cassandra.spark.sparksql.SparkRowIterator;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.RowOrdering;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * Transforms stream of rows from Cassandra by {@link SparkRowIterator}.
 * <p>
 * As rows are being transformed and written to a respective file, if that file is considered to be full,
 * it will take care of switching the writer to the next output file.
 */
public class DataLayerReader
{
    private final DataLayerWrapper dataLayerWrapper;
    private final TransformerOptions options;

    public DataLayerReader(DataLayerWrapper dataLayerWrapper, TransformerOptions options)
    {
        this.dataLayerWrapper = dataLayerWrapper;
        this.options = options;
    }

    public List<Object> read()
    {
        try (RowsWriter rowsWriter = resolveRowsWriter(options))
        {
            rowsWriter.write();
            return rowsWriter.getResults();
        }
    }

    private FileBasedRowsWriter resolveRowsWriter(TransformerOptions options)
    {
        if (options.sorted)
            return new SortedRowsWriter(dataLayerWrapper, options);
        else
            return new UnsortedRowsWriter(dataLayerWrapper, options);
    }

    private static abstract class RowsWriter implements AutoCloseable
    {
        protected final DataLayerWrapper dataLayerWrapper;
        protected final TransformerOptions options;
        protected final StructType structType;
        protected GenericRowWriter rowWriter;

        public RowsWriter(DataLayerWrapper dataLayerWrapper, TransformerOptions options)
        {
            this.dataLayerWrapper = dataLayerWrapper;
            this.options = options;
            structType = this.dataLayerWrapper.getDataLayer().structType();
        }

        public void write()
        {
            try (SparkRowIterator iterator = new SparkRowIterator(dataLayerWrapper.getPartition(),
                                                                  dataLayerWrapper.getDataLayer(),
                                                                  dataLayerWrapper.getDataLayer().structType(),
                                                                  emptyList()))
            {
                // TODO record bude akceptovat AbstractFile a potom metoda na vysledky, genericka
                // TODO record(internalWrite(iterator)
                // TODO v internalWrite bude sink bud na subory alebo na byte array
                internalWrite(iterator);
            }
            catch (Throwable t)
            {
                throw new RuntimeException(t);
            }
            finally
            {
                close();
            }
        }

        @Override
        public void close()
        {
            if (rowWriter != null)
            {
                try
                {
                    rowWriter.close();
                    rowWriter = null;
                }
                catch (Throwable t)
                {
                    throw new TransformerException("Error while closing ParquetRowWriter.", t);
                }
            }
        }

        protected abstract void internalWrite(SparkRowIterator iterator) throws IOException;

        protected abstract void record(Object individualResult);

        public abstract List<Object> getResults();
    }

    private static abstract class FileBasedRowsWriter extends RowsWriter
    {
        private static final Logger logger = LoggerFactory.getLogger(FileBasedRowsWriter.class);

        private final List<AbstractInputOutputFile> outputFiles = new ArrayList<>();
        protected final long maxRowsPerFile;
        protected final Schema avroSchema;
        protected int count = 0;
        protected long start = currentTimeMillis();

        protected FileBasedRowsWriter(DataLayerWrapper dataLayerWrapper, TransformerOptions options)
        {
            super(dataLayerWrapper, options);

            record(dataLayerWrapper.currentDestination());
            avroSchema = SchemaConverters.toAvroType(structType,
                                                     false,
                                                     "root",
                                                     SSTableTransformer.class.getCanonicalName());

            switchWriter(dataLayerWrapper.currentDestination(), options.outputFormat);
            maxRowsPerFile = dataLayerWrapper.getMaxRowsPerParquetFile();
        }

        @Override
        protected void record(Object individualResult)
        {
            outputFiles.add((AbstractInputOutputFile) individualResult);
        }

        @Override
        public List<Object> getResults()
        {
            return new ArrayList<>(outputFiles);
        }

        protected long printDuration(AbstractInputOutputFile outputFile, int count, long start, long end)
        {
            logger.info("Transformed {} rows to {} in {} seconds.",
                        count,
                        outputFile.getPath(),
                        (end - start) / 1000);

            // new start
            return currentTimeMillis();
        }

        protected void switchWriter(AbstractInputOutputFile destination, OutputFormat outputFormat)
        {
            if (!destination.canWrite())
                throw new IllegalStateException("Can not write to " + destination.getPath());

            try
            {
                close();

                switch (outputFormat)
                {
                    case AVRO:
                        rowWriter = new AvroRowWriter(dataLayerWrapper.getDataLayer(),
                                                      avroSchema,
                                                      destination,
                                                      options);
                        break;
                    case PARQUET:
                        rowWriter = new ParquetRowWriter(dataLayerWrapper.getDataLayer(),
                                                         avroSchema,
                                                         destination,
                                                         options);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported output format " + outputFormat.name());
                }
            } catch (Throwable t)
            {
                throw new TransformerException("Error while closing current Spark row consumer", t);
            }
        }
    }

    private static class UnsortedRowsWriter extends FileBasedRowsWriter
    {
        public UnsortedRowsWriter(DataLayerWrapper dataLayerWrapper, TransformerOptions options)
        {
            super(dataLayerWrapper, options);
        }

        @Override
        protected void internalWrite(SparkRowIterator iterator) throws IOException
        {
            while (iterator.next())
            {
                if (count == maxRowsPerFile)
                {
                    start = printDuration(dataLayerWrapper.currentDestination(),
                                          count,
                                          start,
                                          currentTimeMillis());

                    dataLayerWrapper.currentDestination().setCount(count);
                    AbstractInputOutputFile nextDestination = dataLayerWrapper.getNextDestination();
                    switchWriter(nextDestination, options.outputFormat);
                    record(nextDestination);
                    count = 0;
                }

                rowWriter.accept(iterator.get());
                count++;
            }

            if (count != 0)
                printDuration(dataLayerWrapper.currentDestination(), count, start, currentTimeMillis());
        }
    }

    private static class SortedRowsWriter extends FileBasedRowsWriter
    {
        private final Comparator<InternalRow> rowComparator;
        private final InternalRow[] rowsBuffer;

        public SortedRowsWriter(DataLayerWrapper dataLayerWrapper, TransformerOptions options)
        {
            super(dataLayerWrapper, options);
            List<DataType> javaSchema = stream(structType.fields()).map(StructField::dataType).collect(toList());
            Seq<DataType> scalaSchema = JavaConverters.asScalaBuffer(javaSchema).toSeq();
            rowComparator = RowOrdering.createNaturalAscendingOrdering(scalaSchema);

            int arraySize = (int) maxRowsPerFile;
            if (arraySize == -1)
                arraySize = Integer.parseInt(System.getProperty("transformer.buffer.size", "10000000"));

            rowsBuffer = new InternalRow[arraySize];
        }

        @Override
        protected void internalWrite(SparkRowIterator iterator) throws IOException
        {
            boolean shouldSwitch = false;

            while (iterator.next())
            {
                if (shouldSwitch)
                {
                    record(dataLayerWrapper.getNextDestination());
                    switchWriter(dataLayerWrapper.currentDestination(), options.outputFormat);
                    shouldSwitch = false;
                }

                rowsBuffer[count++] = iterator.get();

                if (count == maxRowsPerFile)
                {
                    sortAndWrite(rowsBuffer);
                    start = printDuration(dataLayerWrapper.currentDestination(),
                                          count,
                                          start,
                                          currentTimeMillis());
                    dataLayerWrapper.currentDestination().setCount(count);
                    count = 0;
                    shouldSwitch = true;
                }
            }

            // if we should switch and we have at least something
            if (shouldSwitch && rowsBuffer[0] != null)
            {
                dataLayerWrapper.currentDestination().setCount(count);
                record(dataLayerWrapper.getNextDestination());
                switchWriter(dataLayerWrapper.currentDestination(), options.outputFormat);
            }

            sortAndWrite(rowsBuffer);

            if (count != 0)
                printDuration(dataLayerWrapper.currentDestination(), count, start, currentTimeMillis());
        }

        private void sortAndWrite(InternalRow[] rows)
        {
            Arrays.sort(rows, Comparator.nullsLast(rowComparator));
            for (InternalRow sorted : rows)
            {
                if (sorted != null)
                    rowWriter.accept(sorted);
                else
                    break;
            }

            Arrays.fill(rows, null);
        }
    }
}
