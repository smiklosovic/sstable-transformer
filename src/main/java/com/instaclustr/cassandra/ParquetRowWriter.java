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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Consumes {@link InternalRow} which is basically a Cassandra row.
 */
public class ParquetRowWriter implements Consumer<InternalRow>, AutoCloseable
{
    private final ParquetWriter<GenericRecord> writer;
    private final StructType structType;
    private final Schema schema;
    private final AbstractOutputFile<?> destination;
    private final DataLayer dataLayer;

    /**
     * @param dataLayer      Data layer to use
     * @param schema         Avro schema used for {@link ParquetWriter}.
     * @param destination    Initial destination to write data to.
     * @param options        Transformation options
     * @throws IOException if anything IO-related goes wrong
     */
    public ParquetRowWriter(DataLayer dataLayer,
                            Schema schema,
                            AbstractOutputFile<?> destination,
                            TransformerOptions options) throws IOException
    {
        this.dataLayer = dataLayer;
        this.structType = this.dataLayer.structType();
        this.schema = schema;
        this.destination = destination;
        writer = createWriter(this.destination,
                              options.bloomFilterEnabled,
                              options.compression);
    }

    @Override
    public void accept(InternalRow row)
    {
        try
        {
            writer.write(convertInternalRowToAvro(row, structType, schema));
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Unable to write row", t);
        }
    }

    private ParquetWriter<GenericRecord> createWriter(AbstractOutputFile<?> destination,
                                                      boolean bloomFilterEnabled,
                                                      CompressionCodecName compressionCodecName) throws IOException
    {
        return AvroParquetWriter.<GenericRecord>builder(destination)
                .enablePageWriteChecksum()
                .enableValidation()
                .withBloomFilterEnabled(bloomFilterEnabled)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(compressionCodecName)
                .withSchema(schema)
                .build();
    }

    private static GenericRecord convertInternalRowToAvro(InternalRow row, StructType schema, Schema avroSchema)
    {
        GenericRecord record = new GenericData.Record(avroSchema);

        StructField[] fields = schema.fields();
        for (int i = 0; i < fields.length; i++)
        {
            String name = fields[i].name();
            DataType type = fields[i].dataType();
            Object value;

            if (row.isNullAt(i))
            {
                value = null;
            } else if (type == DataTypes.BooleanType)
            {
                value = row.getBoolean(i);
            } else if (type == DataTypes.StringType)
            {
                value = row.getUTF8String(i).toString();
            } else if (type == DataTypes.ShortType)
            {
                value = row.getShort(i);
            } else if (type == DataTypes.IntegerType)
            {
                value = row.getInt(i);
            } else if (type == DataTypes.LongType)
            {
                value = row.getLong(i);
            } else if (type == DataTypes.DoubleType)
            {
                value = row.getDouble(i);
            } else if (type == DataTypes.FloatType)
            {
                value = row.getFloat(i);
            } else if (type == DataTypes.BinaryType)
            {
                value = row.getBinary(i);
            } else if (type == DataTypes.TimestampType)
            {
                value = row.getLong(i);
            } else if (type == DataTypes.DateType)
            {
                value = row.getInt(i);
            } else
            {
                throw new UnsupportedOperationException("Unsupported data type: " + type);
            }

            record.put(name, value);
        }

        return record;
    }

    @Override
    public void close() throws Exception
    {
        destination.finish();
        writer.close();
    }
}
