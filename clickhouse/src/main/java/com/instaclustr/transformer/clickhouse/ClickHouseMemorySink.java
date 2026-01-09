package com.instaclustr.transformer.clickhouse;

import com.clickhouse.data.ClickHouseFormat;
import com.instaclustr.transformer.api.OutputFormat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class ClickHouseMemorySink extends AbstractClickHouseSink
{
    public ClickHouseMemorySink()
    {
        super(null);
        // for spi
    }

    public ClickHouseMemorySink(ClickHouseConfig config)
    {
        super(config);
    }

    @Override
    public String name()
    {
        return "clickhouse-memory";
    }

    @Override
    public void sink(Object sinkObject) throws Exception
    {
        assert sinkObject instanceof ByteArrayOutputStream : "sink object is not an instance of " + ByteArrayOutputStream.class.getName();

        try (ByteArrayOutputStream outputStream = (ByteArrayOutputStream) sinkObject;
             InputStream is = new ByteArrayInputStream(outputStream.toByteArray()))
        {
            client.insert(config.table, is, ClickHouseFormat.ArrowStream);
        }
    }

    @Override
    public Class<?> inputObjectType()
    {
        return ByteArrayOutputStream.class;
    }

    @Override
    public boolean supports(OutputFormat format)
    {
        return format == OutputFormat.ARROW_STREAM;
    }
}
