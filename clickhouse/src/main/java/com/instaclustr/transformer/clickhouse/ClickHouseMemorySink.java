package com.instaclustr.transformer.clickhouse;

import com.clickhouse.data.ClickHouseFormat;
import com.instaclustr.transformer.api.OutputFormat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

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

    private void processPipe(Object sinkObject) throws Exception
    {
        assert sinkObject instanceof PipedInputStream : "sink object is not an instance of " + PipedOutputStream.class.getName();
        client.insert(config.table, (PipedInputStream) sinkObject, ClickHouseFormat.ArrowStream);
    }

    private void processBuffer(Object sinkObject) throws Exception
    {
        assert sinkObject instanceof ByteArrayOutputStream : "sink object is not an instance of " + ByteArrayOutputStream.class.getName();

        ByteArrayOutputStream outputStream = (ByteArrayOutputStream) sinkObject;
        try (InputStream is = new ByteArrayInputStream(outputStream.toByteArray(), 0, outputStream.size()))
        {
            client.insert(config.table, is, ClickHouseFormat.ArrowStream);
        }
    }

    @Override
    public void sink(Object sinkObject) throws Exception
    {
        switch (config.sinkModel)
        {
            case PIPE:
                processPipe(sinkObject);
                break;
            case BYTE_BUFFER:
                processBuffer(sinkObject);
                break;
            default:
                throw new UnsupportedOperationException("Unsupported sink model: " + config.sinkModel);
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
