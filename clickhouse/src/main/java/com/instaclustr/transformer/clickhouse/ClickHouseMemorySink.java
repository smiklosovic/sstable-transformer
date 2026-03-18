package com.instaclustr.transformer.clickhouse;

import com.clickhouse.data.ClickHouseFormat;
import com.instaclustr.transformer.api.ExposedByteArrayOutputStream;
import com.instaclustr.transformer.api.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PipedInputStream;

public class ClickHouseMemorySink extends AbstractClickHouseSink
{
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseMemorySink.class);

    public ClickHouseMemorySink()
    {
        super(null);
        // for spi
    }

    public ClickHouseMemorySink(ClickHouseConfig config)
    {
        super(config);
    }

    private void processPipe(Object sinkObject)
    {
        if (!(sinkObject instanceof PipedInputStream))
            throw new IllegalArgumentException("sink object is not an instance of " + PipedInputStream.class.getName());

        client.insert(config.table, (PipedInputStream) sinkObject, ClickHouseFormat.ArrowStream);
    }

    private void processBuffer(Object sinkObject) throws Exception
    {
        if (!(sinkObject instanceof ByteArrayOutputStream))
            throw new IllegalArgumentException("sink object is not an instance of " + ByteArrayOutputStream.class.getName());

        ByteArrayOutputStream outputStream = (ByteArrayOutputStream) sinkObject;
        long start = System.currentTimeMillis();

        byte[] data;
        if (outputStream instanceof ExposedByteArrayOutputStream)
            data = ((ExposedByteArrayOutputStream) outputStream).getBuffer();
        else
            data = outputStream.toByteArray();

        try (InputStream is = new ByteArrayInputStream(data, 0, outputStream.size()))
        {
            client.insert(config.table, is, ClickHouseFormat.ArrowStream);
        }
        long stop = System.currentTimeMillis();
        logger.info("Insert took " + (stop - start));
    }

    @Override
    public void sink(Object sinkObject) throws Exception
    {
        switch (config.sinkModel)
        {
            case PIPE:
                processPipe(sinkObject);
                break;
            case ASYNC_BYTE_BUFFER:
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
