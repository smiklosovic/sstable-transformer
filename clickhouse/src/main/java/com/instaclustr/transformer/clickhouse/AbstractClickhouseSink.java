package com.instaclustr.transformer.clickhouse;

import com.clickhouse.client.api.Client;
import com.instaclustr.transformer.api.TransformationSink;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public abstract class AbstractClickhouseSink implements TransformationSink
{
    protected Client client;
    protected ClickhouseConfig config;

    public AbstractClickhouseSink(ClickhouseConfig config)
    {
        if (config != null)
        {
            this.config = config;
            client = initializeClient(config);
        }
    }

    @Override
    public void init(Path sinkConfigFile) throws Exception
    {
        if (sinkConfigFile == null)
            throw new IllegalArgumentException("Sink configuration file is not specified.");

        if (config != null && client != null)
        {
            config = parseConfig(sinkConfigFile);
            client = initializeClient(config);
        }
    }

    private ClickhouseConfig parseConfig(Path sinkConfigFile) throws Exception
    {
        try (InputStream is = Files.newInputStream(sinkConfigFile))
        {
            Properties properties = new Properties();
            properties.load(is);
            return new ClickhouseConfig(properties);
        }
    }

    private Client initializeClient(ClickhouseConfig config)
    {
        if (client == null)
        {
            client = new Client.Builder()
                    .addEndpoint(config.endpoint)
                    .setUsername(config.username)
                    .setPassword(config.password)
                    .build();
        }

        return client;
    }

    @Override
    public void close()
    {
        if (client != null)
        {
            client.close();
            client = null;
        }
    }
}
