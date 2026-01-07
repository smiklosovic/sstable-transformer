package com.instaclustr.transformer.clickhouse;

import com.clickhouse.client.ClickHouseRequest;
import com.clickhouse.client.api.Client;
import com.clickhouse.data.ClickHouseFormat;
import com.instaclustr.transformer.api.AbstractFile;
import com.instaclustr.transformer.api.OutputFormat;
import com.instaclustr.transformer.api.TransformationSink;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

/**
 * Takes {@link AbstractFile} which is backed, presumably, by Parquet / Avro file,
 * and it will insert it into a Clickhouse table.
 * <p>
 * This sink does not check if Parquet / Avro types correspond to correct types
 * in Clickhouse table. If the columns do not match, then sinking will fail and
 * caller needs to deal with it.
 * <p>
 * Only HTTP protocol is possible to use for now. Clickhouse Java library does not
 * support other protocols anyway at the time of coding this solution.
 */
public class ClickhouseSink implements TransformationSink
{
    private Client client;
    private ClickhouseConfig config;

    public static class ClickhouseConfig
    {
        private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:8123";
        private static final String DEFAULT_USERNAME = "clickhouse";
        private static final String DEFAULT_PASSWORD = "clickhouse";

        public String endpoint;
        public String username;
        public String password;
        public String table;

        public ClickhouseConfig(String endpoint,
                                String username,
                                String password,
                                String table)
        {
            this.endpoint = endpoint;
            this.username = username;
            this.password = password;
            this.table = table;

            validate();
        }

        public ClickhouseConfig(Properties properties)
        {
            this(properties.getProperty("endpoint", DEFAULT_ENDPOINT),
                 properties.getProperty("username", DEFAULT_USERNAME),
                 properties.getProperty("password", DEFAULT_PASSWORD),
                 properties.getProperty("table"));
        }

        public void validate()
        {
            if (table == null)
            {
                throw new IllegalStateException("'table' property has to be specified.");
            }
        }
    }

    @Override
    public String name()
    {
        return "clickhouse";
    }

    public ClickhouseSink(ClickhouseConfig config)
    {
        this.config = config;
        client = initializeClient(config);
    }

    @Override
    public void init(Path sinkConfigFile) throws Exception
    {
        if (sinkConfigFile == null)
            throw new IllegalArgumentException("Sink configuration file is not specified.");

        config = parseConfig(sinkConfigFile);
        client = initializeClient(config);
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
    public synchronized void sink(AbstractFile sinkObject) throws Exception
    {
        try (FileInputStream fis = new FileInputStream(sinkObject.getPath()))
        {
            client.insert(config.table, fis, getFormat(sinkObject.getOutputFormat()));
        }
    }

    private ClickHouseFormat getFormat(OutputFormat outputFormat)
    {
        if (outputFormat == OutputFormat.PARQUET)
            return ClickHouseFormat.Parquet;
        else if (outputFormat == OutputFormat.AVRO)
            return ClickHouseFormat.Avro;

        throw new UnsupportedOperationException("Invalid format " + outputFormat);
    }

    @Override
    public Class<?> inputObjectType()
    {
        return AbstractFile.class;
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
