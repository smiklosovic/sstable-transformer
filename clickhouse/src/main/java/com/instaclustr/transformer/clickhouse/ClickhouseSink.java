package com.instaclustr.transformer.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.data.ClickHouseFormat;
import com.instaclustr.transformer.api.AbstractFile;
import com.instaclustr.transformer.api.OutputFormat;
import com.instaclustr.transformer.api.TransformationSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Name of this sink is {@code clickhouse-file-based}.
 * <p>
 * Takes {@link AbstractFile} which is backed, presumably, by Parquet / Avro file,
 * and it will insert it into a Clickhouse table via client's
 * {@link Client#insert(String, InputStream, ClickHouseFormat)}.
 * <p>
 * This sink does not check if Parquet / Avro types correspond to correct types
 * in Clickhouse table. If the columns do not match, then sinking will fail and
 * caller needs to deal with it.
 * <p>
 * Only HTTP protocol is possible to use for now. Clickhouse Java library does not
 * support other protocols anyway at the time of coding this solution.
 * <p>
 */
public class ClickhouseSink implements TransformationSink
{
    private static final Logger logger = LoggerFactory.getLogger(ClickhouseSink.class);

    private Client client;
    private ClickhouseConfig config;

    public static class ClickhouseConfig
    {
        private static final String ENDPOINT_PROPERTY = "endpoint";
        private static final String USERNAME_PROPERTY = "username";
        private static final String PASSWORD_PROPERTY = "password";
        private static final String TABLE_PROPERTY = "table";
        private static final String REMOVE_FILE_AFTER_PROCESSED_PROPERTY = "remove_file_after_processed";

        private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:8123";
        private static final String DEFAULT_USERNAME = "clickhouse";
        private static final String DEFAULT_PASSWORD = "clickhouse";
        private static final String DEFAULT_REMOVE_FILE_AFTER_PROCESSED = "false";

        public String endpoint;
        public String username;
        public String password;
        public String table;
        public boolean removeFileAfterProcessed;

        public ClickhouseConfig(String endpoint,
                                String username,
                                String password,
                                String table,
                                boolean removeFileAfterProcessed)
        {
            this.endpoint = endpoint;
            this.username = username;
            this.password = password;
            this.table = table;
            this.removeFileAfterProcessed = removeFileAfterProcessed;

            validate();
        }

        public ClickhouseConfig(Properties properties)
        {
            this(properties.getProperty(ENDPOINT_PROPERTY, DEFAULT_ENDPOINT),
                 properties.getProperty(USERNAME_PROPERTY, DEFAULT_USERNAME),
                 properties.getProperty(PASSWORD_PROPERTY, DEFAULT_PASSWORD),
                 properties.getProperty(TABLE_PROPERTY),
                 Boolean.parseBoolean(properties.getProperty(REMOVE_FILE_AFTER_PROCESSED_PROPERTY,
                                                             DEFAULT_REMOVE_FILE_AFTER_PROCESSED)));
        }

        public void validate()
        {
            if (table == null)
            {
                throw new IllegalStateException("'" + TABLE_PROPERTY + "' property has to be specified.");
            }
        }
    }

    @Override
    public String name()
    {
        return "clickhouse-file-based";
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
    public synchronized void sink(Object sinkObject) throws Exception
    {
        assert sinkObject instanceof AbstractFile : "sink object is not an instance of " + AbstractFile.class.getName();

        AbstractFile fileToSink = (AbstractFile) sinkObject;
        OutputFormat outputFormat = fileToSink.getOutputFormat();

        if (!supports(outputFormat))
            throw new IllegalStateException("this sink does not support " + outputFormat + " output format.");

        try (FileInputStream fis = new FileInputStream(fileToSink.getPath()))
        {
            client.insert(config.table, fis, getFormat(outputFormat));
            maybeRemoveFile(fileToSink);
        }
    }

    private void maybeRemoveFile(AbstractFile sinkObject)
    {
        if (config.removeFileAfterProcessed)
        {
            try
            {
                Files.delete(Paths.get(sinkObject.getPath()));
            } catch (Throwable t)
            {
                logger.warn(String.format("Unable to delete processed file: %s, reason: %s",
                                          sinkObject.getPath(),
                                          t.getMessage()));
            }
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
    public boolean supports(OutputFormat format)
    {
        return format == OutputFormat.PARQUET || format == OutputFormat.AVRO;
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
