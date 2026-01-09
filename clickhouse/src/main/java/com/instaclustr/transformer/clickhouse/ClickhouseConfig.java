package com.instaclustr.transformer.clickhouse;

import java.util.Properties;

public class ClickhouseConfig
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
