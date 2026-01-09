package com.instaclustr.transformer.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.instaclustr.transformer.api.AbstractFile;
import com.instaclustr.transformer.api.OutputFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.clickhouse.ClickHouseContainer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractClickhouseSinkTest
{
    private static final String CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:latest";
    private static final String CLICKHOUSE_USERNAME = "clickhouse";
    private static final String CLICKHOUSE_PASSWORD = "clickhouse";
    private static final String CLICKHOUSE_INIT_SCRIPT = "init.sql";
    private static final String CLICKHOUSE_DB = "testdb";
    public static final String CLICKHOUSE_TABLE = "my_clickhouse_table";

    public static ClickHouseContainer clickhouse;

    @BeforeAll
    public static void beforeAll()
    {
        clickhouse = new ClickHouseContainer(CLICKHOUSE_IMAGE)
                .withInitScript(CLICKHOUSE_INIT_SCRIPT)  // Run SQL on startup
                .withExposedPorts(8123, 9000)
                .withPassword(CLICKHOUSE_PASSWORD)
                .withUsername(CLICKHOUSE_USERNAME)
                .withEnv("CLICKHOUSE_DB", CLICKHOUSE_DB);

        clickhouse.start();
    }

    @AfterAll
    public static void afterAll()
    {
        clickhouse.stop();
    }

    @BeforeEach
    public void beforeEach()
    {
        try (Client client = getClient())
        {
            client.execute("TRUNCATE TABLE " + CLICKHOUSE_TABLE).get();
        }
        catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    protected ClickhouseFileSink getSink()
    {
        return new ClickhouseFileSink(getConfig());
    }

    public ClickhouseConfig getConfig()
    {
        String url = String.format("http://%s:%d",
                                   clickhouse.getHost(),
                                   clickhouse.getMappedPort(8123));

        return new ClickhouseConfig(url,
                                    clickhouse.getUsername(),
                                    clickhouse.getPassword(),
                                    CLICKHOUSE_TABLE,
                                    false);
    }

    public Client getClient()
    {
        ClickhouseConfig config = getConfig();
        return new Client.Builder()
                .addEndpoint(config.endpoint)
                .setUsername(config.username)
                .setPassword(config.password)
                .build();
    }

    public AbstractFile getFile(String path)
    {
        return new AbstractFile(OutputFormat.fromFileName(path),
                                Paths.get(path),
                                1)
        {
            @Override
            public String getPath()
            {
                return Paths.get("src/test/resources", path).toAbsolutePath().toString();
            }

            @Override
            public AbstractFile next()
            {
                return null;
            }
        };
    }

    public List<List<String>> select() throws Throwable
    {
        List<List<String>> result = new ArrayList<>();

        try (Client client = getClient();
             QueryResponse response = client.query("SELECT * FROM " + CLICKHOUSE_TABLE,
                                                   new QuerySettings().setFormat(ClickHouseFormat.TabSeparated)).get())
        {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputStream())))
            {
                String line;
                while ((line = reader.readLine()) != null)
                {
                    result.add(new ArrayList<>(Arrays.asList(line.split("\t"))));
                }
            }
        }

        return result;
    }
}
