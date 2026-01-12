package com.instaclustr.transformer.clickhouse;

import com.instaclustr.transformer.api.OutputFormat;
import com.instaclustr.transformer.core.SSTableTransformer;
import com.instaclustr.transformer.core.TransformerOptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.List;

public class ClickHouseMemorySinkTest extends AbstractClickhouseSinkTest
{
    @Test
    public void testArrowStreamImport() throws Throwable
    {
        TransformerOptions options = getOptions();

        new SSTableTransformer(options).runTransformation(ClickHouseMemorySink.class);
    }

    private TransformerOptions getOptions()
    {
        TransformerOptions options = new TransformerOptions();
        options.createTableStmt = String.format("CREATE TABLE spark_test.testtable (id int primary key)");
        options.transformationStrategy = TransformerOptions.TransformationStrategy.ONE_FILE_ALL_SSTABLES;
        options.outputFormat = OutputFormat.ARROW_STREAM;

        //options.output = outputDir.toAbsolutePath().toString();
        options.input = List.of(Paths.get("src/test/resources/sstables").toAbsolutePath().toString());
        options.sinkConfig = Paths.get("src/test/resources/clickhouse-sink.properties").toAbsolutePath();

        options.validate();

        return options;
    }
}
