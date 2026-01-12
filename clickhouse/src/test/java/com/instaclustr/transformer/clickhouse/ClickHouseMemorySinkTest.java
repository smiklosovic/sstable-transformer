package com.instaclustr.transformer.clickhouse;

import com.instaclustr.transformer.api.OutputFormat;
import com.instaclustr.transformer.core.SSTableTransformer;
import com.instaclustr.transformer.core.TransformerOptions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Disabled
public class ClickHouseMemorySinkTest extends AbstractClickhouseSinkTest
{
    @Test
    public void testParquetFileImport() throws Throwable
    {
        TransformerOptions options = getOptions(null);

        new SSTableTransformer(options).runTransformation(ClickHouseMemorySink.class);
    }

    private TransformerOptions getOptions(Path outputDir)
    {
        TransformerOptions options = new TransformerOptions();
        options.createTableStmt = String.format("CREATE TABLE spark_test.test (id bigint PRIMARY KEY, course blob, marks bigint)");
        options.transformationStrategy = TransformerOptions.TransformationStrategy.ONE_FILE_ALL_SSTABLES;
        options.outputFormat = OutputFormat.ARROW_STREAM;

        //options.output = outputDir.toAbsolutePath().toString();
        options.input = List.of(Paths.get("src/test/resources/sstables").toAbsolutePath().toString());
        options.sinkConfig = Paths.get("src/test/resources/clickhouse-sink.properties").toAbsolutePath();

        options.validate();

        return options;
    }
}
