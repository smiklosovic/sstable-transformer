package com.instaclustr.transformer.clickhouse;

import com.instaclustr.transformer.api.OutputFormat;
import com.instaclustr.transformer.api.TransformationSink;
import com.instaclustr.transformer.core.DataLayerTransformer;
import com.instaclustr.transformer.core.DataLayerWrapper;
import com.instaclustr.transformer.core.LocalDataLayerWrapper;
import com.instaclustr.transformer.core.TransformerOptions;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

public class ClickHouseMemorySinkTest extends AbstractClickhouseSinkTest
{
    @Test
    public void testParquetFileImport() throws Throwable
    {
        TransformerOptions options = new TransformerOptions();

        TransformationSink transformationSink = new ClickHouseMemorySink();
        DataLayerTransformer dataLayerTransformer = new DataLayerTransformer(options, getDataLayerWrapper(options), transformationSink);
        dataLayerTransformer.transform();

        // assert
    }

    private DataLayerWrapper getDataLayerWrapper(TransformerOptions options)
    {
        LocalDataLayer dataLayer = LocalDataLayer.from(options.forLocalDataLayer());
        dataLayer.setDataFilePaths(Set.of(Paths.get("src/test/resources/sstables").toAbsolutePath()));
        // null - not necessary
        return new LocalDataLayerWrapper(dataLayer, null, 1000);
    }

    private TransformerOptions getOptions(Path outputDir)
    {
        TransformerOptions options = new TransformerOptions();
        options.createTableStmt = String.format("CREATE TABLE spark_test.test (id bigint PRIMARY KEY, course blob, marks bigint)");
        options.transformationStrategy = TransformerOptions.TransformationStrategy.ONE_FILE_ALL_SSTABLES;
        options.outputFormat = OutputFormat.ARROW_STREAM;

        options.output = outputDir.toAbsolutePath().toString();
        //options.input = List.of(getInputDir().toAbsolutePath().toString());

        return options;
    }
}
