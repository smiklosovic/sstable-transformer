package com.instaclustr.transformer.api;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;

/**
 * Implementations might react on a creation of a Parquet / Avro file
 * and transform it further.
 */
public interface TransformationSink extends AutoCloseable
{
    /**
     * @return name of this transformer
     */
    String name();

    /**
     * Method called first on this sink, chance to set it up.
     * <p>
     * The format of a configuration file for a sink is sink's responsibility to manage.
     *
     * @param sinkConfigFile generic file with a configuration.
     */
    void init(Path sinkConfigFile) throws Exception;

    /**
     * @param sinkObject object to sink to this transformer
     */
    void sink(Object sinkObject) throws Exception;

    /**
     * @return type of input object of {@link TransformationSink#sink}.
     */
    Class<?> inputObjectType();

    /**
     * Checks if this sink supports given output format.
     *
     * @param format format to check
     * @return true if this sink supports processing this outpuf format of the primary transformation
     */
    boolean supports(OutputFormat format);

    static void validate(TransformationSink transformationSink, OutputFormat outputFormat)
    {
        if (transformationSink == null)
            return;

        if (outputFormat == OutputFormat.AVRO || outputFormat == OutputFormat.PARQUET)
        {
            if (transformationSink.inputObjectType() != AbstractFile.class)
            {
                throw new IllegalStateException(String.format("Sink %s can not accept anything but %s",
                                                              transformationSink.name(),
                                                              transformationSink.inputObjectType().getName()));
            }
        }
        else if (outputFormat == OutputFormat.ARROW_STREAM)
        {
            if (transformationSink.inputObjectType() != ByteArrayOutputStream.class)
            {
                throw new IllegalStateException(String.format("Sink %s can not accept anything but %s",
                                                              transformationSink.name(),
                                                              transformationSink.inputObjectType().getName()));
            }
        }
        else
        {
            throw new IllegalStateException(String.format("Sink %s is not supporting output format %s",
                                                          transformationSink.name(),
                                                          outputFormat));
        }
    }
}
