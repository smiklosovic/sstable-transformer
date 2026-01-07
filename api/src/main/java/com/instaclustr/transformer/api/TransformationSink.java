package com.instaclustr.transformer.api;

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
    void sink(AbstractFile sinkObject) throws Exception;

    /**
     * @return type of input object of {@link TransformationSink#sink}.
     */
    Class<?> inputObjectType();
}
