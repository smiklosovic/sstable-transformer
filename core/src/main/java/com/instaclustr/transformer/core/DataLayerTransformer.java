/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.instaclustr.transformer.core;

import com.instaclustr.transformer.api.AbstractFile;
import com.instaclustr.transformer.api.TransformationSink;

import java.util.Collection;

/**
 * This class is responsible for taking an instance of {@link DataLayerWrapper}, being it
 * local or remote, and transforming data it is responsible for to Parquet files by iterating over
 * all rows by {@link DataLayerReader}.
 */
public class DataLayerTransformer
{
    private final TransformerOptions options;
    private final DataLayerWrapper dataLayerWrapper;
    private final TransformationSink transformationSink;

    public DataLayerTransformer(TransformerOptions options, DataLayerWrapper dataLayerWrapper)
    {
        this.options = options;
        this.dataLayerWrapper = dataLayerWrapper;
        transformationSink = SPISinkProvider.getSink().orElse(null);

        if (transformationSink != null)
        {
            Class<?> sinkInputType = transformationSink.inputObjectType();

            if (sinkInputType != AbstractFile.class)
                throw new IllegalStateException("Sink can not accept anything but " + AbstractFile.class.getName() + " subtypes for now.");

            try
            {
                transformationSink.init(options.sinkConfig);
            }
            catch (Exception ex)
            {
                throw new IllegalStateException(String.format("Unable to initialize sink '%s' of class '%s': %s",
                                                              transformationSink.name(),
                                                              transformationSink.getClass().getName(),
                                                              ex.getMessage()));
            }
        }
    }

    /**
     * Transforms data.
     *
     * @return list of Parquet files which were created by transformation.
     */
    public Collection<? extends AbstractInputOutputFile> transform()
    {
        try
        {
            Collection<? extends AbstractInputOutputFile> result = new DataLayerReader(dataLayerWrapper, options).read();

            if (transformationSink == null)
                return result;

            for (AbstractFile file : result)
                transformationSink.sink(file);

            return result;
        } catch (Throwable t)
        {
            throw new TransformerException("Unable to transform to " + options.outputFormat + " file(s)", t);
        }
    }
}
