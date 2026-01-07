package com.instaclustr.transformer.core;

import com.instaclustr.transformer.api.TransformationSink;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;

public class SPISinkProvider
{
    public static Optional<TransformationSink> getSink()
    {
        ServiceLoader<TransformationSink> loader = ServiceLoader.load(TransformationSink.class);

        List<TransformationSink> sinks = new ArrayList<>();
        loader.forEach(sinks::add);

        if (sinks.size() > 1)
        {
            throw new IllegalStateException("Not possible to have more than 1 sink on the class path right now.");
        }

        return sinks.stream().findFirst();
    }
}
