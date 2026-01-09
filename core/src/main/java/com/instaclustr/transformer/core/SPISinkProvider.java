package com.instaclustr.transformer.core;

import com.instaclustr.transformer.api.TransformationSink;

import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class SPISinkProvider
{
    public static Provider<TransformationSink> getSinkProvider(String sinkName)
    {
        if (sinkName == null)
            return null;

        List<Provider<TransformationSink>> allProviders = ServiceLoader.load(TransformationSink.class).stream().collect(toList());
        List<Provider<TransformationSink>> providersOfGiveName = allProviders.stream().filter(p -> sinkName.equals(p.get().name())).collect(toList());

        if (providersOfGiveName.isEmpty())
        {
            throw new IllegalStateException(format("There are %s providers, looking for one with name %s",
                                                   TransformationSink.class.getSimpleName(),
                                                   sinkName);
        }

        if (providersOfGiveName.size() > 1)
        {
            throw new IllegalStateException(format("Not possible to have more than 1 implementation of sink of name %s but we have: %s",
                                                   sinkName,
                                                   providersOfGiveName.stream()
                                                           .map(p -> p.get().getClass().getName())
                                                           .collect(toList())));
        }

        return providersOfGiveName.get(0);
    }
}
