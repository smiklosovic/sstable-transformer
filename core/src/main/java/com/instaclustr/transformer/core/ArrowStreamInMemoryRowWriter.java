package com.instaclustr.transformer.core;

import org.apache.spark.sql.catalyst.InternalRow;

import java.util.function.Consumer;

public class ArrowStreamInMemoryRowWriter extends AbstractRowWriter
{
    @Override
    public void accept(InternalRow row)
    {

    }

    @Override
    public void close() throws Exception
    {

    }

    public Object getOutputStream()
    {
        return null;
    }
}
