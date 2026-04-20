package com.spectralogic.util.lang.iterate;

public interface CloseableIterable< T > extends AutoCloseable, Iterable< T >
{
    // Define close to not throw an Exception
    void close();
}
