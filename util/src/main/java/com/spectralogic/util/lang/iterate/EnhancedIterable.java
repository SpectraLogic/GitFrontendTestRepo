/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.iterate;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * An {@link Iterable} that provides additional convenience methods and supports {@link PreProcessing}.
 */
public interface EnhancedIterable< T > extends CloseableIterable< T >, PreProcessing< T >
{
    /**
     * <b>Warning: </b>Calling this method will load all objects from the iterable into memory at once.  This
     * is not allowed if there are more than {@link #MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED} 
     * objects.  You should consider not using this method before this maximum is reached.
     */
    Set< T > toSet();


    /**
     * <b>Warning: </b>Calling this method will load all objects from the iterable into memory at once.  This
     * is not allowed if there are more than {@link #MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED}
     * objects.  You should consider not using this method before this maximum is reached.
     */
    Map<UUID, T > toMap();
    

    /**
     * <b>Warning: </b>Calling this method will load all objects from the iterable into memory at once.  This
     * is not allowed if there are more than {@link #MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED} 
     * objects.  You should consider not using this method before this maximum is reached.
     */
    List< T > toList();
    

    /**
     * @return an {@link EnhancedIterable} for the beans, which is memory-safe to use for any number of 
     * beans  <br><br>
     * 
     * <font color = red>
     * <b>Warning:</b> Be sure to close the {@link EnhancedIterable} when you're done with it via
     * {@link AutoCloseable#close}.  It is usually easiest to accomplish this by using the Java try with
     * resources construct.  Failure to do so will result in a SQL connection resource leak that will be
     * subtle and difficult to track down.
     * <br><br>
     * <b>Warning:</b> Unless the connection pool is unlimited in size, do not make calls into methods that 
     * may result in having to take a SQL connection from within the life of the iterable (after this method 
     * is called and before the iterable is closed) without reserving those connections in advance.  Failure 
     * to comply will create an intermittent live lock that will be subtle and difficult to track down.
     * </font>
     */
    EnhancedIterator< T > iterator();
    
    
    int MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED = 500000;
}
