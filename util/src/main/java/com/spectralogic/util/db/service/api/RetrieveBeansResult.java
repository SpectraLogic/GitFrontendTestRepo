/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service.api;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.util.lang.iterate.CloseableIterable;
import com.spectralogic.util.lang.iterate.EnhancedIterable;

public interface RetrieveBeansResult< T >
{
    /**
     * @return the first bean in the result set, or null if there are no results from the query
     */
    T getFirst();
    
    
    /**
     * @return whether or not the result is empty.
     */
    boolean isEmpty();
    
    
    /**
     * @return the beans in a {@link Set} <br><br>
     * 
     * <b>Warning:</b> This method should only be used when the result count is significantly less than
     * {@link EnhancedIterable#MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED} since all results will be
     * loaded into memory at once.
     */
    Set< T > toSet();


    /**
     * @return the beans in a {@link Map} <br><br>
     *
     * <b>Warning:</b> This method should only be used when the result count is significantly less than
     * {@link EnhancedIterable#MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED} since all results will be
     * loaded into memory at once.
     */
    Map<UUID,T > toMap();


    /**
     * @return the beans in an iterable of {@link Set} <br><br>
     *
     * <font color = red>
     * <b>Warning:</b> Be sure to close the {@link CloseableIterable} when you're done with it via
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
    CloseableIterable< Set< T > > toSetsOf( final int batchSize );
    

    /**
     * @return the beans in a {@link List} <br><br>
     * 
     * <b>Warning:</b> This method should only be used when the result count is significantly less than
     * {@link EnhancedIterable#MAX_NUMBER_OF_RESULTS_BEFORE_STREAMING_IS_REQUIRED} since all results will be
     * loaded into memory at once.
     */
    List< T > toList();


    /**
     * @return the beans in an iterable of {@link List} <br><br>
     *
     * <font color = red>
     * <b>Warning:</b> Be sure to close the {@link CloseableIterable} when you're done with it via
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
    CloseableIterable< List< T > > toListsOf( final int batchSize );
    

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
    EnhancedIterable< T > toIterable();
}
