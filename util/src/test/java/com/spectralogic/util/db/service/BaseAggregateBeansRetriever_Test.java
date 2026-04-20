/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.mockdomain.County;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BaseAggregateBeansRetriever_Test 
{
    @Test
    public void testDiscoverNotAllowed()
    {
        checkRetrieverCallerThrowsUnsupportedOperationException( new RetrieverCaller()
        {
            public void call( final BaseAggregateBeansRetriever< County > beansRetriever )
            {
                beansRetriever.discover( UUID.fromString( "d5ab3344-bcd7-4fa4-b539-020eee6d1872" ) );
            }
        } );
    }
    

    @Test
    public void testGetCountNotAllowed()
    {
        checkRetrieverCallerThrowsUnsupportedOperationException( new RetrieverCaller()
        {
            public void call( final BaseAggregateBeansRetriever< County > beansRetriever )
            {
                beansRetriever.getCount();
            }
        } );
    }
    

    @Test
    public void testGetCountWhenParameterProvidedNotAllowed()
    {
        checkRetrieverCallerThrowsUnsupportedOperationException( new RetrieverCaller()
        {
            public void call( final BaseAggregateBeansRetriever< County > beansRetriever )
            {
                beansRetriever.getCount( Require.nothing() );
            }
        } );
    }
    

    @Test
    public void testGetMinNotAllowed()
    {
        checkRetrieverCallerThrowsUnsupportedOperationException( new RetrieverCaller()
        {
            public void call( final BaseAggregateBeansRetriever< County > beansRetriever )
            {
                beansRetriever.getMin( County.POPULATION, Require.nothing() );
            }
        } );
    }
    

    @Test
    public void testGetMaxNotAllowed()
    {
        checkRetrieverCallerThrowsUnsupportedOperationException( new RetrieverCaller()
        {
            public void call( final BaseAggregateBeansRetriever< County > beansRetriever )
            {
                beansRetriever.getMax( County.POPULATION, Require.nothing() );
            }
        } );
    }
    

    @Test
    public void testGetSumNotAllowed()
    {
        checkRetrieverCallerThrowsUnsupportedOperationException( new RetrieverCaller()
        {
            public void call( final BaseAggregateBeansRetriever< County > beansRetriever )
            {
                beansRetriever.getSum( County.POPULATION, Require.nothing() );
            }
        } );
    }
    

    @Test
    public void testFindSingleResultNotAllowed()
    {
        checkRetrieverCallerThrowsUnsupportedOperationException( new RetrieverCaller()
        {
            public void call( final BaseAggregateBeansRetriever< County > beansRetriever )
            {
                beansRetriever.findSingleResult( NotFoundBehavior.RETURN_NULL, Require.nothing() );
            }
        } );
    }
    

    @Test
    public void testRetrieveAllInternalNotAllowed()
    {
        checkRetrieverCallerThrowsUnsupportedOperationException( new RetrieverCaller()
        {
            public void call( final BaseAggregateBeansRetriever< County > beansRetriever )
            {
                beansRetriever.retrieveIterable( Query.where( Require.nothing() ) );
            }
        } );
    }
    
    
    private interface RetrieverCaller
    {
        void call( final BaseAggregateBeansRetriever< County > beansRetriever );
    }
    

    private void checkRetrieverCallerThrowsUnsupportedOperationException( final RetrieverCaller caller )
    {
        TestUtil.assertThrows(
                "Shoulda thrown an exception because we don't support this method.",
                UnsupportedOperationException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        caller.call( new BaseAggregateBeansRetriever<>( County.class ) );
                    }
                } );
        TestUtil.assertThrows(
                "Shoulda thrown an exception because we don't support this method.",
                UnsupportedOperationException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        caller.call( new BaseAggregateBeansRetriever<>(
                                County.class,
                                GenericFailure.INTERNAL_ERROR ) );
                    }
                } );
    }
}
