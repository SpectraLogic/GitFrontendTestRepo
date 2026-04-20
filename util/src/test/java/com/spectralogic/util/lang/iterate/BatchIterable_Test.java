/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.lang.iterate;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class BatchIterable_Test 
{
    @Test
    public void testIteratorBatchesAsExpectedWhenEvenlyDivisible()
    {
        final List< Integer > list = Arrays.asList( new Integer[] {0,1,2,3,4,5,6,7,8} );
              
        final BatchIterable< Integer, Set< Integer > > iterable =
                new BatchIterable< Integer, Set< Integer > >( list, 3 )
                {
                    @Override
                    protected Set<Integer> instantiateNewCollection()
                    {
                        return new HashSet<>(); 
                    }
                };
        
        int batchIndex = 1;
        for ( Collection< Integer > batch : iterable ) 
        {
            if ( batchIndex == 1 )
            {
                assertEquals( 3, batch.size());
                assertTrue( batch.contains( 0 ) );
                assertTrue( batch.contains( 1 ) );
                assertTrue( batch.contains( 2 ) );
            }
            else if ( batchIndex == 2 )
            {
                assertEquals( 3, batch.size());
                assertTrue( batch.contains( 3 ) );
                assertTrue( batch.contains( 4 ) );
                assertTrue( batch.contains( 5 ) );
            }
            else if ( batchIndex == 3 )
            {
                assertEquals( 3, batch.size());
                assertTrue( batch.contains( 6 ) );
                assertTrue( batch.contains( 7 ) );
                assertTrue( batch.contains( 8 ) );
            }
            else
            {
                fail( "Should notta had any more batches!" );
            }
            batchIndex++;
        }
    }
    
    @Test
    public void testIteratorBatchesAsExpectedWhenNotEvenlyDivisible()
    {
        final List< Integer > list = Arrays.asList( new Integer[] {0,1,2,3,4,5,6,7,8,9} );
              
        final BatchIterable< Integer, Set< Integer > > iterable =
                new BatchIterable< Integer, Set< Integer > >( list, 3 )
                {
                    @Override
                    protected Set<Integer> instantiateNewCollection()
                    {
                        return new HashSet<>(); 
                    }
                };
        
        int batchIndex = 1;
        for ( Collection< Integer > batch : iterable ) 
        {
            if ( batchIndex == 1 )
            {
                assertEquals( 3, batch.size());
                assertTrue( batch.contains( 0 ) );
                assertTrue( batch.contains( 1 ) );
                assertTrue( batch.contains( 2 ) );
            }
            else if ( batchIndex == 2 )
            {
                assertEquals( 3, batch.size());
                assertTrue( batch.contains( 3 ) );
                assertTrue( batch.contains( 4 ) );
                assertTrue( batch.contains( 5 ) );
            }
            else if ( batchIndex == 3 )
            {
                assertEquals( 3, batch.size());
                assertTrue( batch.contains( 6 ) );
                assertTrue( batch.contains( 7 ) );
                assertTrue( batch.contains( 8 ) );
            }
            else if ( batchIndex == 4 )
            {
                assertEquals( 1, batch.size());
                assertTrue( batch.contains( 9 ) );
            }
            else
            {
                fail( "Should notta had any more batches!" );
            }
            batchIndex++;
            
        }        
    }
    
    @Test
    public void testIteratorRejectsBatchSizeZero()
    {
        final List< Integer > list = Arrays.asList( new Integer[] {0,1,2,3,4,5,6,7,8,9} );
              
              
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                    new BatchIterable< Integer, Set< Integer > >( list, 0 )
                    {
                        @Override
                        protected Set<Integer> instantiateNewCollection()
                        {
                            return new HashSet<>(); 
                        }
                    };
            }
        } );
    }
    
    
    @Test
    public void testIteratorBatchesAsExpectedWhenBatchSizeLargerThanList()
    {
        final List< Integer > list = Arrays.asList( new Integer[] {0,1,2,3,4} );
              
        final BatchIterable< Integer, Set< Integer > > iterable =
                new BatchIterable< Integer, Set< Integer > >( list, 2000 )
                {
                    @Override
                    protected Set<Integer> instantiateNewCollection()
                    {
                        return new HashSet<>(); 
                    }
                };
        
        int batchIndex = 1;
        for ( Collection< Integer > batch : iterable ) 
        {
            if ( batchIndex == 1 )
            {
                assertEquals( 5, batch.size());
                assertTrue( batch.contains( 0 ) );
                assertTrue( batch.contains( 1 ) );
                assertTrue( batch.contains( 2 ) );
                assertTrue( batch.contains( 3 ) );
                assertTrue( batch.contains( 4 ) );
            }
            else
            {
                fail( "Should notta had any more batches!" );
            }
            batchIndex++;
        }        
    }
}
