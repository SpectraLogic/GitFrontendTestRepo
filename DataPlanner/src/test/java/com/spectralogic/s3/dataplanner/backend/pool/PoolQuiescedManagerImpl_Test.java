/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool;

import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class PoolQuiescedManagerImpl_Test 
{
    @Test
    public void testConstructorNullNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new PoolQuiescedManagerImpl( null );
                }
            } );
    }
    
    
    @Test
    public void testIsQuiescedNullPoolIdNotAllowed()
    {
        
        
        final PoolQuiescedManager manager = new PoolQuiescedManagerImpl( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    manager.isQuiesced( null );
                }
            } );
    }
    
    
    @Test
    public void testIsQuiescedInvalidPoolIdNotAllowed()
    {
        
        
        final PoolQuiescedManager manager = new PoolQuiescedManagerImpl( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, GenericFailure.NOT_FOUND, new BlastContainer()
        {
            public void test()
                {
                    manager.isQuiesced( UUID.randomUUID() );
                }
            } );
    }
    
    
    @Test
    public void testIsQuiescedReturnsTrueIffPoolIsQuiesced()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool1.setQuiesced( Quiesced.NO ), Pool.QUIESCED );
        final Pool pool2 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool2.setQuiesced( Quiesced.PENDING ), Pool.QUIESCED );
        final Pool pool3 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool3.setQuiesced( Quiesced.YES ), Pool.QUIESCED );
        
        final PoolQuiescedManager manager = new PoolQuiescedManagerImpl( dbSupport.getServiceManager() );
        assertEquals(false, manager.isQuiesced( pool1.getId() ), "Shoulda returned true iff quiesced.");
        assertEquals(true, manager.isQuiesced( pool2.getId() ), "Shoulda returned true iff quiesced.");
        assertEquals(true, manager.isQuiesced( pool3.getId() ), "Shoulda returned true iff quiesced.");

        mockDaoDriver.updateAllBeans( 
                BeanFactory.newBean( DataPathBackend.class ).setActivated( false ),
                DataPathBackend.ACTIVATED );
        assertEquals(true, manager.isQuiesced( pool1.getId() ), "Shoulda returned true iff quiesced.");
        assertEquals(true, manager.isQuiesced( pool2.getId() ), "Shoulda returned true iff quiesced.");
        assertEquals(true, manager.isQuiesced( pool3.getId() ), "Shoulda returned true iff quiesced.");
    }
    
    
    @Test
    public void testVerifyNotQuiescedNullPoolIdNotAllowed()
    {
        
        
        final PoolQuiescedManager manager = new PoolQuiescedManagerImpl( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    manager.verifyNotQuiesced( null );
                }
            } );
    }
    
    
    @Test
    public void testVerifyNotQuiescedInvalidPoolIdNotAllowed()
    {
        
        
        final PoolQuiescedManager manager = new PoolQuiescedManagerImpl( dbSupport.getServiceManager() );
        TestUtil.assertThrows( null, GenericFailure.NOT_FOUND, new BlastContainer()
        {
            public void test()
                {
                    manager.verifyNotQuiesced( UUID.randomUUID() );
                }
            } );
    }
    
    
    @Test
    public void testVerifyNotQuiescedThrowsIffPoolIsQuiesced()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool1.setQuiesced( Quiesced.NO ), Pool.QUIESCED );
        final Pool pool2 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool2.setQuiesced( Quiesced.PENDING ), Pool.QUIESCED );
        final Pool pool3 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool3.setQuiesced( Quiesced.YES ), Pool.QUIESCED );
        
        final PoolQuiescedManager manager = new PoolQuiescedManagerImpl( dbSupport.getServiceManager() );
        manager.verifyNotQuiesced( pool1.getId() );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
                {
                    manager.verifyNotQuiesced( pool2.getId() );
                }
            } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
                {
                    manager.verifyNotQuiesced( pool3.getId() );
                }
            } );

        mockDaoDriver.updateAllBeans( 
                BeanFactory.newBean( DataPathBackend.class ).setActivated( false ),
                DataPathBackend.ACTIVATED );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
                {
                    manager.verifyNotQuiesced( pool1.getId() );
                }
            } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
                {
                    manager.verifyNotQuiesced( pool2.getId() );
                }
            } );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
                {
                    manager.verifyNotQuiesced( pool3.getId() );
                }
            } );
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);
    @BeforeAll
    public static void setUp() {
        dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    }

    @AfterEach
    public void resetDB() {
        dbSupport.reset();
    }
}
