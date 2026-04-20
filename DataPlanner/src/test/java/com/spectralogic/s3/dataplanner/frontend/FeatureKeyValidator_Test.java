/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.FeatureKey;
import com.spectralogic.s3.common.dao.domain.ds3.FeatureKeyType;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.shared.ErrorMessageObservable;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.spectralogic.util.thread.wp.SystemWorkPool;
import com.spectralogic.util.thread.wp.WorkPool;
import com.spectralogic.util.thread.wp.WorkPoolFactory;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class FeatureKeyValidator_Test 
{
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

        public void test() throws Throwable
            {
                new FeatureKeyValidator( null );
            }
        } );
    }
        
     @Test
    public void testExecutorRunsPeriodically()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        mockDaoDriver.createFeatureKey( null, null, new Date( 1000 ) );
        
        final FeatureKeyValidator validator =
                new FeatureKeyValidator( dbSupport.getServiceManager(), Long.valueOf( 100 ) );
        assertNull(mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage(), "Should notta expired key yet.");

        int i = 50;
        while ( --i > 0 && null == mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage() )
        {
            TestUtil.sleep( 20 );
        }
        
        assertNotNull(
                "Shoulda expired key.",
                mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage() );
        validator.shutdown();
    }
    
    
     @Test
    public void testExpiredKeysAreInvalidated()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        mockDaoDriver.createFeatureKey( null, null, new Date() );
        
        TestUtil.sleep( 2 );
        final FeatureKeyValidator validator = new FeatureKeyValidator( dbSupport.getServiceManager(), null );
        validator.run();
        
        assertNotNull(
                "Shoulda expired key.",
                mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage() );
    }
    
    
     @Test
    public void testNonExpiredKeysAreNotInvalidatedForExpiring()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        mockDaoDriver.createFeatureKey( null, null, new Date( System.currentTimeMillis() + 300 ) );
        
        final FeatureKeyValidator validator = new FeatureKeyValidator( dbSupport.getServiceManager(), null );
        validator.run();

        assertNull(mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage(), "Should notta expired key.");
    }
    
    
     @Test
    public void testNonExpiringKeysAreInvalidatedForExpiring()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        mockDaoDriver.createFeatureKey( null, null, null );
        
        TestUtil.sleep( 2 );
        final FeatureKeyValidator validator = new FeatureKeyValidator( dbSupport.getServiceManager(), null );
        validator.run();

        assertNull(mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage(), "Should notta expired key.");
    }
    
    
     @Test
    public void testConcurrentInvocationsWork() throws InterruptedException, ExecutionException
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        mockDaoDriver.createFeatureKey( null, null, new Date() );
        
        final WorkPool wp = WorkPoolFactory.createWorkPool( 16, getClass().getSimpleName() );
        final FeatureKeyValidator validator = new FeatureKeyValidator( dbSupport.getServiceManager(), null );
        
        final Set< Future< ? > > futures = new HashSet<>();
        for ( int i = 0; i < 64; ++i )
        {
            futures.add( wp.submit( validator ) );
        }
        
        TestUtil.sleep( 2 );
        futures.add( wp.submit( validator ) );
        
        for ( final Future< ? > f : futures )
        {
            f.get();
        }
        
        assertNotNull(
                "Shoulda expired key.",
                mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage() );
        wp.shutdown();
    }
    
    
     @Test
    public void testAzureCloudOutKeyInvalidatedOnceItShouldBe()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final AzureTarget target = mockDaoDriver.createAzureTarget( null );
        final S3Target target2 = mockDaoDriver.createS3Target( null );
        mockDaoDriver.putBlobOnAzureTarget( target.getId(), blob.getId() );
        mockDaoDriver.putBlobOnS3Target( target2.getId(), blob2.getId() );
        
        final FeatureKeyValidator validator = new FeatureKeyValidator( dbSupport.getServiceManager(), null );
        validator.run();
        
        final FeatureKey key =
                mockDaoDriver.createFeatureKey( FeatureKeyType.MICROSOFT_AZURE_CLOUD_OUT, null, null );
        validator.run();
        assertNull(mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage(), "Should notta expired key.");

        mockDaoDriver.updateBean(
                key.setLimitValue( Long.valueOf( 9 ) ).setErrorMessage( "blah" ), 
                FeatureKey.LIMIT_VALUE, ErrorMessageObservable.ERROR_MESSAGE );
        validator.run();
        assertEquals("blah", mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage(), "Should notta expired key.");

        mockDaoDriver.updateBean(
                key.setErrorMessage( null ), 
                ErrorMessageObservable.ERROR_MESSAGE );
        validator.run();
        assertTrue(mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage().contains( "9" ), "Shoulda expired key.");
        final Object actual = mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getCurrentValue();
        assertEquals(Long.valueOf( 10 ), actual, "Shoulda updated current value.");

        mockDaoDriver.updateBean(
                key.setLimitValue( Long.valueOf( 10 ) ), 
                FeatureKey.LIMIT_VALUE );
        validator.run();
        assertNotNull(
                "Should notta made key valid again for going back down below limit.",
                mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage() );
        
        mockDaoDriver.updateBean(
                key.setErrorMessage( null ), 
                ErrorMessageObservable.ERROR_MESSAGE );
        validator.run();
        assertNull(mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage(), "Should notta expired key.");
    }
    
    
     @Test
    public void testS3CloudOutKeyInvalidatedOnceItShouldBe()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final S3Object o2 = mockDaoDriver.createObject( null, "o2", 20 );
        final Blob blob2 = mockDaoDriver.getBlobFor( o2.getId() );
        final S3Target target = mockDaoDriver.createS3Target( null );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( null );
        mockDaoDriver.putBlobOnS3Target( target.getId(), blob.getId() );
        mockDaoDriver.putBlobOnAzureTarget( target2.getId(), blob2.getId() );
        
        final FeatureKeyValidator validator = new FeatureKeyValidator( dbSupport.getServiceManager(), null );
        validator.run();
        
        final FeatureKey key =
                mockDaoDriver.createFeatureKey( FeatureKeyType.AWS_S3_CLOUD_OUT, null, null );
        validator.run();
        assertNull(mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage(), "Should notta expired key.");

        mockDaoDriver.updateBean(
                key.setLimitValue( Long.valueOf( 9 ) ).setErrorMessage( "blah" ), 
                FeatureKey.LIMIT_VALUE, ErrorMessageObservable.ERROR_MESSAGE );
        validator.run();
        assertEquals("blah", mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage(), "Should notta expired key.");

        mockDaoDriver.updateBean(
                key.setErrorMessage( null ), 
                ErrorMessageObservable.ERROR_MESSAGE );
        validator.run();
        assertTrue(mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage().contains( "9" ), "Shoulda expired key.");
        final Object actual = mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getCurrentValue();
        assertEquals(Long.valueOf( 10 ), actual, "Shoulda updated current value.");

        mockDaoDriver.updateBean(
                key.setLimitValue( Long.valueOf( 10 ) ), 
                FeatureKey.LIMIT_VALUE );
        validator.run();
        assertNotNull(
                "Should notta made key valid again for going back down below limit.",
                mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage() );
        
        mockDaoDriver.updateBean(
                key.setErrorMessage( null ), 
                ErrorMessageObservable.ERROR_MESSAGE );
        validator.run();
        assertNull(mockDaoDriver.attainOneAndOnly( FeatureKey.class ).getErrorMessage(), "Should notta expired key.");
    }

    private static DatabaseSupport dbSupport = DatabaseSupportFactory.getSupport(DaoDomainsSeed.class, DaoServicesSeed.class);

    @BeforeAll
    public static void resetDb() { dbSupport.reset(); }

    @AfterEach
    public void tearDown() throws InterruptedException {
        if (!SystemWorkPool.getInstance().awaitEmpty(10, TimeUnit.SECONDS)) {
            throw new IllegalStateException("System work pool did not empty in a timely manner.");
        }
        dbSupport.reset();
    }
}
