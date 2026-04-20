/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.PublicCloudReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.rpc.dataplanner.TargetManagementResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.BasicTestsInvocationHandler.MethodInvokeData;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class PeriodicTargetVerifier_Test 
{
    @Test
    public void testConstructorInvalidParamsNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
        public void test()
            {
                new PeriodicTargetVerifier(
                        null,
                        createResource( null ),
                        10 );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
        public void test()
            {
                new PeriodicTargetVerifier(
                        dbSupport.getServiceManager(),
                        null,
                        10 );
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
        public void test()
            {
                new PeriodicTargetVerifier(
                        dbSupport.getServiceManager(),
                        createResource( null ),
                        0 );
            }
        } );
        new PeriodicTargetVerifier(
                dbSupport.getServiceManager(),
                createResource( null ),
                10000 ).shutdown();
    }


    @Test
    public void testAzureVerifyCallsMadeAsExpected()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( "t1" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "t2" );
        final AzureTarget target3 = mockDaoDriver.createAzureTarget( "t3" );
        final AzureTarget target4 = mockDaoDriver.createAzureTarget( "t4" );
        mockDaoDriver.updateBean(
                target1.setAutoVerifyFrequencyInDays( null ),
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target2.setAutoVerifyFrequencyInDays( Integer.valueOf( 1 ) ),
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target3.setAutoVerifyFrequencyInDays( Integer.valueOf( 1 ) ),
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target3.setLastFullyVerified( new Date( System.currentTimeMillis() - 1000L * 3600 * 23 ) ),
                PublicCloudReplicationTarget.LAST_FULLY_VERIFIED );
        mockDaoDriver.updateBean(
                target4.setAutoVerifyFrequencyInDays( Integer.valueOf( 2 ) ),
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target4.setLastFullyVerified( new Date( System.currentTimeMillis() - 1000L * 3600 * 49 ) ),
                PublicCloudReplicationTarget.LAST_FULLY_VERIFIED );

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PeriodicTargetVerifier periodicTargetVerifier =
                new PeriodicTargetVerifier( dbSupport.getServiceManager(), createResource( btih ), 50 );
        TestUtil.sleep( 250 );
        periodicTargetVerifier.shutdown();

        assertResourceCallCount( btih, false, target2.getId(), target4.getId() );
    }


    @Test
    public void testAzureVerifyCallsMadeAsExpectedWhenVerifyThrows()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( "t1" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "t2" );
        final AzureTarget target3 = mockDaoDriver.createAzureTarget( "t3" );
        final AzureTarget target4 = mockDaoDriver.createAzureTarget( "t4" );
        mockDaoDriver.updateBean(
                target1.setAutoVerifyFrequencyInDays( null ),
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target2.setAutoVerifyFrequencyInDays( Integer.valueOf( 1 ) ),
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target3.setAutoVerifyFrequencyInDays( Integer.valueOf( 1 ) ),
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target3.setLastFullyVerified( new Date( System.currentTimeMillis() - 1000L * 3600 * 23 ) ),
                PublicCloudReplicationTarget.LAST_FULLY_VERIFIED );
        mockDaoDriver.updateBean(
                target4.setAutoVerifyFrequencyInDays( Integer.valueOf( 2 ) ),
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target4.setLastFullyVerified( new Date( System.currentTimeMillis() - 1000L * 3600 * 49 ) ),
                PublicCloudReplicationTarget.LAST_FULLY_VERIFIED );

        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            public Object invoke(
                    final Object proxy,
                    final Method method,
                    final Object[] args ) throws Throwable
            {
                throw new RuntimeException( "Oops." );
            }
        } );
        final PeriodicTargetVerifier periodicTargetVerifier =
                new PeriodicTargetVerifier( dbSupport.getServiceManager(), createResource( btih ), 50 );
        TestUtil.sleep( 150 );
        periodicTargetVerifier.shutdown();

        assertResourceCallCount( btih, true, target2.getId(), target4.getId() );
    }


    @Test
    public void testS3VerifyCallsMadeAsExpected()
    {
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final S3Target target1 = mockDaoDriver.createS3Target( "t1" );
        final S3Target target2 = mockDaoDriver.createS3Target( "t2" );
        final S3Target target3 = mockDaoDriver.createS3Target( "t3" );
        final S3Target target4 = mockDaoDriver.createS3Target( "t4" );
        mockDaoDriver.updateBean(
                target1.setAutoVerifyFrequencyInDays( null ), 
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target2.setAutoVerifyFrequencyInDays( Integer.valueOf( 1 ) ), 
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target3.setAutoVerifyFrequencyInDays( Integer.valueOf( 1 ) ), 
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target3.setLastFullyVerified( new Date( System.currentTimeMillis() - 1000L * 3600 * 23 ) ),
                PublicCloudReplicationTarget.LAST_FULLY_VERIFIED );
        mockDaoDriver.updateBean(
                target4.setAutoVerifyFrequencyInDays( Integer.valueOf( 2 ) ), 
                PublicCloudReplicationTarget.AUTO_VERIFY_FREQUENCY_IN_DAYS );
        mockDaoDriver.updateBean(
                target4.setLastFullyVerified( new Date( System.currentTimeMillis() - 1000L * 3600 * 49 ) ),
                PublicCloudReplicationTarget.LAST_FULLY_VERIFIED );
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PeriodicTargetVerifier periodicTargetVerifier = 
                new PeriodicTargetVerifier( dbSupport.getServiceManager(), createResource( btih ), 50 );
        TestUtil.sleep( 250 );
        periodicTargetVerifier.shutdown();
        
        assertResourceCallCount( btih, false, target2.getId(), target4.getId() );
    }
    
    
    private TargetManagementResource createResource( final InvocationHandler ih )
    {
        return InterfaceProxyFactory.getProxy( TargetManagementResource.class, ih );
    }
    
    
    private void assertResourceCallCount( 
            final BasicTestsInvocationHandler btih,
            final boolean extraCallsExpected,
            final UUID ... expectedTargetVerifyCalls )
    {
        final Method methodVerify = 
                ReflectUtil.getMethod( TargetManagementResource.class, "verifyPublicCloudTarget" );
        if ( extraCallsExpected )
        {
            assertTrue(
                    expectedTargetVerifyCalls.length < btih.getMethodCallCount( methodVerify ),
                    "Shoulda made more than " + expectedTargetVerifyCalls.length + " calls."
                     );
        }
        else
        {
            final Map< Method, Integer > expectedCalls = new HashMap<>();
            expectedCalls.put( methodVerify, Integer.valueOf( expectedTargetVerifyCalls.length ) );
            btih.verifyMethodInvocations( expectedCalls );
        }
        
        final List< UUID > actualTargetVerifyCalls = new ArrayList<>();
        final List< MethodInvokeData > mids = btih.getMethodInvokeData( methodVerify );
        for ( final MethodInvokeData mid : mids )
        {
            actualTargetVerifyCalls.add( (UUID)mid.getArgs().get( 1 ) );
        }
        
        assertEquals(
                CollectionFactory.toSet( expectedTargetVerifyCalls ),
                new HashSet<>( actualTargetVerifyCalls ),
                "Shoulda verified targets as expected."
                 );
        btih.reset();
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
