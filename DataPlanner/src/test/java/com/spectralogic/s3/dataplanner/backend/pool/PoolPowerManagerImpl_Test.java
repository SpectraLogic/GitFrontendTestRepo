/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;



import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.ValidatingRpcResourceInvocationHandler;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;

public final class PoolPowerManagerImpl_Test
{
    @Test
    public void testConstructorNullPoolRetrieverNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {

        public void test()
            {
                new PoolPowerManagerImpl( 
                        null,
                        InterfaceProxyFactory.getProxy( PoolEnvironmentResource.class, null ) );
            }
        } );
    }
    

    @Test
    public void testConstructorNullRpcResourceNotAllowed()
    {
        
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new PoolPowerManagerImpl( 
                        dbSupport.getServiceManager().getService( PoolService.class ),
                        null );
            }
        } );
    }
    

    @Test
    public void testPowerOnNullPoolNotAllowed()
    {
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PoolPowerManager powerManager = new PoolPowerManagerImpl( 
                dbSupport.getServiceManager().getService( PoolService.class ),
                InterfaceProxyFactory.getProxy( 
                        PoolEnvironmentResource.class, 
                        new ValidatingRpcResourceInvocationHandler( btih ) ) );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    powerManager.powerOn( null );
                }
            } );
        assertEquals(
                0,
                btih.getTotalCallCount(),
                "Should notta made any invocations on RPC resource."
                 );
    }
    

    @Test
    public void testPowerOffNullPoolNotAllowed()
    {
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( null );
        final PoolPowerManager powerManager = new PoolPowerManagerImpl( 
                dbSupport.getServiceManager().getService( PoolService.class ),
                InterfaceProxyFactory.getProxy( 
                        PoolEnvironmentResource.class, 
                        new ValidatingRpcResourceInvocationHandler( btih ) ) );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    powerManager.powerOff( null );
                }
            } );
        assertEquals(
                0,
                btih.getTotalCallCount(),
                "Should notta made any invocations on RPC resource."
                 );
    }
    

    @Test
    public void testPowerCommandsOnlyGetSentAsRpcRequestIfPoolPowerStatusActuallyChanges()
    {
        
        final BasicTestsInvocationHandler btih = new BasicTestsInvocationHandler( new InvocationHandler()
        {
            public Object invoke( final Object proxy, final Method method, final Object[] args )
                    throws Throwable
            {
                return new RpcResponse<>();
            }
        } );
        final PoolPowerManager powerManager = new PoolPowerManagerImpl( 
                dbSupport.getServiceManager().getService( PoolService.class ),
                InterfaceProxyFactory.getProxy( 
                        PoolEnvironmentResource.class, 
                        new ValidatingRpcResourceInvocationHandler( btih ) ) );

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        mockDaoDriver.updateBean( pool1.setPoweredOn( false ), PoolObservable.POWERED_ON );
        mockDaoDriver.updateBean( pool2.setPoweredOn( false ), PoolObservable.POWERED_ON );
        
        int expectedPowerOnCallCount = 0;
        int expectedPowerOffCallCount = 0;
        assertRpcCallsAsExpected( btih, expectedPowerOnCallCount, expectedPowerOffCallCount );
        
        powerManager.powerOn( pool1.getId() );
        assertRpcCallsAsExpected( btih, ++expectedPowerOnCallCount, expectedPowerOffCallCount );
        assertTrue(mockDaoDriver.attain( pool1 ).isPoweredOn(), "Shoulda been powered on.");
        assertFalse(mockDaoDriver.attain( pool2 ).isPoweredOn(), "Shoulda been powered off initially.");

        powerManager.powerOn( pool1.getId() );
        assertRpcCallsAsExpected( btih, expectedPowerOnCallCount, expectedPowerOffCallCount );
        assertTrue(mockDaoDriver.attain( pool1 ).isPoweredOn(), "Shoulda been powered on.");
        assertFalse(mockDaoDriver.attain( pool2 ).isPoweredOn(), "Shoulda been powered off initially.");

        powerManager.powerOn( pool2.getId() );
        assertRpcCallsAsExpected( btih, ++expectedPowerOnCallCount, expectedPowerOffCallCount );
        assertTrue(mockDaoDriver.attain( pool1 ).isPoweredOn(), "Shoulda been powered on.");
        assertTrue(mockDaoDriver.attain( pool2 ).isPoweredOn(), "Shoulda been powered on.");

        powerManager.powerOn( pool2.getId() );
        assertRpcCallsAsExpected( btih, expectedPowerOnCallCount, expectedPowerOffCallCount );
        
        powerManager.powerOff( pool2.getId() );
        assertRpcCallsAsExpected( btih, expectedPowerOnCallCount, ++expectedPowerOffCallCount );
        
        powerManager.powerOn( pool1.getId() );
        assertRpcCallsAsExpected( btih, expectedPowerOnCallCount, expectedPowerOffCallCount );
        
        powerManager.powerOff( pool2.getId() );
        assertRpcCallsAsExpected( btih, expectedPowerOnCallCount, expectedPowerOffCallCount );
        
        powerManager.powerOn( pool2.getId() );
        assertRpcCallsAsExpected( btih, ++expectedPowerOnCallCount, expectedPowerOffCallCount );
        
        powerManager.powerOff( pool1.getId() );
        assertRpcCallsAsExpected( btih, expectedPowerOnCallCount, ++expectedPowerOffCallCount );
        
        powerManager.powerOff( pool2.getId() );
        assertRpcCallsAsExpected( btih, expectedPowerOnCallCount, ++expectedPowerOffCallCount );
        assertTrue(mockDaoDriver.attain( pool1 ).isPoweredOn(), "Should notta marked as powered off since RPC call to power off does not grntee power dwn.");
        assertTrue(mockDaoDriver.attain( pool2 ).isPoweredOn(), "Should notta marked as powered off since RPC call to power off does not grntee power dwn.");
    }
    
    
    private void assertRpcCallsAsExpected( 
            final BasicTestsInvocationHandler btih, 
            final int expectedPowerOnCallCount, 
            final int expectedPowerOffCallCount )
    {
        final Method mOn = ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOn" );
        final Method mOff = ReflectUtil.getMethod( PoolEnvironmentResource.class, "powerOff" );
        
        final Map< Method, Integer > expectedCalls = new HashMap<>();
        expectedCalls.put( mOn, Integer.valueOf( expectedPowerOnCallCount ) );
        expectedCalls.put( mOff, Integer.valueOf( expectedPowerOffCallCount ) );
        btih.verifyMethodInvocations( expectedCalls );
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
