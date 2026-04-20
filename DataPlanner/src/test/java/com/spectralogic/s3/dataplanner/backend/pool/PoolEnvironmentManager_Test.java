/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailure;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolHealth;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.pool.PoolType;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.common.rpc.pool.domain.PoolEnvironmentInformation;
import com.spectralogic.s3.common.rpc.pool.domain.PoolInformation;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.dataplanner.backend.pool.api.BlobPoolLastAccessedUpdater;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolPowerManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolQuiescedManager;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.reflect.ReflectUtil;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.*;


public final class PoolEnvironmentManager_Test
{
    @Test
    public void testConstructorNullPoolEnvironmentInformationNotAllowed()
    {
        

        final MockPoolEnvironmentResource resource = new MockPoolEnvironmentResource();
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new PoolEnvironmentManager( null, dbSupport.getServiceManager(), resource.m_lockSupport );
                }
            } );
    }
    
    
    @Test
    public void testConstructorNullServiceManagerNotAllowed()
    {
        final MockPoolEnvironmentResource resource = new MockPoolEnvironmentResource();
        resource.m_poolEnvironmentInformation = constructResponse( 3 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new PoolEnvironmentManager( resource.m_rpcResource, null, resource.m_lockSupport );
                }
            } );
    }
    
    
    @Test
    public void testConstructorNullLockSupportNotAllowed()
    {
        
        
        final MockPoolEnvironmentResource resource = new MockPoolEnvironmentResource();
        resource.m_poolEnvironmentInformation = constructResponse( 3 );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new PoolEnvironmentManager( resource.m_rpcResource, dbSupport.getServiceManager(), null );
                }
            } );
    }
    
    
    @Test
    public void testRunReconcilesPoolEnvironmentAndTakesOwnershipOfUnownedPoolsWhenNoLockingIssues()
    {
        
        final PoolService service = dbSupport.getServiceManager().getService( PoolService.class );
        final MockPoolEnvironmentResource resource = new MockPoolEnvironmentResource();
        resource.m_poolEnvironmentInformation = constructResponse( 3 );
        new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport ).run();
        assertEquals(3,  service.getCount(), "Shoulda created initial pool environment.");
        assertEquals(3,  service.getCount(Pool.STATE, PoolState.NORMAL), "Shoulda created initial pool environment.");
        resource.assertTakeOwnershipCalled( 3 );
        
        resource.m_poolEnvironmentInformation = constructResponse( 4 );
        new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport );
        assertEquals(3,  service.getCount(), "Should notta updated pool environment since didn't run updater.");
        resource.assertTakeOwnershipCalled( 0 );
        
        new PoolEnvironmentManager(
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport ).run();
        assertEquals(4,  service.getCount(), "Shoulda updated pool environment.");
        assertEquals(4,  service.getCount(Pool.STATE, PoolState.NORMAL), "Shoulda updated pool environment.");
        resource.assertTakeOwnershipCalled( 4 );

        resource.m_poolEnvironmentInformation = constructResponse( 1 );
        new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport ).run();
        assertEquals(4,  service.getCount(), "Shoulda updated pool environment.");
        assertEquals(1,  service.getCount(Pool.STATE, PoolState.NORMAL), "Shoulda updated pool environment.");
        assertEquals(3,  service.getCount(Pool.STATE, PoolState.LOST), "Shoulda updated pool environment.");
        resource.assertTakeOwnershipCalled( 1 );
        
        new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport ).run();
        assertEquals(4,  service.getCount(), "Shoulda updated pool environment.");
        assertEquals(1,  service.getCount(Pool.STATE, PoolState.NORMAL), "Shoulda updated pool environment.");
        assertEquals(3,  service.getCount(Pool.STATE, PoolState.LOST), "Shoulda updated pool environment.");
        resource.assertTakeOwnershipCalled( 1 );

        resource.m_poolEnvironmentInformation = constructResponse( 5 );
        new PoolEnvironmentManager(
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport ).run();
        assertEquals(5,  service.getCount(), "Shoulda updated pool environment.");
        assertEquals(5,  service.getCount(Pool.STATE, PoolState.NORMAL), "Shoulda updated pool environment.");
        resource.assertTakeOwnershipCalled( 5 );

        final Pool dbPool = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( 
                PoolObservable.GUID, 
                resource.m_poolEnvironmentInformation.getPools()[ 0 ].getGuid() );
        resource.m_poolEnvironmentInformation.getPools()[ 0 ].setPoolId( dbPool.getId() );
        
        new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport ).run();
        assertEquals(5,  service.getCount(), "Shoulda updated pool environment.");
        assertEquals(5,  service.getCount(Pool.STATE, PoolState.NORMAL), "Shoulda updated pool environment.");
        resource.assertTakeOwnershipCalled( 4 );
        
        resource.m_poolEnvironmentInformation.getPools()[ 0 ].setPoolId( UUID.randomUUID() );
        
        new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport ).run();
        assertEquals(5,  service.getCount(), "Shoulda updated pool environment.");
        assertEquals(4,  service.getCount(Pool.STATE, PoolState.NORMAL), "Shoulda updated pool environment.");
        assertEquals(1,  service.getCount(Pool.STATE, PoolState.FOREIGN), "Shoulda updated pool environment.");
        resource.assertTakeOwnershipCalled( 4 );
        
        resource.m_takeOwnershipException = new RuntimeException( "Oops." );
        TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
                {
                    new PoolEnvironmentManager(
                            resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport ).run();
                }
        } );
        resource.assertTakeOwnershipCalled( 1 );
    }
    
    
    @Test
    public void testRunReconcilesPoolEnvironmentAndTakesOwnershipOfUnownedPoolsWhenLockingIssues()
    {
        
        final PoolService service = dbSupport.getServiceManager().getService( PoolService.class );
        final MockPoolEnvironmentResource resource = new MockPoolEnvironmentResource();
        resource.m_poolEnvironmentInformation = constructResponse( 3 );
        
        final PoolQuiescedManager quiescedManager = 
                InterfaceProxyFactory.getProxy( PoolQuiescedManager.class, new InvocationHandler()
                {
                    @Override
                    public Object invoke( final Object proxy, final Method method, final Object[] args ) 
                            throws Throwable
                    {
                        throw new RuntimeException( "Uh uh uh... No lock for you!" );
                    }
                } );
        
        final PoolLockSupport< PoolTask > lockSupport = new PoolLockSupportImpl<>( 
                InterfaceProxyFactory.getProxy( BlobPoolLastAccessedUpdater.class, null ),
                InterfaceProxyFactory.getProxy( PoolPowerManager.class, null ),
                quiescedManager );
        final PoolEnvironmentManager manager = new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), lockSupport );
        manager.run();
        assertEquals(3,  service.getCount(), "Shoulda created initial pool environment.");
        assertEquals(3, service.getCount(Pool.STATE, PoolState.BLANK), "Shoulda created initial pool environment.");
        resource.assertTakeOwnershipCalled( 0 );
        assertTrue(
                manager.needsAnotherRun(),
                "Shoulda noted that another run is needed to take ownerships."
                );

        final PoolEnvironmentManager manager2 = new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport );
        manager2.run();
        assertEquals(3,  service.getCount(), "Shoulda created initial pool environment.");
        assertEquals(3, service.getCount(Pool.STATE, PoolState.NORMAL), "Shoulda created initial pool environment.");
        resource.assertTakeOwnershipCalled( 3 );
        assertFalse(manager2.needsAnotherRun(), "Should notta noted that another run is needed to take ownerships.");
    }
    
    
    @Test
    public void testPoolStateUpdatedCorrectlyBasedOnRpcResponsesThatComeBack()
    {
        
        final PoolService service = dbSupport.getServiceManager().getService( PoolService.class );
        final MockPoolEnvironmentResource resource = new MockPoolEnvironmentResource();
        final PoolEnvironmentManager manager = new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport );
        for ( final PoolState originalState : PoolState.values() )
        {
            resource.m_poolEnvironmentInformation = constructResponse( 5 );
            manager.run();
            assertEquals(5,  service.getCount(), "Shoulda updated pool environment.");

            dbSupport.getDataManager().updateBeans( 
                    CollectionFactory.toSet( Pool.STATE ), 
                    BeanFactory.newBean( Pool.class ).setState( originalState ),
                    Require.nothing() );
            final Pool dbPool = dbSupport.getServiceManager().getRetriever( Pool.class ).attain( 
                    PoolObservable.GUID, 
                    resource.m_poolEnvironmentInformation.getPools()[ 0 ].getGuid() );
            final String lostPoolGuid = resource.m_poolEnvironmentInformation.getPools()[ 4 ].getGuid();
            resource.m_poolEnvironmentInformation = constructResponse( 4 );
            resource.m_poolEnvironmentInformation.getPools()[ 0 ].setPoolId( dbPool.getId() );
            resource.m_poolEnvironmentInformation.getPools()[ 1 ].setPoolId( UUID.randomUUID() );
            manager.run();

            assertEquals(5,  service.getCount(), "Shoulda retained pool environment.");
            final Pool p1 = service.attain(
                    PoolObservable.GUID,
                    resource.m_poolEnvironmentInformation.getPools()[ 0 ].getGuid() );
            final Pool p2 = service.attain( 
                    PoolObservable.GUID,
                    resource.m_poolEnvironmentInformation.getPools()[ 1 ].getGuid() );
            final Pool p3 = service.attain( 
                    PoolObservable.GUID,
                    resource.m_poolEnvironmentInformation.getPools()[ 2 ].getGuid() );
            final Pool p4 = service.attain( 
                    PoolObservable.GUID,
                    resource.m_poolEnvironmentInformation.getPools()[ 3 ].getGuid() );
            final Pool p5 = service.attain( 
                    PoolObservable.GUID,
                    lostPoolGuid );
            assertEquals(PoolState.NORMAL, p1.getState(), "Shoulda marked formerly " + originalState + " pool now owned normal as normal.");
            final Object expected = ( PoolState.IMPORT_IN_PROGRESS == originalState || PoolState.IMPORT_PENDING == originalState ) ?
                    originalState
                    : PoolState.FOREIGN;
            assertEquals(expected, p2.getState(), "Shoulda marked formerly " + originalState + " pool now foreign as normal.");
            assertEquals(PoolState.NORMAL, p3.getState(), "Shoulda marked formerly " + originalState + " pool now unowned normal as normal.");
            assertEquals(PoolState.NORMAL, p4.getState(), "Shoulda marked formerly " + originalState + " pool now unowned normal as normal.");
            assertEquals(PoolState.LOST, p5.getState(), "Shoulda marked formerly " + originalState + " pool now lost as lost.");
        }
    }
    
    
    @Test
    public void testPoolLossEventResultsInNoPoolPartitionFailureIfNotIllegalSinceNoNeedToRetainPoolAtAll()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final MockPoolEnvironmentResource resource = new MockPoolEnvironmentResource();
        final PoolEnvironmentManager manager = new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport );

        resource.m_poolEnvironmentInformation = constructResponse( 1 );
        manager.run();
        final Pool pool =
                dbSupport.getServiceManager().getRetriever( Pool.class ).attain( Require.nothing() );
        mockDaoDriver.updateBean(
                pool.setAssignedToStorageDomain( true ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        resource.m_poolEnvironmentInformation = constructResponse( 0 );
        manager.run();
        mockDaoDriver.attainAndUpdate( pool );
        assertEquals(PoolState.LOST, pool.getState(), "Shoulda recorded pool as lost.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Should notta generated a failure since ejection wasn't illegal.");
    }
    
    @Test
    public void testPoolLossEventResultsInNoPoolPartitionFailureIfNotIllegalSincePoolAllocatedToStorDomPermittingEjctn()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.updateBean(
                storageDomain.setMediaEjectionAllowed( true ), 
                StorageDomain.MEDIA_EJECTION_ALLOWED );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final MockPoolEnvironmentResource resource = new MockPoolEnvironmentResource();
        final PoolEnvironmentManager manager = new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport );
        resource.m_poolEnvironmentInformation = constructResponse( 1 );
        manager.run();
        final Pool pool =
                dbSupport.getServiceManager().getRetriever( Pool.class ).attain( Require.nothing() );
        final StorageDomainMember sdm =
                mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), pool.getPartitionId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), blob.getId() );
        mockDaoDriver.updateBean(
                pool.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        resource.m_poolEnvironmentInformation = constructResponse( 0 );
        manager.run();
        mockDaoDriver.attainAndUpdate( pool );
        assertEquals(PoolState.LOST, pool.getState(), "Shoulda recorded pool as lost.");
        assertEquals(0,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Should notta generated a failure since ejection wasn't illegal.");
    }
    
    @Test
    public void testPoolLossEventResultsInPoolPartitionFailureIfIllegalSincePoolAllocatedToStorDomDisallowingEjection()
    {
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final StorageDomain storageDomain = mockDaoDriver.createStorageDomain( "sd1" );
        mockDaoDriver.updateBean(
                storageDomain.setMediaEjectionAllowed( false ), 
                StorageDomain.MEDIA_EJECTION_ALLOWED );
        final S3Object o = mockDaoDriver.createObject( null, "o1" );
        final Blob blob = mockDaoDriver.getBlobFor( o.getId() );
        final MockPoolEnvironmentResource resource = new MockPoolEnvironmentResource();
        final PoolEnvironmentManager manager = new PoolEnvironmentManager( 
                resource.m_rpcResource, dbSupport.getServiceManager(), resource.m_lockSupport );
        
        resource.m_poolEnvironmentInformation = constructResponse( 1 );
        manager.run();
        final Pool pool =
                dbSupport.getServiceManager().getRetriever( Pool.class ).attain( Require.nothing() );
        final StorageDomainMember sdm =
                mockDaoDriver.addPoolPartitionToStorageDomain( storageDomain.getId(), pool.getPartitionId() );
        mockDaoDriver.putBlobOnPool( pool.getId(), blob.getId() );
        mockDaoDriver.updateBean(
                pool.setAssignedToStorageDomain( true ).setStorageDomainMemberId( sdm.getId() ),
                PersistenceTarget.ASSIGNED_TO_STORAGE_DOMAIN, PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        resource.m_poolEnvironmentInformation = constructResponse( 0 );
        manager.run();
        mockDaoDriver.attainAndUpdate( pool );
        assertEquals(PoolState.LOST, pool.getState(), "Shoulda recorded pool as lost.");
        assertEquals(1,  dbSupport.getServiceManager().getRetriever(StorageDomainFailure.class).getCount(), "Shoulda generated a failure since ejection was illegal.");
    }
    
    
    private final static class MockPoolEnvironmentResource
    {
        @SuppressWarnings( "unchecked" )
        private MockPoolEnvironmentResource()
        {
            final MockInvocationHandler ihTakeOwnershipOfPool = MockInvocationHandler.forMethod( 
                    m_methodTakeOwnershipOfPool, 
                    new InvocationHandler()
                    {
                        @Override
                        public Object invoke( final Object proxy, final Method method, final Object[] args )
                                throws Throwable
                        {
                            if ( null != m_takeOwnershipException )
                            {
                                throw m_takeOwnershipException;
                            }
                            return new RpcResponse<>( null );
                        }
                    }, 
                    null );
            
            final MockInvocationHandler ihGetPoolEnvironment = MockInvocationHandler.forMethod( 
                    m_methodGetPoolEnvironment, 
                    new InvocationHandler()
                    {
                        @Override
                        public Object invoke( final Object proxy, final Method method, final Object[] args )
                                throws Throwable
                        {
                            return new RpcResponse<>( m_poolEnvironmentInformation );
                        }
                    }, 
                    ihTakeOwnershipOfPool );
            
            m_rpcResourceBtih = new BasicTestsInvocationHandler( ihGetPoolEnvironment );
            m_rpcResource = InterfaceProxyFactory.getProxy( 
                    PoolEnvironmentResource.class, m_rpcResourceBtih );
            
            m_lockSupportBtih = new BasicTestsInvocationHandler( null );
            m_lockSupport = InterfaceProxyFactory.getProxy( PoolLockSupport.class, m_lockSupportBtih );
        }
        
        
        private void assertTakeOwnershipCalled( final int numberOfTimes )
        {
            m_numTakeOwnershipCalls += numberOfTimes;
            final Object actual2 = m_rpcResourceBtih.getMethodCallCount( m_methodTakeOwnershipOfPool );
            assertEquals(m_numTakeOwnershipCalls, actual2, "Shoulda taken ownership of all pools.");
            final Object actual1 = m_lockSupportBtih.getMethodCallCount( m_methodAcquireLock );
            assertEquals(m_numTakeOwnershipCalls, actual1, "Shoulda taken ownership of all pools.");
            final Object actual = m_lockSupportBtih.getMethodCallCount( m_methodReleaseLock );
            assertEquals(m_numTakeOwnershipCalls, actual, "Shoulda taken ownership of all pools.");
        }
        
        
        private int m_numTakeOwnershipCalls;
        private volatile PoolEnvironmentInformation m_poolEnvironmentInformation;
        private volatile RuntimeException m_takeOwnershipException;
        private final BasicTestsInvocationHandler m_lockSupportBtih;
        private final PoolLockSupport< PoolTask > m_lockSupport;
        private final BasicTestsInvocationHandler m_rpcResourceBtih;
        private final PoolEnvironmentResource m_rpcResource;
        private final Method m_methodTakeOwnershipOfPool = 
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "takeOwnershipOfPool" );
        private final Method m_methodGetPoolEnvironment = 
                ReflectUtil.getMethod( PoolEnvironmentResource.class, "getPoolEnvironment" );
        private final Method m_methodAcquireLock = 
                ReflectUtil.getMethod( PoolLockSupport.class, "acquireWriteLock" );
        private final Method m_methodReleaseLock = 
                ReflectUtil.getMethod( PoolLockSupport.class, "releaseLock" );
    } // end inner class def
    
    
    private PoolEnvironmentInformation constructResponse( final int numPools )
    {
        final PoolEnvironmentInformation retval = BeanFactory.newBean( PoolEnvironmentInformation.class );
        final List< PoolInformation > pools = new ArrayList<>();
        for ( int i = 0; i < numPools; ++i )
        {
            pools.add( constructPool( i ) );
        }
        retval.setPools( CollectionFactory.toArray( PoolInformation.class, pools ) );
        return retval;
    }
    
    
    private PoolInformation constructPool( final int num )
    {
        final PoolInformation retval = BeanFactory.newBean( PoolInformation.class );
        retval.setGuid( "guid" + num );
        retval.setName( "pool" + num );
        retval.setAvailableCapacity( num * 10 );
        retval.setHealth( PoolHealth.values()[ num % PoolHealth.values().length ] );
        retval.setMountpoint( "mountpoint" + num );
        retval.setPoweredOn( 0 == num % 2 );
        retval.setReservedCapacity( num );
        retval.setTotalCapacity( num * 100 );
        retval.setType( PoolType.values()[ num % PoolType.values().length ] );
        retval.setUsedCapacity(
                retval.getTotalCapacity() - retval.getAvailableCapacity() - retval.getReservedCapacity() );
        return retval;
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
