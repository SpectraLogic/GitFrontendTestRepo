/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.mock;

import java.lang.reflect.Array;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.orm.BucketRM;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.dao.service.ds3.BlobService;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.dao.service.ds3.S3ObjectService;
import com.spectralogic.s3.common.platform.api.TapeEjector;
import com.spectralogic.s3.common.platform.cache.MockDiskManager;
import com.spectralogic.s3.common.platform.domain.DetailedJobToReplicate;
import com.spectralogic.s3.common.platform.spectrads3.JobReplicationSupport;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.rpc.dataplanner.DataPolicyManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.PoolManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TapeManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.TargetManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.*;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.dispatch.RequestDispatcher;
import com.spectralogic.s3.server.dispatch.RequestDispatcherImpl;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.bean.BeanUtils;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.BasicTestsInvocationHandler;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.mock.NullInvocationHandler;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.frmwrk.RpcMethodReturnType;
import com.spectralogic.util.net.rpc.server.RpcResponse;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.ValidatingRpcResourceInvocationHandler;

public final class MockHttpRequestSupport
{
    public MockHttpRequestSupport()
    {
        m_dbSupport = DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        m_plannerInterfaceBtih = 
                new BasicTestsInvocationHandler( MockInvocationHandler.forReturnType(
                        RpcFuture.class,
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args ) 
                                    throws Throwable
                            {
                                final InvocationHandler plannerInterfaceIh = m_plannerInterfaceIh;
                                if ( null != plannerInterfaceIh )
                                {
                                    final Object retval = plannerInterfaceIh.invoke( proxy, method, args );
                                    if ( null != retval )
                                    {
                                        return retval;
                                    }
                                }
                                if ( "replicatePutJob".equals( method.getName() ) )
                                {
                                    final DetailedJobToReplicate jtr = (DetailedJobToReplicate)args[ 0 ];
                                    final UUID dataPolicyId = new BucketRM(jtr.getBucketId(), m_dbSupport.getServiceManager()).unwrap().getDataPolicyId();
                                    return new RpcResponse<>( new JobReplicationSupport(
                                            m_dbSupport.getServiceManager(), 
                                            jtr ).commit(
                                                 new MockDiskManager( m_dbSupport.getServiceManager() ), createPersistenceTargetInfo(dataPolicyId, m_dbSupport)) );
                                }
                                
                                if ( UUID.class == method.getAnnotation( RpcMethodReturnType.class ).value() )
                                {
                                    if ( method.getName().contains( "Get" ) || method.getName().equals( "createIomJob" ) )
                                    {
                                        final CreateGetJobParams params = (CreateGetJobParams)args[ 0 ];
                                        return new RpcResponse<>( fakeCreateGetJob(
                                                JobRequestType.GET,
                                                params,
                                                params.getReplicatedJobId(),
                                                params.getChunkOrderGuarantee() ) );
                                    }
                                    else if ( method.getName().contains( "Verify" ) )
                                    {
                                        final CreateVerifyJobParams params = (CreateVerifyJobParams)args[ 0 ];
                                        return new RpcResponse<>( fakeCreateGetJob(
                                                JobRequestType.VERIFY,
                                                params,
                                                null,
                                                JobChunkClientProcessingOrderGuarantee.NONE ) );
                                    }
                                    else
                                    {
                                        throw new UnsupportedOperationException(
                                                "Tried to call method " + method.getName()
                                                + " on data planner, but we don't know "
                                                + "how to handle this method." );
                                    }
                                }
                                
                                if ( "getBlobsInCache".equals( method.getName() ) )
                                {
                                    final BlobsInCacheInformation retval = 
                                            BeanFactory.newBean( BlobsInCacheInformation.class );
                                    retval.setBlobsInCache( (UUID[])Array.newInstance( UUID.class, 0 ) );
                                    return new RpcResponse<>( retval );
                                }
                                
                                if ( "cleanUpCompletedJobsAndJobChunks".equals( method.getName() ) )
                                {
                                    m_dbSupport.getServiceManager().getService( JobService.class )
                                        .cleanUpCompletedJobsAndJobChunks( 
                                                InterfaceProxyFactory.getProxy( 
                                                        JobProgressManager.class, null ),
                                                InterfaceProxyFactory.getProxy( 
                                                        TapeEjector.class, null ),
                                                new Object() );
                                    return new RpcResponse<>( null );
                                }
                                
                                final RpcFuture< ? > future = new RpcResponse<>( null );
                                return future;
                            }
                        },
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args ) 
                                    throws Throwable
                            {
                                final InvocationHandler plannerInterfaceIh = m_plannerInterfaceIh;
                                if ( null == plannerInterfaceIh )
                                {
                                    return NullInvocationHandler.getInstance().invoke( proxy, method, args );
                                }
                                return plannerInterfaceIh.invoke( proxy, method, args );
                            }
                        } ) );
        m_dataPolicyInterfaceBtih = 
                new BasicTestsInvocationHandler( MockInvocationHandler.forReturnType(
                        RpcFuture.class,
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args ) 
                                    throws Throwable
                            {
                                final InvocationHandler dataPolicyInterfaceIh = m_dataPolicyInterfaceIh;
                                if ( null != dataPolicyInterfaceIh )
                                {
                                    final Object retval = dataPolicyInterfaceIh.invoke( proxy, method, args );
                                    if ( null != retval )
                                    {
                                        return retval;
                                    }
                                }
                                return method.invoke(
                                      new MockDataPolicyManagementResource( m_dbSupport.getServiceManager() ),
                                      args );
                            }
                        },
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args ) 
                                    throws Throwable
                            {
                                final InvocationHandler dataPolicyInterfaceIh = m_dataPolicyInterfaceIh;
                                if ( null == dataPolicyInterfaceIh )
                                {
                                    return NullInvocationHandler.getInstance().invoke( proxy, method, args );
                                }
                                return dataPolicyInterfaceIh.invoke( proxy, method, args );
                            }
                        } ) );
        m_tapeInterfaceBtih = 
                new BasicTestsInvocationHandler( MockInvocationHandler.forReturnType(
                        RpcFuture.class,
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args ) 
                                    throws Throwable
                            {
                                final InvocationHandler tapeInterfaceIh = m_tapeInterfaceIh;
                                if ( null != tapeInterfaceIh )
                                {
                                    final Object retval = tapeInterfaceIh.invoke( proxy, method, args );
                                    if ( null != retval )
                                    {
                                        return retval;
                                    }
                                }
                                if ( TapeFailuresInformation.class == method.getAnnotation( 
                                        RpcMethodReturnType.class ).value() )
                                {
                                    if ( null == args[ 0 ] )
                                    {
                                        return new RpcResponse<>(
                                                BeanFactory.newBean( TapeFailuresInformation.class )
                                                .setFailures( CollectionFactory.toArray(
                                                        TapeFailureInformation.class, 
                                                        new HashSet< TapeFailureInformation >() ) ) );
                                    }
                                    return new RpcResponse<>( null );
                                }
                                
                                final RpcFuture< ? > future = new RpcResponse<>( null );
                                return future;
                            }
                        },
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args ) 
                                    throws Throwable
                            {
                                final InvocationHandler tapeInterfaceIh = m_tapeInterfaceIh;
                                if ( null == tapeInterfaceIh )
                                {
                                    return NullInvocationHandler.getInstance().invoke( proxy, method, args );
                                }
                                return tapeInterfaceIh.invoke( proxy, method, args );
                            }
                        } ) );
        m_poolInterfaceBtih = 
                new BasicTestsInvocationHandler( MockInvocationHandler.forReturnType(
                        RpcFuture.class,
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args ) 
                                    throws Throwable
                            {
                                final InvocationHandler poolInterfaceIh = m_poolInterfaceIh;
                                if ( null != poolInterfaceIh )
                                {
                                    final Object retval = poolInterfaceIh.invoke( proxy, method, args );
                                    if ( null != retval )
                                    {
                                        return retval;
                                    }
                                }
                                
                                final RpcFuture< ? > future = new RpcResponse<>( null );
                                return future;
                            }
                        },
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args ) 
                                    throws Throwable
                            {
                                final InvocationHandler poolInterfaceIh = m_poolInterfaceIh;
                                if ( null == poolInterfaceIh )
                                {
                                    return NullInvocationHandler.getInstance().invoke( proxy, method, args );
                                }
                                return poolInterfaceIh.invoke( proxy, method, args );
                            }
                        } ) );
        m_targetInterfaceBtih = 
                new BasicTestsInvocationHandler( MockInvocationHandler.forReturnType(
                        RpcFuture.class,
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args ) 
                                    throws Throwable
                            {
                                final InvocationHandler targetInterfaceIh = m_targetInterfaceIh;
                                if ( null != targetInterfaceIh )
                                {
                                    final Object retval = targetInterfaceIh.invoke( proxy, method, args );
                                    if ( null != retval )
                                    {
                                        return retval;
                                    }
                                }

                                return method.invoke(
                                      new MockTargetManagementResource( m_dbSupport.getServiceManager(),
                                              new MockDataPolicyManagementResource( m_dbSupport.getServiceManager() ) ),
                                      args );
                            }
                        },
                        new InvocationHandler()
                        {
                            public Object invoke( Object proxy, Method method, Object[] args ) 
                                    throws Throwable
                            {
                                final InvocationHandler targetInterfaceIh = m_targetInterfaceIh;
                                if ( null == targetInterfaceIh )
                                {
                                    return NullInvocationHandler.getInstance().invoke( proxy, method, args );
                                }
                                return targetInterfaceIh.invoke( proxy, method, args );
                            }
                        } ) );
        m_requestDispatcher = new RequestDispatcherImpl(
                m_dbSupport.getServiceManager(),
                InterfaceProxyFactory.getProxy( 
                        DataPlannerResource.class,
                        new ValidatingRpcResourceInvocationHandler( m_plannerInterfaceBtih ) ),
                InterfaceProxyFactory.getProxy( 
                        DataPolicyManagementResource.class,
                        new ValidatingRpcResourceInvocationHandler( m_dataPolicyInterfaceBtih ) ),
                InterfaceProxyFactory.getProxy( 
                        TapeManagementResource.class,
                        new ValidatingRpcResourceInvocationHandler( m_tapeInterfaceBtih ) ),
                InterfaceProxyFactory.getProxy( 
                        PoolManagementResource.class,
                        new ValidatingRpcResourceInvocationHandler( m_poolInterfaceBtih ) ),
                InterfaceProxyFactory.getProxy( 
                        TargetManagementResource.class,
                        new ValidatingRpcResourceInvocationHandler( m_targetInterfaceBtih ) )  );
    }
    
    
    RequestDispatcher getRequestDispatcher()
    {
        return m_requestDispatcher;
    }
    
    
    public DatabaseSupport getDatabaseSupport()
    {
        return m_dbSupport;
    }
    
    
    public InvocationHandler getPlannerInterfaceIh()
    {
        return m_plannerInterfaceIh;
    }
    
    
    public BasicTestsInvocationHandler getPlannerInterfaceBtih()
    {
        return m_plannerInterfaceBtih;
    }
    
    
    public BasicTestsInvocationHandler getDataPolicyBtih()
    {
        return m_dataPolicyInterfaceBtih;
    }
    
    
    public BasicTestsInvocationHandler getTapeInterfaceBtih()
    {
        return m_tapeInterfaceBtih;
    }
    
    
    public BasicTestsInvocationHandler getPoolInterfaceBtih()
    {
        return m_poolInterfaceBtih;
    }
    
    
    public BasicTestsInvocationHandler getTargetInterfaceBtih()
    {
        return m_targetInterfaceBtih;
    }
    
    
    public BasicTestsInvocationHandler getNotificationEventDispatcherBtih()
    {
        return m_nedBtih;
    }
    
    
    public void setPlannerInterfaceIh( final InvocationHandler ih )
    {
        m_plannerInterfaceIh = ih;
    }
    
    
    public void setDataPolicyInterfaceIh( final InvocationHandler ih )
    {
        m_dataPolicyInterfaceIh = ih;
    }
    
    
    public void setTapeInterfaceIh( final InvocationHandler ih )
    {
        m_tapeInterfaceIh = ih;
    }
    
    
    public void setPoolInterfaceIh( final InvocationHandler ih )
    {
        m_poolInterfaceIh = ih;
    }
    
    
    public void setTargetInterfaceIh( final InvocationHandler ih )
    {
        m_targetInterfaceIh = ih;
    }
    
    
    private UUID fakeCreateGetJob(
            final JobRequestType requestType,
            final CreateVerifyJobParams params,
            final UUID customJobId,
            final JobChunkClientProcessingOrderGuarantee chunkClientProcessingOrderGuarantee )
    {
        final UUID [] blobIdsToGet = params.getBlobIds();
        final S3ObjectService objectService =
                m_dbSupport.getServiceManager().getService( S3ObjectService.class );
        final BlobService blobService =
                m_dbSupport.getServiceManager().getService( BlobService.class );
        final Blob[] blobsToGet = new Blob[blobIdsToGet.length];
        UUID bucketId = null;
        for ( int i = 0; i < blobIdsToGet.length; i++ )
        {
            blobsToGet[i] = blobService.attain( blobIdsToGet[i] );
            final UUID thisBucketId = objectService.attain( blobsToGet[ i ].getObjectId() ).getBucketId();
            if ( bucketId == null )
            {
                bucketId = thisBucketId;
            }
            else if ( !bucketId.equals( thisBucketId ) )
            {
                throw new RuntimeException( "Can't create single job with multiple buckets." );
            }
        }

        final Job job = BeanFactory.newBean( Job.class )
                .setRequestType( requestType )
                .setBucketId( bucketId )
                .setUserId( params.getUserId() )
                .setPriority( params.getPriority() )
                .setChunkClientProcessingOrderGuarantee( chunkClientProcessingOrderGuarantee )
                .setAggregating( params.isAggregating() )
                .setNaked( params.isNaked() )
                .setImplicitJobIdResolution( params.isImplicitJobIdResolution() )
                .setName( params.getName() );
        job.setId( customJobId );
        m_dbSupport.getServiceManager().getService( JobService.class ).create( job );
        
        for ( int i = 0; i < blobIdsToGet.length; ++i )
        {
            final int chunkNumber = (int)m_dbSupport.getDataManager().getMax(
                    JobEntry.class,
                    JobEntry.CHUNK_NUMBER,
                    Require.nothing() ) + 1;
            final JobEntry chunk = BeanFactory.newBean( JobEntry.class )
                    .setChunkNumber( chunkNumber ).setJobId( job.getId() ).setBlobId( blobsToGet[ i ].getId() );
            m_dbSupport.getDataManager().createBean( chunk );
        }
        return job.getId();
    }

    private PersistenceTargetInfo createPersistenceTargetInfo(final UUID dataPolicyId, final DatabaseSupport dbSupport) {
        final BeansServiceManager serviceManager = dbSupport.getServiceManager();
        final MockDaoDriver driver = new MockDaoDriver(dbSupport);
        driver.createStorageDomain("sd1");
        final Set<UUID> storageDomainIds = BeanUtils.toMap(serviceManager.getRetriever(StorageDomain.class).retrieveAll().toSet()).keySet();
        final PersistenceTargetInfo pti = BeanFactory.newBean(PersistenceTargetInfo.class);
        pti.setStorageDomainIds(CollectionFactory.toArray(UUID.class, storageDomainIds));
        return pti;
    }


    private volatile InvocationHandler m_plannerInterfaceIh;
    private volatile InvocationHandler m_dataPolicyInterfaceIh;
    private volatile InvocationHandler m_tapeInterfaceIh;
    private volatile InvocationHandler m_poolInterfaceIh;
    private volatile InvocationHandler m_targetInterfaceIh;
    
    private final DatabaseSupport m_dbSupport;
    private final RequestDispatcher m_requestDispatcher;
    private final BasicTestsInvocationHandler m_plannerInterfaceBtih;
    private final BasicTestsInvocationHandler m_dataPolicyInterfaceBtih;
    private final BasicTestsInvocationHandler m_tapeInterfaceBtih;
    private final BasicTestsInvocationHandler m_poolInterfaceBtih;
    private final BasicTestsInvocationHandler m_targetInterfaceBtih;
    private final BasicTestsInvocationHandler m_nedBtih = new BasicTestsInvocationHandler( null );
}
