/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.frontend;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.ImportPoolDirective;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.service.pool.BlobPoolService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.rpc.dataplanner.PoolManagementResource;
import com.spectralogic.s3.common.rpc.dataplanner.domain.ImportPersistenceTargetDirectiveRequest;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolBlobStore;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.FailureTypeObservableException;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.lang.iterate.EnhancedIterable;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture;
import com.spectralogic.util.net.rpc.server.BaseRpcResource;
import com.spectralogic.util.net.rpc.server.RpcServer;

public final class PoolManagementResourceImpl extends BaseRpcResource implements PoolManagementResource
{
    public PoolManagementResourceImpl( 
            final RpcServer rpcServer, 
            final PoolBlobStore poolBlobStore,
            final BeansServiceManager serviceManager )
    {
        m_poolBlobStore = poolBlobStore;
        m_serviceManager = serviceManager;
        Validations.verifyNotNull( "Pool blob store", m_poolBlobStore );
        Validations.verifyNotNull( "Service manager", m_serviceManager );
        
        rpcServer.register( null, this );
    }

    
    public RpcFuture< ? > compactPool( final UUID poolId, BlobStoreTaskPriority priority )
    {
        if ( null == priority )
        {
            priority = BlobStoreTaskPriority.BACKGROUND;
        }
        
        m_poolBlobStore.compactPool( priority, poolId );
        return null;
    }
    
    
    public RpcFuture< ? > formatPool( final UUID poolId )
    {
        m_poolBlobStore.formatPool( poolId );
        return null;
    }
    
    
    public RpcFuture< ? > destroyPool( final UUID poolId )
    {
        m_poolBlobStore.destroyPool( poolId );
        return null;
    }
    
    
    public RpcFuture< ? > importPool(
            final UUID poolId, 
            final ImportPersistenceTargetDirectiveRequest importDirective )
    {
        final ImportPoolDirective importPoolDirective = BeanFactory.newBean( ImportPoolDirective.class )
                .setPoolId( poolId );
        BeanCopier.copy( importPoolDirective, importDirective );
        m_poolBlobStore.importPool(
                importDirective.getPriority(),
                importPoolDirective );
        
        return null;
    }
    
    
    public RpcFuture< ? > cancelImportPool( final UUID poolId )
    {
        if ( null == poolId )
        {
            for ( final Pool pool : m_serviceManager.getRetriever( Pool.class ).retrieveAll(
                    Pool.STATE, PoolState.IMPORT_PENDING ).toSet() )
            {
                m_poolBlobStore.cancelImportPool( pool.getId() );
            }
            return null;
        }
        
        m_poolBlobStore.cancelImportPool( poolId );
        return null;
    }

    
    public RpcFuture< ? > verifyPool( final UUID poolId, BlobStoreTaskPriority priority )
    {
        if ( null == priority )
        {
            priority = BlobStoreTaskPriority.BACKGROUND;
        }
        
        m_poolBlobStore.verify( priority, poolId );
        return null;
    }
    
    
    public RpcFuture< ? > cancelVerifyPool( final UUID poolId )
    {
        if ( null == poolId )
        {
            for ( final Pool pool : m_serviceManager.getRetriever( Pool.class ).retrieveAll().toSet() )
            {
                try
                {
                    m_poolBlobStore.cancelVerifyPool( pool.getId() );
                }
                catch ( final RuntimeException ex )
                {
                    LOG.info( "Failed to cancel verify on pool " + pool.getId() + ".", ex );
                }
            }
        }
        else
        {
            m_poolBlobStore.cancelVerifyPool( poolId );
        }
        
        return null;
    }

    
    public RpcFuture< ? > forcePoolEnvironmentRefresh()
    {
        m_poolBlobStore.refreshEnvironmentNow();
        return null;
    }


    public RpcFuture< ? > deletePermanentlyLostPool( final UUID poolId )
    {
        synchronized ( m_poolBlobStore.getEnvironmentStateLock() )
        {
            final BeansServiceManager transaction = m_serviceManager.startTransaction();
            try
            {
                deletePermanentlyLostPersistenceTarget( transaction, poolId );
                transaction.commitTransaction();
            }
            finally
            {
                transaction.closeTransaction();
            }
        }
        
        return null;
    }
    
    
    public void deletePermanentlyLostPersistenceTarget(
            final BeansServiceManager transaction,
            final UUID poolId )
    {
        final Pool pool = transaction.getRetriever( Pool.class ).attain( poolId );
        if ( PoolState.LOST != pool.getState() )
        {
            throw new FailureTypeObservableException(
                    GenericFailure.CONFLICT, 
                    "Only " + PoolState.LOST + " pools can be permanently deleted." );
        }
        
    	// create a new connection for processing the lost blobs to prevent a deadlock from
    	// occurring over the db connection held by the EnhancedIterable
    	final BeansServiceManager subTransaction = m_serviceManager.startTransaction();
        try ( final EnhancedIterable< BlobPool > iterable = 
                transaction.getRetriever( BlobPool.class ).retrieveAll(
                        Require.beanPropertyEquals( BlobPool.POOL_ID, poolId ) ).toIterable() )
        {
            final Set< UUID > blobIds = new HashSet<>();
            for ( final BlobPool bp : iterable )
            {
                blobIds.add( bp.getBlobId() );
                if ( 10000 == blobIds.size() )
                {
                    deletePermanentlyLostPersistenceTarget( subTransaction, poolId, blobIds );
                    blobIds.clear();
                }
            }
            deletePermanentlyLostPersistenceTarget( subTransaction, poolId, blobIds );
            subTransaction.commitTransaction();
        }
        finally
        {
        	subTransaction.closeTransaction();
        }
        transaction.getService( PoolService.class ).delete( poolId );
    }
    
    
    private void deletePermanentlyLostPersistenceTarget( 
            final BeansServiceManager transaction,
            final UUID poolId,
            final Set< UUID > blobIds )
    {
        transaction.getService( BlobPoolService.class ).blobsLost( 
                "Pool has been marked as permanently lost.", 
                poolId, 
                blobIds );
    }
    
    
    private final PoolBlobStore m_poolBlobStore;
    private final BeansServiceManager m_serviceManager;
    
    private final static Logger LOG = Logger.getLogger( PoolManagementResourceImpl.class );
}
