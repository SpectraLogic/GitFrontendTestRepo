/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainFailureType;
import com.spectralogic.s3.common.dao.domain.ds3.SystemFailureType;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolState;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.shared.PoolObservable;
import com.spectralogic.s3.common.dao.orm.PoolRM;
import com.spectralogic.s3.common.dao.service.ds3.StorageDomainFailureService;
import com.spectralogic.s3.common.dao.service.ds3.SystemFailureService;
import com.spectralogic.s3.common.dao.service.pool.PoolService;
import com.spectralogic.s3.common.dao.service.shared.ActiveFailures;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.common.rpc.pool.domain.PoolEnvironmentInformation;
import com.spectralogic.s3.common.rpc.pool.domain.PoolInformation;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.util.bean.BeanCopier;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.lang.Duration;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.MockInvocationHandler;
import com.spectralogic.util.net.rpc.frmwrk.RpcFuture.Timeout;

final class PoolEnvironmentManager implements Runnable
{
    PoolEnvironmentManager( 
            final PoolEnvironmentResource poolEnvironmentResource,
            final BeansServiceManager serviceManager,
            final PoolLockSupport< PoolTask > lockSupport )
    {
        m_rpcResource = poolEnvironmentResource;
        m_serviceManager = serviceManager;
        m_lockSupport = lockSupport;
        Validations.verifyNotNull( "RPC resource", m_rpcResource );
        Validations.verifyNotNull( "Service manager", m_serviceManager );
        Validations.verifyNotNull( "Lock support", m_lockSupport );
    }
    
    
    @Override
    synchronized public void run()
    {
        final ActiveFailures failures = 
                m_serviceManager.getService( SystemFailureService.class ).startActiveFailures(
                        SystemFailureType.RECONCILE_POOL_ENVIRONMENT_FAILED );
        try
        {
            runInternal();
        }
        catch ( final RuntimeException ex )
        {
            failures.add( ex );
            throw ex;
        }
        finally
        {
            failures.commit();
        }
    }
    
    
    private void runInternal()
    {
        m_needsAnotherRun = false;
        final Duration duration = new Duration();
        final PoolEnvironmentInformation poolEnvironment = 
                m_rpcResource.getPoolEnvironment().get( Timeout.DEFAULT );
        
        final Map< String, Pool > dbPools = new HashMap<>();
        for ( final Pool pool : m_serviceManager.getRetriever( Pool.class ).retrieveAll().toSet() )
        {
            dbPools.put( pool.getGuid(), pool );
        }
        
        for ( final PoolInformation rpcPool : poolEnvironment.getPools() )
        {
            final Pool dbPool;
            if ( dbPools.containsKey( rpcPool.getGuid() ) )
            {
                dbPool = dbPools.remove( rpcPool.getGuid() );
            }
            else
            {
                dbPool = BeanFactory.newBean( Pool.class ).setState( PoolState.FOREIGN );
                BeanCopier.copy( dbPool, rpcPool );
                m_serviceManager.getService( PoolService.class ).create( dbPool );
            }
            
            BeanCopier.copy( dbPool, rpcPool );
            updatePool( dbPool, getPoolState( dbPool, rpcPool ) );
        }
        
        for ( final Pool lostPool : dbPools.values() )
        {
            poolDowned( lostPool );
        }
        
        LOG.info( "Reconciled pool environment in " + duration + ".  There were " 
                + poolEnvironment.getPools().length + " pools." );
    }
    
    
    private PoolState getPoolState( final Pool dbPool, final PoolInformation rpcPool )
    {
        if ( dbPool.getId().equals( rpcPool.getPoolId() ) )
        {
            return PoolState.NORMAL;
        }
        if ( null != rpcPool.getPoolId() )
        {
        	switch ( dbPool.getState() )
        	{
        		case IMPORT_IN_PROGRESS:
        		case IMPORT_PENDING:
        			return dbPool.getState();
        		default:
        			return PoolState.FOREIGN;
        	}
        }
        
        LOG.info( "Pool " + dbPool.getId() + " is unowned.  Will take ownership of it." );
        final PoolTask lockHolder = InterfaceProxyFactory.getProxy( 
                PoolTask.class,
                MockInvocationHandler.forToString( "Take ownership of pool" ) );
        try
        {
            m_lockSupport.acquireWriteLock( dbPool.getId(), lockHolder, 0, dbPool.getAvailableCapacity());
        }
        catch ( final Exception ex )
        {
            m_needsAnotherRun = true;
            LOG.info( "Cannot acquire write lock, so cannot take ownership at this time.", ex );
            return PoolState.BLANK;
        }
        try
        {
            m_rpcResource.takeOwnershipOfPool( dbPool.getGuid(), dbPool.getId() ).get( Timeout.LONG );
        }
        finally
        {
            m_lockSupport.releaseLock( lockHolder );
        }
        return PoolState.NORMAL;
    }
    
    
    private void updatePool( final Pool dbPool, final PoolState newState )
    {
        final Set< String > propertiesToModify = CollectionFactory.toSet( 
                PoolObservable.AVAILABLE_CAPACITY,
                PoolObservable.HEALTH,
                PoolObservable.POWERED_ON,
                PoolObservable.RESERVED_CAPACITY,
                PoolObservable.TOTAL_CAPACITY,
                PoolObservable.USED_CAPACITY,
                NameObservable.NAME );
        if ( newState != dbPool.getState() )
        {
            dbPool.setState( newState );
            propertiesToModify.add( Pool.STATE );
        }
        
        m_serviceManager.getService( PoolService.class ).update( 
                dbPool, 
                CollectionFactory.toArray( String.class, propertiesToModify ) );
    }
    
    
    private void poolDowned( final Pool pool )
    {
        if ( PoolState.LOST == pool.getState() )
        {
            return;
        }
        
        LOG.warn( "Pool down: " + pool );
        m_serviceManager.getService( PoolService.class ).update(
                pool.setState( PoolState.LOST ),
                Pool.STATE );
        if ( null != pool.getStorageDomainMemberId() )
        {
            final StorageDomain storageDomain = 
                    new PoolRM( pool, m_serviceManager ).getStorageDomainMember().getStorageDomain().unwrap();
            if ( !storageDomain.isMediaEjectionAllowed() )
            {
                m_serviceManager.getService( StorageDomainFailureService.class ).create(
                        storageDomain.getId(),
                        StorageDomainFailureType.ILLEGAL_EJECTION_OCCURRED, 
                        "Pool " + pool.getId() + " (" + pool.getGuid() 
                        + ") was illegally ejected.  It was allocated to storage domain " 
                        + storageDomain.getName() + ", which does not permit media ejection.  ",
                        null );
            }
        }
    }
    
    
    public boolean needsAnotherRun()
    {
        return m_needsAnotherRun;
    }
    

    private volatile boolean m_needsAnotherRun;
    private final PoolEnvironmentResource m_rpcResource;
    private final BeansServiceManager m_serviceManager;
    private final PoolLockSupport< PoolTask > m_lockSupport;
    
    private final static Logger LOG = Logger.getLogger( PoolEnvironmentManager.class );
}
