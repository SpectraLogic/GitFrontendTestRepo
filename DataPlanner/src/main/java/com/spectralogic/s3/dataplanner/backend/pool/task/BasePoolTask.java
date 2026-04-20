/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.pool.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.common.rpc.pool.PoolEnvironmentResource;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskNoLongerValidException;
import com.spectralogic.s3.dataplanner.backend.frmwrk.BaseTask;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockSupport;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolLockingException;
import com.spectralogic.s3.dataplanner.backend.pool.api.PoolTask;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.exception.ExceptionUtil;
import lombok.NonNull;

abstract class BasePoolTask extends BaseTask implements PoolTask
{
    protected BasePoolTask(@NonNull final BlobStoreTaskPriority priority,
                           @NonNull final BeansServiceManager serviceManager,
                           @NonNull final PoolEnvironmentResource poolEnvironmentResource,
                           @NonNull final PoolLockSupport<PoolTask> lockSupport,
                           @NonNull final DiskManager diskManager,
                           @NonNull final JobProgressManager jobProgressManager)
    {
        super( priority, serviceManager);
        m_poolEnvironmentResource = poolEnvironmentResource;
        m_lockSupport = lockSupport;
        m_diskManager = diskManager;
        m_jobProgressManager = jobProgressManager;
    }
    
    
    final public void prepareForExecutionIfPossible()
    {
        if ( BlobStoreTaskState.READY != getRawState() )
        {
            throw new IllegalStateException(
                    "Cannot prepare for start when " + this + " is in state " + getState() + "." );
        }
        
        try
        {
            m_poolId = selectPool();
        }
        catch ( final RuntimeException ex )
        {
            try
            {
                if ( PoolLockingException.class.isAssignableFrom( ex.getClass() ) )
                {
                    LOG.info( "Cannot execute " + toString() + ": " 
                              + ExceptionUtil.getRootCauseReadableMessage( ex ) );
                    return;
                }
                invalidateTaskAndThrow( ex );
            }
            catch ( final BlobStoreTaskNoLongerValidException ex2 )
            {
                LOG.warn( "Task threw exception selecting a pool, so it's invalid now: " + getDescription(),
                          ex2 );
                m_poolId = null;
                getLockSupport().releaseLock( this );
            }
        }
        
        if ( null != getPoolId() )
        {
            preparedForExecution();
            LOG.info( "Prepared to execute " + toString() + ".  Will use pool: " + getPoolId() );
        }
    }
    
    
    abstract protected UUID selectPool();
    
    
    @Override
    protected void performPreRunValidations()
    {
        // empty
    }


    final protected PoolEnvironmentResource getPoolEnvironmentResource()
    {
        return m_poolEnvironmentResource;
    }
    
    
    final protected PoolLockSupport< PoolTask > getLockSupport()
    {
        return m_lockSupport;
    }
    
    
    final protected DiskManager getDiskManager()
    {
        return m_diskManager;
    }
    
    
    final protected JobProgressManager getJobProgressManager()
    {
        return m_jobProgressManager;
    }


    public final UUID getDriveId()
    {
        return null;
    }
    
    
    public final UUID getTapeId()
    {
        return null;
    }


    public final UUID getPoolId()
    {
        return m_poolId;
    }
    
    
    public final String getTargetType()
    {
        return null;
    }
    
    
    public final UUID getTargetId()
    {
        return null;
    }
    
    
    public final Pool getPool()
    {
        return getServiceManager().getRetriever( Pool.class ).attain( getPoolId() );
    }


    private volatile UUID m_poolId;
    
    private final PoolEnvironmentResource m_poolEnvironmentResource;
    private final PoolLockSupport< PoolTask > m_lockSupport;
    private final DiskManager m_diskManager;
    private final JobProgressManager m_jobProgressManager;
}
