/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner.backend.target.task;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BlobStoreTaskPriority;
import com.spectralogic.s3.common.dao.domain.shared.Quiesced;
import com.spectralogic.s3.common.dao.domain.target.ReplicationTarget;
import com.spectralogic.s3.common.dao.domain.target.TargetState;
import com.spectralogic.s3.common.dao.service.ds3.JobProgressManager;
import com.spectralogic.s3.common.platform.cache.DiskManager;
import com.spectralogic.s3.common.rpc.dataplanner.domain.BlobStoreTaskState;
import com.spectralogic.s3.dataplanner.backend.api.BlobStoreTaskNoLongerValidException;
import com.spectralogic.s3.dataplanner.backend.frmwrk.BaseTask;
import com.spectralogic.s3.dataplanner.backend.target.api.TargetTask;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.service.api.BeansServiceManager;

abstract class BaseTargetTask< T extends DatabasePersistable & ReplicationTarget< T >, CF >
    extends BaseTask implements TargetTask< T, CF >
{
    protected BaseTargetTask( final Class< T > targetType,
                              final UUID targetId,
                              final DiskManager diskManager,
                              final JobProgressManager jobProgressManager,
                              final BeansServiceManager serviceManager,
                              final CF connectionFactory,
                              final BlobStoreTaskPriority priority )
    {
        super( priority, serviceManager );
        m_targetType = targetType;
        m_targetId = targetId;
        m_diskManager = diskManager;
        m_connectionFactory = connectionFactory;
        m_jobProgressManager = jobProgressManager;
    }
    
    
    final public void prepareForExecutionIfPossible()
    {
        if ( null == m_serviceManager )
        {
            throw new IllegalStateException( "Init params were never set." );
        }
        if ( BlobStoreTaskState.READY != getRawState() )
        {
            throw new IllegalStateException(
                    "Cannot prepare for start when " + this + " is in state " + getState() + "." );
        }

        try
        {
            LOG.info( "Preparing to execute " + this + "..." );
            if (!prepareForExecution()) {
                return;
            }
        }
        catch ( final RuntimeException ex )
        {
            try
            {
                invalidateTaskAndThrow( ex );
            }
            catch ( final BlobStoreTaskNoLongerValidException ex2 )
            {
                LOG.warn( "Task threw exception selecting a target, so it's invalid now: " + getDescription(),
                        ex2 );
                return;
            }
        }

        final T target = getTarget();
        if ( TargetState.ONLINE == target.getState() && Quiesced.NO == target.getQuiesced() )
        {
            preparedForExecution();
            LOG.info( "Prepared to execute " + toString() + ".  Will use target: " + getTargetId() );
            return;
        }

        LOG.info( "Cannot execute " + toString() + " at this time since target " + target.getId() + " is " 
                  + target.getState() + ", "
                  + ReplicationTarget.QUIESCED + "=" + target.getQuiesced() + "." );
    }
    
    
    abstract protected boolean prepareForExecution();
    
    
    @Override
    protected void performPreRunValidations()
    {
        // empty
    }


    final protected CF getConnectionFactory()
    {
        return m_connectionFactory;
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
        return null;
    }
    

    public final String getTargetType()
    {
        return m_targetType.getSimpleName();
    }
    
    
    public final UUID getTargetId()
    {
        return m_targetId;
    }
    
    
    public final T getTarget()
    {
        return m_serviceManager.getRetriever( m_targetType ).attain( m_targetId );
    }
    

    private final CF m_connectionFactory;
    protected final DiskManager m_diskManager;
    protected final JobProgressManager m_jobProgressManager;
    protected final UUID m_targetId;
    
    protected final Class< T > m_targetType;
}
