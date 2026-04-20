/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.AutoInspectMode;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.domain.ds3.UnavailableMediaUsagePolicy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.BeansRetrieverInitializer;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.thread.RecurringRunnableExecutor;

final class DataPathBackendServiceImpl
    extends BaseService< DataPathBackend > implements DataPathBackendService
{
    DataPathBackendServiceImpl()
    {
        super( DataPathBackend.class );
        addInitializer( new DataPathBackendInitializer() );
    }
    
    
    private final class DataPathBackendInitializer implements BeansRetrieverInitializer
    {
        @Override
        public void initialize()
        {
            final DataPathBackend dbp = retrieve( Require.nothing() );
            if ( null == dbp )
            {
                final UUID id = UUID.randomUUID();
                create( (DataPathBackend)BeanFactory.newBean( DataPathBackend.class )
                        .setActivated( true )
                        .setAutoInspect( AutoInspectMode.MINIMAL )
                        .setAutoActivateTimeoutInMins( null )
                        .setUnavailableMediaPolicy( UnavailableMediaUsagePolicy.DISALLOW )
                        .setInstanceId( id )
                        .setId( id ) );
                LOG.info( "Initialized data path backend configuration." );
            }
        }
    } // end inner class def
    
    
    public boolean isActivated()
    {
        return attain( Require.nothing() ).isActivated();
    }

    
    @Override
    public void dataPathRestarted()
    {
        final DataPathBackend dpb = attain( Require.nothing() );
        boolean activated = false;
        // If there is no timeout configured, then always activate the data path backend.
        // Deactivate the backend if the dataplanner has been down for more than the timeout period.
        if (dpb.getAutoActivateTimeoutInMins() == null) {
            activated = true;
        } else {
            final Date lastHeartbeat = dpb.getLastHeartbeat();
            if (lastHeartbeat != null) {
                final long millisSinceLastHeartbeat = System.currentTimeMillis() - dpb.getLastHeartbeat().getTime() ;
                final long timeoutInMillis = dpb.getAutoActivateTimeoutInMins() * 60 * 1000;
                activated = millisSinceLastHeartbeat < timeoutInMillis;
            }
        }

        update( dpb.setActivated( activated ), DataPathBackend.ACTIVATED );

        if ( !activated ) {
            update(dpb.setAutoActivateTimeoutInMins(0), DataPathBackend.AUTO_ACTIVATE_TIMEOUT_IN_MINS);
        }

        LOG.info( "Data path backend " + ( ( activated ) ? "has been auto-activated." : "is deactivated." ) );
        
        m_heartbeatSender.start();
    }
    
    
    private final class HeartbeatSenderWorker implements Runnable
    {
        public void run()
        {
            getDataManager().updateBeans(
                    CollectionFactory.toSet( DataPathBackend.LAST_HEARTBEAT ), 
                    BeanFactory.newBean( DataPathBackend.class ).setLastHeartbeat( new Date() ), 
                    Require.nothing() );
        }
    } // end inner class def
    
    
    private final RecurringRunnableExecutor m_heartbeatSender = 
            new RecurringRunnableExecutor( new HeartbeatSenderWorker(), 1000 * 30 );
}
