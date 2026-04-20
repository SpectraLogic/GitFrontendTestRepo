/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.lang.ref.WeakReference;
import java.util.Date;

import com.spectralogic.s3.common.dao.domain.ds3.Node;
import com.spectralogic.s3.common.platform.lang.ConfigurationInformationProvider;
import com.spectralogic.util.thread.wp.SystemWorkPool;

final class HeartbeatSender implements Runnable
{
    HeartbeatSender( final Node thisNode, final NodeServiceImpl service )
    {
        m_node = thisNode;
        m_service = new WeakReference<>( service );
    }
    
    
    Node getThisNode()
    {
        return m_node;
    }
    
    
    public void run()
    {
        final NodeServiceImpl service = m_service.get();
        if ( null == service )
        {
            return;
        }
        
        service.update(
                m_node.setLastHeartbeat( new Date() ), 
                Node.LAST_HEARTBEAT );
        if ( m_firstRun )
        {
            m_firstRun = false;
            return;
        }
        
        SystemWorkPool.getInstance().submit( new Runnable()
        {
            public void run()
            {
                final ConfigurationInformationProvider cip = 
                        ConfigurationInformationProvider.getInstance();
                service.update( 
                        m_node.setDataPathIpAddress( cip.getDataPathIpAddress() )
                                  .setDataPathHttpPort( cip.getDataPathHttpPort() )
                                  .setDataPathHttpsPort( cip.getDataPathHttpsPort() ),
                        Node.DATA_PATH_IP_ADDRESS, Node.DATA_PATH_HTTP_PORT, Node.DATA_PATH_HTTPS_PORT );
            }
        } );   
    }
    

    private volatile boolean m_firstRun = true;
    private final Node m_node;
    private final WeakReference< NodeServiceImpl > m_service;
} // end inner class def