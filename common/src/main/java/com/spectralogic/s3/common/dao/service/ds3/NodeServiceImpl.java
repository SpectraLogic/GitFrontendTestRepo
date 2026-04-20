/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;


import com.spectralogic.s3.common.dao.domain.ds3.Node;
import com.spectralogic.s3.common.dao.domain.shared.SerialNumberObservable;
import com.spectralogic.s3.common.platform.lang.HardwareInformationProvider;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.db.service.api.BeansRetrieverInitializer;
import com.spectralogic.util.thread.RecurringRunnableExecutor;

final class NodeServiceImpl extends BaseService< Node > implements NodeService
{
    NodeServiceImpl()
    {
        super( Node.class );
        addInitializer( new NodeServiceInitializer() );
    }
    
    
    private final class NodeServiceInitializer implements BeansRetrieverInitializer
    {
        @Override
        public void initialize()
        {
            final Node node = retrieve(
                    SerialNumberObservable.SERIAL_NUMBER,
                    HardwareInformationProvider.getSerialNumber() );
            if ( null == node )
            {
                create( BeanFactory.newBean( Node.class )
                        .setName( HardwareInformationProvider.getSerialNumber() )
                        .setSerialNumber( HardwareInformationProvider.getSerialNumber() ) );
            }
            
            final Node thisNode = attain(
                    SerialNumberObservable.SERIAL_NUMBER,
                    HardwareInformationProvider.getSerialNumber() );
            m_heartbeatSender = new HeartbeatSender( thisNode, NodeServiceImpl.this );
            m_heartbeatSender.run();
            
            m_heartbeatSenderExecutor = new RecurringRunnableExecutor(
                    m_heartbeatSender, 
                    Node.INTERVAL_BETWEEN_HEARTBEATS_IN_SECS * 1000 ).start();
        }

        // We store the executor to prevent its premature garbage collection
        @SuppressWarnings( "unused" )
        private volatile Object m_heartbeatSenderExecutor; 
    } // end inner class def


    public Node getThisNode()
    {
        if ( getServiceManager().isTransaction() )
        {
            return getServiceManager().getTransactionSource().getService( NodeService.class ).getThisNode();
        }
        return m_heartbeatSender.getThisNode();
    }
    
    
    private volatile HeartbeatSender m_heartbeatSender;
}
