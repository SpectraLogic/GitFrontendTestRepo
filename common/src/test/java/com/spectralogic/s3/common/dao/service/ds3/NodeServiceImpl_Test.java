/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Date;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.platform.lang.HardwareInformationProvider;
import com.spectralogic.util.db.service.BeansServiceManagerImpl;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class NodeServiceImpl_Test 
{
    @Test
    public void testNodeCreatedIfNecessaryForThisNode()
    {
        final Date start = new Date();
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final Date end = new Date();
        
        final NodeService nodeService = dbSupport.getServiceManager().getService( NodeService.class );
        assertNotNull(
                nodeService.getThisNode(),
                "Shoulda reported what this node was."
                 );
        assertTrue(nodeService.getThisNode().getLastHeartbeat().getTime() >= start.getTime(), "Last heartbeat date should be when the service came up and initialized.");
        assertTrue(nodeService.getThisNode().getLastHeartbeat().getTime() <= end.getTime(), "Last heartbeat date should be when the service came up and initialized.");

        TestUtil.sleep( 1 );
        final Date start2 = new Date();
        final BeansServiceManager serviceManager2 = BeansServiceManagerImpl.create( 
                dbSupport.getServiceManager().getNotificationEventDispatcher(),
                dbSupport.getDataManager(), 
                CollectionFactory.< Class< ? > >toSet( NodeService.class ) );
        assertTrue(serviceManager2.getService( NodeService.class ).getThisNode().getLastHeartbeat().getTime() 
                >= start2.getTime(), "Last heartbeat date should be when the service came up and initialized.");
        final Object expected2 = HardwareInformationProvider.getSerialNumber();
        assertEquals(expected2, nodeService.getThisNode().getSerialNumber(), "Serial number for node shoulda equaled what the hardware information provider had.");
        final Object expected1 = serviceManager2.getService( NodeService.class ).getThisNode().getId();
        assertEquals(expected1, nodeService.getThisNode().getId(), "Instance in db shoulda been preserved across service managers.");
        final Object expected = HardwareInformationProvider.getSerialNumber();
        assertEquals(expected, nodeService.getThisNode().getName(), "Name shoulda defaulted to the node serial number.");
    }
    
    
    @Test
    public void testTransactionReturnsThisNodeCorrectly()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final BeansServiceManager transaction = dbSupport.getServiceManager().startTransaction();
        assertSame(
                dbSupport.getServiceManager().getService( NodeService.class ).getThisNode(),
                transaction.getService( NodeService.class ).getThisNode(),
                "Transaction shoulda delegated to source to get the node instance."
                 );
        transaction.closeTransaction();
    }
}
