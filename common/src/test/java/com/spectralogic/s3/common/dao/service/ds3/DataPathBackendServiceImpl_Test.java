/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import java.util.Date;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.DataPathBackend;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

public final class DataPathBackendServiceImpl_Test
{
    @Test
    public void testDataPathRestartedWorksCorrectlyWhenAutoActivationDisabled()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final DataPathBackendService service =
                dbSupport.getServiceManager().getService( DataPathBackendService.class );
        final DataPathBackend dpb = mockDaoDriver.attainOneAndOnly( DataPathBackend.class );
        assertTrue(
                service.isActivated(),
                "Shoulda activated by default.");
        assertTrue(
                dpb.isActivated(),
                "Shoulda activated by default.");
        assertNull(
                dpb.getAutoActivateTimeoutInMins(),
                "Shoulda configured auto activation by default."
                 );

        mockDaoDriver.updateBean(
                dpb.setAutoActivateTimeoutInMins( null ),
                DataPathBackend.AUTO_ACTIVATE_TIMEOUT_IN_MINS );
        service.dataPathRestarted();
        assertTrue(
                service.isActivated(),
                "Should notta auto-activated."
                );
        assertNull(
                mockDaoDriver.attain( dpb ).getAutoActivateTimeoutInMins(),
                "Shoulda cleared auto activate."
                 );
    }


    @Test
    public void testDataPathRestartedWorksCorrectlyWhenDataPathRestartedWithinAutoActivationTimeout()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final DataPathBackendService service =
                dbSupport.getServiceManager().getService( DataPathBackendService.class );
        final DataPathBackend dpb = mockDaoDriver.attainOneAndOnly( DataPathBackend.class );
        assertTrue(
                service.isActivated(),
                "Shoulda activated by default.");
        assertTrue(
                dpb.isActivated(),
                "Shoulda activated by default.");
        assertNull(
                dpb.getAutoActivateTimeoutInMins(),
                "Shoulda configured auto activation by default."
                 );

        mockDaoDriver.updateBean(
                dpb.setLastHeartbeat( new Date( System.currentTimeMillis() - 1000L * 60 * 3 ) ),
                DataPathBackend.LAST_HEARTBEAT );
        service.dataPathRestarted();
        assertTrue(
                service.isActivated(),
                "Shoulda auto-activated.");
        assertNull(
                mockDaoDriver.attain( dpb ).getAutoActivateTimeoutInMins(),
                "Should notta cleared auto activate."
                );
    }


    @Test
    public void testDataPathRestartedWorksCorrectlyWhenDataPathRestartedOutsideAutoActivationTimeout()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final DataPathBackendService service =
                dbSupport.getServiceManager().getService( DataPathBackendService.class );
        final DataPathBackend dpb = mockDaoDriver.attainOneAndOnly( DataPathBackend.class );
        assertTrue(
                service.isActivated(),
                "Shoulda activated by default.");
        assertTrue(
                dpb.isActivated(),
                "Shoulda activated by default.");
        assertNull(
                dpb.getAutoActivateTimeoutInMins(),
                "Shoulda configured auto activation by default."
                 );

        mockDaoDriver.updateBean(
                dpb.setLastHeartbeat( new Date( System.currentTimeMillis() - 1000L * 60 * 60 * 24 * 10 ) ),
                DataPathBackend.LAST_HEARTBEAT );
        service.dataPathRestarted();
        assertTrue(
                service.isActivated(),
                "Should notta auto-activated.");
        assertNull(
                mockDaoDriver.attain( dpb ).getAutoActivateTimeoutInMins(),
                "Shoulda cleared auto activate."
                 );
    }
}
