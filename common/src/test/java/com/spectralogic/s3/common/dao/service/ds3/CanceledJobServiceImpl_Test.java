/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.ds3;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.CanceledJob;
import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.dao.domain.ds3.JobRequestType;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

public final class CanceledJobServiceImpl_Test 
{

    @Test
    public void testMarkAsTimedOutDoesSoWhenJobCanceled()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final CanceledJob job1 = mockDaoDriver.createCanceledJob( null, null, null, null );
        final CanceledJob job2 = mockDaoDriver.createCanceledJob( null, null, null, null );
        
        dbSupport.getServiceManager().getService( CanceledJobService.class ).markAsTimedOut( job1.getId() );

        assertTrue(mockDaoDriver.attain( job1 ).isCanceledDueToTimeout(), "Shoulda marked job1 only as being timed out.");
        assertFalse(
                mockDaoDriver.attain( job2 ).isCanceledDueToTimeout(),
                "Shoulda marked job1 only as being timed out."
                 );
    }
    
    
    @Test
    public void testMarkAsTimedOutDoesSoWhenJobTruncated()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        final Job job1 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        final Job job2 = mockDaoDriver.createJob( null, null, JobRequestType.GET );
        
        dbSupport.getServiceManager().getService( CanceledJobService.class ).markAsTimedOut( job1.getId() );

        assertTrue(mockDaoDriver.attain( job1 ).isTruncatedDueToTimeout(), "Shoulda marked job1 only as being timed out.");
        assertFalse(
                mockDaoDriver.attain( job2 ).isTruncatedDueToTimeout(),
                "Shoulda marked job1 only as being timed out."
                 );
    }
}
