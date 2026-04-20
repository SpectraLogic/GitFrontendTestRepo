/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.util.Date;

import com.spectralogic.s3.common.dao.domain.ds3.*;
import com.spectralogic.s3.common.dao.domain.ds3.JobEntry;
import com.spectralogic.s3.common.dao.service.ds3.JobService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.db.service.api.BeansServiceManager;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.marshal.DateMarshaler;

import org.junit.jupiter.api.Test;

public final class ModifyActiveJobRequestHandler_Test 
{
    @Test
    public void testModifyJobCreatedAtIntoThePastAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final Job job = mockDaoDriver
                .createJob( null, null, JobRequestType.PUT )
                .setOriginalSizeInBytes( 12345L );
        final S3Object object = mockDaoDriver.createObject(
                null,
                MockDaoDriver.DEFAULT_OBJECT_NAME,
                12345L );
        final Blob blob = mockDaoDriver.getBlobFor( object.getId() );
        final JobEntry chunk = mockDaoDriver.createJobEntry( job.getId() );
        mockDaoDriver.createJobEntries(CollectionFactory.toSet( blob ) );
        final BeansServiceManager serviceManager = support.getDatabaseSupport().getServiceManager();
        serviceManager.getService( JobService.class ).update( job, JobObservable.ORIGINAL_SIZE_IN_BYTES );

        Date date = new Date( 987654 );
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/active_job/" + job.getId().toString() ).addParameter(
                        JobObservable.CREATED_AT, String.valueOf( date.getTime() ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( DateMarshaler.marshal( date ) );

        date = new Date( 997654 );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT, 
                "_rest_/active_job/" + job.getId().toString() ).addParameter(
                        JobObservable.CREATED_AT, DateMarshaler.marshal( date ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( DateMarshaler.marshal( date ) );
    }
}
