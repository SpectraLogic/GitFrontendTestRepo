/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import com.spectralogic.s3.common.dao.domain.ds3.JobCreationFailed;
import com.spectralogic.s3.common.dao.domain.ds3.JobCreationFailedType;
import com.spectralogic.s3.common.dao.service.ds3.JobCreationFailedService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class DeleteJobCreationFailureRequestHandler_Test 
{
    @Test
    public void testDeleteJobCreationFailuresDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );

        dbSupport.getServiceManager().getService(JobCreationFailedService.class).create(
                "testUser",
                JobCreationFailedType.TAPES_MUST_BE_ONLINED,
                CollectionFactory.toList(CollectionFactory.toList("ABC123L6")),
                "error!",
                60
        );

        final JobCreationFailed failure = mockDaoDriver.attainOneAndOnly(JobCreationFailed.class);

        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE,
                "_rest_/" + RestDomainType.JOB_CREATION_FAILED + "/" + failure.getId());
        driver.run();
        driver.assertHttpResponseCodeEquals( 204);

        assertEquals(
                0,
                mockDaoDriver.retrieveAll(JobCreationFailed.class).stream().count(),
                "Should have deleted failure"

        );
    }
}
