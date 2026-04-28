/*******************************************************************************
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.nio.charset.Charset;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.NamingConventionType;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class StageObjectsJobRequestHandler_Test 
{
    @Test
    public void testCreateEmptyJobNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = (
                "<Objects></Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT,
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_stage" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    

    @Test
    public void testCreateStageJobCreatesStageJobWhenNoOptionalParametersSpecified()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createObject( null, "o1" );
        mockDaoDriver.createObject( null, "o2" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();

        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"o1\" />"
                + "<Object NAME=\"o2\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.PUT,
                "_rest_/bucket/" + MockDaoDriver.DEFAULT_BUCKET_NAME );
        driver.addParameter( "operation", "start_bulk_stage" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "ChunkClientProcessingOrderGuarantee=\"NONE\"" );
    }
}
