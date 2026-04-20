package com.spectralogic.s3.server.handler.reqhandler.spectrads3.job;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Job;
import com.spectralogic.s3.common.rpc.dataplanner.DataPlannerResource;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.reflect.ReflectUtil;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CloseAggregatingJobRequestHandler_Test 
{
    @Test
    public void testCloseAggregatingJobDelegatesToPlannerResource()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final String userName = "test_user";
        final String bucketName = "existing_bucket";
        final String goodObjectName = "non_existent_object";
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID userId = mockDaoDriver.createUser( userName ).getId();
        mockDaoDriver.createBucket( userId, bucketName ).getId();
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final byte[] requestPayload = (
                "<Objects>"
                + "<Object Name=\"" + goodObjectName + "1\" Size=\"2048\" />"
                + "<Object Name=\"" + goodObjectName + "2\" SIZE=\"1024\" />"
                + "</Objects>" ).getBytes( Charset.forName( "UTF-8" ) );
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( userName ),
                RequestType.PUT, 
                "_rest_/bucket/" + bucketName );
        driver.addParameter( "operation", "start_bulk_put" );
        driver.setRequestPayload( requestPayload );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        final Job job = mockDaoDriver.attainOneAndOnly( Job.class );
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.JOB + "/" + job.getId() )
            .addParameter( RequestParameterType.CLOSE_AGGREGATING_JOB.toString(), "");
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        final Method method = ReflectUtil.getMethod(
                DataPlannerResource.class, "closeAggregatingJob" );
        assertEquals(
                1,
                support.getPlannerInterfaceBtih().getMethodCallCount( method ),
                "Shoulda invoked data planner rpc resource."
                );
    }
}
