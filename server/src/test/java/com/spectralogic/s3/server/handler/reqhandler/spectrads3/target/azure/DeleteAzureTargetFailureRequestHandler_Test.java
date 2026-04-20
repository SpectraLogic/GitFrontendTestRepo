/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.target.AzureTarget;
import com.spectralogic.s3.common.dao.domain.target.AzureTargetFailure;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;

public final class DeleteAzureTargetFailureRequestHandler_Test 
{
    @Test
    public void testDeleteDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final AzureTarget target1 = mockDaoDriver.createAzureTarget( "a" );
        final AzureTarget target2 = mockDaoDriver.createAzureTarget( "b" );
        final AzureTarget target3 = mockDaoDriver.createAzureTarget( "c" );
        
        final AzureTargetFailure failureToDelete = BeanFactory.newBean( AzureTargetFailure.class )
                .setErrorMessage( "AAA" )
                .setTargetId( target1.getId() )
                .setType( TargetFailureType.values()[ 0 ] );
        support.getDatabaseSupport().getDataManager().createBean( failureToDelete );
        support.getDatabaseSupport().getDataManager().createBean(
                BeanFactory.newBean( AzureTargetFailure.class )
                .setErrorMessage( "BBB" )
                .setTargetId( target2.getId() )
                .setType( TargetFailureType.values()[ 0 ] ) );
        support.getDatabaseSupport().getDataManager().createBean( 
                BeanFactory.newBean( AzureTargetFailure.class )
                .setErrorMessage( "CCC" )
                .setTargetId( target3.getId() )
                .setType( TargetFailureType.values()[ 0 ] ) );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/azure_target_failure" )
                    .addParameter( "targetId", target1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "AAA" );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/azure_target_failure/" + failureToDelete.getId() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/azure_target_failure" )
                    .addParameter( "targetId", target1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( "AAA" );
    }
}
