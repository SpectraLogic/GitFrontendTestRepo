/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.s3;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.S3TargetFailure;
import com.spectralogic.s3.common.dao.domain.target.TargetFailureType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;

public final class GetS3TargetFailuresRequestHandler_Test 
{
    @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final S3Target target1 = mockDaoDriver.createS3Target( "A" );
        final S3Target target2 = mockDaoDriver.createS3Target( "B" );
        final S3Target target3 = mockDaoDriver.createS3Target( "C" );
        
        support.getDatabaseSupport().getDataManager().createBean( 
                BeanFactory.newBean( S3TargetFailure.class )
                .setErrorMessage( "AAA" )
                .setTargetId( target1.getId() )
                .setType( TargetFailureType.values()[ 0 ] ) );
        support.getDatabaseSupport().getDataManager().createBean( 
                BeanFactory.newBean( S3TargetFailure.class )
                .setErrorMessage( "BBB" )
                .setTargetId( target2.getId() )
                .setType( TargetFailureType.values()[ 0 ] ) );
        support.getDatabaseSupport().getDataManager().createBean( 
                BeanFactory.newBean( S3TargetFailure.class )
                .setErrorMessage( "CCC" )
                .setTargetId( target3.getId() )
                .setType( TargetFailureType.values()[ 0 ] ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/s3_target_failure" )
                    .addParameter( "targetId", target1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "AAA" );
        driver.assertResponseToClientDoesNotContain( "BBB" );
        driver.assertResponseToClientDoesNotContain( "CCC" );
    }
}
