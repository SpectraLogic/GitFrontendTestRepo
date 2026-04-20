/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.pool;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailure;
import com.spectralogic.s3.common.dao.domain.pool.PoolFailureType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;

public final class GetPoolFailuresRequestHandler_Test 
{
    @Test
    public void testRequestHandlerReturnsCorrectFilteredResponseForGetPoolFailures()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final Pool pool3 = mockDaoDriver.createPool();
        
        support.getDatabaseSupport().getDataManager().createBean( BeanFactory.newBean( PoolFailure.class )
                .setErrorMessage( "AAA" )
                .setPoolId( pool1.getId() )
                .setType( PoolFailureType.values()[ 0 ] ) );
        support.getDatabaseSupport().getDataManager().createBean( BeanFactory.newBean( PoolFailure.class )
                .setErrorMessage( "BBB" )
                .setPoolId( pool2.getId() )
                .setType( PoolFailureType.values()[ 0 ] ) );
        support.getDatabaseSupport().getDataManager().createBean( BeanFactory.newBean( PoolFailure.class )
                .setErrorMessage( "CCC" )
                .setPoolId( pool3.getId() )
                .setType( PoolFailureType.values()[ 0 ] ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/pool_failure" )
                    .addParameter( "poolId", pool1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "AAA" );
        driver.assertResponseToClientDoesNotContain( "BBB" );
        driver.assertResponseToClientDoesNotContain( "CCC" );
    }
}
