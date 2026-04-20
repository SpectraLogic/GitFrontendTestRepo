/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailure;
import com.spectralogic.s3.common.dao.domain.tape.TapePartitionFailureType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;

public final class GetTapePartitionFailuresRequestHandler_Test 
{
    @Test
    public void testtestRequestHandlerReturnsCorrectFilteredResponseForGetTapePartitionFailures()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        mockDaoDriver.createUser( MockDaoDriver.DEFAULT_USER_NAME );
        final TapePartition tp1 = mockDaoDriver.createTapePartition( null, "tp1" );
        final TapePartition tp2 = mockDaoDriver.createTapePartition( null, "tp2" );
        final TapePartition tp3 = mockDaoDriver.createTapePartition( null, "tp3" );
        
        support.getDatabaseSupport().getDataManager().createBean( 
                BeanFactory.newBean( TapePartitionFailure.class )
                .setErrorMessage( "AAA" )
                .setPartitionId( tp1.getId() )
                .setType( TapePartitionFailureType.values()[ 0 ] ) );
        support.getDatabaseSupport().getDataManager().createBean(
                BeanFactory.newBean( TapePartitionFailure.class )
                .setErrorMessage( "BBB" )
                .setPartitionId( tp1.getId() )
                .setType( TapePartitionFailureType.values()[ 0 ] ) );
        support.getDatabaseSupport().getDataManager().createBean(
                BeanFactory.newBean( TapePartitionFailure.class )
                .setErrorMessage( "CCC" )
                .setPartitionId( tp2.getId() )
                .setType( TapePartitionFailureType.values()[ 0 ] ) );
        support.getDatabaseSupport().getDataManager().createBean(
                BeanFactory.newBean( TapePartitionFailure.class )
                .setErrorMessage( "DDD" )
                .setPartitionId( tp3.getId() )
                .setType( TapePartitionFailureType.values()[ 0 ] ) );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( MockDaoDriver.DEFAULT_USER_NAME ),
                RequestType.GET, 
                "_rest_/tape_partition_failure" )
                    .addParameter( "partitionId", tp1.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "AAA" );
        driver.assertResponseToClientContains( "BBB" );
        driver.assertResponseToClientDoesNotContain( "CCC" );
        driver.assertResponseToClientDoesNotContain( "DDD" );
    }
}
