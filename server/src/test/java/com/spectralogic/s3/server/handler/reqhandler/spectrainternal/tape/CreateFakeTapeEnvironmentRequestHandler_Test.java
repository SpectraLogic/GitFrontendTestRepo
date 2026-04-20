/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrainternal.tape;


import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.domain.tape.TapeLibrary;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class CreateFakeTapeEnvironmentRequestHandler_Test
{
    @Test
    public void testCreateFakesOutEverythingNecessaryToCreateBucket()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.TAPE_ENVIRONMENT );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( TapePartition.class ).attain(
                Require.nothing() );
        support.getDatabaseSupport().getServiceManager().getRetriever( TapeLibrary.class ).attain(
                Require.nothing() );
        support.getDatabaseSupport().getServiceManager().getRetriever( StorageDomain.class ).attain(
                Require.nothing() );
        support.getDatabaseSupport().getServiceManager().getRetriever( DataPolicy.class ).attain(
                Require.nothing() );
        
        final MockHttpRequestDriver driverUser = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL ).addParameter( NameObservable.NAME, "jason" );
        driverUser.run();
        driverUser.assertHttpResponseCodeEquals( 201 );
        
        final MockHttpRequestDriver driverBucket = new MockHttpRequestDriver(
                support, 
                true, 
                new MockUserAuthorizationStrategy( "jason" ), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.BUCKET ).addParameter( NameObservable.NAME, "b1" );
        driverBucket.run();
        driverBucket.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( Bucket.class ).attain(
                Require.nothing() );
    }
}
