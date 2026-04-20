/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.tape;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.tape.TapeDensityDirective;
import com.spectralogic.s3.common.dao.domain.tape.TapeDriveType;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.http.RequestType;

public final class CreateTapeDensityDirectiveRequestHandler_Test 
{
    @Test
    public void testtestCreateDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, null );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.TAPE_DENSITY_DIRECTIVE )
                .addParameter( TapeDensityDirective.PARTITION_ID, partition.getName() )
                .addParameter( TapeDensityDirective.TAPE_TYPE, TapeType.TS_JK.toString() )
                .addParameter( TapeDensityDirective.DENSITY, TapeDriveType.TS1140.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        support.getDatabaseSupport().getServiceManager().getRetriever( TapeDensityDirective.class ).attain(
                Require.nothing() );
    }
    
    
    @Test
    public void testtestCreateForNonTsDensityNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, null );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.TAPE_DENSITY_DIRECTIVE )
                .addParameter( TapeDensityDirective.PARTITION_ID, partition.getName() )
                .addParameter( TapeDensityDirective.TAPE_TYPE, TapeType.TS_JK.toString() )
                .addParameter( TapeDensityDirective.DENSITY, TapeDriveType.LTO7.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testtestCreateForNonTsMediaNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, null );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.TAPE_DENSITY_DIRECTIVE )
                .addParameter( TapeDensityDirective.PARTITION_ID, partition.getName() )
                .addParameter( TapeDensityDirective.TAPE_TYPE, TapeType.LTO5.toString() )
                .addParameter( TapeDensityDirective.DENSITY, TapeDriveType.TS1140.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testtestCreateForTsCleaningMediaNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, null );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.TAPE_DENSITY_DIRECTIVE )
                .addParameter( TapeDensityDirective.PARTITION_ID, partition.getName() )
                .addParameter( TapeDensityDirective.TAPE_TYPE, TapeType.TS_CLEANING_TAPE.toString() )
                .addParameter( TapeDensityDirective.DENSITY, TapeDriveType.TS1140.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
    }
    
    
    @Test
    public void testtestCreateDuplicateDirectiveNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final TapePartition partition = mockDaoDriver.createTapePartition( null, null );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.TAPE_DENSITY_DIRECTIVE )
                .addParameter( TapeDensityDirective.PARTITION_ID, partition.getName() )
                .addParameter( TapeDensityDirective.TAPE_TYPE, TapeType.TS_JK.toString() )
                .addParameter( TapeDensityDirective.DENSITY, TapeDriveType.TS1140.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 201 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.TAPE_DENSITY_DIRECTIVE )
                .addParameter( TapeDensityDirective.PARTITION_ID, partition.getName() )
                .addParameter( TapeDensityDirective.TAPE_TYPE, TapeType.TS_JK.toString() )
                .addParameter( TapeDensityDirective.DENSITY, TapeDriveType.TS1140.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.POST, 
                "_rest_/" + RestDomainType.TAPE_DENSITY_DIRECTIVE )
                .addParameter( TapeDensityDirective.PARTITION_ID, partition.getName() )
                .addParameter( TapeDensityDirective.TAPE_TYPE, TapeType.TS_JK.toString() )
                .addParameter( TapeDensityDirective.DENSITY, TapeDriveType.TS1150.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 409 );
    }
}
