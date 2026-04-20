/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.capacity;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.S3ObjectType;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.tape.ImportExportConfiguration;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.tape.TapeLibrary;
import com.spectralogic.s3.common.dao.domain.tape.TapePartition;
import com.spectralogic.s3.common.dao.domain.tape.TapeState;
import com.spectralogic.s3.common.dao.domain.tape.TapeType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import org.junit.jupiter.api.Test;


public final class GetSystemCapacitySummaryRequestHandler_Test 
{
    @Test
    public void testRequestHandlerDoesNotErr()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        final Bucket bucket1 = mockDaoDriver.createBucket( null, "bucket1" );
        final Bucket bucket2 = mockDaoDriver.createBucket( null, "bucket2" );
        
        final S3Object o1 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o1 );
        final S3Object o2 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket1.getId() ).setName( "o2" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o2 );
        final S3Object o3 = BeanFactory.newBean( S3Object.class )
                .setBucketId( bucket2.getId() ).setName( "o1" ).setType( S3ObjectType.DATA );
        dbSupport.getDataManager().createBean( o3 );
        
        final TapeLibrary library = BeanFactory.newBean( TapeLibrary.class )
                .setManagementUrl( "a" ).setName( "library" ).setSerialNumber( "lsn" );
        dbSupport.getDataManager().createBean( library );
        
        final TapePartition partition = BeanFactory.newBean( TapePartition.class )
                .setName( "p" ).setSerialNumber( "sn" ).setLibraryId( library.getId() )
                .setImportExportConfiguration( ImportExportConfiguration.values() [ 0 ] );
        dbSupport.getDataManager().createBean( partition );
        
        final StorageDomain sd1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain sd2 = mockDaoDriver.createStorageDomain( "sd2" );
        final StorageDomainMember sdm1a = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd1.getId(), partition.getId(), TapeType.values()[ 1 ] );
        final StorageDomainMember sdm1b = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd1.getId(), partition.getId(), TapeType.values()[ 0 ] );
        final StorageDomainMember sdm2 = mockDaoDriver.addTapePartitionToStorageDomain( 
                sd2.getId(), partition.getId(), TapeType.values()[ 0 ] );
        final Tape t1 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 1000 ) )
                .setTotalRawCapacity( Long.valueOf( 1500 ) )
                .setType( TapeType.values()[ 1 ] )
                .setBarCode( "a" )
                .setStorageDomainMemberId( sdm1a.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t1 );
        final Tape t2 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 1000 ) )
                .setTotalRawCapacity( Long.valueOf( 1500 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "b" )
                .setBucketId( bucket1.getId() )
                .setStorageDomainMemberId( sdm1b.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t2 );
        final Tape t3 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 1000 ) )
                .setTotalRawCapacity( Long.valueOf( 1500 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "c" )
                .setBucketId( bucket2.getId() )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t3 );
        final Tape t4 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.NORMAL )
                .setAvailableRawCapacity( Long.valueOf( 10000 ) )
                .setTotalRawCapacity( Long.valueOf( 15000 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "d" )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t4 );
        final Tape t5 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.EJECTED )
                .setAvailableRawCapacity( Long.valueOf( 1 ) )
                .setTotalRawCapacity( Long.valueOf( 2 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "e" )
                .setStorageDomainMemberId( sdm2.getId() )
                .setAssignedToStorageDomain( true )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t5 );
        final Tape t6 = BeanFactory.newBean( Tape.class )
                .setState( TapeState.EJECTED )
                .setAvailableRawCapacity( Long.valueOf( 99999 ) )
                .setTotalRawCapacity( Long.valueOf( 99999 ) )
                .setType( TapeType.values()[ 0 ] )
                .setBarCode( "f" )
                .setPartitionId( partition.getId() );
        dbSupport.getDataManager().createBean( t6 );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CAPACITY_SUMMARY );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "<PhysicalAllocated>4502</PhysicalAllocated>" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CAPACITY_SUMMARY )
            .addParameter( CapacitySummaryOptionalParams.TAPE_TYPE, TapeType.values()[ 0 ].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "<PhysicalAllocated>3002</PhysicalAllocated>" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.CAPACITY_SUMMARY )
            .addParameter( CapacitySummaryOptionalParams.TAPE_TYPE, TapeType.values()[ 0 ].toString() )
            .addParameter( CapacitySummaryOptionalParams.TAPE_STATE, TapeState.NORMAL.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( "<PhysicalAllocated>3000</PhysicalAllocated>" );
    }
}
