/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import java.io.UnsupportedEncodingException;

import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DegradedBlob;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomainMember;
import com.spectralogic.s3.common.dao.domain.pool.BlobPool;
import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.s3.common.dao.domain.pool.SuspectBlobPool;
import com.spectralogic.s3.common.dao.domain.shared.PersistenceTarget;
import com.spectralogic.s3.common.dao.domain.tape.BlobTape;
import com.spectralogic.s3.common.dao.domain.tape.SuspectBlobTape;
import com.spectralogic.s3.common.dao.domain.tape.Tape;
import com.spectralogic.s3.common.dao.domain.target.BlobS3Target;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.domain.target.SuspectBlobS3Target;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;


public final class MarkSuspectBlobS3TargetsAsDegradedRequestHandler_Test
{

    @Test
    public void testMarkAllDoesSoIfForceFlagUsed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final S3Target target1 = mockDaoDriver.createS3Target( "t1" );
        final S3Target target2 = mockDaoDriver.createS3Target( "t2" );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( tape1.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnS3Target( target1.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( tape2.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnPool( pool2.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnS3Target( target2.getId(), b1.getId() ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember tsdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        final StorageDomainMember psdm = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(), pool1.getPartitionId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target1.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        mockDaoDriver.updateBean(
                tape1.setStorageDomainMemberId( tsdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean(
                tape2.setStorageDomainMemberId( tsdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean(
                pool1.setStorageDomainMemberId( psdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean(
                pool2.setStorageDomainMemberId( psdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.SUSPECT_BLOB_S3_TARGET );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.SUSPECT_BLOB_S3_TARGET )
            .addParameter( RequestParameterType.FORCE.toString(), "" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        assertEquals(2,  support.getDatabaseSupport().getServiceManager().getRetriever(
                SuspectBlobTape.class).getCount(), "Shoulda deleted all suspect records of the appropriate type.");
        assertEquals(2,  support.getDatabaseSupport().getServiceManager().getRetriever(
                SuspectBlobPool.class).getCount(), "Shoulda deleted all suspect records of the appropriate type.");
        assertEquals(0,  support.getDatabaseSupport().getServiceManager().getRetriever(
                SuspectBlobS3Target.class).getCount(), "Shoulda deleted all suspect records of the appropriate type.");
        assertEquals(2,  support.getDatabaseSupport().getServiceManager().getRetriever(
                DegradedBlob.class).getCount(), "Shoulda recorded degradation.");
    }
    
    
     @Test
    public void testMarkSpecificIdsDoesSo() throws UnsupportedEncodingException
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        
        final Tape tape1 = mockDaoDriver.createTape();
        final Tape tape2 = mockDaoDriver.createTape();
        final Pool pool1 = mockDaoDriver.createPool();
        final Pool pool2 = mockDaoDriver.createPool();
        final S3Target target1 = mockDaoDriver.createS3Target( "t1" );
        final S3Target target2 = mockDaoDriver.createS3Target( "t2" );
        
        final S3Object o1 = mockDaoDriver.createObject( null, "o1" );
        final Blob b1 = mockDaoDriver.getBlobFor( o1.getId() );
        final BlobTape bTape = 
                mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( tape1.getId(), b1.getId() ) );
        final BlobPool bPool = 
                mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnPool( pool1.getId(), b1.getId() ) );
        final BlobS3Target bTarget =
                mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnS3Target( target1.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnTape( tape2.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnPool( pool2.getId(), b1.getId() ) );
        mockDaoDriver.makeSuspect( mockDaoDriver.putBlobOnS3Target( target2.getId(), b1.getId() ) );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final StorageDomain sd = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomainMember tsdm = mockDaoDriver.addTapePartitionToStorageDomain(
                sd.getId(), tape1.getPartitionId(), tape1.getType() );
        final StorageDomainMember psdm = mockDaoDriver.addPoolPartitionToStorageDomain(
                sd.getId(), pool1.getPartitionId() );
        mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, sd.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target1.getId() );
        mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );
        mockDaoDriver.updateBean(
                tape1.setStorageDomainMemberId( tsdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean(
                tape2.setStorageDomainMemberId( tsdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean(
                pool1.setStorageDomainMemberId( psdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );
        mockDaoDriver.updateBean(
                pool2.setStorageDomainMemberId( psdm.getId() ),
                PersistenceTarget.STORAGE_DOMAIN_MEMBER_ID );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.SUSPECT_BLOB_S3_TARGET );
        driver.setRequestPayload( 
                ( "<ids><id>" + bTape.getId() + "</id><id>" + bPool.getId() 
                  + "</id><id>" + bTarget.getId() + "</id></ids>" ).getBytes( "UTF8" ) );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        assertEquals(2,  support.getDatabaseSupport().getServiceManager().getRetriever(
                SuspectBlobTape.class).getCount(), "Shoulda deleted specified suspect records of the appropriate type.");
        assertEquals(2,  support.getDatabaseSupport().getServiceManager().getRetriever(
                SuspectBlobPool.class).getCount(), "Shoulda deleted specified suspect records of the appropriate type.");
        assertEquals(1,  support.getDatabaseSupport().getServiceManager().getRetriever(
                SuspectBlobS3Target.class).getCount(), "Shoulda deleted specified suspect records of the appropriate type.");
        assertEquals(1,  support.getDatabaseSupport().getServiceManager().getRetriever(
                DegradedBlob.class).getCount(), "Shoulda recorded degradation.");
    }
}
