/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.DataIsolationLevel;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataPersistenceRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.StorageDomain;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetDegradedDataPersistenceRulesRequestHandler_Test 
{
     @Test
    public void testGetDegradedRulesOnlyReturnsRulesThatHaveDegradation()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID jasonId = mockDaoDriver.createUser( "jason" ).getId();
        final UUID barryId = mockDaoDriver.createUser( "barry" ).getId();
        final UUID bucketJasonId = mockDaoDriver.createBucket( jasonId, "bucketjason" ).getId();
        final UUID bucketBarryId = mockDaoDriver.createBucket( barryId, "bucketbarry" ).getId();
        final UUID adminId = mockDaoDriver.createUser( "admin" ).getId();
        mockDaoDriver.addUserMemberToGroup( 
                support.getDatabaseSupport().getServiceManager().getService( GroupService.class )
                .getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId(), adminId );
        
        final DataPolicy dataPolicy = mockDaoDriver.attainOneAndOnly( DataPolicy.class );
        final StorageDomain storageDomain1 = mockDaoDriver.createStorageDomain( "sd1" );
        final StorageDomain storageDomain2 = mockDaoDriver.createStorageDomain( "sd2" );
        final DataPersistenceRule rule1 = mockDaoDriver.createDataPersistenceRule(
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain1.getId() );
        final DataPersistenceRule rule2 = mockDaoDriver.createDataPersistenceRule(
                DataIsolationLevel.BUCKET_ISOLATED, 
                dataPolicy.getId(), DataPersistenceRuleType.PERMANENT, storageDomain2.getId() );

        mockDaoDriver.createObject( bucketJasonId, "o1" );
        final S3Object jasonObject = mockDaoDriver.createObject( bucketJasonId, "o2" );
        final Blob jasonBlob = mockDaoDriver.getBlobFor( jasonObject.getId() );
        mockDaoDriver.createObject( bucketBarryId, "o1" );
        final S3Object barryObject = mockDaoDriver.createObject( bucketBarryId, "o2" );
        final Blob barryBlob = mockDaoDriver.getBlobFor( barryObject.getId() );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "barry" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.DEGRADED_DATA_PERSISTENCE_RULE );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( rule1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( rule2.getId().toString() );
        
        mockDaoDriver.createDegradedBlob( jasonBlob.getId(), rule1.getId() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "barry" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.DEGRADED_DATA_PERSISTENCE_RULE );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( rule1.getId().toString() );
        driver.assertResponseToClientDoesNotContain( rule2.getId().toString() );
        
        mockDaoDriver.createDegradedBlob( barryBlob.getId(), rule2.getId() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "barry" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.DEGRADED_DATA_PERSISTENCE_RULE );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( rule1.getId().toString() );
        driver.assertResponseToClientContains( rule2.getId().toString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "barry" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.DEGRADED_DATA_PERSISTENCE_RULE )
            .addParameter(
                    DataPersistenceRule.ISOLATION_LEVEL, 
                    DataIsolationLevel.BUCKET_ISOLATED.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( rule1.getId().toString() );
        driver.assertResponseToClientContains( rule2.getId().toString() );
    }
}
