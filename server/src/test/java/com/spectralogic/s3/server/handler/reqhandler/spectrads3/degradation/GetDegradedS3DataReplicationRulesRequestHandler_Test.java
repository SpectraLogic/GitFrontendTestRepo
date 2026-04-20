/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.degradation;

import java.util.UUID;



import com.spectralogic.s3.common.dao.domain.ds3.Blob;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.DataReplicationRuleType;
import com.spectralogic.s3.common.dao.domain.ds3.S3DataReplicationRule;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.target.S3Target;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import org.junit.jupiter.api.Test;

public final class GetDegradedS3DataReplicationRulesRequestHandler_Test 
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
        final S3Target target1 = mockDaoDriver.createS3Target( "sd1" );
        final S3Target target2 = mockDaoDriver.createS3Target( "sd2" );
        final S3DataReplicationRule rule1 = mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target1.getId() );
        final S3DataReplicationRule rule2 = mockDaoDriver.createS3DataReplicationRule(
                dataPolicy.getId(), DataReplicationRuleType.PERMANENT, target2.getId() );

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
                "_rest_/" + RestDomainType.DEGRADED_S3_DATA_REPLICATION_RULE );
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
                "_rest_/" + RestDomainType.DEGRADED_S3_DATA_REPLICATION_RULE );
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
                "_rest_/" + RestDomainType.DEGRADED_S3_DATA_REPLICATION_RULE );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientContains( rule1.getId().toString() );
        driver.assertResponseToClientContains( rule2.getId().toString() );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "barry" ),
                RequestType.GET, 
                "_rest_/" + RestDomainType.DEGRADED_S3_DATA_REPLICATION_RULE )
            .addParameter(
                    DataReplicationRule.TARGET_ID, 
                    target2.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        driver.assertResponseToClientDoesNotContain( rule1.getId().toString() );
        driver.assertResponseToClientContains( rule2.getId().toString() );
    }
}
