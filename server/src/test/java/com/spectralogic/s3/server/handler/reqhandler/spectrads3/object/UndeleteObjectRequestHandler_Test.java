package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.BucketAclPermission;
import com.spectralogic.s3.common.dao.domain.ds3.BuiltInGroup;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.dao.service.ds3.GroupService;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.NamingConventionType;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class UndeleteObjectRequestHandler_Test 
{
	@Test
    public void testUndeleteObjectRespondsCorrectly()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID jasonId = mockDaoDriver.createUser( "jason" ).getId();
        final DataPolicy dataPolicy = mockDaoDriver.createABMConfigSingleCopyOnTape();
        mockDaoDriver.updateBean(
        		dataPolicy.setVersioning( VersioningLevel.KEEP_MULTIPLE_VERSIONS ), DataPolicy.VERSIONING );
        final UUID bucketJasonId = mockDaoDriver.createBucket( jasonId, dataPolicy.getId(), "bucketjason" ).getId();
        final UUID adminId = mockDaoDriver.createUser( "admin" ).getId();
        mockDaoDriver.addUserMemberToGroup( 
                support.getDatabaseSupport().getServiceManager().getService( GroupService.class )
                .getBuiltInGroup( BuiltInGroup.ADMINISTRATORS ).getId(), adminId );
        mockDaoDriver.addBucketAcl( bucketJasonId, null, jasonId, BucketAclPermission.DELETE );

        final S3Object object1 = mockDaoDriver.createObject( bucketJasonId, "jasonsong.mp3" );
        final S3Object object2 = mockDaoDriver.createObject( bucketJasonId, "jasonsong.mp3" );
        mockDaoDriver.updateBean( object2.setLatest( false ),S3Object.LATEST );

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( "name", "jasonsong.mp3" )
                        .addParameter( "bucketId", "bucketjason" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        System.out.println( driver.getResponseToClientAsString() );
        assertFalse(
                driver.getResponseToClientAsString().contains( object1.getId().toString() ),
                "Shoulda returned correct version."
                 );
        assertTrue(
                driver.getResponseToClientAsString().contains( object2.getId().toString() ),
                "Shoulda returned correct version."
                );
                
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( "name", "jasonsong.mp3" )
                        .addParameter( "bucketId", "bucketjason" )
                        .addParameter( "versionId", object2.getId().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( "Should have been allowed to specify correct versionID.", 200 );
        
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( "jason" ),
                RequestType.PUT, 
                "_rest_/object" ).addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        NamingConventionType.CAMEL_CASE_WITH_FIRST_LETTER_UPPERCASE.name() )
                        .addParameter( "name", "jasonsong.mp3" )
                        .addParameter( "bucketId", "bucketjason" )
                        .addParameter( "versionId", UUID.randomUUID().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( "Should have given 404 for nonsense version ID.", 404 );
    }
}
