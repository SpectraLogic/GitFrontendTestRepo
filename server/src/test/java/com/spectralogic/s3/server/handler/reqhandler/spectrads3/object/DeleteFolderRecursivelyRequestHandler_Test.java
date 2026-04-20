/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.object;

import java.util.Date;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.ds3.Bucket;
import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.S3Object;
import com.spectralogic.s3.common.dao.domain.ds3.User;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockUserAuthorizationStrategy;
import com.spectralogic.s3.server.request.api.RequestParameterType;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class DeleteFolderRecursivelyRequestHandler_Test 
{
    @Test
    public void testDeleteFolderDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final User user = mockDaoDriver.createUser( "user1" );
        mockDaoDriver.grantOwnerPermissionsToEveryUser();
        
        final Bucket b1 = mockDaoDriver.createBucket( null, "b1" );
        final DataPolicy dataPolicy = mockDaoDriver.getDataPolicyFor( b1.getId() );
        mockDaoDriver.updateBean( 
                dataPolicy.setVersioning( VersioningLevel.KEEP_MULTIPLE_VERSIONS ),
                DataPolicy.VERSIONING );
        final S3Object o11 = mockDaoDriver.createObject( b1.getId(), "movies/a" );
        final S3Object o12 = mockDaoDriver.createObject( b1.getId(), "movies/b" );
        final S3Object o13 = mockDaoDriver.createObject( b1.getId(), "movies/final/a" );
        final S3Object o14 = mockDaoDriver.createObject( b1.getId(), "movies/raw/" );
        mockDaoDriver.createObject( b1.getId(), "movies/raw/a", 20, new Date( 0 ) );
        final S3Object o15 = mockDaoDriver.createObject( b1.getId(), "movies/raw/a", new Date( 1000 ) );
        mockDaoDriver.createObject( b1.getId(), "music/a" );
        mockDaoDriver.createObject( b1.getId(), "a" );

        final Bucket b2 = mockDaoDriver.createBucket( null, "b2" );
        mockDaoDriver.createObject( b2.getId(), "movies/a" );
        mockDaoDriver.createObject( b2.getId(), "movies/b" );
        mockDaoDriver.createObject( b2.getId(), "movies/final/a" );
        mockDaoDriver.createObject( b2.getId(), "movies/raw/" );
        mockDaoDriver.createObject( b2.getId(), "movies/raw/a" );
        mockDaoDriver.createObject( b2.getId(), "music/a" );
        mockDaoDriver.createObject( b2.getId(), "a" );
        
        int invocationNumber = -1;
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.FOLDER.toString() + "/" + "movies/final/" )
            .addParameter( RequestParameterType.RECURSIVE.toString(), "" )
            .addParameter( RequestParameterType.BUCKET_ID.toString(), b1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        assertEquals(15,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(), "Shoulda called data planner to delete objects.");
        assertEquals(null, support.getTargetInterfaceBtih().getMethodInvokeData()
                .get( ++invocationNumber ).getArgs().get( 0 ), "Shoulda sent down null user id for mock internal request.");
        final Object expected4 = CollectionFactory.toSet( o13.getId() );
        assertEquals(expected4, CollectionFactory.toSet( (UUID[])
                        support.getTargetInterfaceBtih().getMethodInvokeData()
                        .get( invocationNumber ).getArgs().get( 2 ) ), "Shoulda called target manager to delete objects.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockUserAuthorizationStrategy( user.getName() ),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.FOLDER.toString() + "/" + "movies/raw" )
            .addParameter( RequestParameterType.RECURSIVE.toString(), "" )
            .addParameter( RequestParameterType.BUCKET_ID.toString(), b1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        assertEquals(15,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(), "Shoulda called data planner to delete objects.");
        final Object expected3 = CollectionFactory.toSet( o14.getId(), o15.getId() );
        assertEquals(expected3, CollectionFactory.toSet( (UUID[])
                        support.getTargetInterfaceBtih().getMethodInvokeData()
                        .get( ++invocationNumber ).getArgs().get( 2 ) ), "Shoulda called target manager to delete objects.");
        final Object expected2 = user.getId();
        assertEquals(expected2, support.getTargetInterfaceBtih().getMethodInvokeData()
                .get( invocationNumber ).getArgs().get( 0 ), "Shoulda sent down non-null user id for non-mock internal request.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.FOLDER.toString() + "/" + "movies/raw/" )
            .addParameter( RequestParameterType.RECURSIVE.toString(), "" )
            .addParameter( RequestParameterType.BUCKET_ID.toString(), b1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        assertEquals(15,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(), "Shoulda called data planner to delete objects.");
        final Object expected1 = CollectionFactory.toSet( o14.getId(), o15.getId() );
        assertEquals(expected1, CollectionFactory.toSet( (UUID[])
                        support.getTargetInterfaceBtih().getMethodInvokeData()
                        .get( ++invocationNumber ).getArgs().get( 2 ) ), "Shoulda called target manager to delete objects.");

        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.FOLDER.toString() + "/" + "movies" )
            .addParameter( RequestParameterType.RECURSIVE.toString(), "" )
            .addParameter( RequestParameterType.BUCKET_ID.toString(), b1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );
        assertEquals(15,  dbSupport.getServiceManager().getRetriever(S3Object.class).getCount(), "Shoulda called data planner to delete objects.");
        assertEquals(4,  support.getTargetInterfaceBtih().getTotalCallCount(), "Shoulda called target manager to delete objects and for single replicate call.");
        assertEquals(0,  support.getPlannerInterfaceBtih().getTotalCallCount(), "Should notta called planner manager.");
        final Object expected = CollectionFactory.toSet( o11.getId(), o12.getId(), o13.getId(), o14.getId(), o15.getId() );
        assertEquals(expected, CollectionFactory.toSet( (UUID[])
                        support.getTargetInterfaceBtih().getMethodInvokeData()
                        .get( ++invocationNumber ).getArgs().get( 2 ) ), "Shoulda called target manager to delete objects.");
    }
    
    
    @Test
    public void testDeleteNonFolderNotAllowed()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( dbSupport );
        
        final Bucket b1 = mockDaoDriver.createBucket( null, "b1" );
        mockDaoDriver.createObject( b1.getId(), "movies/a" );
        mockDaoDriver.createObject( b1.getId(), "movies/b" );
        mockDaoDriver.createObject( b1.getId(), "movies/final/a" );
        mockDaoDriver.createObject( b1.getId(), "movies/raw/" );
        mockDaoDriver.createObject( b1.getId(), "movies/raw/a" );
        mockDaoDriver.createObject( b1.getId(), "music/a" );
        mockDaoDriver.createObject( b1.getId(), "a" );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.DELETE, 
                "_rest_/" + RestDomainType.FOLDER.toString() + "/" + "movies/a" )
            .addParameter( RequestParameterType.RECURSIVE.toString(), "" )
            .addParameter( RequestParameterType.BUCKET_ID.toString(), b1.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 404 );
    }
}
