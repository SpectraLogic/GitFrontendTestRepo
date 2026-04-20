/*******************************************************************************
 *
 * Copyright C 2016, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.target.azure;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.shared.ImportDirective;
import com.spectralogic.s3.common.dao.domain.shared.ImportPublicCloudTargetDirective;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.s3.server.request.rest.RestOperationType;
import com.spectralogic.util.http.RequestType;

public final class ImportAzureTargetRequestHandler_Test 
{
    @Test
    public void testImportWithRequiredParamsOnlyCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID targetId = mockDaoDriver.createAzureTarget( null ).getId();

        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + targetId.toString() )
                        .addParameter( "operation", RestOperationType.IMPORT.toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 400 );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + targetId.toString() )
                        .addParameter( "operation", RestOperationType.IMPORT.toString() )
                        .addParameter( 
                                ImportPublicCloudTargetDirective.CLOUD_BUCKET_NAME, 
                                "cbn" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        assertEquals(targetId, ( (ImportPublicCloudTargetDirective< ? >)
                        support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 0 ) )
                        .getTargetId(), "Shoulda formatted only the expected target id.");
    }
    
    
    @Test
    public void testImportWithAllOptionalParamsCallsDataPlanner()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final UUID targetId = mockDaoDriver.createAzureTarget( null ).getId();
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "dp1" );
        mockDaoDriver.createUser( "abc" );

        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.AZURE_TARGET + "/" + targetId.toString() )
                        .addParameter( "operation", RestOperationType.IMPORT.toString() )
                        .addParameter( ImportDirective.USER_ID, "abc" )
                        .addParameter( ImportDirective.DATA_POLICY_ID, dataPolicy.getName() )
                        .addParameter( 
                                ImportPublicCloudTargetDirective.CLOUD_BUCKET_NAME, 
                                "cbn" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 204 );

        assertEquals(targetId, ( (ImportPublicCloudTargetDirective< ? >)
                        support.getTargetInterfaceBtih().getMethodInvokeData().get( 0 ).getArgs().get( 0 ) )
                        .getTargetId(), "Shoulda formatted only the expected target id.");
    }
}
