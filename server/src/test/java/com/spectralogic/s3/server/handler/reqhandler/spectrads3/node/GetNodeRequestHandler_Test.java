/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.node;

import com.spectralogic.s3.common.dao.domain.ds3.Node;
import com.spectralogic.s3.common.dao.service.ds3.NodeService;
import com.spectralogic.s3.server.mock.MockAnonymousAuthorizationStrategy;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import org.junit.jupiter.api.Test;

public final class GetNodeRequestHandler_Test 
{
    @Test
    public void testGetDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final NodeService nodeService = dbSupport.getServiceManager().getService( NodeService.class );
        final Node node1 = BeanFactory.newBean( Node.class ).setName( "apple" ).setSerialNumber( "apple" );
        dbSupport.getDataManager().createBean( node1 );
        final Node node2 = nodeService.getThisNode();
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockAnonymousAuthorizationStrategy(),
                RequestType.GET, 
                "_rest_/" + RestDomainType.NODE.toString() + "/" + node2.getName() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver.assertResponseToClientDoesNotContain( node1.getId().toString() );
        driver.assertResponseToClientContains( node2.getId().toString() );
    }
}
