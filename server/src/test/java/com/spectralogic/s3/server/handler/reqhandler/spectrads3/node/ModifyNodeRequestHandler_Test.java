/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler.spectrads3.node;


import com.spectralogic.s3.common.dao.domain.ds3.Node;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.dao.service.ds3.NodeService;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public final class ModifyNodeRequestHandler_Test 
{
    @Test
    public void testModifyDoesSo()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();

        final DatabaseSupport dbSupport = support.getDatabaseSupport();
        final NodeService nodeService = dbSupport.getServiceManager().getService( NodeService.class );
        final Node node1 = BeanFactory.newBean( Node.class ).setName( "apple" ).setSerialNumber( "apple" );
        dbSupport.getDataManager().createBean( node1 );
        
        final MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.PUT, 
                "_rest_/" + RestDomainType.NODE.toString() + "/" + node1.getName() )
                        .addParameter( NameObservable.NAME, "newname" );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );

        assertEquals(
                "newname",
                nodeService.attain( node1.getId() ).getName(),
                "Shoulda updated node."
                 );
    }
}
