/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler;



import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.platform.aws.S3HeaderType;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.NamingConventionType;
import org.junit.jupiter.api.Test;

public final class NamingConventionHeader_Test 
{
    @Test
    public void testNamingConventionHeaderWorks()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL ).addParameter( NameObservable.NAME, "jason" );
        driver.run();
        driver.assertResponseToClientContains( "<AuthId>" );
        
        driver = new MockHttpRequestDriver( 
                support,
                true,
                new MockInternalRequestAuthorizationStrategy(),
                RequestType.POST, 
                "_rest_/" + RestDomainType.USER_INTERNAL ).addParameter( NameObservable.NAME, "barry" )
                .addHeader(
                        S3HeaderType.NAMING_CONVENTION,
                        "underscore" );
        driver.run();
        driver.assertResponseToClientContains( "<auth_id>" );
        
        for ( final NamingConventionType nc : NamingConventionType.values() )
        {
            driver = new MockHttpRequestDriver( 
                    support,
                    true,
                    new MockInternalRequestAuthorizationStrategy(),
                    RequestType.POST, 
                    "_rest_/" + RestDomainType.USER_INTERNAL )
                        .addParameter( NameObservable.NAME, "nc" + nc ).addHeader(
                            S3HeaderType.NAMING_CONVENTION,
                            ( NamingConventionType.CONCAT_LOWERCASE == nc ) ?
                                    nc.name() : nc.convert( nc.name() ) );
            driver.run();
            driver.assertResponseToClientContains( "<" + nc.convert( "authId" ) + ">" );
            
            for ( final NamingConventionType inc : NamingConventionType.values() )
            {
                if ( inc == nc )
                {
                    continue;
                }
                driver.assertResponseToClientDoesNotContain( "<" + inc.convert( "authId" ) + ">" );
            }
        }
    }
}
