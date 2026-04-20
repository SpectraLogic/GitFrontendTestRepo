/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.server.handler.reqhandler;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

import com.spectralogic.s3.common.dao.domain.ds3.DataPolicy;
import com.spectralogic.s3.common.dao.domain.ds3.VersioningLevel;
import com.spectralogic.s3.common.dao.domain.shared.NameObservable;
import com.spectralogic.s3.common.testfrmwrk.MockDaoDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestDriver;
import com.spectralogic.s3.server.mock.MockHttpRequestSupport;
import com.spectralogic.s3.server.mock.MockInternalRequestAuthorizationStrategy;
import com.spectralogic.s3.server.request.rest.RestDomainType;
import com.spectralogic.util.http.RequestType;
import com.spectralogic.util.lang.CollectionFactory;
import org.junit.jupiter.api.Test;

public final class ModifyBeanRequestHandlerIntegration_Test 
{
    @Test
    public void testModifyOnlyAttemptsToModifyPropertiesThatHaveActuallyChanged()
    {
        final MockHttpRequestSupport support = new MockHttpRequestSupport();
        final MockDaoDriver mockDaoDriver = new MockDaoDriver( support.getDatabaseSupport() );
        final DataPolicy dataPolicy = mockDaoDriver.createDataPolicy( "policy1" );
        support.setDataPolicyInterfaceIh( new InvocationHandler()
        {
            public Object invoke( final Object proxy, final Method method, final Object[] args )
                    throws Throwable
            {
                final String [] propsToModify = (String[])args[ 1 ];
                if ( CollectionFactory.toSet( propsToModify ).contains( DataPolicy.VERSIONING ) )
                {
                    throw new RuntimeException( "Cannot change versioning." );
                }
                return null;
            }
        } );
        
        MockHttpRequestDriver driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_POLICY + "/" + dataPolicy.getName() )
            .addParameter( NameObservable.NAME, "newname" )
            .addParameter( DataPolicy.VERSIONING, dataPolicy.getVersioning().toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 200 );
        
        driver = new MockHttpRequestDriver(
                support, 
                true, 
                new MockInternalRequestAuthorizationStrategy(), 
                RequestType.PUT, 
                "_rest_/" + RestDomainType.DATA_POLICY + "/" + "newname" )
            .addParameter( NameObservable.NAME, "newname" )
            .addParameter( 
                    DataPolicy.VERSIONING, 
                    VersioningLevel.values()[ ( dataPolicy.getVersioning().ordinal() + 1 ) % 2].toString() );
        driver.run();
        driver.assertHttpResponseCodeEquals( 500 );
    }
}
