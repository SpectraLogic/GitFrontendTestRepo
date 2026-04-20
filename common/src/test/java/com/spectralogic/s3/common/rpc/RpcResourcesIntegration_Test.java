/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.rpc;



import com.spectralogic.util.find.PackageContentFinder;
import com.spectralogic.util.net.rpc.frmwrk.RpcResource;
import com.spectralogic.util.net.rpc.frmwrk.RpcResourceUtil;
import com.spectralogic.util.predicate.UnaryPredicate;
import org.junit.jupiter.api.Test;

public final class RpcResourcesIntegration_Test
{
    @Test
    public void testAllRpcResourcesForValidity()
    {
        final PackageContentFinder finder = new PackageContentFinder( 
                RpcResourcesIntegration_Test.class.getPackage().getName(), 
                RpcServerPort.class, 
                null );
        int testedClasses = 0;
        for ( Class< ? > clazz : finder.getClasses( new RpcResourceFilter() ) )
        {
            ++testedClasses;
            @SuppressWarnings( "unchecked" )
            final Class< RpcResource > castedClass = (Class< RpcResource >)clazz;
            RpcResourceUtil.validate( castedClass );
        }
        if ( 0 == testedClasses )
        {
            throw new RuntimeException( "Nothing was tested." );
        }
    }
    
    
    private final static class RpcResourceFilter implements UnaryPredicate< Class< ? > >
    {
        public boolean test( final Class< ? > element )
        {
            return ( RpcResource.class.isAssignableFrom( element ) );
        }
    } // end inner class def
}
