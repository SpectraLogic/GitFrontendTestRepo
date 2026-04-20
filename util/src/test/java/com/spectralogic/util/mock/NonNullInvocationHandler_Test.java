/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class NonNullInvocationHandler_Test
{
    @Test
    public void testInvocationsReturnNonNull()
    {
        @SuppressWarnings( "unchecked" )
        final List< Object > proxy = InterfaceProxyFactory.getProxy( 
                List.class, 
                NonNullInvocationHandler.getInstance() );
        assertNotNull(
                Integer.valueOf( proxy.size() ),
                "Shoulda returned non-null."
                 );
        assertNotNull(
                proxy.toString(),
                "Shoulda returned non-null."
                 );
        assertNotNull(
                Boolean.valueOf( proxy.add( null ) ),
                "Shoulda returned non-null."
                 );
        assertNotNull(
                proxy.get( 2 ),
                "Shoulda returned non-null."
                 );
    }
}
