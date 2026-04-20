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

public final class NullInvocationHandler_Test 
{
    @Test
    public void testInvocationsReturnNull()
    {
        @SuppressWarnings( "unchecked" )
        final List< Object > proxy = InterfaceProxyFactory.getProxy( 
                List.class, 
                NullInvocationHandler.getInstance() );
        assertNull(
                proxy.toString(),
                "Shoulda returned non-null."
                 );
        assertNull(
                proxy.get( 2 ),
                "Shoulda returned non-null."
                 );
    }
}
