/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.mock;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class ConstantResponseInvocationHandler_Test
{
    @Test
    public void testInvocationHandlerReturnsConstantResponse() throws Throwable
    {
        assertEquals(
                null,
                new ConstantResponseInvocationHandler( null ).invoke( null, null, null ),
                "Shoulda returned constant response."
                 );
        assertEquals(
                "abc",
                new ConstantResponseInvocationHandler( "abc" ).invoke( null, null, null ),
                "Shoulda returned constant response."
               );
    }
}
