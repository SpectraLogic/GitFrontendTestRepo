/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.net.rpc.frmwrk;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotating an RPC method with this annotation permits that method to return null.  <br><br>
 * 
 * Annotating an RPC method request parameter with this annotation permits that request parameter to be null.
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( { ElementType.METHOD, ElementType.PARAMETER } )
public @interface NullAllowed
{
    // empty
}
