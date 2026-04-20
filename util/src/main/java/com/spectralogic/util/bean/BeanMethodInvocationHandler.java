/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationHandler;


/**
 * Annotate SimpleBeanSafeToProxy interface methods that aren't bean properties with this annotation to
 * define their InvocationHandler.
 */
@Target( ElementType.METHOD )
@Retention( RetentionPolicy.RUNTIME )
public @interface BeanMethodInvocationHandler
{
    /**
     * @return InvocationHandler to use for the bean method
     */
    Class< ? extends InvocationHandler > value();
}
