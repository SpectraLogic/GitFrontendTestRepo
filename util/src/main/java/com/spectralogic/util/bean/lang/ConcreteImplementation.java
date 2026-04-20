/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean.lang;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Beans that are marked as {@link SimpleBeanSafeToProxy} that should not be generated reflectively and 
 * dynamically, but rather, for performance reasons, should be generated using a concrete implementation, 
 * should use this annotation to define their concrete implementation class. <br><br>
 * 
 * Only define a concrete implementation if at least one of the following applies:
 * <ul>
 * <li> The memory footprint for bean instances is <u>critical</u> (there will be millions or at least
 *      hundreds of thousands of instances).  This is the most common reason for using concrete 
 *      implementations.  <br><br>
 *      
 * <li> Performance creating bean instances of the type is <u>critical</u> (creating concrete instances is ~4X
 *      faster than creating reflective instances); however, method invocation performance is not 
 *      significantly affected by using reflective instances.  This is rarely a valid reason though, since 
 *      reflective instantiation is plenty fast for nearly all circumstances.
 * </ul>
 * 
 * <font color = red><b>Warning</b><br>Your concrete implementation may not violate any of the contracts 
 * of {@link SimpleBeanSafeToProxy} since the interface implements {@link SimpleBeanSafeToProxy}.  You 
 * may not define a concrete implementation on a {@link SimpleBeanSafeToProxy} for any purpose other than 
 * improving performance.</font>
 */
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.TYPE )
public @interface ConcreteImplementation
{
    /**
     * <font color = red><b>Warning</b><br>Your concrete implementation may not violate any of the contracts 
     * of {@link SimpleBeanSafeToProxy} since the interface implements {@link SimpleBeanSafeToProxy}.  You 
     * may not define a concrete implementation on a {@link SimpleBeanSafeToProxy} for any purpose other than 
     * improving performance.</font>
     * 
     * @return the concrete implementation type
     */
    Class< ? > value();
}
