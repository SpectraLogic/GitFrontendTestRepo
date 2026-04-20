/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean.lang;

import com.spectralogic.util.marshal.Marshalable;

/**
 * A JavaBean is any type that adheres to the Java Beans Specification.
 * 
 * This interface is intended to be extended by bean interface definitions and
 * designates a bean interface as being simple enough to safely proxy.  
 * Infrastructure components that proxy beans may look for this annotation to
 * confirm that proxying is in fact appropriate and safe.  A bean can be safely 
 * proxied if all of the following conditions are satisfied:<br><br>
 * 
 * 1) There are no indexed properties.<br><br>
 * 
 * 2) Every bean property must be independent and behave regularly.  That is, 
 *    for every bean property A, if there exists a setter method for A, it only 
 *    affects property A and it always affects it such that, if there exists a
 *    reader method for A, said reader will return the value passed into said 
 *    setter method for A.<br><br>
 * 
 * 3) All methods on the interface are for bean properties or have a
 *    BeanMethodInvocationHandler annotation to define the method's 
 *    implementation.<br><br>
 * 
 * 4) Reader-only bean properties, if their type is a SimpleBeanSafeToProxy, will
 *    automatically be initialized with an instance of that type.  Reader-only
 *    properties of any other type will be initialized to Java variable defaults
 *    (e.g. null for String or 0 for int). <br><br>
 * 
 * In general, a simple bean that is safe to proxy should always be proxied
 * (i.e. it should not have an implementation).
 *
 */
public interface SimpleBeanSafeToProxy extends Marshalable
{
    // empty
}
