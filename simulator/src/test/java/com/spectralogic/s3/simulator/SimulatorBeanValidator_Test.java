/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.simulator;

import com.spectralogic.util.bean.BeanValidator;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.find.PackageContentFinder;
import com.spectralogic.util.predicate.UnaryPredicate;
import org.junit.jupiter.api.Test;

public final class SimulatorBeanValidator_Test
{
    @Test
    public void testPcoClassesPassBeanValidator()
    {
        final PackageContentFinder finder = new PackageContentFinder( 
                SimulatorBeanValidator_Test.class.getPackage().getName(), 
                Simulator.class, 
                null );
        for ( Class< ? > clazz : finder.getClasses( new PcoClassFilter() ) )
        {
            BeanValidator.test( clazz );
        }
    }
    
    
    private final static class PcoClassFilter implements UnaryPredicate< Class<?> >
    {
        public boolean test( final Class< ? > element )
        {
            if ( element.getName().contains( "_Test" ) )
            {
                return false;
            }
            return ( SimpleBeanSafeToProxy.class.isAssignableFrom( element ) );
        }
    } // end inner class def
}
