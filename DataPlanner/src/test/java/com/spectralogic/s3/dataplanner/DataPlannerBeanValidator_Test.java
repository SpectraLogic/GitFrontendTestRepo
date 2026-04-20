/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.dataplanner;



import com.spectralogic.util.bean.BeanValidator;
import com.spectralogic.util.bean.lang.SimpleBeanSafeToProxy;
import com.spectralogic.util.find.PackageContentFinder;
import com.spectralogic.util.predicate.UnaryPredicate;
import org.junit.jupiter.api.Test;

public final class DataPlannerBeanValidator_Test
{
    @Test
    public void testPcoClassesPassBeanValidator()
    {
        final PackageContentFinder finder = new PackageContentFinder( 
                DataPlannerBeanValidator_Test.class.getPackage().getName(), 
                DataPlanner.class, 
                null );
//        int testedClasses = 0;
        for ( Class< ? > clazz : finder.getClasses( new PcoClassFilter() ) )
        {
//            ++testedClasses;
            BeanValidator.test( clazz );
        }
//        if ( 0 == testedClasses )
//        {
//            throw new RuntimeException( "Nothing was tested." );
//        }
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
