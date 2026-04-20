/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.bean.lang;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotate bean getter methods that should be used to order the results with this annotation.  If multiple
 * getter methods have this annotation, results will be ordered by value from 1,2,...,N.
 */
@Documented
@Retention( RetentionPolicy.RUNTIME )
@Target( ElementType.METHOD )
public @interface SortBy
{
    int value() default 1;
    
    
    Direction direction() default Direction.ASCENDING;
    
    
    public enum Direction
    {
        ASCENDING( 1, "ASC" ),
        DESCENDING( -1, "DESC" )
        ;
        
        private Direction( final int comparisonMultiplier, final String sqlDirection )
        {
            m_comparisonMultiplier = comparisonMultiplier;
            m_sqlDirection = sqlDirection;
        }
        
        public int getComparisonMultiplier()
        {
            return m_comparisonMultiplier;
        }
    
    
        public String getSqlDirection()
        {
            return m_sqlDirection;
        }
    
        private final int m_comparisonMultiplier;
        private final String m_sqlDirection;
    }
}
