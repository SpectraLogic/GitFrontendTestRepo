/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.util.Collection;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.query.AggregatingWhereClause.AggregationType;
import com.spectralogic.util.db.query.BeanPropertyWhereClause.ComparisonType;
import com.spectralogic.util.db.query.BeanPropertyWhereClause.MultipleBeanPropertiesAggregationMode;
import com.spectralogic.util.lang.CollectionFactory;

public final class Require
{
    private Require()
    {
        // singleton
    }
    
    
    public static WhereClause all( final Collection< WhereClause > whereClausesToAndTogether )
    {
        return all( CollectionFactory.toArray( WhereClause.class, whereClausesToAndTogether ) );
    }
    
    
    public static WhereClause all( final WhereClause ... whereClausesToAndTogether )
    {
        return new AggregatingWhereClause( AggregationType.AND, whereClausesToAndTogether );
    }
    
    
    public static WhereClause any( final Collection< WhereClause > whereClausesToOrTogether )
    {
        return any( CollectionFactory.toArray( WhereClause.class, whereClausesToOrTogether ) );
    }
    
    
    public static WhereClause any( final WhereClause ... whereClausesToOrTogether )
    {
        return new AggregatingWhereClause( AggregationType.OR, whereClausesToOrTogether );
    }
    
    
    /**
     * Creates an exists clause for a type which the current type references with a foreign key.
     * 
     * @param propertyName The property that references another type.
     */
    public static WhereClause exists( 
            final String propertyName,
            final WhereClause nestedTypeWhereClause )
    {
        return new ExistsWhereClause( propertyName, nestedTypeWhereClause );
    }


    /**
     * Creates an NOT exists clause for a type which the current type references with a foreign key.
     *
     * @param propertyName The property that references another type.
     * @param nestedTypeWhereClause The clause that must be true for each nested type
     */
    public static WhereClause every(
            final String propertyName,
            final WhereClause nestedTypeWhereClause )
    {
        return Require.not(Require.exists(
                propertyName,
                Require.not(nestedTypeWhereClause)));
    }


    /**
     * Creates an exists clause for a type that has a foreign key to the current type.
     * 
     * @param nestedType The type that has a foreign key to the current type.
     * @param nestedTypePropertyName The property that references the current type.
     */
    public static WhereClause exists(
            final Class< ? extends DatabasePersistable > nestedType, 
            final String nestedTypePropertyName, 
            final WhereClause nestedTypeWhereClause )
    {
        return new ExistsWhereClause( nestedType, nestedTypePropertyName, nestedTypeWhereClause );
    }


    /**
     * Creates an exists clause that will compare specified properties of the current type and a nested type.
     *
     * @param propertyName The property we will use for comparison.
     * @param nestedType The nested type we will use in the exists clause.
     * @param nestedTypePropertyName The nested property we will use for comparison.
     * @param nestedTypeWhereClause The clause that must be true for each nested type.
     */
    public static WhereClause exists(
            final String propertyName,
            final Class< ? extends DatabasePersistable > nestedType,
            final String nestedTypePropertyName,
            final WhereClause nestedTypeWhereClause )
    {
        return new ExistsWhereClause( propertyName, nestedType, nestedTypePropertyName, nestedTypeWhereClause );
    }


    /**
     * Creates an NOT exists clause for a type that has a foreign key to the current type.
     *
     * @param nestedType The type that has a foreign key to the current type.
     * @param nestedTypePropertyName The property that references the current type.
     * @param nestedTypeWhereClause The clause that must be true for each nested type
     */
    public static WhereClause every(
            final Class< ? extends DatabasePersistable > nestedType,
            final String nestedTypePropertyName,
            final WhereClause nestedTypeWhereClause )
    {
        return Require.not(Require.exists(
                nestedType,
                nestedTypePropertyName,
                Require.not(nestedTypeWhereClause)));
    }
    
    
    public static WhereClause nothing()
    {
        return AllResultsWhereClause.INSTANCE;
    }
    
    
    public static WhereClause not( final WhereClause whereClause )
    {
        return new NegationWhereClause( whereClause );
    }


    public static WhereClause beanPropertyNull(final String beanProperty) {
        return beanPropertyEquals(beanProperty, null);
    }


    public static WhereClause beanPropertyNotNull(final String beanProperty) {
        return Require.not(beanPropertyNull(beanProperty));
    }

    
    public static WhereClause beanPropertyEqualsOneOf(
            final String beanProperty,
            final Collection< ? > values)
    {
        return beanPropertyEqualsOneOf( beanProperty, CollectionFactory.toArray( Object.class, values ) );
    }
    
    
    public static WhereClause beanPropertyEqualsOneOf(
            final String beanProperty,
            final Object ... values )
    {
        if ( 0 == values.length )
        {
            return not( Require.nothing() );
        }
        return new BeanPropertyInWhereClause( beanProperty, CollectionFactory.toList( values ) );
    }
    
    
    public static BeanPropertyWhereClause beanPropertyEquals( 
            final String beanProperty, 
            final Object value )
    {
        return new BeanPropertyWhereClause( ComparisonType.EQUALS, beanProperty, value );
    }
    
    
    /**
     * @param matchString - A normal string containing any number of % and/or _
     * wildcard characters. % matches zero or more characters and _ matches a
     * single character. To provide a literal % or _, escape it with \ ("\\" in
     * Java). To escape all special characters in a string, use Sanitize.patternLiteral().
     */
    public static BeanPropertyWhereClause beanPropertyMatchesInsensitive( final String beanProperty,
            final String matchString )
    {
        return new BeanPropertyWhereClause( ComparisonType.MATCHES_INSENSITIVE, beanProperty, matchString );
    }


    public static WhereClause oneOfBeanPropertiesMatchesInsensitive( final String matchString, final String ... beanProperty)
    {
        WhereClause retval = Require.not(Require.nothing());
        for (final String property : beanProperty) {
            retval = Require.any(retval, Require.beanPropertyMatchesInsensitive(property, matchString));
        }
        return retval;
    }


    public static WhereClause oneOfBeanPropertiesMatches( final String matchString, final String ... beanProperty)
    {
        WhereClause retval = Require.not(Require.nothing());
        for (final String property : beanProperty) {
            retval = Require.any(retval, Require.beanPropertyMatches(property, matchString));
        }
        return retval;
    }

    
    public static BeanPropertyWhereClause beanPropertyMatches( final String beanProperty,
            final String matchString )
    {
        return new BeanPropertyWhereClause( ComparisonType.MATCHES, beanProperty, matchString );
    }
    
    
    public static BeanPropertyWhereClause beanPropertyLessThan(
            final String beanProperty,
            final Object value )
    {
        return new BeanPropertyWhereClause( ComparisonType.LESS_THAN, beanProperty, value );
    }
    
    
    public static BeanPropertyWhereClause beanPropertyGreaterThan(
            final String beanProperty,
            final Object value )
    {
        return new BeanPropertyWhereClause( ComparisonType.GREATER_THAN, beanProperty, value );
    }
    
    
    public static BeanPropertyWhereClause beanPropertiesSumLessThan(
            final String beanProperty1,
            final String beanProperty2,
            final Object value )
    {
        return new BeanPropertyWhereClause(
                ComparisonType.LESS_THAN, 
                MultipleBeanPropertiesAggregationMode.SUM, 
                new String [] { beanProperty1, beanProperty2 }, 
                value );
    }
    
    
    public static BeanPropertyWhereClause beanPropertiesSumGreaterThan(
            final String beanProperty1,
            final String beanProperty2,
            final Object value )
    {
        return new BeanPropertyWhereClause(
                ComparisonType.GREATER_THAN, 
                MultipleBeanPropertiesAggregationMode.SUM, 
                new String [] { beanProperty1, beanProperty2 }, 
                value );
    }
}
