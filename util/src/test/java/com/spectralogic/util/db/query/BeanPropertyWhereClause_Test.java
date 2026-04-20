/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.lang.Identifiable;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.mockdomain.County;
import com.spectralogic.util.db.mockdomain.School;
import com.spectralogic.util.db.mockdomain.SchoolType;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockdomain.TeacherType;
import com.spectralogic.util.db.query.BeanPropertyWhereClause.ComparisonType;
import com.spectralogic.util.db.query.BeanPropertyWhereClause.MultipleBeanPropertiesAggregationMode;
import com.spectralogic.util.exception.DaoException;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.mock.InterfaceProxyFactory;
import com.spectralogic.util.mock.NullInvocationHandler;
import com.spectralogic.util.testfrmwrk.TestBean;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class BeanPropertyWhereClause_Test
{

    @Test
    public void testConstructorNullComparisionTypeNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BeanPropertyWhereClause( null, TestBean.INT_PROP, null );
            }
         } );
    }
    

    @Test
    public void testConstructorNullPropertyNameNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
            {
                new BeanPropertyWhereClause( ComparisonType.EQUALS, null, null );
            }
            } );
    }


    @Test
    public void testInvalidComparisonsFail()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() {
                final BeanPropertyWhereClause bpwc = new BeanPropertyWhereClause(ComparisonType.EQUALS, School.ID, "invalid string");
                bpwc.toSql(School.class, new ArrayList<>());
            }
        } );
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() {
                final BeanPropertyWhereClause bpwc = new BeanPropertyWhereClause(ComparisonType.EQUALS, School.ACTIVE, "invalid string");
                bpwc.toSql(School.class, new ArrayList<>());
            }
        } );
    }

    
    @Test
    public void testToSqlWithEqualsComparisonAndNonNullValueReturnsCorrectSqlClause()
    {
        checkToSqlWithNonNullValue( ComparisonType.EQUALS, "Boulder County", "name = ?" );
    }
    
    
    @Test
    public void testToSqlWithMatchesInsensitiveComparisonAndNonNullValueReturnsCorrectSqlClause()
    {
        checkToSqlWithNonNullValue( ComparisonType.MATCHES_INSENSITIVE, "%Boulder%County%", "name ILIKE ?" );
    }
    
    
    @Test
    public void testToSqlWithMatchesComparisonAndNonNullValueReturnsCorrectSqlClause()
    {
        checkToSqlWithNonNullValue( ComparisonType.MATCHES, "%Boulder%County%", "name LIKE ?" );
    }

    
    @Test
    public void testToSqlWithGreaterThanComparisonAndNonNullValueReturnsCorrectSqlClause()
    {
        checkToSqlWithNonNullValue( ComparisonType.GREATER_THAN, "Boulder County", "name > ?" );
    }

    
    @Test
    public void testToSqlWithLessThanComparisonAndNonNullValueReturnsCorrectSqlClause()
    {
        checkToSqlWithNonNullValue( ComparisonType.LESS_THAN, "Boulder County", "name < ?" );
    }


    private void checkToSqlWithNonNullValue(
            final ComparisonType comparisonType,
            final String inputParameter,
            final String expectedSql )
    {
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( comparisonType, County.NAME, inputParameter );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( County.class, parameters );
        
        assertNotNull( sql,"Shoulda returned an sql where clause."  );
        assertEquals(expectedSql, sql, "Shoulda returned equality comparison SQL.");

        assertEquals(1,  parameters.size(), "Shoulda added a parameter.");
        assertEquals(inputParameter, parameters.get( 0 ), "Shoulda added the expected parameter.");
    }

    
    @Test
    public void testMultipleBeanPropsToSqlWithGreaterThanComparisonAndNonNullValueReturnsCorrectSqlClause()
    {
        checkMultipleBeanPropertiesToSqlWithNonNullValue(
                ComparisonType.GREATER_THAN, Integer.valueOf( 2 ), "warnings_issued + years_of_service > ?" );
    }

    
    @Test
    public void testMultipleBeanPropsToSqlWithLessThanComparisonAndNonNullValueReturnsCorrectSqlClause()
    {
        checkMultipleBeanPropertiesToSqlWithNonNullValue( 
                ComparisonType.LESS_THAN, Integer.valueOf( 2 ), "warnings_issued + years_of_service < ?" );
    }


    private void checkMultipleBeanPropertiesToSqlWithNonNullValue(
            final ComparisonType comparisonType,
            final Integer inputParameter,
            final String expectedSql )
    {
        final BeanPropertyWhereClause beanPropertyWhereClause = new BeanPropertyWhereClause( 
                comparisonType, 
                MultipleBeanPropertiesAggregationMode.SUM,
                new String [] { Teacher.WARNINGS_ISSUED, Teacher.YEARS_OF_SERVICE },
                inputParameter );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( Teacher.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals(expectedSql, sql, "Shoulda returned equality comparison SQL.");

        assertEquals(1,  parameters.size(), "Shoulda added a parameter.");
        assertEquals(inputParameter, parameters.get( 0 ), "Shoulda added the expected parameter.");
    }
    
    
    @Test
    public void testMultipleBeanPropsToSqlWhereBeanPropTypesMismatchNotAllowed()
    {
        final BeanPropertyWhereClause beanPropertyWhereClause = new BeanPropertyWhereClause( 
                ComparisonType.GREATER_THAN, 
                MultipleBeanPropertiesAggregationMode.SUM,
                new String [] { Teacher.WARNINGS_ISSUED, Teacher.NAME },
                Integer.valueOf( 2 ) );
        final List< Object > parameters = new ArrayList<>();
        TestUtil.assertThrows( null, UnsupportedOperationException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                beanPropertyWhereClause.toSql( Teacher.class, parameters );
            }
        } );
    }

    
    @Test
    public void testToStringDoesNotBlowUp()
    {
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, School.TYPE, SchoolType.PUBLIC );
        assertNotNull(
                "Shoulda returned an actual value from toString().",
                beanPropertyWhereClause.toString() );
    }

    
    @Test
    public void testComparisonOfSecretNotAllowed()
    {
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, Teacher.PASSWORD, "" );
        final List< Object > parameters = new ArrayList<>();
        TestUtil.assertThrows(
                "Shoulda thrown a security check dao exception.",
                DaoException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        beanPropertyWhereClause.toSql( Teacher.class, parameters );
                    }
                } );
    }

    
    @Test
    public void testToSqlWithEqualsComparisonAndEnumValueReturnsCorrectSqlClause()
    {
        final SchoolType desiredSchoolType = SchoolType.PUBLIC;
        
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, School.TYPE, desiredSchoolType );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( School.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals("type = CAST(? AS mockdomain.school_type)", sql, "Shoulda returned equality comparison SQL.");

        assertEquals(1,  parameters.size(), "Shoulda added a parameter.");
        assertEquals(desiredSchoolType, parameters.get( 0 ), "Shoulda added the expected parameter.");
    }

    
    @Test
    public void testToSqlWithEqualsComparisonAndDynamicValueProviderReturnsCorrectSqlClause()
    {
        final Object dynamicValueProvider = InterfaceProxyFactory.getProxy(
                DynamicValueProvider.class,
                NullInvocationHandler.getInstance() );
        
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, County.NAME, dynamicValueProvider );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( County.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals("name IS NULL", sql, "Shoulda returned null comparison SQL.");

        assertEquals(0,  parameters.size(), "Should nota added a parameter.");
    }

    
    @Test
    public void testToSqlWithEqualsComparisonAndDynamicValueProviderReturnsSqlClauseWithCorrectValue()
    {
        final String value1 = "edf9f90f-b789-4841-87fc-398044702dab";
        checkDynamicValueProvider(
                County.class,
                Identifiable.ID,
                "id = ?",
                value1,
                UUID.fromString( value1 ) );

        final String value2 = "1234567890";
        checkDynamicValueProvider(
                County.class,
                County.POPULATION,
                "population = ?",
                value2,
                Long.valueOf( value2 ) );

        final String value3 = "Tue, 30 Sep 2014 18:13:41 EDT";
        checkDynamicValueProvider(
                Teacher.class,
                Teacher.DATE_OF_BIRTH,
                "date_of_birth = ?",
                value3,
                parseDate( value3 ) );

        final String value4 = "10";
        checkDynamicValueProvider(
                Teacher.class,
                Teacher.YEARS_OF_SERVICE,
                "years_of_service = ?",
                value4,
                Integer.valueOf( value4 ) );

        final String value5 = "TEACHER";
        checkDynamicValueProvider(
                Teacher.class,
                Teacher.TYPE,
                "type = CAST(? AS mockdomain.teacher_type)",
                value5,
                TeacherType.TEACHER );
    }


    private static Date parseDate( final String str )
    {
        try
        {
            return new SimpleDateFormat( "EEE, dd MMM yyyy HH:mm:ss z" ).parse( str );
        }
        catch ( final ParseException ex )
        {
            throw new RuntimeException( ex );
        }
    }


    private static void checkDynamicValueProvider(
            final Class< ? extends DatabasePersistable > clazz,
            final String field,
            final String dbExpression,
            final String value,
            final Object parameterValue )
    {
        final BeanPropertyWhereClause beanPropertyWhereClause = new BeanPropertyWhereClause(
                ComparisonType.EQUALS,
                field,
                new StaticDynamicValueProvider( value ) );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( clazz, parameters );
        assertEquals(dbExpression, sql, "Shoulda returned equals comparison SQL.");
        assertEquals(CollectionFactory.toList( parameterValue ), parameters, "Shoulda added the expected parameter.");
    }
    
    
    private static final class StaticDynamicValueProvider implements DynamicValueProvider
    {
        private StaticDynamicValueProvider( final Object value )
        {
            this.m_value = value;
        }
        
        @Override
        public Object getValue()
        {
            return m_value;
        }

        private final Object m_value;
    }

    
    @Test
    public void testToSqlWithEqualsComparisonAndUuidStringValueReturnsCorrectSqlClause()
    {
        final String desiredUuid = "c9433efe-a628-44e6-a536-5577d0578901";
        final UUID expectedValue = UUID.fromString( desiredUuid );
        
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, Identifiable.ID, desiredUuid );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( County.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals("id = ?", sql, "Shoulda returned equality comparison SQL.");

        assertEquals(1,  parameters.size(), "Shoulda added a parameter.");
        assertEquals(expectedValue, parameters.get( 0 ), "Shoulda added the expected parameter.");
    }

    
    @Test
    public void testToSqlWithEqualsComparisonAndUuidValueReturnsCorrectSqlClause()
    {
        final UUID desiredUuid = UUID.fromString( "c9433efe-a628-44e6-a536-5577d0578901" );
        
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, Identifiable.ID, desiredUuid );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( County.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals("id = ?", sql, "Shoulda returned equality comparison SQL.");

        assertEquals(1,  parameters.size(), "Shoulda added a parameter.");
        assertEquals(desiredUuid, parameters.get( 0 ), "Shoulda added the expected parameter.");
    }

    
    @Test
    public void testToSqlWithEqualsComparisonAndDateStringValueReturnsCorrectSqlClause()
    {
        final String desiredDate = "Thu Sep 28 20:29:30 MST 2000";
        @SuppressWarnings( "deprecation" )
        final Date expectedValue = new Date( desiredDate );
        
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, Teacher.DATE_OF_BIRTH, desiredDate );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( Teacher.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals("date_of_birth = ?", sql, "Shoulda returned equality comparison SQL.");

        assertEquals(1,  parameters.size(), "Shoulda added a parameter.");
        assertEquals(expectedValue, parameters.get( 0 ), "Shoulda added the expected parameter.");
    }

    @Test
    public void testToSqlWithEqualsComparisonAndDateValueReturnsCorrectSqlClause()
    {
        @SuppressWarnings( "deprecation" )
        final Date desiredDate = new Date( "Thu Sep 28 20:29:30 MST 2000" );
        
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, Teacher.DATE_OF_BIRTH, desiredDate );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( Teacher.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals("date_of_birth = ?", sql, "Shoulda returned equality comparison SQL.");

        assertEquals(1,  parameters.size(), "Shoulda added a parameter.");
        assertEquals(desiredDate, parameters.get( 0 ), "Shoulda added the expected parameter.");
    }

    
    @Test
    public void testToSqlWithEqualsComparisonAndLongStringValueReturnsCorrectSqlClause()
    {
        final long expectedValue = 123456789;
        final String desiredLong = Long.toString( expectedValue );
        
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, County.POPULATION, desiredLong );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( County.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals("population = ?", sql, "Shoulda returned equality comparison SQL.");

        assertEquals(1,  parameters.size(), "Shoulda added a parameter.");
        assertEquals(Long.valueOf( expectedValue ), parameters.get( 0 ), "Shoulda added the expected parameter.");
    }

    
    @Test
    public void testToSqlWithEqualsComparisonAndLongValueReturnsCorrectSqlClause()
    {
        final Long desiredLong = Long.valueOf( 123456789 );
        
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, County.POPULATION, desiredLong );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( County.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals("population = ?", sql, "Shoulda returned equality comparison SQL.");

        assertEquals(1,  parameters.size(), "Shoulda added a parameter.");
        assertEquals(desiredLong, parameters.get( 0 ), "Shoulda added the expected parameter.");
    }

    
    @Test
    public void testToSqlWithEqualsComparisonAndIntegerStringValueReturnsCorrectSqlClause()
    {
        final int expectedValue = 123456789;
        final String desiredInteger = Integer.toString( expectedValue );
        
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, Teacher.WARNINGS_ISSUED, desiredInteger );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( Teacher.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals("warnings_issued = ?", sql, "Shoulda returned equality comparison SQL.");

        assertEquals(1,  parameters.size(), "Shoulda added a parameter.");
        assertEquals(Integer.valueOf( expectedValue ), parameters.get( 0 ), "Shoulda added the expected parameter.");
    }

    
    @Test
    public void testToSqlWithEqualsComparisonAndIntValueReturnsCorrectSqlClause()
    {
        final Integer desiredInteger = Integer.valueOf( 123456789 );
        
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, Teacher.WARNINGS_ISSUED, desiredInteger );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( Teacher.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals("warnings_issued = ?", sql, "Shoulda returned equality comparison SQL.");

        assertEquals(1,  parameters.size(), "Shoulda added a parameter.");
        assertEquals(desiredInteger, parameters.get( 0 ), "Shoulda added the expected parameter.");
    }
    
    
    @Test
    public void testToSqlWithEqualsComparisonAndNullValueReturnsCorrectSqlClause()
    {
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( ComparisonType.EQUALS, County.NAME, null );
        final List< Object > parameters = new ArrayList<>();
        final String sql = beanPropertyWhereClause.toSql( County.class, parameters );
        
        assertNotNull( "Shoulda returned an sql where clause.", sql );
        assertEquals("name IS NULL", sql, "Shoulda returned equality comparison SQL.");

        assertEquals(0,  parameters.size(), "Should nota added a parameter.");
    }
    
    
    @Test
    public void testToSqlWithMatchesInsensitiveComparisonAndNullValueNotAllowed()
    {
        checkComparisonWithNullNotAllowed( ComparisonType.MATCHES_INSENSITIVE );
    }
    
    
    @Test
    public void testToSqlWithMatchesComparisonAndNullValueNotAllowed()
    {
        checkComparisonWithNullNotAllowed( ComparisonType.MATCHES );
    }
    
    
    @Test
    public void testToSqlWithGreaterThanComparisonAndNullValueNotAllowed()
    {
        checkComparisonWithNullNotAllowed( ComparisonType.GREATER_THAN );
    }
    
    
    @Test
    public void testToSqlWithLessThanComparisonAndNullValueNotAllowed()
    {
        checkComparisonWithNullNotAllowed( ComparisonType.LESS_THAN );
    }
    
    
    @Test
    public void testToSqlWithMatchesInsensitiveComparisonAndIntNotAllowed()
    {
        TestUtil.assertThrows(
                "Shoulda thrown an exception.", UnsupportedOperationException.class,
                () -> new BeanPropertyWhereClause( ComparisonType.MATCHES_INSENSITIVE, Teacher.YEARS_OF_SERVICE,
                        Integer.valueOf( 10 ) ) );
    }
    
    
    @Test
    public void testToSqlWithMatchesComparisonAndIntNotAllowed()
    {
        TestUtil.assertThrows( "Shoulda thrown an exception.", UnsupportedOperationException.class,
                () -> new BeanPropertyWhereClause( ComparisonType.MATCHES, Teacher.YEARS_OF_SERVICE,
                        Integer.valueOf( 10 ) ) );
    }


    private void checkComparisonWithNullNotAllowed( final ComparisonType comparisonType )
    {
        final BeanPropertyWhereClause beanPropertyWhereClause =
                new BeanPropertyWhereClause( comparisonType, County.NAME, null );
        final List< Object > parameters = new ArrayList<>();
        TestUtil.assertThrows(
                "Shoulda thrown an exception.",
                UnsupportedOperationException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        beanPropertyWhereClause.toSql( County.class, parameters );
                    }
                } );
    }
}
