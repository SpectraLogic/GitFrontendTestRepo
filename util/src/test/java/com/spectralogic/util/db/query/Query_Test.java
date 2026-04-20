/*******************************************************************************
 *
 * Copyright C 2017, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.util.ArrayList;
import java.util.List;

import com.spectralogic.util.bean.BeanSQLOrdering;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.db.mockdomain.County;
import com.spectralogic.util.db.mockdomain.Principal;
import com.spectralogic.util.db.mockdomain.PrincipalSchool;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.query.Query.LimitableRetrievable;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public final class Query_Test 
{
    @Test
    public void testCanBuildWhereClausjQuery()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = Query
            .where( Require.beanPropertyEquals( County.NAME, "Boulder" ) )
            .toSql( County.class, sqlParameters );
        assertEquals("SELECT * FROM mockdomain.county WHERE name = ?", sql, "Shoulda created the expected SQL statement.");

        assertEquals(CollectionFactory.< Object >toList( "Boulder" ),
                sqlParameters,
                "Shoulda added the expected parameters.");
    }
    
    
    @Test
    public void testCanBuildOrderByQuery()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( PrincipalSchool.PRINCIPAL_ID, SortBy.Direction.DESCENDING );
        ordering.add( PrincipalSchool.SCHOOL_ID, SortBy.Direction.ASCENDING );
        final String sql = Query.where( Require.nothing() )
                                .orderBy( ordering )
                                .toSql( PrincipalSchool.class, sqlParameters );
        assertEquals("SELECT * FROM mockdomain.principal_school WHERE true ORDER BY principal_id DESC, school_id ASC", sql, "Shoulda created the expected SQL statement.");
        final Object expected = CollectionFactory.toList();
        assertEquals(expected, sqlParameters, "Shoulda added the expected parameters.");
    }
    
    
    @Test
    public void testOrderByQuerySingleAttributeWithNoIndexNotAllowed()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( Principal.TYPE, SortBy.Direction.ASCENDING );
        final LimitableRetrievable query = Query
                .where( Require.nothing() )
                .orderBy( ordering );
        TestUtil.assertThrows(
                "Shoulda thrown an exception because the order by columns did not have an index.",
                UnsupportedOperationException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        query.toSql( Principal.class, sqlParameters );
                    }
                } );
    }
    
    
    @Test
    public void testOrderByQueryMultiAttributeWithNoIndexNotAllowed()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( Principal.NAME, SortBy.Direction.ASCENDING );
        ordering.add( Principal.TYPE, SortBy.Direction.DESCENDING );
        final LimitableRetrievable query = Query
                .where( Require.nothing() )
                .orderBy( ordering );
        TestUtil.assertThrows(
                "Shoulda thrown an exception because the order by columns did not have an index.",
                UnsupportedOperationException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        query.toSql( Principal.class, sqlParameters );
                    }
                } );
    }
    
    
    @Test
    public void testOrderByQueryMultiAttributeWithCloseButNoIndexNotAllowed()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( Teacher.DATE_OF_BIRTH, SortBy.Direction.ASCENDING );
        ordering.add( Teacher.TYPE, SortBy.Direction.ASCENDING );
        final LimitableRetrievable query = Query
                .where( Require.nothing() )
                .orderBy( ordering );
        TestUtil.assertThrows(
                "Shoulda thrown an exception because the order by columns did not have an index.",
                UnsupportedOperationException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        query.toSql( Teacher.class, sqlParameters );
                    }
                } );
    }
    
    
    @Test
    public void testOrderByPropertyThatDoesNotExistOnTypeNotAllowed()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( Principal.TYPE, SortBy.Direction.ASCENDING );
        final LimitableRetrievable query = Query
                .where( Require.nothing() )
                .orderBy( ordering );
        TestUtil.assertThrows(
                "Shoulda thrown an exception because prop does not exist on type.",
                IllegalArgumentException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        query.toSql( PrincipalSchool.class, sqlParameters );
                    }
                } );
    }
    
    
    @Test
    public void testCanBuildOrderByWhenNonUniqueIndex()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( Principal.NAME, SortBy.Direction.ASCENDING );
        final String sql = Query
                .where( Require.nothing() )
                .orderBy( ordering )
                .limit( 10 )
                .toSql( Principal.class, sqlParameters );
        assertEquals("SELECT * FROM mockdomain.principal WHERE true" + " ORDER BY name ASC LIMIT ?", sql, "Shoulda created the expected SQL statement.");
        final Object expected = CollectionFactory.< Object >toList( Integer.valueOf( 10 ) );
        assertEquals(expected, sqlParameters, "Shoulda added the expected parameters.");
    }
    
    
    @Test
    public void testCanBuildOrderByWhenForeignKeyIndex()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( PrincipalSchool.PRINCIPAL_ID, SortBy.Direction.ASCENDING );
        final String sql = Query.where( Require.nothing() )
                                .orderBy( ordering )
                                .limit( 10 )
                                .toSql( PrincipalSchool.class, sqlParameters );
        assertEquals("SELECT * FROM mockdomain.principal_school WHERE true" + " ORDER BY principal_id ASC LIMIT ?", sql, "Shoulda created the expected SQL statement.");
        final Object expected = CollectionFactory.< Object >toList( Integer.valueOf( 10 ) );
        assertEquals(expected, sqlParameters, "Shoulda added the expected parameters.");
    }
    
    
    @Test
    public void testCanBuildOrderByWithLimitQuery()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( PrincipalSchool.PRINCIPAL_ID, SortBy.Direction.ASCENDING );
        ordering.add( PrincipalSchool.SCHOOL_ID, SortBy.Direction.ASCENDING );
        final String sql = Query.where( Require.nothing() )
                                .orderBy( ordering )
                                .limit( 10 )
                                .toSql( PrincipalSchool.class, sqlParameters );
        assertEquals("SELECT * FROM mockdomain.principal_school WHERE true ORDER BY principal_id ASC, school_id ASC LIMIT ?", sql, "Shoulda created the expected SQL statement.");
        final Object expected = CollectionFactory.< Object >toList( Integer.valueOf( 10 ) );
        assertEquals(expected, sqlParameters, "Shoulda added the expected parameters.");
    }
    
    
    @Test
    public void testCanBuildOrderByWithOffsetQuery()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( PrincipalSchool.PRINCIPAL_ID, SortBy.Direction.ASCENDING );
        ordering.add( PrincipalSchool.SCHOOL_ID, SortBy.Direction.ASCENDING );
        final String sql = Query.where( Require.nothing() )
                                .orderBy( ordering )
                                .offset( 10 )
                                .toSql( PrincipalSchool.class, sqlParameters );
        assertEquals("SELECT * FROM mockdomain.principal_school WHERE true ORDER BY principal_id ASC, school_id ASC OFFSET ?", sql, "Shoulda created the expected SQL statement.");
        final Object expected = CollectionFactory.< Object >toList( Integer.valueOf( 10 ) );
        assertEquals(expected, sqlParameters, "Shoulda added the expected parameters.");
    }
    
    
    @Test
    public void testCanBuildOrderByWithLimitAndOffsetQuery()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final BeanSQLOrdering ordering = new BeanSQLOrdering();
        ordering.add( PrincipalSchool.PRINCIPAL_ID, SortBy.Direction.ASCENDING );
        ordering.add( PrincipalSchool.SCHOOL_ID, SortBy.Direction.ASCENDING );
        final String sql = Query.where( Require.nothing() )
                                .orderBy( ordering )
                                .limit( 11 )
                                .offset( 12 )
                                .toSql( PrincipalSchool.class, sqlParameters );
        assertEquals("SELECT * FROM mockdomain.principal_school WHERE true ORDER BY principal_id ASC, school_id ASC LIMIT ? OFFSET ?", sql, "Shoulda created the expected SQL statement.");
        final Object expected = CollectionFactory.< Object >toList( Integer.valueOf( 11 ), Integer.valueOf( 12 ) );
        assertEquals(expected, sqlParameters, "Shoulda added the expected parameters.");
    }
}
