/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.mockdomain.County;
import com.spectralogic.util.db.mockdomain.Principal;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockdomain.TeacherSchool;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class ExistsWhereClause_Test 
{
    @Test
    public void testToSqlReturnsExpectedClauseWhenExplicitNestedTypeProvided()
    {
        final ExistsWhereClause existsWhereClause = new ExistsWhereClause(
                TeacherSchool.class,
                TeacherSchool.TEACHER_ID,
                AllResultsWhereClause.INSTANCE );
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = existsWhereClause.toSql( Teacher.class, sqlParameters );
        assertEquals(
                "EXISTS (SELECT * FROM mockdomain.teacher_school WHERE mockdomain.teacher.id = mockdomain.teacher_school.teacher_id AND (true))",
                sql,
                "Shoulda returned the expected exists clause."
                 );
        assertTrue(sqlParameters.isEmpty(), "Shoulda left the sql parameters empty.");
    }
    
    
    @Test
    public void testToSqlReturnsExpectedClauseWhenImplicitNestedTypeUsed()
    {
        final ExistsWhereClause existsWhereClause = new ExistsWhereClause(
                TeacherSchool.TEACHER_ID,
                AllResultsWhereClause.INSTANCE );
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = existsWhereClause.toSql( TeacherSchool.class, sqlParameters );
        assertEquals(
                "EXISTS (SELECT * FROM mockdomain.teacher WHERE mockdomain.teacher_school.teacher_id = mockdomain.teacher.id AND (true))",
                sql,
                "Shoulda returned the expected exists clause."
                 );
        assertTrue(sqlParameters.isEmpty(), "Shoulda left the sql parameters empty.");
    }
    
    
    @Test
    public void testToSqlWhenImplicitNestedTypeAndNoReferenceNotAllowed()
    {
        final ExistsWhereClause existsWhereClause = new ExistsWhereClause(
                County.NAME,
                AllResultsWhereClause.INSTANCE );
        final List< Object > sqlParameters = new ArrayList<>();
        TestUtil.assertThrows(
                "Shoulda thrown an exception when there wasn't any other type to link.",
                RuntimeException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        existsWhereClause.toSql( County.class, sqlParameters );
                    }
                } );
    }
    
    
    @Test
    public void testExistsClauseWhenExplicitNestedTypeAndNoReferenceNotAllowed()
    {
        final ExistsWhereClause existsWhereClause = new ExistsWhereClause(
                Teacher.class,
                Teacher.COMMENTS,
                AllResultsWhereClause.INSTANCE );
        final List< Object > sqlParameters = new ArrayList<>();
        TestUtil.assertThrows(
                "Shoulda thrown an exception when the other type to link was invalid.",
                RuntimeException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        existsWhereClause.toSql( County.class, sqlParameters );
                    }
                } );
    }
    
    
    @Test
    public void testExistsClauseWhenExplicitNestedTypeAndNoCorrectReferenceNotAllowed()
    {
        final ExistsWhereClause existsWhereClause = new ExistsWhereClause(
                TeacherSchool.class,
                TeacherSchool.TEACHER_ID,
                AllResultsWhereClause.INSTANCE );
        final List< Object > sqlParameters = new ArrayList<>();
        TestUtil.assertThrows(
                "Shoulda thrown an exception when the other type to link was invalid.",
                RuntimeException.class,
                new BlastContainer()
                {
                    public void test() throws Throwable
                    {
                        existsWhereClause.toSql( County.class, sqlParameters );
                    }
                } );
    }


    @Test
    public void testToSqlReturnsExpectedClauseWhenBothPropertiesSpecified()
    {
        final ExistsWhereClause existsWhereClause = new ExistsWhereClause(
                Teacher.TYPE,
                Principal.class,
                Principal.TYPE,
                AllResultsWhereClause.INSTANCE );
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = existsWhereClause.toSql( Teacher.class, sqlParameters );
        assertEquals(
                "EXISTS (SELECT * FROM mockdomain.principal WHERE mockdomain.teacher.type = mockdomain.principal.type AND (true))",
                sql,
                "Shoulda returned the expected exists clause for matching non-id properties."
                 );
        assertTrue(sqlParameters.isEmpty(), "Shoulda left the sql parameters empty.");
    }
}
