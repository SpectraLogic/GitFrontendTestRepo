/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.service;

import java.util.Date;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockdomain.TeacherType;
import com.spectralogic.util.db.mockservice.TeacherService;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class RetrieveBeansResultImpl_Test 
{
    @Test
    public void testConstructorNullNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new RetrieveBeansResultImpl<>( null );
                }
            } );
    }
    
    
    @Test
    public void testToSetDelegatesToIterable()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Teacher.class )
                .setName( "a" ).setDateOfBirth( new Date() ).setType( TeacherType.TEACHER ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Teacher.class )
                .setName( "b" ).setDateOfBirth( new Date() ).setType( TeacherType.TEACHER ) );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(Teacher.class).retrieveAll().toSet().size(), "Shoulda returned set of teachers.");
    }
    
    
    @Test
    public void testToListDelegatesToIterable()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Teacher.class )
                .setName( "a" ).setDateOfBirth( new Date() ).setType( TeacherType.TEACHER ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Teacher.class )
                .setName( "b" ).setDateOfBirth( new Date() ).setType( TeacherType.TEACHER ) );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(Teacher.class).retrieveAll().toList().size(), "Shoulda returned set of teachers.");
    }
    
            
    @Test
    public void testToIterableReturnsIterable()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Teacher.class )
                .setName( "a" ).setDateOfBirth( new Date() ).setType( TeacherType.TEACHER ) );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Teacher.class )
                .setName( "b" ).setDateOfBirth( new Date() ).setType( TeacherType.TEACHER ) );
        assertEquals(2,  dbSupport.getServiceManager().getRetriever(Teacher.class)
                .retrieveAll().toIterable().toSet().size(), "Shoulda returned set of teachers.");
    }
    
    
    @Test
    public void testGetFirstReturnsNullWhenNoResults()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );
        
        dbSupport.getDataManager().createBean( BeanFactory.newBean( Teacher.class )
                .setName( "a" ).setDateOfBirth( new Date() ).setType( TeacherType.TEACHER ) );
        assertEquals("a", dbSupport.getServiceManager().getRetriever( Teacher.class )
                .retrieveAll().getFirst().getName(), "Shoulda returned set of teachers.");
    }
    
    
    @Test
    public void testGetFirstReturnsNonNullWhenResults()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, TeacherService.class );

        assertEquals(null, dbSupport.getServiceManager().getRetriever( Teacher.class ).retrieveAll().getFirst(), "Shoulda returned set of teachers.");
    }
}
