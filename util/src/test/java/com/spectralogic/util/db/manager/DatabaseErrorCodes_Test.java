/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager;

import java.util.Date;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockdomain.TeacherType;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.db.mockservice.TeacherService;
import com.spectralogic.util.exception.GenericFailure;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class DatabaseErrorCodes_Test 
{
    @Test
    public void testConflictThrownIfUniqueConstraintViolation()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );

        final Date date = new Date( 10000 );
        dbSupport.getServiceManager().getService( TeacherService.class ).create( 
                BeanFactory.newBean( Teacher.class )
                .setName( "Bob" )
                .setDateOfBirth( date )
                .setType( TeacherType.values()[ 0 ] ) );
        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                dbSupport.getServiceManager().getService( TeacherService.class ).create( 
                        BeanFactory.newBean( Teacher.class )
                        .setName( "Bob" )
                        .setDateOfBirth( date )
                        .setType( TeacherType.values()[ 0 ] ) );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                dbSupport.getServiceManager().getService( TeacherService.class ).create( 
                        BeanFactory.newBean( Teacher.class )
                        .setName( "Bob" )
                        .setDateOfBirth( new Date() ) );
            }
        } );
        
        final Teacher other = BeanFactory.newBean( Teacher.class )
                .setName( "Bob" )
                .setDateOfBirth( new Date() )
                .setType( TeacherType.values()[ 0 ] );
        dbSupport.getServiceManager().getService( TeacherService.class ).create( other );

        TestUtil.assertThrows( null, GenericFailure.CONFLICT, new BlastContainer()
        {
            public void test()
            {
                dbSupport.getServiceManager().getService( TeacherService.class ).update( 
                        other.setDateOfBirth( date ), 
                        Teacher.DATE_OF_BIRTH );
            }
        } );
        TestUtil.assertThrows( null, GenericFailure.BAD_REQUEST, new BlastContainer()
        {
            public void test()
            {
                dbSupport.getServiceManager().getService( TeacherService.class ).update( 
                        other.setType( null ),
                        Teacher.TYPE );
            }
        } );
    }
}
