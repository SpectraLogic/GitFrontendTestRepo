/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.testfrmwrk;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.log4j.Logger;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.mockdomain.School;
import com.spectralogic.util.db.mockdomain.SchoolType;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.lang.Duration;



public final class DatabaseSupportFactory_Test
{
    @Test
    public void testDatabaseSupportReturnedIsNullOrNonNullButDoesNotBlowUp()
    {
        final Duration duration = new Duration();
        final DatabaseSupport support =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        LOG.info( "Time to get first " + DatabaseSupport.class.getSimpleName() + ": " + duration );
        
        if ( null == support )
        {
            return;
        }
        assertEquals(
                0,
                support.getDataManager().getCount( School.class, Require.nothing() ),
                "Should notta had any data to start with."
               );
        assertEquals(
                0,
                support.getServiceManager().getService( CountyService.class ).getCount(),
                "Should notta had any data to start with."
                );
        support.getDataManager().createBean( BeanFactory.newBean( School.class ).setAddress( "address" )
                .setName( "name" ).setType( SchoolType.CHARTER ) );
        assertEquals(
                1,
                support.getDataManager().getCount( School.class, Require.nothing() ),
                "Shoulda successfully created the single school."
                 );
        
        duration.reset();
        final DatabaseSupport support2 =
            DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        
        LOG.info( "Time to get second " + DatabaseSupport.class.getSimpleName() + ": " + duration );
        
        assertEquals(
                0,
                support.getDataManager().getCount( School.class, Require.nothing() ),
                "Shoulda reset database when getting support."
                );

        support.getDataManager().createBean( BeanFactory.newBean( School.class ).setAddress( "address" )
                .setName( "name" ).setType( SchoolType.CHARTER ) );
        assertEquals(
                1,
                support.getDataManager().getCount( School.class, Require.nothing() ),
                "Shoulda successfully created the single school."
                 );
        support.reset();
        assertEquals(
                0,
                support2.getDataManager().getCount( School.class, Require.nothing() ),
                "Shoulda reset database."
               );
    }
    
    
    private final static Logger LOG = Logger.getLogger( DatabaseSupportFactory_Test.class );
}
