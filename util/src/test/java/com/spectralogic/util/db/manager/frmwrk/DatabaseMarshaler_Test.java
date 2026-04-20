/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.frmwrk;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.mockdomain.School;
import com.spectralogic.util.db.mockdomain.SchoolType;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.db.query.Query;
import com.spectralogic.util.db.query.Require;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;

public final class DatabaseMarshaler_Test 
{
    @Test
    public void testEscapedStringsHandledSuccessfully()
    {
        TestUtil.assertJvmEncodingIsUtf8();
                
        final List< String > stringsToTest = new ArrayList<>();
        stringsToTest.add( "\\n" );
        stringsToTest.add( "\n" );
        stringsToTest.add( "\r" );
        stringsToTest.add( "\b" );
        stringsToTest.add( "\t" );
        stringsToTest.add( "\u000b" );
        stringsToTest.add( "\f" );
        
        for( final String stringToTest : stringsToTest )
        {
            dbEscapeCharHelper( stringToTest );
        }
    }
    
    
    private void dbEscapeCharHelper( final String testString )
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final DataManager dataManager = dbSupport.getDataManager();
        final Set< School > schools = createSchools( testString );
        final DataManager transaction = dataManager.startTransaction();
        try
        {
            transaction.createBeans( schools );
            transaction.commitTransaction();
        }
        finally
        {
            transaction.closeTransaction();
        }
        
        final Set< School > storedSchools =
                dataManager.getBeans( School.class, Query.where( Require.nothing() ) ).toSet();
        boolean foundEscapedString = false;
        for ( final School school : storedSchools )
        {
            if( school.getAddress().equals( testString ) )
            {
                foundEscapedString = true;
                break;
            }
        }
        assertTrue(
                foundEscapedString,
                "Shoulda found escaped string." );
    }
    
    
    private static Set< School > createSchools( final String ... addresses )
    {
        final Set< School > schools = new HashSet<>();
        int i = 0;
        for ( final String address : addresses )
        {
            schools.add( BeanFactory.newBean( School.class )
                    .setName( "School #" + Integer.toString( i ) )
                    .setAddress( address )
                    .setType( SchoolType.PUBLIC ) );
            ++i;
        }
        return schools;
    }
}
