/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.validate;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.manager.DataSource;
import com.spectralogic.util.db.manager.postgres.PostgresDataManager;
import com.spectralogic.util.db.mockdomain.County;
import com.spectralogic.util.db.mockdomain.Principal;
import com.spectralogic.util.db.mockservice.PrincipalService;
import com.spectralogic.util.lang.Validations;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

public final class DataSourceNuker_Test 
{
    @Test
    public void testNukerRetainsTableContentWhenDataSourceCompatible()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Principal.class, PrincipalService.class );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( County.class ).setName( "Boulder" ) );
        
        nukeDatabase( dbSupport );
        dbSupport.getServiceManager().getRetriever( County.class ).attain( County.NAME, "Boulder" );
    }
    
    
    @Test
    public void testNukerRetainsTableContentWhenDataSourceIncompatibleButTableRestorable()
                throws IOException
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Principal.class, PrincipalService.class );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( County.class ).setName( "Boulder" ) );

        executeSql( dbSupport, "ALTER TABLE mockdomain.school DROP COLUMN address;" );
        nukeDatabase( dbSupport );
        dbSupport.getServiceManager().getRetriever( County.class ).attain( County.NAME, "Boulder" );
        DatabaseSupportFactory.reset();
    }
    
    
    @Test
    public void testNukerDoesNotRestoreTableContentWhenIncompatibleAndTableNonRestorable() 
            throws IOException
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Principal.class, PrincipalService.class );
        dbSupport.getDataManager().createBean( BeanFactory.newBean( County.class ).setName( "Boulder" ) );

        executeSql( dbSupport, "ALTER TABLE mockdomain.county DROP COLUMN population;" );
        nukeDatabase( dbSupport );
        assertNull(
                dbSupport.getServiceManager().getRetriever( County.class ).retrieve(
                        County.NAME, "Boulder" ),
                "Should notta been able to restore county."
                 );
        DatabaseSupportFactory.reset();
    }
    
    
    private void executeSql( final DatabaseSupport dbSupport, final String sqlCommand ) throws IOException
    {
        final File file = File.createTempFile( getClass().getName(), null );
        file.deleteOnExit();
        try
        {
            final FileWriter out = new FileWriter( file );
            out.write( sqlCommand );
            out.close();
            dbSupport.executeSql( file );
        }
        finally
        {
            file.delete();
        }
    }
    
    
    private void nukeDatabase( final DatabaseSupport dbSupport )
    {
        DataSourceValidator.clearAlreadyValidatedTablesCache();
        final DataSource dataSource = dbSupport.newDataSource();
        final DataManager dataManager = new PostgresDataManager( 
                3, new HashSet< Class< ? > >( dbSupport.getDataManager().getSupportedTypes() ) );
        try
        {
            dataManager.setDataSource( dataSource );
        }
        catch ( final RuntimeException ex )
        {
            Validations.verifyNotNull( "Shut up CodePro.", ex );
        }
        new DataSourceNuker( 
                dataManager,
                dataSource.establishConnection(), 
                dataManager.getSupportedTypes() ).run();
        dataManager.shutdown();
    }
}
