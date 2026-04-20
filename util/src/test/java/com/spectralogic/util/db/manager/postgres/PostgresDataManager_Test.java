/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.manager.postgres;

import java.util.HashSet;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.db.mockdomain.Teacher;
import com.spectralogic.util.db.mockservice.CountyService;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class PostgresDataManager_Test 
{
    @Test
    public void testSetDatasourceWithNullValidationThrowsIllegalArgumentException()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    new PostgresDataManager( 42, null );
                }
            } );
    }
    
    
    @Test
    public void testSetDatasource2ndTimeThrowsIllegalArgumentException()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                dbSupport.getDataManager().setDataSource( null );
            }
            } );
    }
    
    
    @Test
    public void testChangeToUnsafeModeWithCompatibleDataSourceThrowsIllegalStateException()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final DataManager dataManager = new PostgresDataManager(
                Integer.MAX_VALUE,
                new HashSet< Class< ? > >( dbSupport.getDataManager().getSupportedTypes() ) );
        dataManager.setDataSource( dbSupport.getDataSource() );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                dataManager.toUnsafeModeForIncompatibleDataSource();
            }
            } );
    }
    
    
    @Test
    public void testGetDataDirectoryBeforeSettingDataDirectoryThrowsIllegalStateException()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final DataManager dataManager = new PostgresDataManager(
                Integer.MAX_VALUE,
                new HashSet< Class< ? > >( dbSupport.getDataManager().getSupportedTypes() ) );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                dataManager.getDataDirectory();
            }
             } );
    }
    
    
    @Test
    public void testGetPhysicalSpaceStateBeforeSettingDataDirectoryThrowsIllegalStateException()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final DataManager dataManager = new PostgresDataManager(
                Integer.MAX_VALUE,
                new HashSet< Class< ? > >( dbSupport.getDataManager().getSupportedTypes() ) );
        TestUtil.assertThrows( null, IllegalStateException.class, new BlastContainer()
        {
            public void test()
            {
                dataManager.getFreeToTotalDiskSpaceRatioState();
            }
            } );
    }
    
    
    @Test
    public void testGetPhysicalSpaceStateWorks()
    {
        final DatabaseSupport dbSupport =
                DatabaseSupportFactory.getSupport( Teacher.class, CountyService.class );
        final DataManager dataManager = new PostgresDataManager(
                Integer.MAX_VALUE,
                new HashSet< Class< ? > >( dbSupport.getDataManager().getSupportedTypes() ) );
        dataManager.setDataSource( dbSupport.getDataSource() );
        assertNotNull(  dataManager.getFreeToTotalDiskSpaceRatioState() );
    }
}
