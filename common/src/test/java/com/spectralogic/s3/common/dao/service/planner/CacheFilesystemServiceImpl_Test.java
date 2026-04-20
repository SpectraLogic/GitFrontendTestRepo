/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.planner;

import java.util.UUID;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.s3.common.dao.domain.DaoDomainsSeed;
import com.spectralogic.s3.common.dao.domain.ds3.Node;
import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.s3.common.dao.service.DaoServicesSeed;
import com.spectralogic.util.bean.BeanFactory;
import com.spectralogic.util.db.manager.DataManager;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.testfrmwrk.DatabaseSupport;
import com.spectralogic.util.testfrmwrk.DatabaseSupportFactory;

public final class CacheFilesystemServiceImpl_Test 
{
    @Test
    public void testCacheFileystemsAlwaysComeBackWithTrailingFilePathSeparator()
    {
        final DatabaseSupport dbSupport =
            DatabaseSupportFactory.getSupport( DaoDomainsSeed.class, DaoServicesSeed.class );
        
        CacheFilesystem fs1 = BeanFactory.newBean( CacheFilesystem.class )
                .setNodeId( createNodeId( dbSupport.getDataManager() ) )
                .setPath( "apple" );
        dbSupport.getDataManager().createBean( fs1 );
        
        CacheFilesystem fs2 = BeanFactory.newBean( CacheFilesystem.class )
                .setNodeId( createNodeId( dbSupport.getDataManager() ) )
                .setPath( "orange" + Platform.FILE_SEPARATOR );
        dbSupport.getDataManager().createBean( fs2 );
        
        fs1 = dbSupport.getServiceManager().getService( CacheFilesystemService.class ).attain( fs1.getId() );
        fs2 = dbSupport.getServiceManager().getService( CacheFilesystemService.class ).attain( fs2.getId() );
        
        assertEquals(
                "apple" + Platform.FILE_SEPARATOR,
                fs1.getPath(),
                "Shoulda corrected path to have a trailing file separator."
                 );
        assertEquals(
                "orange" + Platform.FILE_SEPARATOR,
                fs2.getPath(),
                "Shoulda left path as is since it was correct."
                 );
    }
    
    
    private UUID createNodeId( final DataManager dataManager )
    {
        final Node node = BeanFactory.newBean( Node.class )
                .setName( UUID.randomUUID().toString() ).setSerialNumber( UUID.randomUUID().toString() );
        dataManager.createBean( node );
        return node.getId();
    }
}
