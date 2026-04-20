/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.s3.common.dao.service.planner;

import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.util.db.service.BaseService;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.iterate.PreProcessor;

final class CacheFilesystemServiceImpl extends BaseService< CacheFilesystem > 
    implements CacheFilesystemService
{
    CacheFilesystemServiceImpl()
    {
        super( CacheFilesystem.class );
        addCustomBeanPopulationProcessor( new CustomPopulator() );
    }
    
    
    private final class CustomPopulator implements PreProcessor< CacheFilesystem >
    {
        public void process( final CacheFilesystem fs )
        {
            if ( fs.getPath().endsWith( Platform.FILE_SEPARATOR ) )
            {
                return;
            }
            update( fs.setPath( fs.getPath() + Platform.FILE_SEPARATOR ), CacheFilesystem.PATH );
        }
    } // end inner class def
}
