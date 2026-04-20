package com.spectralogic.s3.common.platform.cache;

import java.io.File;
import java.util.UUID;

import com.spectralogic.s3.common.dao.domain.planner.CacheFilesystem;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;


public final class CacheUtils 
{
    private CacheUtils()
    {
        // singleton
    }
    

    /**
     * @param file (either the cache persistence blob file, or the pool persistence blob file)
     */
    public static UUID getBlobId( final File file )
    {
        Validations.verifyNotNull( "file", file );

        String name = file.getName();
        final int periodIndex = name.indexOf( '.' );
        if ( 0 < periodIndex )
        {
            name = name.substring( 0, periodIndex );
        }
        
        return UUID.fromString( name );
    }
    
    
    /**
     * @return the cache persistence path for the blob
     */
    public static String getPath( final CacheFilesystem filesystem, final UUID blobId )
    {
        Validations.verifyNotNull( "Cache filesystem", filesystem );
        Validations.verifyNotNull( "Blob", blobId );
        
        String rootPath = filesystem.getPath();
        if ( !rootPath.endsWith( Platform.FILE_SEPARATOR ) )
        {
            rootPath += Platform.FILE_SEPARATOR;
        }
        return rootPath + getFullPath( blobId );
    }
    

    /**
     * @return the cache persistence path for the blob
     */
    private static String getFullPath( final UUID blobId )
    {
        Validations.verifyNotNull( "Blob id", blobId );
        String path = getTierPath( blobId );
        path += blobId.toString();
        return path;
    }
    
    
    public static String getTierPath( final UUID blobId )
    {
        String path = blobId.toString()
                            .substring( 0, 2 ) + Platform.FILE_SEPARATOR;
        path += blobId.toString()
                      .substring( 2, 4 ) + Platform.FILE_SEPARATOR;
        return path;
    }
    
    
    /**
     * Cache file entries that are valid shall have a file with the same name plus the cache file valid suffix
     * to note that that entry is complete and valid.  For example, if a file exists /usr/cache/abc but a
     * file does not exist /usr/cache/abc.v, then /usr/cache/abc is said to be incomplete or invalid.
     */
    public final static String CACHE_FILE_VALID_SUFFIX = ".v";
}
