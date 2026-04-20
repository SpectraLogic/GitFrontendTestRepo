/*
 *
 * Copyright C 2018, Spectra Logic Corporation and/or its affiliates.
 * All rights reserved.
 *
 */
package com.spectralogic.s3.dataplanner.backend.pool.frmwrk;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.spectralogic.s3.common.dao.domain.pool.Pool;
import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.lang.Validations;


public final class PoolUtils
{
    private PoolUtils()
    {
        // singleton
    }
    
    
    public static List< Path > getDirectories( final Path dir )
    {
        final List< Path > dirlist = new ArrayList<>();
        try ( DirectoryStream< Path > stream = Files.newDirectoryStream( dir ) )
        {
            for ( final Path aStream : stream )
            {
                if ( Files.isDirectory( aStream ) )
                {
                    dirlist.add( aStream );
                }
            }
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to get directory entries from '" + dir + "'.", ex );
        }
        return dirlist;
    }
    
    
    public static List< Path > getEntries( final Path dir )
    {
        final List< Path > entries = new ArrayList<>();
        try ( DirectoryStream< Path > stream = Files.newDirectoryStream( dir ) )
        {
            for ( final Path aStream : stream )
            {
                if ( !Files.isDirectory( aStream ) )
                {
                    entries.add( aStream );
                }
            }
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to get file entries from '" + dir + "'.", ex );
        }
        return entries;
    }
    
    
    public static String[] getInfoFileSuffixes()
    {
        return INFO_FILE_SUFFIXES.clone();
    }
    
    
    public static Path getMetadataFile( Path file )
    {
        return Paths.get( file.toString() + METADATA );
    }
    
    
    public static Path getMetadataPartFile( Path file )
    {
        return Paths.get( file.toString() + METADATA_PART );
    }
    
    
    /**
     * @return the pool persistence path for the blob, of the form /poolmountpoint/b1/22/22222/33333,
     * where the bucket name is b1, the object id is 22222, and the blob id is 33333 <br><br>
     *
     * This hierarchy is intended to keep the filesystem relatively flat, but provide useful hierarchy by
     * organizing blobs into objects, and objects into buckets.  It also hashes the object id by taking the
     * first 2 characters of the object id and making that a parent folder to prevent a single level of the
     * hierarchy from exceeding millions of folders, since some filesystem tools will not scale well to
     * handle that.  <br><br><br>
     *
     * <b>Scalability analysis of filesystem hierarchy: <br><br></b>
     * <ol>
     * <li>Buckets
     * <ul>
     * <li>Typical range of number of buckets is from 1s to several thousand.
     * <li>General scalability of the system will degrade if it exceeds 1M.
     * </ul>
     * <li>Objects Per Bucket
     * <ul>
     * <li>Typical range of number of objects is from hundreds to several million.
     * <li>Number of objects in a bucket may scale to hundreds of millions.
     * <li>General scalability of the system will degrade if the average object size is less than 5MB.
     * <li>Rebuild times will become unacceptable if the maximum usable pool size exceeds about 200TB
     *     (assuming 10TB drives).
     * <li>The worst case number of objects on a single pool we should handle well may be calculated given
     *     the constraints aforementioned, and by assuming that every object belongs to the same bucket.
     * <li>The worst case number of objects on a single pool is about 40M.  This would mean 1296 object hash
     *     folders under the bucket folder and roughly 32K object folders under every object hash folder.
     * </ul>
     * <li>Blobs Per Object
     * <ul>
     * <li>Typical range of number of blobs is from 1 to several thousand.
     * <li>General scalability of the system will degrade if the average blob size is less than 5MB.
     * <li>At a 5MB blob size, a 5TB object would require about 1M blobs
     *     (this is a worst-case scenario using AWS multi-part upload, which is limited to 5TB).
     * <li>At a 100MB blob size, a 100TB object would require about 1M blobs (this is a worst-case scenario
     *     using DS3 properly for a very large object).
     * </ul>
     * </ol>
     */
    public static Path getPath( final Pool pool, final String bucketName, final UUID objectId,
            final UUID blobId )
    {
        Validations.verifyNotNull( "Pool", pool );
        Validations.verifyNotNull( "Bucket", bucketName );
        if ( null == objectId && null != blobId )
        {
            throw new IllegalArgumentException(
                    "If the blob id is specified, the object id must be specified." );
        }
        
        String retval = pool.getMountpoint() + Platform.FILE_SEPARATOR + bucketName;
        if ( null != objectId )
        {
            retval += Platform.FILE_SEPARATOR + objectId.toString()
                                                        .substring( 0, 2 )
                      + Platform.FILE_SEPARATOR  + objectId;
        }
        if ( null != blobId )
        {
            retval += Platform.FILE_SEPARATOR + blobId;
        }
    
        return Paths.get( retval );
    }
    
    
    public static Path getPropsFile( Path file )
    {
        return Paths.get( file.toString() + PROPS );
    }
    
    
    public static Path getPropsPartFile( Path file )
    {
        return Paths.get( file.toString() + PROPS_PART );
    }
    
    
    public static Path getTrashPath( final Pool pool )
    {
        return PoolUtils.getPath( pool, PoolUtils.TRASH_DIRECTORY, null, null );
    }
    
    
    public static Path getCompactedUnknownMarkPath( final Pool pool )
    {
        return PoolUtils.getPath( pool, COMPACTED_UNKNOWN_MARK_FILE, null, null );
    }
    
    
    public static boolean isInfoFile( Path file )
    {
        // Files that are not blob data
        final String fileName = file.getFileName()
                                    .toString();
        return Arrays.stream( INFO_FILE_SUFFIXES )
                     .anyMatch( fileName::endsWith );
    }
    
    
    public static void renameFile( final Path source, final Path destination )
    {
        try
        {
            Files.move( source, destination, StandardCopyOption.REPLACE_EXISTING );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( "Failed to rename file " + source + " to " + destination, ex );
        }
    }
    
    
    public static void verifyObjectDir( final Path dirObject )
    {
        final Path fileObjectProps = PoolUtils.getPropsFile( dirObject );
        final Path fileObjectPropsPart = PoolUtils.getPropsPartFile( dirObject );
        final Path fileObjectMetadata = PoolUtils.getMetadataFile( dirObject );
        final Path fileObjectMetadataPart = PoolUtils.getMetadataPartFile( dirObject );
        if ( Files.exists( dirObject ) && ( !Files.exists( fileObjectProps ) || !Files.exists( fileObjectMetadata ) ) )
        {
            LOG.warn( "Object directory '" + dirObject + "' exists, but is not valid.  Deleting..." );
            try
            {
                Files.delete( dirObject );
                
                for ( final Path file : new Path[]{ fileObjectProps, fileObjectPropsPart, fileObjectMetadata,
                        fileObjectMetadataPart } )
                {
                    try
                    {
                        Files.deleteIfExists( file );
                    }
                    catch ( IOException e )
                    {
                        LOG.warn( "Failed to delete: " + file );
                    }
                }
            }
            catch ( final DirectoryNotEmptyException ex )
            {
                throw new RuntimeException( "Object Directory " + dirObject + " was not valid, but not empty", ex );
            }
            catch ( final IOException ex1 )
            {
                throw new RuntimeException( "Could not delete invalid directory " + dirObject, ex1 );
            }
            LOG.info( "Successfully deleted invalid object directory: " + dirObject );
        }
    }
    
    
    private final static String FILE_PART_SUFFIX = ".part";
    private final static String PROPS = ".props";
    private final static String PROPS_PART = PROPS + FILE_PART_SUFFIX;
    private final static String METADATA = ".metadata";
    private final static String METADATA_PART = METADATA + FILE_PART_SUFFIX;
    private final static String[] INFO_FILE_SUFFIXES = { PROPS, PROPS_PART, METADATA, METADATA_PART };
    
    private final static Logger LOG = Logger.getLogger( PoolUtils.class );
    private final static String COMPACTED_UNKNOWN_MARK_FILE = "spectra-compacted";
    public final static String TRASH_DIRECTORY = "spectra-trash";
}
