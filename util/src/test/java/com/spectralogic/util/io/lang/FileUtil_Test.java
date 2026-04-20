/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.io.lang;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.spectralogic.util.lang.Platform;
import com.spectralogic.util.security.FastMD5;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class FileUtil_Test 
{

    @AfterEach
    public void tearDown() throws Exception
    {
        FastMD5.initAll();
    }
    
    
    @Test
    public void testgetCanonicalPathNullExistingAndNonDeletedFile()
    {
        assertTrue(FileUtil.getCanonicalPath( null ).contains( "null file object" ), "we get a meaningfull string for null input");

        try
        {
            final File file = new File( "aaa" );

            final String canPath = FileUtil.getCanonicalPath( file );
            assertTrue(canPath.contains( file.getName() ), "we get the cannonical path ");
            assertTrue(canPath.contains( "does not exist" ), "we get the cannonical path ");

            file.createNewFile();
            final String canPathExists = FileUtil.getCanonicalPath( file );
            assertTrue(canPathExists.length() > file.getName().length(), "canPathExists t1");
            assertTrue(canPathExists.contains( Platform.FILE_SEPARATOR ), "canPathExists t2");
            assertTrue(canPathExists.contains( file.getName() ), "canPathExists t3 ");

            file.delete();

            final String canPathDeletedFile = FileUtil.getCanonicalPath( file );
            assertTrue(canPathDeletedFile.contains( "does not exist" ), "canPathDeletedFile t1");
            assertTrue(canPathDeletedFile.contains( file.getName() ), "canPathDeletedFile t1");
        }
        catch ( final Exception ex ) 
        {
            assertTrue(false, "We got an exception " + ex);
        }
    }
    
    
    @Test
    public void testTempDirComesFromJavaSystemProperties()
    {
        final File f = FileUtil.getTempDirCheckWrite();
        final File javaTmpDir = new File( System.getProperty( "java.io.tmpdir" ) );
        assertTrue(f.exists() & f.canWrite() && f.getAbsolutePath().equals( javaTmpDir.getAbsolutePath() ), "temp dir is java sys pro temp dir " + Platform.NEWLINE
                + f.getAbsolutePath() + Platform.NEWLINE + javaTmpDir);
    }


    @Test
    public void testUnJarNulls()
    {
        Throwable exception = TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                FileUtil.unJarSetExecuteCleanDestDirIfErrors( null, FileUtil.getTempDirCheckWrite() );
            }
        } );
        assertTrue(exception.getMessage().contains(
                        "'jarPath' parameter is null. Other parameters: destDir:" ), "non existent jar file error" + Platform.NEWLINE);

        exception = TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                FileUtil.unJarSetExecuteCleanDestDirIfErrors( FastMD5.getFastMD5NativeJarFile(), null );
            }
        } );

        assertTrue(exception.getMessage().contains(
                        "destDir is null, or it does not exist and could not be created" ), "non existent tmp folder " + Platform.NEWLINE);
    }


    @Test
    public void testUnJarFastMD5JarFileDoesNotExist()
    {
        final File ftmp = new File( FileUtil.getTempDirCheckWrite(), "non-existent.jar" );
        ftmp.delete();
        
        final Throwable ex = TestUtil.assertThrows( null, RuntimeException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                FileUtil.unJarSetExecuteCleanDestDirIfErrors( ftmp, FileUtil.getTempDirCheckWrite() );
            }
        } );
        final String message = "non existent jar file error" + Platform.NEWLINE
        + "error=" + ex.getMessage() + Platform.NEWLINE;
        assertTrue(ex.getMessage().contains( "jarPath" )
                && ex.getMessage().contains( ", does not exist. Other parameters: destDir:" ), message);
        ftmp.delete();
    }
    

    @Test
    public void testUnJarFastMD5TmpDirDoesNotExist()
    {
        final File ftmp = new File( FileUtil.getTempDirCheckWrite(), "non-existent-tmp-sub-dir" );
        ftmp.delete();

        FileUtil.unJarSetExecuteCleanDestDirIfErrors( FastMD5.getFastMD5NativeJarFile(), ftmp );
        assertTrue(ftmp.exists(), "non existent tmp folder " + Platform.NEWLINE);

        ftmp.delete();
    }


    @Test
    public void testUnJarFastMD5JarOKInTempFolder()
    {
        final File unjarDir = new File( FileUtil.getTempDirCheckWrite(), FAST_MD5_TMP_SUBDIR_HARDCODED );
        FileUtil.cleanDirectoryQuietly( unjarDir );
        final File fastMD5NativeJar = FastMD5.getFastMD5NativeJarFile();

        FileUtil.unJarSetExecuteCleanDestDirIfErrors( fastMD5NativeJar, unjarDir );         

        final File freeBsdHardcodedFile = new File( unjarDir, 
                "fast-md5-native-2.7.1/freebsd_x86/MD5.so" );
        assertTrue(freeBsdHardcodedFile.exists()
                && freeBsdHardcodedFile.canRead(), "hardcoded path, test file exists.");
        assertTrue(freeBsdHardcodedFile.canExecute(), "hardcoded path, test can execute.");
        try
        {
            FileUtils.cleanDirectory( unjarDir );
            assertTrue(!freeBsdHardcodedFile.exists(), "cleanup was done, at least 1of the files doesn't exist");
        }
        catch ( final Exception ex ) 
        {
            LOG.error( "deleting the temp folder failed because:" + ex.getMessage() );
        }
    }


    @Test
    public void testUnJarFastMD5JarTestDoNotOverwrite() 
    {
        final File tmpDir = new File( FileUtil.getTempDirCheckWrite(), FAST_MD5_TMP_SUBDIR_HARDCODED );
        FileUtil.cleanDirectoryQuietly( tmpDir );
        final File fastMD5NativeJar = FastMD5.getFastMD5NativeJarFile();
        final File freeBsdFile = new File( tmpDir, "fast-md5-native-2.7.1/freebsd_x86/MD5.so" );
        // final Path freeBsdPath =  Paths.get( freeBsdFile.toURI() );
        
        FileUtil.unJarSetExecuteCleanDestDirIfErrors( fastMD5NativeJar, tmpDir ); 
        final long lastMod = freeBsdFile.lastModified();
        // final FileTime creationTime = (FileTime) Files.getAttribute( freeBsdPath, "basic:creationTime" );
        TestUtil.sleep( 2 );

        FileUtil.unJarSetExecuteCleanDestDirIfErrors( fastMD5NativeJar, tmpDir ); 
        final long lastMod2 = freeBsdFile.lastModified();        
        // final FileTime creationTime2 = (FileTime) Files.getAttribute( freeBsdPath, "basic:creationTime" );
        TestUtil.sleep( 2 );

        FileUtil.unJarSetExecuteCleanDestDirIfErrors( fastMD5NativeJar, tmpDir ); 
        final long lastMod3 = freeBsdFile.lastModified();
        // final FileTime creationTime3 = (FileTime) Files.getAttribute( freeBsdPath, "basic:creationTime" );

        assertTrue(lastMod == lastMod2 && lastMod2 == lastMod3, "timestamp did not change :" + Platform.NEWLINE + lastMod + Platform.NEWLINE
                + lastMod2 + Platform.NEWLINE + lastMod3);
        final String message = "quick test 1 " + Platform.NEWLINE + "exists=" + freeBsdFile.exists()
                + Platform.NEWLINE
                + "can read=" + freeBsdFile.canRead() + Platform.NEWLINE
                + "can execute=" + freeBsdFile.canExecute();
        assertTrue(freeBsdFile.exists() && freeBsdFile.canRead() && freeBsdFile.canExecute(), message);

        /*        assertTrueMod( "timestamp did not change - windows keeps the files in teh reccycle bin:" 
                + Platform.NEWLINE 
                + creationTime + Platform.NEWLINE + creationTime2 + Platform.NEWLINE + creationTime3,
                creationTime.toMillis() == creationTime3.toMillis() 
                && creationTime.toMillis() == lastMod3 );
        */        
        FileUtil.cleanDirectoryQuietly( tmpDir );
    }


    @Test
    public void testCompressNullDestinationFileNotAllowed()
    {
        final File source = createSourceFile();
        source.deleteOnExit();
        final Set< File > sourceFiles = new HashSet<>();
        sourceFiles.add( source );

        try
        {
            FileUtil.compressFilesIntoZipFile( null, sourceFiles );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull( ex,
                    "Shoulda thrown illegal argument exception." );
        }
    }


    @Test
    public void testCompressNullSourceFilesSetNotAllowed()
    {
        final File destination = createDestinationFile();
        destination.deleteOnExit();
        try
        {
            FileUtil.compressFilesIntoZipFile( destination, null );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull(
                    ex,
                    "Shoulda thrown illegal argument exception." );
        }
    }


    @Test
    public void testCompressEmptySourceFilesSetNotAllowed()
    {
        final File destination = createDestinationFile();
        destination.deleteOnExit();
        final Set< File > sourceFiles = new HashSet<>();

        try
        {
            FileUtil.compressFilesIntoZipFile( destination, sourceFiles );
            fail( "Shoulda thrown exception." ); 
        }
        catch ( final IllegalArgumentException ex )
        {
            assertNotNull( ex,
                    "Shoulda thrown illegal argument exception." );
        }
    }


    @Test
    public void testCompressCompletesSuccessfully()
    {
        final File destination = createDestinationFile();
        final File source1 = createSourceFile();
        final File source2 = createSourceFile();
        destination.deleteOnExit();
        source1.deleteOnExit();
        source2.deleteOnExit();
        final Set< File > sourceFiles = new HashSet<>();
        sourceFiles.add( source1 );
        sourceFiles.add( source2 );

        final String contents = "I am the contents of the file."; 
        fillFileWithContents( source1, contents );

        assertTrue(FileUtil.compressFilesIntoZipFile( destination, sourceFiles ), "Shoulda compressed source files successfully.");
    }


    private File createSourceFile()
    {
        try
        {
            return File.createTempFile( 
                    this.getClass().getSimpleName(), 
                    "sourceTemp" );             
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
    }


    private File createDestinationFile()
    {
        try
        {
            return File.createTempFile( 
                    this.getClass().getSimpleName(), 
                    "destinationTemp" );             
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
    }


    private static void fillFileWithContents( final File file, final String contents )
    {
        FileOutputStream outputStream = null;
        try
        {
            outputStream = new FileOutputStream( file );
            outputStream.write( contents.getBytes() );
        }
        catch ( final IOException ex )
        {
            throw new RuntimeException( ex );
        }
        finally
        {
            closeChannel( outputStream );
        }
    }


    private static boolean closeChannel( final Closeable channel )
    {
        boolean success = true;
        try
        {
            if ( null != channel )
            {
                channel.close();                    
            }
        }
        catch ( final IOException e )
        {
            LOG.warn( "Problem when closing channel.", e ); 
            success = false;
        }

        return success;
    }


    private final static Logger LOG = Logger.getLogger( FileUtil_Test.class );
    private final static String FAST_MD5_TMP_SUBDIR_HARDCODED = "fast-md5-temp";
}
