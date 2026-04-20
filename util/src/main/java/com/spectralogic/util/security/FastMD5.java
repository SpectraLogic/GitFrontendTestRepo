/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.security;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;

import org.apache.log4j.Logger;

import com.spectralogic.util.io.lang.FileUtil;
import com.spectralogic.util.lang.Platform;
import com.twmacinta.util.MD5;


public final class FastMD5
{
    public FastMD5()
    {       
        final File libFileName = getNativeLibForOsArchAndCheckReadWrite();
        System.setProperty( "com.twmacinta.util.MD5.NATIVE_LIB_FILE", libFileName.getAbsolutePath() );
        m_fastMD5NativeObjInstance = new MD5();
        if ( !MD5.initNativeLibrary() )
        {
            final String message = "Fast MD5 NATIVE MODE is NOT enabled, " +
                    "even though we found a native library file that we could read '"
                    + libFileName + "'.";
            throw new RuntimeException( message );
        }
    }


    public void update( final byte [] buffer, final int offset, final int length )
    {
        m_fastMD5NativeObjInstance.Update( buffer, offset, length );
    }


    public void updateWhenBufferLengthDoesNotChange( final byte [] buffer )
    {
        m_fastMD5NativeObjInstance.Update( buffer );
    }


    /**
     * This method returns the digest and resets the MD5 internal state,
     * similar to what the Java MessageDigest does
     * 
     * @return the array of bytes for the resulting hash value.
     */
    public byte [] digestAndReset()
    {
        final byte[] fastMD5 = m_fastMD5NativeObjInstance.Final();
        m_fastMD5NativeObjInstance.Init();
        return fastMD5;
    }


    synchronized static File getNativeLibForOsArchAndCheckReadWrite()
    {
        if ( s_nativeLibFileForOsArch != null && s_nativeLibFileForOsArch.exists() )
        {
            return s_nativeLibFileForOsArch;
        }

        if ( s_nativeLibFileForOsArch != null && !s_nativeLibFileForOsArch.exists() )
        {
            LOG.warn( "The FastMD5 was previously initialized with lib file "
                    + FileUtil.getCanonicalPath( s_nativeLibFileForOsArch )
                    + " from dir " + FileUtil.getCanonicalPath( s_tempExpandedNativeJarDir ) 
                    + ", but the lib file does not exist anymore. Trying to initialize and unjar again..." );
            s_tempExpandedNativeJarDir = null;
            s_nativeLibFileForOsArch = null;
        }
        
        final File unjarTmpDir = unJarFastMD5NativeIfItDoesNotExistAndReturnPath();
        final String subPathForNativeLib = getSubPathForNativeLibForOsArch();
        try
        {
            s_nativeLibFileForOsArch = Paths.get( 
                    unjarTmpDir.getAbsolutePath(), subPathForNativeLib ).toFile();
            if ( !s_nativeLibFileForOsArch.canRead() || !s_nativeLibFileForOsArch.canExecute() )
            {
                final String message = "The native lib file we found ( " 
                        + FileUtil.getCanonicalPath( s_nativeLibFileForOsArch ) 
                        + " ) has incomplete permissions"
                        + Platform.NEWLINE + ", does exist=" + s_nativeLibFileForOsArch.exists()
                        + Platform.NEWLINE + ", canRead=" + s_nativeLibFileForOsArch.canRead()
                        + Platform.NEWLINE + ", canExecute=" + s_nativeLibFileForOsArch.canExecute()
                        + Platform.NEWLINE + ", canWrite=" + s_nativeLibFileForOsArch.canWrite()
                        + Platform.NEWLINE + ", getAbsolutePath=" 
                        + s_nativeLibFileForOsArch.getAbsolutePath()
                        + Platform.NEWLINE + ", getCanonicalPath=" 
                        + s_nativeLibFileForOsArch.getCanonicalPath()
                        + Platform.NEWLINE + ", unjarTmpDir=" + unjarTmpDir;

                throw new RuntimeException( message );
            }
        }
        catch ( final Exception ex )
        {
            final String message = "Could not load the FastMD5 native library " + Platform.NEWLINE
                    + ", jarParentDirPath="
                    + ( ( s_tempExpandedNativeJarDir == null ) ? null : s_tempExpandedNativeJarDir ) + NL
                    + ", relPathNativeLib=" 
                    + ( ( subPathForNativeLib == null ) ? null : subPathForNativeLib ) + NL
                    + ", nativeLibFile=" 
                    + Objects.toString( s_nativeLibFileForOsArch, "" ) + NL
                    + ", OsName: '" + getOsNameLow() + "', and OsArch: '" + getOsArchLow();
            throw new RuntimeException( message, ex );
        }
        return s_nativeLibFileForOsArch;
    }


    synchronized static File unJarFastMD5NativeIfItDoesNotExistAndReturnPath()
    {
        if ( s_tempExpandedNativeJarDir != null )            
        {
            return s_tempExpandedNativeJarDir;
        }
        s_tempExpandedNativeJarDir = new File( FileUtil.getTempDirCheckWrite(), 
                FAST_MD5_TMP_SUBDIR );
        final File fastMD5NativeJar = FastMD5.getFastMD5NativeJarFile();
        FileUtil.unJarSetExecuteCleanDestDirIfErrors( fastMD5NativeJar, s_tempExpandedNativeJarDir );
        return s_tempExpandedNativeJarDir;
    }


    public static File getFastMD5NativeJarFile()
    {
        if ( s_fastMD5NativeJarFile == null )
        {
            s_fastMD5NativeJarFile = new File( 
                    getFrontendLibFolderFromMD5ClassResource().toFile(), 
                    FAST_MD5_NATIVE_JAR_NAME + ".jar" );
        }
        return s_fastMD5NativeJarFile;
    }

    
    static Path getFrontendLibFolderFromMD5ClassResource()
    {
        if ( s_frontendLibPath != null )
        {
            return s_frontendLibPath;
        }
        // the code below could throw too many ( types of ) exceptions with the same end result,
        // so wrapping everything into 1 try/catch instead of having many if/else/throws
        try
        {     
            final String md5ResourcePath = '/' + MD5.class.getName().replace( '.', '/' ) + ".class";
            final URL jarStyleURL = MD5.class.getResource( md5ResourcePath );
            final Path fileStyleURI = Paths.get( new URI( jarStyleURL.getFile() ) ).normalize();   
            final String fileStyleString = fileStyleURI.toString();
            //always has !, have to hardcode " ! "
            final String jarDirString = fileStyleString.substring( 0, fileStyleString.indexOf( '!' ) );
            final Path jarDirPath = Paths.get( jarDirString );
            s_frontendLibPath = jarDirPath.getParent();
        }
        catch ( final Exception ex ) 
        {
            final String message = "Exception mesage: '" + ex.getMessage() + "'."
                    + " Could not find the FastMD5 jar parent dir, with: " 
                    + "OsName: '" + getOsNameLow() + "', and OsArch: '" + getOsArchLow();
            throw new RuntimeException( message, ex );
        }
        return s_frontendLibPath;
    }
    
    
    static String getSubPathForNativeLibForOsArch()
    {
        final String osName = getOsNameLow();
        final String osArch = getOsArchLow();
        if ( osName == null || osArch == null )
        {
            return null;
        }

        // Followed the logic in the FastMD5, including hardcoding the forward slashes
        // We have to duplicate this logic because we cannot modify the FastMD5 and 
        // because we need the native lib relative sub path, so we can set it in System.properrties
        String post = null;
        final String pre = FAST_MD5_NATIVE_JAR_NAME + "/";
        if ( osName.equals( "freebsd" ) ) 
        {
            if ( osArch.equals( "amd64" ) )
            {
                post = "freebsd_amd64/MD5.so";
            }
            else if ( osArch.endsWith( "86" ) )
            {
                post = "freebsd_x86/MD5.so";
            }
        }
        else if ( osName.startsWith( "windows" ) )
        {
            if ( osArch.endsWith( "86" ) )
            {
                post = "win32_x86/MD5.dll";
            }
            else if ( osArch.equals( "amd64" ) )
            {
                post = "win_amd64/MD5.dll";
            }
        }
        else if ( osName.equals( "linux" ) )
        {
            if ( osArch.endsWith( "86" ) )
            {
                post = "linux_x86/MD5.so";
            }
            else if ( osArch.equals( "amd64" ) )
            {
                post = "linux_amd64/MD5.so";
            }
            else if ( osArch.equals( "aarch64" ) )
            {
                post = "linux_aarch64/MD5.so";
            }
        }
        else if ( osName.startsWith( "mac os x" ) )
        {
            if ( osArch.endsWith( "86" ) )
            {
                post = "darwin_x86/MD5.jnilib";
            }
            else if ( osArch.equals( "ppc" ) )
            {
                post = "darwin_ppc/MD5.jnilib";
            }
            else if ( osArch.equals( "x86_64" ) )
            {
                post = "darwin_x86_64/MD5.jnilib";
            }
            else if ( osArch.equals( "aarch64" ) )
            {
                post = "darwin_aarch64/MD5.jnilib";
            }
        }
        return ( post == null ) ? null : pre + post;
    }


    private static String getOsNameLow()
    {
        final String osName = System.getProperty( "os.name" );
        return ( osName == null ) ? null : osName.toLowerCase();
    }


    private static String getOsArchLow()
    {
        final String osArch = System.getProperty( "os.arch" );
        return ( osArch == null ) ? null : osArch.toLowerCase();
    }
    
    
    /**
     * For testing purposes only.
     * Tests that blow away FastMD5 native dir contents must call this as tearDown.
     */
    public static void initAll()
    {
        s_fastMD5NativeJarFile = null;
        s_nativeLibFileForOsArch = null;
        s_tempExpandedNativeJarDir = null;
        s_frontendLibPath = null;
    }

    
    private final static String NL = Platform.NEWLINE;        
    private final static String FAST_MD5_TMP_SUBDIR = "fast-md5-temp";
    private final static String FAST_MD5_NATIVE_JAR_NAME = "fast-md5-native-2.7.1";
    private final MD5 m_fastMD5NativeObjInstance;
    private static File s_fastMD5NativeJarFile;
    private static File s_nativeLibFileForOsArch;
    private static File s_tempExpandedNativeJarDir;
    private static Path s_frontendLibPath;
    private final static Logger LOG = Logger.getLogger( FastMD5.class );    
}