/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.devtool;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import com.spectralogic.util.lang.Platform;
import com.twmacinta.util.MD5;

public final class ChecksumBenchmark
{    
    public static void main( String[] args ) // $codepro.audit.disable illegalMainMethod
    {        
        final String className = ChecksumBenchmark.class.getSimpleName();
        outLn( "Param1 can be: medium, long, md5, all" 
                + NL + "Param2 is the path to the native library, the dll or so file" );
        outLn( SEP_LINE + NL + "TO COMPILE I USED: "
                + NL + "javac" +
                " -cp \".:./*:./fast-md5/build\"" +
                " -d ./" +
                " com/spectralogic/util/manualrun/" + className + ".java" );
        outLn( SEP_LINE + NL + "TO RUN i used server, 64bit, and optimization for garbage collection: "
                + NL + "java -d64 -server -XX:+UseParallelGC -XX:+UseParallelOldGC" +
                " -cp \".:./*:./fast-md5/build\"" +
                " com.spectralogic.util.manualrun." + className + 
                " all" +
                " ./fast-md5/src/lib/arch/freebsd_amd64/MD5.so" );

        final ChecksumBenchmark test = new ChecksumBenchmark();

        if ( args != null && args.length >= 1 )
        {
            final String args0Low = args[ 0 ].toLowerCase();
            if ( args0Low.contains( "medium" ) )            
            {
                test.testRunOneMediumBenchmark();
            }
            else if ( args0Low.contains( "long" ) )
            {
                test.testRunOneLongBenchmark();
            }
            else if ( args0Low.contains( "md5" ) )
            {
                test.testRunAllMD5Benchmarks();
            }
            else if ( args0Low.contains( "all" ) )
            {
                test.testRunAllBenchmarks();
            }
        }
        else
        {
            outLn( NL +  "Please specify a parameter or 2" );
        }
    }
    
    /**
     * Benchmarks are inherently biased because they measure code in an unnatural location.
     * This code snippet is not "the best benchmark to run"
     * It is just an option to compute the DIFFERENCE between implementations 
     */
    public void testRunAllBenchmarks() 
    {
        runAllBenchmarks( false );
    }


    public void testRunAllMD5Benchmarks() 
    {
        runAllBenchmarks( true );
    }


    public void testRunOneQuickBenchmark( ) 
    {
        runBenchmark( 32, 10, true );        
    }


    public void testRunOneMediumBenchmark() 
    {
        runBenchmark( 48, 15, true );        
    }


    public void testRunOneLongBenchmark( )
    {
        runBenchmark( 64, 100, true );        
    }


    private void runAllBenchmarks( final boolean onlyMD5 )
    {
        displaySystemInfoOnce();
        //set this prop manually till we clear the license 
        final int[] chunkMB = new int[]{ 1, 4, 16, 32, 64, 128 };
        final int[] nrRuns = new int[]{ 20, 100, 1000 };
        final boolean[] finalizeAndInitDigestEveryRun = new boolean[] { true, false };
        for ( int i = 0; i < chunkMB.length; i++ )
        {
            for ( int j = 0; j < nrRuns.length; j++ )
            {
                for ( int k = 0; k < finalizeAndInitDigestEveryRun.length; k++ )
                {
                    if ( onlyMD5 )
                    {
                        runMD5Benchmark( chunkMB[ i ], nrRuns[ j ], finalizeAndInitDigestEveryRun[ k ] );
                    }
                    else
                    {
                        runBenchmark( chunkMB[ i ], nrRuns[ j ], finalizeAndInitDigestEveryRun[ k ] );
                    }
                }
            }
        }
    }


    private void runMD5Benchmark( final int chunkMB, final int nrRuns, 
            final boolean finalizeAndInitDigestEveryRun )
    {
        displaySystemInfoOnce();
        outLn( NL + "********* Run each test: " + nrRuns + " times" 
                + CSV_SEP + " With: " + chunkMB + " MB chunks" 
                + CSV_SEP + " Initialize Digest on every run= " + finalizeAndInitDigestEveryRun + NL );

        // generate data in memory
        final byte[] byteArray = new byte[ chunkMB * 1024 * 1024 ];
        ThreadLocalRandom.current().nextBytes( byteArray );

        fastMD5MakeSureYouCompileWithJIT( byteArray, nrRuns, finalizeAndInitDigestEveryRun );        
        javaDigest( "MD5", nrRuns, byteArray, finalizeAndInitDigestEveryRun );        
    }


    private void runBenchmark( final int chunkMB, final int nrRuns, 
            final boolean finalizeAndInitDigestEveryRun ) 
    {
        displaySystemInfoOnce();
        outLn( NL + "************** Run each test: " + nrRuns + " times" + CSV_SEP 
                + "with: " + chunkMB + "MB chunks" + CSV_SEP 
                + "Initialize Digest on every run=" + finalizeAndInitDigestEveryRun + NL );        

        // generate data in memory
        final byte[] byteArray = new byte[ chunkMB * 1024 * 1024 ];
        ThreadLocalRandom.current().nextBytes( byteArray );
        //new Random().nextBytes( byte2 );

        fastMD5MakeSureYouCompileWithJIT( byteArray, nrRuns, finalizeAndInitDigestEveryRun );
        outLn( "" );

        javaDigest( "MD5", nrRuns, byteArray, finalizeAndInitDigestEveryRun );
        javaDigest( "SHA-1", nrRuns, byteArray, finalizeAndInitDigestEveryRun );
        javaDigest( "SHA-256", nrRuns, byteArray, finalizeAndInitDigestEveryRun );
        javaDigest( "SHA-512", nrRuns, byteArray, finalizeAndInitDigestEveryRun );
        outLn( "" );

        getCRCJavaZip( byteArray, nrRuns, finalizeAndInitDigestEveryRun );
        outLn( "" );

        getCrc32cIoNetty( byteArray, nrRuns, finalizeAndInitDigestEveryRun );        
    }


    private byte[]  javaDigest( final String algo, final int nrRuns, final byte[] byteArray, 
            final boolean finalizeAndInitDigestEveryRun )
    {
        final byte[] ret;
        try
        {
            final int chunkMB = byteArray.length / ( 1024 * 1024 );
            MessageDigest digest = MessageDigest.getInstance( algo );
            tryForceGarbageCollection();
            final long start = System.currentTimeMillis();
            for ( int i = 0; i < nrRuns; i++ ) 
            {    
                digest.update( byteArray );
                if ( finalizeAndInitDigestEveryRun )
                {
                    digest.digest(); //final computations reset the digest
                }
            }
            if ( !finalizeAndInitDigestEveryRun )
            {
                digest.digest(); //final computations reset the digest
            }
            final long end = System.currentTimeMillis();
            final long totalSecs = ( end - start ) / 1000;        
            digest = MessageDigest.getInstance( algo );
            digest.update( byteArray );
            ret = digest.digest();   
            printNice( digest.getProvider().getName(),  digest.getAlgorithm(), totalSecs, nrRuns, 
                    chunkMB, digest.getClass(), finalizeAndInitDigestEveryRun );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        return ret;
    }


    /**
     * http://www.twmacinta.com/myjava/fast_md5.php
     * 
     * @param byteArray
     * @param nrRuns
     * @param totalMB
     */
    private byte[] fastMD5MakeSureYouCompileWithJIT( final byte[] byteArray, final int nrRuns, 
            final boolean finalizeAndInitDigestEveryRun )
    {          
        final byte[] ret;
        try
        {
            final Class<?> fastMD5Class = Class.forName( "com.spectralogic.util.io." +
                    "ThreadedDataMover$FastMD5ChecksumComputer" );
            final Method getFindAndCheckTheNativeLib = fastMD5Class.getDeclaredMethod( 
                    "getFindAndCheckTheNativeLib" );
            getFindAndCheckTheNativeLib.setAccessible( true );
            final File nativeLib = ( File )getFindAndCheckTheNativeLib.invoke( null );
            System.setProperty( "com.twmacinta.util.MD5.NATIVE_LIB_FILE",  nativeLib.getAbsolutePath() );

            final MD5 fastMD5Digest = new MD5(); //by default is native
            fastMD5Digest.Init();
            if ( !MD5.initNativeLibrary() )
            {
                throw new Exception( "Fast MD5 NATIVE MODE is NOT enabled, " +
                        "so the benchmark does not make much sense" );              
            }
            tryForceGarbageCollection();
            final long start = System.currentTimeMillis();
            for ( int i = 0; i < nrRuns; i++ ) 
            {            
                fastMD5Digest.Update( byteArray );
                fastMD5Digest.Final();
                if ( finalizeAndInitDigestEveryRun )
                {
                    fastMD5Digest.Init(); //final computations reset the digest
                }
            }
            if ( !finalizeAndInitDigestEveryRun )
            {
                fastMD5Digest.Init(); //final computations reset the digest
            }

            final long totalSecs = ( System.currentTimeMillis() - start ) / 1000;
            fastMD5Digest.Init();
            fastMD5Digest.Update( byteArray );
            ret = fastMD5Digest.Final();
            final int chunkMB = byteArray.length / ( 1024 * 1024 );
            printNice( "Fast MD5", "MD5", totalSecs, nrRuns, chunkMB, fastMD5Digest.getClass(), 
                    finalizeAndInitDigestEveryRun );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        return ret;
    }


    private long getCRCJavaZip( final byte [] byteArray, final int nrRuns, 
            final boolean finalizeAndInitDigestEveryRun )      
    {
        final int chunkMB = byteArray.length / ( 1024 * 1024 );
        final CRC32 crc32JavaZip = new CRC32();
        tryForceGarbageCollection();
        final long start = System.currentTimeMillis();
        for ( int i = 0; i < nrRuns; i++ ) 
        {            
            crc32JavaZip.update( byteArray );
            crc32JavaZip.getValue();
        }
        final long totalSecs = ( System.currentTimeMillis() - start ) / 1000;
        //STOP TIMER
        crc32JavaZip.reset();
        crc32JavaZip.update( byteArray );
        final long crc32 = crc32JavaZip.getValue();
        printNice( "crc32JavaZip",  "CRC32", totalSecs, nrRuns, chunkMB,
                crc32JavaZip.getClass(), finalizeAndInitDigestEveryRun );
        return crc32;
    }


    private long getCrc32cIoNetty( final byte [] byteArray, final int nrRuns, 
            final boolean finalizeAndInitDigestEveryRun )
    {
        final long crc32c;
        try
        {
            final int chunkMB = byteArray.length / ( 1024 * 1024 );
            final Constructor< ? > conCRC32c = Class.forName( "io.netty.handler.codec.compression.Crc32c" )
                    .getDeclaredConstructor();          
            conCRC32c.setAccessible( true );
            final Checksum crc = ( Checksum )conCRC32c.newInstance();                
            tryForceGarbageCollection();
            final long start = System.currentTimeMillis();
            for ( int i = 0; i < nrRuns; i++ ) 
            {            
                crc.update( byteArray, 0, byteArray.length );                
                crc.getValue();                
            }
            final long totalSecs = ( System.currentTimeMillis() - start ) / 1000;
            //STOP TIMER
            crc.reset();
            crc.update( byteArray, 0, byteArray.length );                
            crc32c = crc.getValue();  
            printNice( "getCrc32C_IoNetty",  "CRC32c", totalSecs, nrRuns, chunkMB, 
                    conCRC32c.getName(),  finalizeAndInitDigestEveryRun );
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
        return crc32c;
    }


    /**
     * https://www.openssl.org/source/
     * @throws Exception
     */
    @SuppressWarnings( "unused" )
    private void openSSL()
    {
        try
        {
            final String cmd = "openssl pkcs8 -inform der -nocrypt -in test.der -out result.pem";
            final Runtime runtime = Runtime.getRuntime();
            final Process proc = runtime.exec( cmd );
            final BufferedReader in = new BufferedReader( new InputStreamReader( proc.getInputStream() ) );
            String line = in.readLine();
            while ( line != null ) 
            {
                outLn( line );
                line = in.readLine();
            }
        }
        catch ( final Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }


    private void printNice( final String provider, final String algo, 
            final long totalSecs, int nrRuns, final int chunkMB, Class< ? > digestClass, 
            final boolean finalizeAndInitDigestEveryRun )
    {
        printNice( provider, algo, totalSecs, nrRuns, chunkMB, digestClass.getName(), 
                finalizeAndInitDigestEveryRun );
    }


    private void printNice( final String provider, final String algo, 
            final long totalSecs, final int nrRuns, final int chunkMB, final String digestClassName,
            final boolean finalizeAndInitDigestEveryRun )
    {
        outLn( provider + CSV_SEP + algo + CSV_SEP
                + ( ( totalSecs == 0 ) ? "INSTANT" : ( ( nrRuns * chunkMB / totalSecs ) + " MB/s" ) )
                + CSV_SEP + "\t" + totalSecs + "sec for " + nrRuns + " * " + chunkMB + " MB" 
                + CSV_SEP + " initDigestEveryRun=" + finalizeAndInitDigestEveryRun 
                // + CSV_SEP + "\t\t" + hashStr 
                + CSV_SEP + " Class=" + digestClassName );

    }


    static private void outLn( final String str )
    {
        final PrintStream ps = System.out;
        ps.print( str );
        ps.flush();
    }


    private void tryForceGarbageCollection()
    {
        Thread.yield();
        Object obj = new Object();
        final WeakReference< ? > ref = new WeakReference<>( obj );
        obj = null;
        while ( ref.get() != null ) 
        {
            System.gc();
            Thread.yield();
        }
        Thread.yield();
        System.gc ();
        Thread.yield();
        System.runFinalization ();
        Thread.yield();
    }


    static private void displaySystemInfoOnce()
    {
        if ( s_displayedSysInfoOnce )
        {
            return;
        }

        try
        {
            final InetAddress addr = InetAddress.getLocalHost();
            s_hostInetName = addr.getHostName();
        }
        catch ( final UnknownHostException ex )
        {
            outLn( "EXCEPTION: " + ex );
        }
        s_displayedSysInfoOnce = true;
        outLn( SEP_LINE );
        outLn( "Host Inet name: " + s_hostInetName + "; Computer name: " + s_computerName );
        outLn( "OS: " + OSNAME + CSV_SEP + "Arch: " + OSARCH + CSV_SEP + " OsVer: " + OSVER );
        outLn( "JAVA ver: " + JAVAVERSION + CSV_SEP + "Runtime ver: " + JAVARUNTIMEVERSION + CSV_SEP 
                + "VM name: " + JAVAVMNAME + CSV_SEP + "Class ver: " + JAVACLASSVERSION );
        outLn( SEP_LINE );
    }


    private static final String SEP_LINE = 
            "==========================================================";
    private final static char CSV_SEP = ';';
    private final static String NL = Platform.NEWLINE;
    private static boolean s_displayedSysInfoOnce = false;
    private final static String OSNAME = System.getProperty( "os.name" );
    private final static String OSARCH = System.getProperty( "os.arch" );
    private final static String OSVER = System.getProperty( "os.version" );
    private final static String JAVAVERSION = System.getProperty( "java.version" );
    private final static String JAVARUNTIMEVERSION = System.getProperty( "java.runtime.version" );
    private final static String JAVAVMNAME = System.getProperty( "java.vm.name" );
    private final static String JAVACLASSVERSION = System.getProperty( "java.class.version" ); 
    private static String s_computerName = "Unknown";
    private static String s_hostInetName = "Unknown";
    static 
    {         
        final Map<String, String> env = System.getenv();
        if ( env.containsKey( "COMPUTERNAME" ) )
        {
            s_computerName = env.get( "COMPUTERNAME" );
        }
        else if ( env.containsKey( "HOSTNAME" ) )
        {
            s_computerName = env.get( "HOSTNAME" );
        }
    }
}