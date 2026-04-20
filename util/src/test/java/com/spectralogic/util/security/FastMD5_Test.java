/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.security;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.MessageDigest;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.commons.codec.binary.Base64;
import org.apache.log4j.Logger;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;
import com.twmacinta.util.MD5;


public final class FastMD5_Test 
{
    @AfterEach
    public void tearDown() throws Exception
    {
        FastMD5.initAll();
        resetSystemProperties();
    }
    
    
    @Test
    public void testThreadedDataMovedModifyBufferLength()
    {
        try
        {
            final byte[] data1MB = new byte[ 1 * 1024 * 1024 ];
            ThreadLocalRandom.current().nextBytes( data1MB );

            final Class<?> bufferClass = Class.forName( "com.spectralogic.util.io." +
                    "ThreadedDataMover$Buffer" );
            final Constructor< ? >bufferConstructor = bufferClass.getDeclaredConstructor( byte[].class );
            bufferConstructor.setAccessible( true );
            final Object bufferInstance = bufferConstructor.newInstance( data1MB );
            final Field bugLength = bufferClass.getDeclaredField( "m_length" );
            bugLength.setAccessible( true );
            bugLength.setInt( bufferInstance, data1MB.length );

            final Class<?> fastMD5Class = Class.forName( "com.spectralogic.util.io." +
                    "ThreadedDataMover$FastMD5ChecksumComputer" );
            final Constructor< ? >fastMD5Constructor = fastMD5Class.getDeclaredConstructor( 
                    EMPTY_CLASS_ARRAY );
            fastMD5Constructor.setAccessible( true );
            final Object fastMD5Instance = fastMD5Constructor.newInstance();
            final Method processBuffer = fastMD5Class.getMethod( "processBuffer", bufferClass );
            processBuffer.setAccessible( true );
            processBuffer.invoke( fastMD5Instance, bufferInstance );
            final Method getChecksum = fastMD5Class.getMethod( "getChecksum" );
            getChecksum.setAccessible( true );
            final byte[] fastMD5Checksum = (byte[]) getChecksum.invoke( fastMD5Instance,
                    EMPTY_OBJECT_ARRAY );
            final String fastMD5Base64 = Base64.encodeBase64String( fastMD5Checksum );
            //java
            final MessageDigest javaMD5Digest = MessageDigest.getInstance( "MD5" );
            javaMD5Digest.update( data1MB );
            final String javaMD5Base64 = Base64.encodeBase64String( javaMD5Digest.digest() );

            assertTrue(javaMD5Base64.equals( fastMD5Base64 ), "FastMD from INSIDE Threaded DataMover( " + fastMD5Base64
                        + " ) should same as Java MD5 ( " + javaMD5Base64 + " )");

            // we now modify the Buffer object's m_length variable 
            final int somethingSmallerThanHalf =  data1MB.length -  123;
            bugLength.setInt( bufferInstance, somethingSmallerThanHalf );
            final int readBack = bugLength.getInt( bufferInstance );
            assertTrue(somethingSmallerThanHalf == readBack, "The buffer lenght value we set, should read back correctly.");
            // THIS IS THE TEST - this call of processBuffer should process the new lenght
            processBuffer.invoke( fastMD5Instance, bufferInstance );
            final byte[] fastMD5ChecksumPartialBuffer = (byte[]) getChecksum.invoke( fastMD5Instance, 
                    EMPTY_OBJECT_ARRAY );
            final String fastMD5Base64PartialBuffer = Base64.encodeBase64String( 
                    fastMD5ChecksumPartialBuffer );
            //java
            final MessageDigest javaMD5DigestPartialBuffer = MessageDigest.getInstance( "MD5" );
            javaMD5DigestPartialBuffer.update( data1MB, 0, somethingSmallerThanHalf );
            final String javaMD5Base64PartialBuffer = Base64.encodeBase64String( 
                    javaMD5DigestPartialBuffer.digest() ); 

            final FastMD5 fastMD5StandAlone = new FastMD5();
            fastMD5StandAlone.update( data1MB, 0, somethingSmallerThanHalf );
            final String fastMD5StandaloneBase64PartialBuffer = Base64.encodeBase64String( 
                    fastMD5StandAlone.digestAndReset() );
            //update again to test the reset worked
            fastMD5StandAlone.update( data1MB, 0, somethingSmallerThanHalf );
            final String fastMD5StandaloneCheckResetWorkedBase64PartialBuffer = Base64.encodeBase64String( 
                    fastMD5StandAlone.digestAndReset() );

            assertTrue(fastMD5StandaloneCheckResetWorkedBase64PartialBuffer.equals(
                                fastMD5Base64PartialBuffer ), "Partial Buffer FastMD from Threaded DataMover( " + fastMD5Base64PartialBuffer
                        + " ) should same as Standalone Fast MD5 with a SECOND pass on update ( "
                        + fastMD5StandaloneBase64PartialBuffer + " ) ");

            assertTrue(fastMD5StandaloneBase64PartialBuffer.equals( fastMD5Base64PartialBuffer ), "Partial Buffer FastMD from Threaded DataMover( " + fastMD5Base64PartialBuffer
                        + " ) should same as Standalone Fast MD5( " + fastMD5StandaloneBase64PartialBuffer + " )");

            assertTrue(javaMD5Base64PartialBuffer.equals( fastMD5Base64PartialBuffer ), "Partial Buffer FastMD from Threaded DataMover( " + fastMD5Base64PartialBuffer
                        + " ) should same as Java MD5 ( " + javaMD5Base64PartialBuffer + " )");
        }
        catch ( final Exception ex ) 
        {
            final String message = "Code should not throw any exception but it throws:" + ex.getMessage();
            assertTrue(false, message);
        }
        resetSystemProperties();
    }


    @Test
    public void testThreadedDataResetsChecksumAfterGetChecksumMethod()
    {
        try
        {
            final byte[] data1kb = new byte[ 1024 ];
            ThreadLocalRandom.current().nextBytes( data1kb );

            final Class<?> bufferClass = Class.forName( "com.spectralogic.util.io." +
                    "ThreadedDataMover$Buffer" );
            final Constructor< ? >bufferConstructor = bufferClass.getDeclaredConstructor( byte[].class );
            bufferConstructor.setAccessible( true );
            final Object bufferInstance = bufferConstructor.newInstance( data1kb );
            final Field bufLength = bufferClass.getDeclaredField( "m_length" );
            bufLength.setAccessible( true );
            //make the length less than the byte array
            bufLength.setInt( bufferInstance, data1kb.length - 123 );

            final Class<?> fastMD5Class = Class.forName( "com.spectralogic.util.io." +
                    "ThreadedDataMover$FastMD5ChecksumComputer" );
            final Constructor< ? >fastMD5Constructor = fastMD5Class.getDeclaredConstructor( 
                    EMPTY_CLASS_ARRAY );
            fastMD5Constructor.setAccessible( true );
            final Object fastMD5Instance = fastMD5Constructor.newInstance();
            final Method processBuffer = fastMD5Class.getMethod( "processBuffer", bufferClass );
            processBuffer.setAccessible( true );
            processBuffer.invoke( fastMD5Instance, bufferInstance );
            final Method getChecksum = fastMD5Class.getMethod( "getChecksum" );
            getChecksum.setAccessible( true );
            final byte[] fastMD5Checksum = (byte[]) getChecksum.invoke( fastMD5Instance,
                    EMPTY_OBJECT_ARRAY );
            final String fastMD5Base64 = Base64.encodeBase64String( fastMD5Checksum );
            // invoke again to test reset happened
            processBuffer.invoke( fastMD5Instance, bufferInstance );            
            final String fastMD5Base64InvokeTwice = Base64.encodeBase64String( 
                    (byte[]) getChecksum.invoke( fastMD5Instance, EMPTY_OBJECT_ARRAY ) );
            // invoke again to test reset happened
            processBuffer.invoke( fastMD5Instance, bufferInstance );
            final String fastMD5Base64InvokeThrice = Base64.encodeBase64String( 
                    (byte[]) getChecksum.invoke( fastMD5Instance, EMPTY_OBJECT_ARRAY ) );

            assertTrue(fastMD5Base64.equals( fastMD5Base64InvokeTwice )
                        && fastMD5Base64InvokeTwice.equals( fastMD5Base64InvokeThrice ), "Test than calling getChecksum resets the Checksum implementation.");
        }
        catch ( final Exception ex ) 
        {
            final String message = "Code should not throw any exception but it throws:" + ex.getMessage();
            assertTrue(false, message);
        }
        resetSystemProperties();
    }


    @Test
    public void testVariationsOfCreatingFastMD5ToTestNativeMode()
    {
        final String libName = FastMD5.getNativeLibForOsArchAndCheckReadWrite().getAbsolutePath();
        System.setProperty( "com.twmacinta.util.MD5.NATIVE_LIB_FILE", libName );            
        new MD5();
        final String message4 = "Once initialized in native mode shoud stay like that =" + MD5.initNativeLibrary();
        assertTrue(MD5.initNativeLibrary(), message4);

        MD5.initNativeLibrary( false );
        new MD5();
        final String message3 = "Once initialized in native mode shoud stay like that =" + MD5.initNativeLibrary();
        assertTrue(MD5.initNativeLibrary(), message3);

        MD5.initNativeLibrary( true );
        System.setProperty( "com.twmacinta.util.MD5.NATIVE_LIB_FILE", libName );            
        new MD5();
        final String message2 = "Once initialized in native mode shoud stay like that =" + MD5.initNativeLibrary();
        assertTrue(MD5.initNativeLibrary(), message2);

        MD5.initNativeLibrary( true );
        System.setProperty( "com.twmacinta.util.MD5.NATIVE_LIB_FILE", "asdfasdasdasd" );            
        new MD5();
        final String message1 = "Once initialized in native mode shoud stay like that =" + MD5.initNativeLibrary();
        assertTrue(MD5.initNativeLibrary(), message1);

        MD5.initNativeLibrary( false );
        System.setProperty( "com.twmacinta.util.MD5.NATIVE_LIB_FILE", libName );            
        new MD5();
        final String message = "Once initialized in native mode shoud stay like that =" + MD5.initNativeLibrary();
        assertTrue(MD5.initNativeLibrary(), message);
        resetSystemProperties();
    }


    @Test
    public void testFastMD5IsNativeAndTestItRunsFasterThanJavaInCaseAnyImplementationEverWillChange()
    {
        try
        {
            testFastMD5IsNativeAndTestItRunsFasterThanJavaInternal( 32 );
        }
        catch ( final RuntimeException ex )
        {
            LOG.warn( "First test failed.  Will re-run longer test.", ex );
            testFastMD5IsNativeAndTestItRunsFasterThanJavaInternal( 64 );
        }
    }


    private void testFastMD5IsNativeAndTestItRunsFasterThanJavaInternal( final int chunkMB )     
    {
        try
        {
            final FastMD5 fastMD5 = new FastMD5();
            final boolean isFastMD5InNativeMode = MD5.initNativeLibrary();
            assertTrue(isFastMD5InNativeMode, "Fast MD5 should be in NATIVE MODE=" + isFastMD5InNativeMode);
            final boolean isFastMD5NativeInNativeMode = MD5.initNativeLibrary();
            assertTrue(isFastMD5NativeInNativeMode, "Fast MD5 Native should be in NATIVE MODE=" + isFastMD5NativeInNativeMode);

            final int nrRuns = 10;
            final byte[] byteArray = new byte[ chunkMB * 1024 * 1024 ];
            ThreadLocalRandom.current().nextBytes( byteArray );

            // run the fast MD5
            final long startTimeMD5 = System.currentTimeMillis();
            for ( int i = 0; i < nrRuns; i++ )
            {
                fastMD5.updateWhenBufferLengthDoesNotChange( byteArray );
            }
            final byte[] md5ByteArray = fastMD5.digestAndReset();
            final long timeTotalFastMD5 = System.currentTimeMillis() - startTimeMD5;
            final String asHexFastMD5 = MD5.asHex( md5ByteArray );

            //run the java MD5
            final MessageDigest digest = MessageDigest.getInstance( "MD5" );
            final long startTimeJavaMD5 = System.currentTimeMillis();
            for ( int i = 0; i < nrRuns; i++ ) 
            {    
                digest.update( byteArray );
            }
            final byte[] digestJava = digest.digest(); 
            final long timeTotalJavaMD5 = System.currentTimeMillis() - startTimeJavaMD5;
            final String asHexJavaMD5 = MD5.asHex( digestJava );

            assertEquals( asHexFastMD5, asHexJavaMD5,"Java and FastMD5, MD5 should be the same" );
            assertTrue(timeTotalFastMD5 > 0.5 * timeTotalJavaMD5, "FastMD5 time( " + timeTotalFastMD5 + " ) should be at least 50% faster for these "
                        + "smaller runs ( usually is 100%) than Java's MD5 ( "
                        + timeTotalJavaMD5 + " )");
        }
        catch ( final Exception ex ) 
        {
            final String message = "Code should not throw any exception but it throws:" + ex.getMessage();
            assertTrue(false, message);
        }
        resetSystemProperties();
    }


    @Test
    public void testFastMD5getSubPathForNativeLibWrongPropsReturnsNull()
    {
        System.setProperty( "os.name", "blah.os.name" );
        System.setProperty( "os.arch", "blah.os.arch" );        
        final String libSubPath = FastMD5.getSubPathForNativeLibForOsArch();
        assertTrue(libSubPath == null, "Should return null since we cannot find the architecture");
        resetSystemProperties();
    }


    @Test
    public void testFastMD5getSubPathForNativeLibNullProps()
    {
        System.clearProperty( "os.name" );
        System.clearProperty( "os.arch" );
        final String libSubPath = FastMD5.getSubPathForNativeLibForOsArch();
        assertTrue(libSubPath == null, "Should return null since the os.name and os.arch are null");
        resetSystemProperties();
    }


    @Test
    public void testFastMD5getFindAndCheckTheNativeLibWorks()
    {    
        try
        {        
            final File libFile = FastMD5.getNativeLibForOsArchAndCheckReadWrite();
            assertTrue(libFile.getPath().contains( "fast-md5-native-2.7.1" ), "libFile ( " + libFile.getPath()
                        + " ) should contain the fast md5 native lib folder ");
        }
        catch ( final Exception ex )
        {
            final String message = "Code should not throw any exception but it throws:" + ex.getMessage();
            assertTrue(false, message);
        }
        resetSystemProperties();
    }


    @Test
    public void testFastMD5getFindAndCheckTheNativeLibThrowsException()
    {    
        FastMD5.initAll();
        final Throwable myException = TestUtil.assertThrows( null, RuntimeException.class, 
                new BlastContainer()
        {
            public void test()
            {
                System.setProperty( "os.name", "blah.os.name" );
                System.setProperty( "os.arch", "blah.os.arch" );
                FastMD5.getNativeLibForOsArchAndCheckReadWrite();
            }
        }
                );
        assertTrue(myException.getMessage().contains( "blah.os.name" ), "Exception contains the os name.");
        assertTrue(myException.getMessage().contains( "blah.os.arch" ), "Exception contains the os arch.");
        assertTrue(myException.getMessage().contains( "Could not load the FastMD5 native library" ), "Exception contains a nice error message.");
        resetSystemProperties();
    }

   
    private void resetSystemProperties()
    {
        System.setProperty( "os.name", OS_NAME_ORIG );
        System.setProperty( "os.arch", OS_ARCH_ORIG );        
    }
    
    
    private final static String OS_NAME_ORIG = System.getProperty( "os.name" );
    private final static String OS_ARCH_ORIG = System.getProperty( "os.arch" );        

    private final static Class<?>[] EMPTY_CLASS_ARRAY = new Class< ? >[ 0 ];
    private final static Object[] EMPTY_OBJECT_ARRAY = new Object[ 0 ];
    private final static Logger LOG = Logger.getLogger( FastMD5_Test.class );
}