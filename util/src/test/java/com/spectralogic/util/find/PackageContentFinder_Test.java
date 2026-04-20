/*******************************************************************************
 *
 * Copyright C 2015, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.find;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public class PackageContentFinder_Test 
{
    @Test
    public void testPackageContentFinderThrowsIllegalArgumentExceptionWithoutRequiredParams()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                final PackageContentFinder pcf = new PackageContentFinder( null, null, null );
                pcf.getClass();
            }
        } );
    }
    
    
    @Test
    public void testPackageContentFinderThrowsIllegalArgumentExceptionWhenGivenDirectoryForJarsDoesntExist()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test() throws Throwable
            {
                final PackageContentFinder pcf = new PackageContentFinder( null, 
                        PackageContentFinder.class, "/tmp/phantom_directory_42" );
                pcf.getClass();
            }
        } );
    }
    
    
    @Test
    public void testPackageContentFinderThrowsIllegalArgumentExceptionWhenGivenDirectoryForJarsIsAFile()
            throws IOException
    {
        final File myTempFile = File.createTempFile( "emptyTestFile", null );
        myTempFile.deleteOnExit();
        try
        {
            TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
            {
                public void test() throws Throwable
                {               
                    final PackageContentFinder pcf = new PackageContentFinder( null, 
                            PackageContentFinder.class, myTempFile.toString() );
                    pcf.getClass();
                }
            } );   
        }
        finally
        {
            myTempFile.delete();
        }
    }
    
    
    @Test
    public void testGivenDirectoryForJarsContainsNonJarFilesThrowsIllegalArgumentException()
            throws IOException
    {
        final String tempDir = System.getProperty( "java.io.tmpdir" );
        final File myTempFile = File.createTempFile( "emptyTestFile", null );
        myTempFile.deleteOnExit();
        try
        {
            TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
            {
                public void test() throws Throwable
                {
                    final PackageContentFinder pcf = new PackageContentFinder( null, 
                            PackageContentFinder.class, tempDir );
                    pcf.getClass();
                }
            } );
        }
        finally
        {
            myTempFile.delete();
        }
    }
}
