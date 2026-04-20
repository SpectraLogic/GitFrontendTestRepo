/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.codegen.cli;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.mockdomain.County;
import com.spectralogic.util.lang.CollectionFactory;
import com.spectralogic.util.testfrmwrk.TestUtil;
import com.spectralogic.util.testfrmwrk.TestUtil.BlastContainer;

public final class GenerateCode_Test 
{    
    @Test
    public void testGenerateCode0ProgramArgumentsNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    GenerateCode.main( CollectionFactory.toArray( String.class, new ArrayList< String >() ) );
                }
            } );
    }
    
    
    @Test
    public void testGenerateCode1ProgramArgumentsNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    GenerateCode.main( new String [] { "a" } );
                }
            } );
    }
    
    
    @Test
    public void testGenerateCode2ProgramArgumentsNotAllowed()
    {
        TestUtil.assertThrows( null, IllegalArgumentException.class, new BlastContainer()
        {
            public void test()
                {
                    GenerateCode.main( new String [] { "a", "b" } );
                }
            } );
    }
    
    
    @Test
    public void testGenerateCodeCorrectProgramArgumentsDoesNotBlowUp() throws IOException
    {
        final File sqlFile = File.createTempFile( getClass().getSimpleName(), ".sql" );
        final File cFile = File.createTempFile( getClass().getSimpleName(), ".h" );
        sqlFile.delete();
        cFile.delete();
        
        assertFalse(
                sqlFile.exists(),
                "Before running the code generator, the file shouldn't have existed.");
        assertFalse(
                cFile.exists(),
                "Before running the code generator, the file shouldn't have existed.");
        
        GenerateCode.main( new String [] 
                { County.class.getName(), sqlFile.getAbsolutePath(), cFile.getAbsolutePath() } );
        
        assertTrue(
                sqlFile.exists(),
                "After running the code generator, the file should have existed.");
        assertTrue(
                cFile.exists(),
                "After running the code generator, the file should have existed.");
        
        sqlFile.delete();        
        for( File tempFile : cFile.listFiles() )
        {
            tempFile.delete();
        }
        cFile.delete();
    }
}
