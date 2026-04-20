/*******************************************************************************
 *
 * Copyright C 2014, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.query;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import com.spectralogic.util.db.mockdomain.County;

public final class AllResultsWhereClause_Test
{
    @Test
    public void testToSqlReturnsTrueClause()
    {
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = AllResultsWhereClause.INSTANCE.toSql( County.class, sqlParameters );
        assertEquals( "true", sql,
                "Shoulda returned a true sql clause."  );
        assertTrue(sqlParameters.isEmpty(), "Shoulda had empty parameters.");
    }
    
    
    @Test
    public void testToStringDoesNotBlowUp()
    {
        assertNotNull(
                AllResultsWhereClause.INSTANCE.toString(),
                "Shoulda returned a non-null .toString() result."
                 );
    }
}
