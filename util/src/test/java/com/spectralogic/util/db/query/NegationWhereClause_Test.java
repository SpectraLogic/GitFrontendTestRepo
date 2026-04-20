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

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.mockdomain.County;

public final class NegationWhereClause_Test
{
    @Test
    public void testToSqlReturnsExpectedClause()
    {
        final NegationWhereClause negationWhereClause = new NegationWhereClause( new StubWhereClause() );
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = negationWhereClause.toSql( County.class, sqlParameters  );
        assertEquals( "NOT ClauseToNegate",
                sql,
                "Shoulda returned the expected sql clause." );
        assertEquals( 1,
                sqlParameters.size() ,
                "Shoulda add exactly 1 parameter." );
        assertEquals( "parameter",
                sqlParameters.get( 0 ),
                "Shoulda added the correct parameter."  );
    }
    
    
    @Test
    public void testToStringDoesNotBlowUp()
    {
        final NegationWhereClause negationWhereClause =
                new NegationWhereClause( new WhereClause()
                {
                    public String toSql(
                            final Class< ? extends DatabasePersistable > clazz,
                            final List< Object > sqlParameters )
                    {
                        sqlParameters.add( "parameter" );
                        return "ClauseToNegate";
                    }
                } );
        assertNotNull( "Shoulda returned some kind of real string.", negationWhereClause.toString() );
    }
    
    
    private final class StubWhereClause implements WhereClause
    {
        public String toSql(
                final Class< ? extends DatabasePersistable > clazz,
                final List< Object > sqlParameters )
        {
            sqlParameters.add( "parameter" );
            return "ClauseToNegate";
        }
    }// end inner class
}
