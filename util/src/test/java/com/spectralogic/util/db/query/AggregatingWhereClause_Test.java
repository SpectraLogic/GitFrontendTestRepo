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
import com.spectralogic.util.db.query.AggregatingWhereClause.AggregationType;

public final class AggregatingWhereClause_Test 
{
    @Test
    public void testToSqlReturnsTrueClauseWhenNoWhereClausesAnded()
    {
        checkToSqlReturnsExpectedWhenNoWhereClausesProvided( AggregationType.AND, "true" );
    }
    
    
    @Test
    public void testToSqlReturnsTrueClauseWhenNoWhereClausesOred()
    {
        checkToSqlReturnsExpectedWhenNoWhereClausesProvided( AggregationType.OR, "false" );
    }


    private void checkToSqlReturnsExpectedWhenNoWhereClausesProvided(
            final AggregationType operator, 
            final String expectedQuery )
    {
        final AggregatingWhereClause aggregatingWhereClause =
                new AggregatingWhereClause( operator );
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = aggregatingWhereClause.toSql( County.class, sqlParameters );

        assertNotNull( "Shoulda returned a query.", sql );
        assertEquals(expectedQuery, sql, "Shoulda returned a correct where clause.");
        assertEquals(0,  sqlParameters.size(), "Shoulda added no parameters.");
    }
    
    
    @Test
    public void testToSqlReturnsOriginalClauseWhenOneWhereClauseAnded()
    {
        checkToSqlReturnsOriginalClauseWhenOneWhereClauseProvided( AggregationType.AND );
    }
    
    
    @Test
    public void testToSqlReturnsOriginalClauseWhenOneWhereClauseOred()
    {
        checkToSqlReturnsOriginalClauseWhenOneWhereClauseProvided( AggregationType.OR );
    }


    private void checkToSqlReturnsOriginalClauseWhenOneWhereClauseProvided( final AggregationType operator )
    {
        final String innerWhereClause = "InnerWhereClause";
        final AggregatingWhereClause aggregatingWhereClause = new AggregatingWhereClause(
                operator,
                new PlaceHolderWhereClause( innerWhereClause ) );
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = aggregatingWhereClause.toSql( County.class, sqlParameters );

        assertNotNull( "Shoulda returned a query.", sql );
        assertEquals("(InnerWhereClause)", sql, "Shoulda returned a parenthesized where clause.");

        assertEquals(1,  sqlParameters.size(), "Shoulda added one parameter.");
        assertEquals(innerWhereClause, sqlParameters.get( 0 ), "Shoulda added the first parameter.");
    }
    

    @Test
    public void testToSqlReturnsAndedClauseWhenTwoWhereClausesAnded()
    {
        checkToSqlReturnsCombinedClauseWhenTwoWhereClausesProvided(
                AggregationType.AND,
                "(FirstWhereClause AND SecondWhereClause)" );
    }
    

    @Test
    public void testToSqlReturnsOredClauseWhenTwoWhereClausesOred()
    {
        checkToSqlReturnsCombinedClauseWhenTwoWhereClausesProvided(
                AggregationType.AND,
                "(FirstWhereClause AND SecondWhereClause)" );
    }


    private void checkToSqlReturnsCombinedClauseWhenTwoWhereClausesProvided(
            final AggregationType operator,
            final String expectedWhereClause )
    {
        final String firstWhereClause = "FirstWhereClause";
        final String secondWhereClause = "SecondWhereClause";
        
        final AggregatingWhereClause aggregatingWhereClause = new AggregatingWhereClause(
                operator,
                new PlaceHolderWhereClause( firstWhereClause ),
                new PlaceHolderWhereClause( secondWhereClause ) );
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = aggregatingWhereClause.toSql( County.class, sqlParameters );

        assertNotNull( "Shoulda returned a query.", sql );
        assertEquals(expectedWhereClause, sql, "Shoulda returned the aggregation of the two clauses.");

        assertEquals(2,  sqlParameters.size(), "Shoulda added two parameters.");
        assertEquals(firstWhereClause, sqlParameters.get( 0 ), "Shoulda added the first parameter.");
        assertEquals(secondWhereClause, sqlParameters.get( 1 ), "Shoulda added the second parameter.");
    }
    

    @Test
    public void testToSqlReturnsAndedClauseWhenThreeWhereClausesAnded()
    {
        checkToSqlReturnsAggregatedClauseWhenThreeWhereClausesProvided(
                AggregationType.AND,
                "(FirstWhereClause AND SecondWhereClause AND ThirdWhereClause)" );
    }
    

    @Test
    public void testToSqlReturnsOredClauseWhenThreeWhereClausesOred()
    {
        checkToSqlReturnsAggregatedClauseWhenThreeWhereClausesProvided(
                AggregationType.OR,
                "(FirstWhereClause OR SecondWhereClause OR ThirdWhereClause)" );
    }


    private void checkToSqlReturnsAggregatedClauseWhenThreeWhereClausesProvided(
            final AggregationType operator,
            final String expectedWhereClause )
    {
        final String firstWhereClause = "FirstWhereClause";
        final String secondWhereClause = "SecondWhereClause";
        final String thirdWhereClause = "ThirdWhereClause";
        
        final AggregatingWhereClause aggregatingWhereClause = new AggregatingWhereClause(
                operator,
                new PlaceHolderWhereClause( firstWhereClause ),
                new PlaceHolderWhereClause( secondWhereClause ),
                new PlaceHolderWhereClause( thirdWhereClause ) );
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = aggregatingWhereClause.toSql( County.class, sqlParameters );

        assertNotNull( "Shoulda returned a query.", sql );
        assertEquals(expectedWhereClause, sql, "Shoulda returned the aggregation of the three clauses.");

        assertEquals(3,  sqlParameters.size(), "Shoulda added three parameters.");
        assertEquals(firstWhereClause, sqlParameters.get( 0 ), "Shoulda added the first parameter.");
        assertEquals(secondWhereClause, sqlParameters.get( 1 ), "Shoulda added the second parameter.");
        assertEquals(thirdWhereClause, sqlParameters.get( 2 ), "Shoulda added the third parameter.");
    }
    

    @Test
    public void testToSqlReturnsSingleClauseWhenOneClauseIsNull()
    {
        final AggregatingWhereClause aggregatingWhereClause = new AggregatingWhereClause(
                AggregationType.AND,
                null,
                new PlaceHolderWhereClause( "WhereClause" ) );
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = aggregatingWhereClause.toSql( County.class, sqlParameters );
        assertEquals("(WhereClause)", sql, "Shoulda had the correct sql.");
        assertEquals(1,  sqlParameters.size(), "Shoulda had exactly one parameter.");
        assertEquals("WhereClause", sqlParameters.get( 0 ), "Shoulda had the correct parameter.");
    }
    

    @Test
    public void testToSqlReturnsSingleClauseWhenOneClauseReturnsNullSql()
    {
        final AggregatingWhereClause aggregatingWhereClause = new AggregatingWhereClause(
                AggregationType.AND,
                new WhereClause()
                {
                    public String toSql(
                            final Class< ? extends DatabasePersistable > clazz,
                            final List< Object > sqlParameters )
                    {
                        return null;
                    }
                },
                new PlaceHolderWhereClause( "WhereClause" ) );
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = aggregatingWhereClause.toSql( County.class, sqlParameters );
        assertEquals("(WhereClause)", sql, "Shoulda had the correct sql.");
        assertEquals(1,  sqlParameters.size(), "Shoulda had exactly one parameter.");
        assertEquals("WhereClause", sqlParameters.get( 0 ), "Shoulda had the correct parameter.");
    }
    

    @Test
    public void testToSqlReturnsSingleClauseWhenOneClauseEmpty()
    {
        final AggregatingWhereClause aggregatingWhereClause = new AggregatingWhereClause(
                AggregationType.AND,
                new WhereClause()
                {
                    public String toSql(
                            final Class< ? extends DatabasePersistable > clazz,
                            final List< Object > sqlParameters )
                    {
                        return "";
                    }
                },
                new PlaceHolderWhereClause( "WhereClause" ) );
        final List< Object > sqlParameters = new ArrayList<>();
        final String sql = aggregatingWhereClause.toSql( County.class, sqlParameters );
        assertEquals("(WhereClause)", sql, "Shoulda had the correct sql.");
        assertEquals(1,  sqlParameters.size(), "Shoulda had exactly one parameter.");
        assertEquals("WhereClause", sqlParameters.get( 0 ), "Shoulda had the correct parameter.");
    }
    
    
    @Test
    public void testToStringDoesNotBlowUp()
    {
        final PlaceHolderWhereClause firstWhereClause = new PlaceHolderWhereClause( "FirstWhereClause" );
        final PlaceHolderWhereClause secondWhereClause = new PlaceHolderWhereClause( "SecondWhereClause" );

        AggregatingWhereClause aggregatingWhereClause = new AggregatingWhereClause( AggregationType.AND );
        assertNotNull(
                "Shoulda returned some kind of value from toString()",
                aggregatingWhereClause.toString() );
        
        aggregatingWhereClause = new AggregatingWhereClause( AggregationType.AND, firstWhereClause );
        assertNotNull(
                "Shoulda returned some kind of value from toString()",
                aggregatingWhereClause.toString() );
        
        aggregatingWhereClause = new AggregatingWhereClause(
                AggregationType.AND,
                firstWhereClause,
                secondWhereClause );
        assertNotNull(
                "Shoulda returned some kind of value from toString()",
                aggregatingWhereClause.toString() );
    }
    
    
    private static final class PlaceHolderWhereClause implements WhereClause
    {
        private PlaceHolderWhereClause( String whereClauseName )
        {
            m_whereClauseName = whereClauseName;
        }


        public String toSql(
                final Class< ? extends DatabasePersistable > clazz,
                final List< Object > sqlParameters )
        {
            sqlParameters.add( m_whereClauseName );
            return m_whereClauseName;
        }
        
        
        private final String m_whereClauseName;
    }// end inner class
}
