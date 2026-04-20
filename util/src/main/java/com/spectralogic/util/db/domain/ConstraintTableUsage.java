/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.ReadOnly;
import com.spectralogic.util.db.lang.Schema;
import com.spectralogic.util.db.lang.TableName;
import com.spectralogic.util.db.lang.shared.ExcludeFromDatabasePersistence;

import java.util.UUID;

@Schema( "information_schema" )
@TableName( "constraint_table_usage" )
public interface ConstraintTableUsage extends DatabasePersistable, ReadOnly
{
    @ExcludeFromDatabasePersistence
    UUID getId();

    String TABLE_SCHEMA = "tableSchema";

    String getTableSchema();

    void setTableSchema( final String tableSchema );

    String TABLE_NAME = "tableName";
    
    String getTableName();
    
    void setTableName( final String tableName );
    
    
    String CONSTRAINT_NAME = "constraintName";
    
    String getConstraintName();

    void setConstraintName( final String constraintName );
}
