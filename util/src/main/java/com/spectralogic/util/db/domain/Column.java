/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.domain;

import java.util.UUID;

import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.ReadOnly;
import com.spectralogic.util.db.lang.Schema;
import com.spectralogic.util.db.lang.TableName;
import com.spectralogic.util.db.lang.shared.ExcludeFromDatabasePersistence;

@Schema( "information_schema" )
@TableName( "columns" )
public interface Column extends DatabasePersistable, ReadOnly
{
    @ExcludeFromDatabasePersistence
    UUID getId();
    
    
    String TABLE_SCHEMA = "tableSchema";
    
    String getTableSchema();
    
    void setTableSchema( final String tableSchema );
    
    
    String TABLE_NAME = "tableName";
    
    String getTableName();
    
    void setTableName( final String tableName );
    
    
    String COLUMN_NAME = "columnName";
    
    String getColumnName();
    
    void setColumnName( final String columnName );
    
    
    String COLUMN_DEFAULT = "columnDefault";
    
    String getColumnDefault();
    
    void setColumnDefault( final String columnDefault );
    
    
    String IS_NULLABLE = "isNullable";
    
    String getIsNullable();
    
    void setIsNullable( final String isNullable );
    
    
    String DATA_TYPE = "dataType";
    
    String getDataType();
    
    void setDataType( final String dataType );
}
