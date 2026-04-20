/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockdomain;

import java.util.Date;

import com.spectralogic.util.bean.lang.DefaultIntegerValue;
import com.spectralogic.util.bean.lang.Optional;
import com.spectralogic.util.bean.lang.Secret;
import com.spectralogic.util.bean.lang.SortBy;
import com.spectralogic.util.bean.lang.SortBy.Direction;
import com.spectralogic.util.db.lang.ConfigureSqlLogLevels;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.MustMatchRegularExpression;
import com.spectralogic.util.db.lang.SqlLogLevels;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@ConfigureSqlLogLevels( SqlLogLevels.ALL_OPERATIONS_LOGGED_AT_DEBUG_LEVEL )
@UniqueIndexes(
{
    @Unique({ Teacher.DATE_OF_BIRTH, Teacher.NAME }),
    @Unique( Teacher.SSN )
})
public interface Teacher extends DatabasePersistable
{
    String TYPE = "type";
    
    TeacherType getType();
    
    Teacher setType( final TeacherType name );
    
    
    String NAME = "name";
    
    String getName();
    
    Teacher setName( final String name );
    
    
    String PASSWORD = "password";
    
    @Optional
    @Secret
    String getPassword();
    
    Teacher setPassword( final String value );
    
    
    String DATE_OF_BIRTH = "dateOfBirth";
    
    Date getDateOfBirth();
    
    Teacher setDateOfBirth( final Date dateOfBirth );
    
    
    String SSN = "ssn";
    
    @Optional
    @MustMatchRegularExpression( "[0-9][0-9][0-9]\\-[0-9][0-9]\\-[0-9][0-9][0-9][0-9]" )
    String getSsn();
    
    Teacher setSsn( final String ssn );
    
    
    String COMMENTS = "comments";
    
    @Optional
    String getComments();
    
    Teacher setComments( final String comments );
    
    
    String YEARS_OF_SERVICE = "yearsOfService";
    
    @SortBy( 1 )
    int getYearsOfService();
    
    Teacher setYearsOfService( final int yearsOfService );
    
    
    String WARNINGS_ISSUED = "warningsIssued";
    
    @SortBy( value = 2, direction = Direction.DESCENDING )
    @DefaultIntegerValue( 0 )
    int getWarningsIssued();
    
    Teacher setWarningsIssued( final int warningsIssued );
}
