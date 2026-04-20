/*******************************************************************************
 *
 * Copyright C 2013, Spectra Logic Corporation and/or its affiliates.  
 * All rights reserved.
 *
 ******************************************************************************/
package com.spectralogic.util.db.mockdomain;

import java.util.UUID;

import com.spectralogic.util.db.lang.CascadeDelete;
import com.spectralogic.util.db.lang.DatabasePersistable;
import com.spectralogic.util.db.lang.References;
import com.spectralogic.util.db.lang.Unique;
import com.spectralogic.util.db.lang.UniqueIndexes;

@UniqueIndexes(
{
    @Unique({ TeacherSchool.SCHOOL_ID, TeacherSchool.TEACHER_ID })
})
public interface TeacherSchool extends DatabasePersistable
{
    String TEACHER_ID = "teacherId";
    
    @CascadeDelete
    @References( Teacher.class )
    UUID getTeacherId();
    
    TeacherSchool setTeacherId( final UUID teacherId );
    
    
    String SCHOOL_ID = "schoolId";

    @References( School.class )
    UUID getSchoolId();
    
    TeacherSchool setSchoolId( final UUID schoolId );
}
