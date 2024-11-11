/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.platform.plugin.kettle;

import java.util.Arrays;
import java.util.List;

import org.pentaho.platform.api.engine.IUserRoleListService;
import org.pentaho.platform.api.mt.ITenant;

@SuppressWarnings( { "nls", "unchecked" } )
public class StubUserRoleListService implements IUserRoleListService {

  public List getAllRoles() {
    // TODO Auto-generated method stub
    return null;
  }

  public List getAllUsers() {
    // TODO Auto-generated method stub
    return null;
  }

  public List getUsersInRole( String role ) {
    // TODO Auto-generated method stub
    return null;
  }

  public List getRolesForUser( String userName ) {
    return Arrays.asList( "FL_GATOR", "FS_SEMINOLE" );
  }

  @Override
  public List<String> getAllRoles( ITenant arg0 ) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getAllUsers( ITenant arg0 ) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getRolesForUser( ITenant arg0, String arg1 ) {
    return Arrays.asList( "FL_GATOR", "FS_SEMINOLE" );
  }

  @Override
  public List<String> getUsersInRole( ITenant arg0, String arg1 ) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> getSystemRoles() {
    // TODO Auto-generated method stub
    return null;
  }

}
